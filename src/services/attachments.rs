// SPDX-License-Identifier: AGPL-3.0-or-later
//! Attachment uploads and downloads.
//!
//! Flow:
//!   1. `RequestPresignedUpload` creates a pending row and returns a SigV4 PUT URL.
//!   2. The client uploads the file directly to S3.
//!   3. `ConfirmUpload` checks the object exists and marks the row ready.
//!
//! Authorization uses the `x-sunbeam-object-id` header to carry the card id.
//! The permission dispatch matrix checks that card, and handlers receiving an
//! `attachment_id` in the body also verify the attachment belongs to that
//! card so the body cannot override the header.
//!
//! The `card_attachments` table stores `mimetype`, `size`, and `created_at`
//! (not `mime_type`, `size_bytes`, or `uploaded_at`), so all SQL uses the
//! real column names.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::id::Id;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use sqlx::PgPool;
use sqlx::Row;
use tonic::{Request, Response, Status};
use tracing::{error, warn};

use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_dispatch::CheckedObjectId;
use crate::integrations::s3::{S3Client, S3Error, sanitize_filename};
use crate::pb::attachment_service_server::AttachmentService;
use crate::pb::{
    Attachment, ConfirmUploadRequest, ConfirmUploadResponse, DeleteAttachmentRequest,
    DeleteAttachmentResponse, ListAttachmentsByCardRequest, ListAttachmentsByCardResponse,
    RequestPresignedDownloadRequest, RequestPresignedDownloadResponse,
    RequestPresignedUploadRequest, RequestPresignedUploadResponse,
};

// ── Service struct ───────────────────────────────────────────────────────────

pub struct AttachmentServiceImpl {
    pub pool: PgPool,
    pub s3: Arc<S3Client>,
    pub upload_expires_secs: u64,
    pub download_expires_secs: u64,
}

// ── Timestamp helpers ────────────────────────────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn now_plus_secs(secs: u64) -> Timestamp {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        .saturating_add(secs);
    Timestamp {
        seconds: t as i64,
        nanos: 0,
    }
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> Status {
    error!(error = %err, "{msg}");
    Status::internal(msg)
}

fn s3_err_to_status(err: S3Error, op: &str) -> Status {
    match err {
        S3Error::NotFound => Status::not_found("object not found in S3"),
        S3Error::Forbidden => {
            warn!(op, "S3 returned 403 — likely misconfiguration");
            Status::permission_denied("S3 access denied")
        }
        S3Error::Http(e) => {
            error!(op, error = %e, "S3 HTTP error");
            Status::internal("S3 request failed")
        }
        S3Error::Unexpected(status, body) => {
            error!(op, %status, body = %body, "S3 unexpected response");
            Status::internal("S3 unexpected response")
        }
    }
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

fn subject_from_request<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| Status::unauthenticated("missing auth context"))
}

fn checked_object_id<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<CheckedObjectId>()
        .map(|c| c.0.clone())
        .ok_or_else(|| Status::internal("missing CheckedObjectId extension"))
}

fn tenant_id_from_request<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| Status::unauthenticated("missing tenant context"))
}

// ── Row builder ───────────────────────────────────────────────────────────────

fn attachment_from_row(row: &sqlx::postgres::PgRow) -> Attachment {
    let id: Id = row.get("id");
    let card_id: Id = row.get("card_id");
    let s3_key: String = row.get("s3_key");
    let filename: String = row.get("filename");
    let mimetype: String = row.get("mimetype");
    let size: i64 = row.get("size");
    let uploaded_by: String = row.get("uploaded_by");
    let created_at: DateTime<Utc> = row.get("created_at");

    Attachment {
        id: id.to_string(),
        card_id: card_id.to_string(),
        s3_key,
        filename,
        mime_type: mimetype,
        size_bytes: size,
        uploaded_by,
        uploaded_at: Some(to_proto_ts(created_at)),
    }
}

// ── impl AttachmentService ────────────────────────────────────────────────────

#[tonic::async_trait]
impl AttachmentService for AttachmentServiceImpl {
    // ── RequestPresignedUpload ────────────────────────────────────────────────

    async fn request_presigned_upload(
        &self,
        request: Request<RequestPresignedUploadRequest>,
    ) -> Result<Response<RequestPresignedUploadResponse>, Status> {
        let tenant_id = tenant_id_from_request(&request)?;
        let subject = subject_from_request(&request)?;
        let card_id_str = checked_object_id(&request)?;
        let card_id = card_id_str
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid card_id in x-sunbeam-object-id"))?;

        let req = request.into_inner();

        if req.filename.is_empty() {
            return Err(Status::invalid_argument("filename is required"));
        }
        if req.mime_type.is_empty() {
            return Err(Status::invalid_argument("mime_type is required"));
        }

        let attachment_id = Id::new();
        let safe_filename = sanitize_filename(&req.filename);
        let s3_key = format!(
            "kanban/cards/{}/{}/{}",
            card_id, attachment_id, safe_filename
        );

        // INSERT pending row (size=0 initially; ConfirmUpload fills it from HEAD).
        sqlx::query(
            r#"
            INSERT INTO card_attachments (id, tenant_id, card_id, filename, mimetype, size, s3_key, uploaded_by)
            VALUES ($1, $2, $3, $4, $5, 0, $6, $7)
            "#,
        )
        .bind(attachment_id)
        .bind(&tenant_id)
        .bind(card_id)
        .bind(&safe_filename)
        .bind(&req.mime_type)
        .bind(&s3_key)
        .bind(&subject)
        .execute(&self.pool)
        .await
        .map_err(|e| internal("failed to insert attachment row", e))?;

        let presigned_url = self
            .s3
            .presign_put(&s3_key, &req.mime_type, self.upload_expires_secs);
        let expires_at = now_plus_secs(self.upload_expires_secs);

        Ok(Response::new(RequestPresignedUploadResponse {
            presigned_url,
            s3_key,
            attachment_id: attachment_id.to_string(),
            expires_at: Some(expires_at),
        }))
    }

    // ── ConfirmUpload ─────────────────────────────────────────────────────────

    async fn confirm_upload(
        &self,
        request: Request<ConfirmUploadRequest>,
    ) -> Result<Response<ConfirmUploadResponse>, Status> {
        let tenant_id = tenant_id_from_request(&request)?;
        let checked_card_id = checked_object_id(&request)?;
        let req = request.into_inner();

        let attachment_id = req
            .attachment_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid attachment_id"))?;

        // Fetch the attachment row; verify card_id matches the checked object.
        let row = sqlx::query(
            "SELECT id, card_id, filename, mimetype, size, s3_key, uploaded_by, created_at FROM card_attachments WHERE id = $1 AND tenant_id = $2",
        )
        .bind(attachment_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch attachment", e))?
        .ok_or_else(|| Status::not_found("attachment not found"))?;

        let db_card_id: Id = row.get("card_id");
        if db_card_id.to_string() != checked_card_id {
            warn!(
                attachment_id = %attachment_id,
                checked_card_id = %checked_card_id,
                db_card_id = %db_card_id,
                "confirm_upload: card_id mismatch — possible header-vs-body bypass attempt"
            );
            return Err(Status::permission_denied(
                "attachment does not belong to the authorized card",
            ));
        }

        let s3_key: String = row.get("s3_key");

        // HEAD the object to verify it exists and get actual size.
        let head = self
            .s3
            .head_object(&s3_key)
            .await
            .map_err(|e| s3_err_to_status(e, "ConfirmUpload/HEAD"))?;

        // UPDATE size from the actual HEAD response.
        let updated = sqlx::query(
            r#"
            UPDATE card_attachments
            SET size = $3
            WHERE id = $1 AND tenant_id = $2
            RETURNING id, card_id, filename, mimetype, size, s3_key, uploaded_by, created_at
            "#,
        )
        .bind(attachment_id)
        .bind(&tenant_id)
        .bind(head.size)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to confirm attachment", e))?;

        Ok(Response::new(ConfirmUploadResponse {
            attachment: Some(attachment_from_row(&updated)),
        }))
    }

    // ── RequestPresignedDownload ──────────────────────────────────────────────

    async fn request_presigned_download(
        &self,
        request: Request<RequestPresignedDownloadRequest>,
    ) -> Result<Response<RequestPresignedDownloadResponse>, Status> {
        let tenant_id = tenant_id_from_request(&request)?;
        let checked_card_id = checked_object_id(&request)?;
        let req = request.into_inner();

        let attachment_id = req
            .attachment_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid attachment_id"))?;

        let row = sqlx::query(
            "SELECT card_id, s3_key FROM card_attachments WHERE id = $1 AND tenant_id = $2",
        )
        .bind(attachment_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch attachment", e))?
        .ok_or_else(|| Status::not_found("attachment not found"))?;

        let db_card_id: Id = row.get("card_id");
        if db_card_id.to_string() != checked_card_id {
            warn!(
                attachment_id = %attachment_id,
                checked_card_id = %checked_card_id,
                db_card_id = %db_card_id,
                "request_presigned_download: card_id mismatch"
            );
            return Err(Status::permission_denied(
                "attachment does not belong to the authorized card",
            ));
        }

        let s3_key: String = row.get("s3_key");
        let presigned_url = self.s3.presign_get(&s3_key, self.download_expires_secs);
        let expires_at = now_plus_secs(self.download_expires_secs);

        Ok(Response::new(RequestPresignedDownloadResponse {
            presigned_url,
            expires_at: Some(expires_at),
        }))
    }

    // ── DeleteAttachment ──────────────────────────────────────────────────────

    async fn delete_attachment(
        &self,
        request: Request<DeleteAttachmentRequest>,
    ) -> Result<Response<DeleteAttachmentResponse>, Status> {
        let tenant_id = tenant_id_from_request(&request)?;
        let checked_card_id = checked_object_id(&request)?;
        let req = request.into_inner();

        let attachment_id = req
            .attachment_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid attachment_id"))?;

        let row = sqlx::query(
            "SELECT card_id, s3_key FROM card_attachments WHERE id = $1 AND tenant_id = $2",
        )
        .bind(attachment_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch attachment", e))?
        .ok_or_else(|| Status::not_found("attachment not found"))?;

        let db_card_id: Id = row.get("card_id");
        if db_card_id.to_string() != checked_card_id {
            warn!(
                attachment_id = %attachment_id,
                checked_card_id = %checked_card_id,
                db_card_id = %db_card_id,
                "delete_attachment: card_id mismatch"
            );
            return Err(Status::permission_denied(
                "attachment does not belong to the authorized card",
            ));
        }

        let s3_key: String = row.get("s3_key");

        // Delete from S3 first — if it fails, log a warning but still delete
        // the SQL row (orphan will be GC'd by a future sweeper — Stage 7e).
        if let Err(e) = self.s3.delete_object(&s3_key).await {
            warn!(
                attachment_id = %attachment_id,
                s3_key = %s3_key,
                error = %e,
                "delete_attachment: S3 delete failed; proceeding with SQL delete — orphan will be swept"
            );
        }

        sqlx::query("DELETE FROM card_attachments WHERE id = $1 AND tenant_id = $2")
            .bind(attachment_id)
            .bind(&tenant_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete attachment row", e))?;

        Ok(Response::new(DeleteAttachmentResponse {}))
    }

    // ── ListAttachmentsByCard ─────────────────────────────────────────────────

    async fn list_attachments_by_card(
        &self,
        request: Request<ListAttachmentsByCardRequest>,
    ) -> Result<Response<ListAttachmentsByCardResponse>, Status> {
        // The dispatch matrix authorized by card_id (from x-sunbeam-object-id).
        // CheckedObjectId is the card_id; we also accept it from the request body
        // for symmetry, but the authoritative value is the checked extension.
        let tenant_id = tenant_id_from_request(&request)?;
        let card_id_str = checked_object_id(&request)?;
        let card_id = card_id_str
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid card_id in x-sunbeam-object-id"))?;

        let rows = sqlx::query(
            r#"
            SELECT id, card_id, filename, mimetype, size, s3_key, uploaded_by, created_at
            FROM card_attachments
            WHERE card_id = $1 AND tenant_id = $2
            ORDER BY created_at DESC NULLS LAST
            "#,
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list attachments", e))?;

        let attachments = rows.iter().map(attachment_from_row).collect();
        Ok(Response::new(ListAttachmentsByCardResponse { attachments }))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test env config ──────────────────────────────────────────────────────

    fn s3_config() -> crate::integrations::s3::S3Config {
        // Read under ENV_LOCK: the `integrations::s3` unit tests clear S3_*
        // under the same lock.
        use crate::config::ENV_LOCK;
        let _guard = ENV_LOCK.lock().unwrap();
        crate::integrations::s3::S3Config::from_env()
    }

    async fn setup() -> (
        crate::test_support::containers::TestInfra,
        AttachmentServiceImpl,
    ) {
        let infra = crate::test_support::containers::setup().await;
        let cfg = s3_config();
        let svc = AttachmentServiceImpl {
            pool: infra.pool.clone(),
            s3: Arc::new(S3Client::new(cfg)),
            upload_expires_secs: 900,
            download_expires_secs: 300,
        };
        (infra, svc)
    }

    fn authed_request_with_object<T>(body: T, subject: &str, object_id: &str) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut().insert(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            subject,
        ));
        req.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        req
    }

    // Clean up a card_attachments row directly.
    async fn cleanup_attachment(pool: &PgPool, id: Id) {
        let tenant_id = crate::test_support::test_tenant_id();
        let _ = sqlx::query("DELETE FROM card_attachments WHERE id = $1 AND tenant_id = $2")
            .bind(id)
            .bind(&tenant_id)
            .execute(pool)
            .await;
    }

    /// Create a project, board, column, and card for tests and return the card id.
    /// Wraps `test_support::seed_card_chain` so existing tests can call it directly.
    async fn seed_card_chain(pool: &PgPool) -> Id {
        let (_project_id, _board_id, _column_id, card_id) =
            crate::test_support::seed_card_chain(pool).await;
        card_id
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn request_presigned_upload_returns_signed_url() {
        let (infra, svc) = setup().await;
        let pool = infra.pool.clone();

        let card_id = seed_card_chain(&pool).await;
        let subject = format!("user:test-{}", Id::new());

        let resp = svc
            .request_presigned_upload(authed_request_with_object(
                RequestPresignedUploadRequest {
                    card_id: card_id.to_string(),
                    filename: "test.pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    size_bytes: 1024,
                },
                &subject,
                &card_id.to_string(),
            ))
            .await
            .expect("request_presigned_upload failed")
            .into_inner();

        assert!(
            resp.presigned_url.contains("X-Amz-Signature="),
            "URL must contain X-Amz-Signature, got: {}",
            resp.presigned_url
        );
        assert!(
            resp.presigned_url
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"),
            "URL must contain algorithm"
        );
        assert!(
            !resp.attachment_id.is_empty(),
            "attachment_id must be non-empty"
        );
        assert!(!resp.s3_key.is_empty(), "s3_key must be non-empty");
        assert!(resp.expires_at.is_some(), "expires_at must be present");

        // Cleanup
        let att_id = resp.attachment_id.parse::<Id>().unwrap();
        cleanup_attachment(&pool, att_id).await;
        let _ = sqlx::query("DELETE FROM cards WHERE id = $1")
            .bind(card_id)
            .execute(&pool)
            .await;
    }

    #[tokio::test]
    async fn presigned_upload_then_confirm_then_download_round_trip() {
        let (infra, svc) = setup().await;
        let pool = infra.pool.clone();
        let s3_cfg = s3_config();
        let http = reqwest::Client::new();

        let card_id = seed_card_chain(&pool).await;
        let subject = format!("user:test-{}", Id::new());

        // Step 1: RequestPresignedUpload
        let upload_resp = svc
            .request_presigned_upload(authed_request_with_object(
                RequestPresignedUploadRequest {
                    card_id: card_id.to_string(),
                    filename: "hello.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    size_bytes: 5,
                },
                &subject,
                &card_id.to_string(),
            ))
            .await
            .expect("request_presigned_upload failed")
            .into_inner();

        let attachment_id = upload_resp.attachment_id.clone();
        let s3_key = upload_resp.s3_key.clone();
        let file_content = b"hello";

        // Step 2: PUT directly to S3 via presigned URL
        let put_resp = http
            .put(&upload_resp.presigned_url)
            .header("content-type", "text/plain")
            .body(file_content.to_vec())
            .send()
            .await
            .expect("PUT to presigned URL failed");
        assert!(
            put_resp.status().is_success(),
            "S3 PUT failed with status: {}",
            put_resp.status()
        );

        // Step 3: ConfirmUpload
        let confirmed = svc
            .confirm_upload(authed_request_with_object(
                ConfirmUploadRequest {
                    attachment_id: attachment_id.clone(),
                },
                &subject,
                &card_id.to_string(),
            ))
            .await
            .expect("confirm_upload failed")
            .into_inner()
            .attachment
            .expect("attachment missing");

        assert_eq!(confirmed.id, attachment_id);
        assert_eq!(
            confirmed.size_bytes, 5,
            "size should match uploaded content"
        );

        // Step 4: RequestPresignedDownload
        let download_resp = svc
            .request_presigned_download(authed_request_with_object(
                RequestPresignedDownloadRequest {
                    attachment_id: attachment_id.clone(),
                },
                &subject,
                &card_id.to_string(),
            ))
            .await
            .expect("request_presigned_download failed")
            .into_inner();

        // Step 5: GET via presigned URL and verify byte-identical content
        let get_resp = http
            .get(&download_resp.presigned_url)
            .send()
            .await
            .expect("GET from presigned URL failed");
        assert!(get_resp.status().is_success(), "S3 GET failed");
        let body = get_resp.bytes().await.expect("failed to read body");
        assert_eq!(
            body.as_ref(),
            file_content,
            "downloaded content must be byte-identical"
        );

        // Teardown: delete from S3 + SQL
        let s3_client = S3Client::new(s3_cfg);
        let _ = s3_client.delete_object(&s3_key).await;
        let att_id = attachment_id.parse::<Id>().unwrap();
        cleanup_attachment(&pool, att_id).await;
        let _ = sqlx::query("DELETE FROM cards WHERE id = $1")
            .bind(card_id)
            .execute(&pool)
            .await;
    }

    #[tokio::test]
    async fn confirm_upload_rejects_when_file_not_in_s3() {
        let (infra, svc) = setup().await;
        let pool = infra.pool.clone();

        let card_id = seed_card_chain(&pool).await;
        let subject = format!("user:test-{}", Id::new());

        // Create a pending attachment but never PUT to S3.
        let upload_resp = svc
            .request_presigned_upload(authed_request_with_object(
                RequestPresignedUploadRequest {
                    card_id: card_id.to_string(),
                    filename: "missing.bin".to_string(),
                    mime_type: "application/octet-stream".to_string(),
                    size_bytes: 100,
                },
                &subject,
                &card_id.to_string(),
            ))
            .await
            .expect("request_presigned_upload failed")
            .into_inner();

        let attachment_id = upload_resp.attachment_id.clone();

        // ConfirmUpload before the PUT — should fail with not_found.
        let result = svc
            .confirm_upload(authed_request_with_object(
                ConfirmUploadRequest {
                    attachment_id: attachment_id.clone(),
                },
                &subject,
                &card_id.to_string(),
            ))
            .await;

        assert!(
            result.is_err(),
            "confirm_upload should fail when file is not in S3"
        );
        let status = result.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::NotFound,
            "expected NotFound, got: {:?}",
            status.code()
        );

        // Cleanup
        let att_id = attachment_id.parse::<Id>().unwrap();
        cleanup_attachment(&pool, att_id).await;
        let _ = sqlx::query("DELETE FROM cards WHERE id = $1")
            .bind(card_id)
            .execute(&pool)
            .await;
    }

    #[tokio::test]
    async fn delete_attachment_removes_from_s3_and_db() {
        let (infra, svc) = setup().await;
        let pool = infra.pool.clone();
        let s3_cfg = s3_config();
        let http = reqwest::Client::new();

        let card_id = seed_card_chain(&pool).await;
        let subject = format!("user:test-{}", Id::new());

        // Upload a real file.
        let upload_resp = svc
            .request_presigned_upload(authed_request_with_object(
                RequestPresignedUploadRequest {
                    card_id: card_id.to_string(),
                    filename: "to-delete.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    size_bytes: 7,
                },
                &subject,
                &card_id.to_string(),
            ))
            .await
            .expect("upload request failed")
            .into_inner();

        let attachment_id = upload_resp.attachment_id.clone();
        let s3_key = upload_resp.s3_key.clone();

        http.put(&upload_resp.presigned_url)
            .header("content-type", "text/plain")
            .body(b"deleted".to_vec())
            .send()
            .await
            .expect("PUT failed");

        svc.confirm_upload(authed_request_with_object(
            ConfirmUploadRequest {
                attachment_id: attachment_id.clone(),
            },
            &subject,
            &card_id.to_string(),
        ))
        .await
        .expect("confirm failed");

        // DeleteAttachment
        svc.delete_attachment(authed_request_with_object(
            DeleteAttachmentRequest {
                attachment_id: attachment_id.clone(),
            },
            &subject,
            &card_id.to_string(),
        ))
        .await
        .expect("delete_attachment failed");

        // Verify SQL row is gone.
        let tenant_id = crate::test_support::test_tenant_id();
        let att_id = attachment_id.parse::<Id>().unwrap();
        let sql_row: Option<Id> =
            sqlx::query("SELECT id FROM card_attachments WHERE id = $1 AND tenant_id = $2")
                .bind(att_id)
                .bind(&tenant_id)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .map(|r| r.get("id"));
        assert!(sql_row.is_none(), "SQL row should be gone after delete");

        // Verify S3 object is gone (HEAD should 404).
        let s3_client = S3Client::new(s3_cfg);
        let head_result = s3_client.head_object(&s3_key).await;
        assert!(
            matches!(head_result, Err(S3Error::NotFound)),
            "S3 object should be gone after delete, got: {:?}",
            head_result
        );

        let _ = sqlx::query("DELETE FROM cards WHERE id = $1")
            .bind(card_id)
            .execute(&pool)
            .await;
    }

    #[tokio::test]
    async fn list_attachments_by_card_returns_only_that_cards_rows() {
        let (infra, svc) = setup().await;
        let pool = infra.pool.clone();
        let s3_cfg = s3_config();

        let card_a = seed_card_chain(&pool).await;
        let card_b = seed_card_chain(&pool).await;
        let subject = format!("user:test-{}", Id::new());

        // Insert attachment for card_a directly (skip S3 — we only need SQL rows for list test).
        let att_a1 = Id::new();
        let att_a2 = Id::new();
        let att_b1 = Id::new();

        let tenant_id = crate::test_support::test_tenant_id();
        for (att_id, cid) in &[(att_a1, card_a), (att_a2, card_a), (att_b1, card_b)] {
            sqlx::query(
                "INSERT INTO card_attachments (id, tenant_id, card_id, filename, mimetype, size, s3_key, uploaded_by) VALUES ($1, $2, $3, 'f.txt', 'text/plain', 0, $4, $5)"
            )
            .bind(att_id)
            .bind(&tenant_id)
            .bind(cid)
            .bind(format!("kanban/cards/{}/{}/f.txt", cid, att_id))
            .bind(&subject)
            .execute(&pool)
            .await
            .expect("insert failed");
        }

        let list = svc
            .list_attachments_by_card(authed_request_with_object(
                ListAttachmentsByCardRequest {
                    card_id: card_a.to_string(),
                },
                &subject,
                &card_a.to_string(),
            ))
            .await
            .expect("list_attachments_by_card failed")
            .into_inner();

        assert_eq!(
            list.attachments.len(),
            2,
            "should only see card_a's 2 attachments"
        );
        let ids: Vec<&str> = list.attachments.iter().map(|a| a.id.as_str()).collect();
        assert!(
            ids.contains(&att_a1.to_string().as_str())
                || ids.iter().any(|&id| id == att_a1.to_string())
        );
        assert!(
            !ids.iter().any(|&id| id == att_b1.to_string()),
            "card_b's attachment must not appear"
        );

        // Cleanup
        for att_id in &[att_a1, att_a2, att_b1] {
            cleanup_attachment(&pool, *att_id).await;
        }
        let s3_client = S3Client::new(s3_cfg);
        for (att_id, cid) in &[(att_a1, card_a), (att_a2, card_a), (att_b1, card_b)] {
            let _ = s3_client
                .delete_object(&format!("kanban/cards/{}/{}/f.txt", cid, att_id))
                .await;
        }
        for cid in &[card_a, card_b] {
            let _ = sqlx::query("DELETE FROM cards WHERE id = $1")
                .bind(cid)
                .execute(&pool)
                .await;
        }
    }

    #[tokio::test]
    async fn confirm_upload_rejects_when_card_id_mismatches_checked_object_id() {
        // Security regression test: attacker presents attachment_id for card X
        // but puts card Y in the x-sunbeam-object-id header (authorized for Y).
        let (infra, svc) = setup().await;
        let pool = infra.pool.clone();

        let card_legit = seed_card_chain(&pool).await;
        let card_attacker = seed_card_chain(&pool).await;
        let subject = format!("user:test-{}", Id::new());

        // Create an attachment on card_legit.
        let upload_resp = svc
            .request_presigned_upload(authed_request_with_object(
                RequestPresignedUploadRequest {
                    card_id: card_legit.to_string(),
                    filename: "secret.pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    size_bytes: 42,
                },
                &subject,
                &card_legit.to_string(),
            ))
            .await
            .expect("upload request failed")
            .into_inner();

        let attachment_id = upload_resp.attachment_id.clone();

        // Attacker calls ConfirmUpload with card_attacker as the checked object
        // but the attachment_id belongs to card_legit.
        let result = svc
            .confirm_upload(authed_request_with_object(
                ConfirmUploadRequest {
                    attachment_id: attachment_id.clone(),
                },
                &subject,
                &card_attacker.to_string(), // WRONG card_id in header
            ))
            .await;

        assert!(
            result.is_err(),
            "confirm_upload must reject card_id mismatch"
        );
        let status = result.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::PermissionDenied,
            "expected PermissionDenied for card_id mismatch, got: {:?}",
            status.code()
        );

        // Cleanup
        let att_id = attachment_id.parse::<Id>().unwrap();
        cleanup_attachment(&pool, att_id).await;
        for cid in &[card_legit, card_attacker] {
            let _ = sqlx::query("DELETE FROM cards WHERE id = $1")
                .bind(cid)
                .execute(&pool)
                .await;
        }
    }
}
