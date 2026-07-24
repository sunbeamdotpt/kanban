// SPDX-License-Identifier: AGPL-3.0-or-later
//! Label catalog CRUD.
//!
//! Labels are project-owned resources within a tenant. Global labels have no
//! `project_id` (NULL in the database, empty string on the wire) and are
//! visible to every project in the same tenant via `ListLabels`.
//! Project-scoped labels reuse the parent project's permissions:
//!   * `view`   → list
//!   * `manage` → create, update, and delete
//!
//! Global (tenant-wide) writes require `manage` on at least one project of
//! the tenant, because there is no tenant-level OpenFGA object.
//!
//! The dynamic `sqlx` API is used throughout, so `cargo check` works without a
//! live database connection.

use std::sync::Arc;

use crate::id::Id;
use buffa_types::google::protobuf::FieldMask;
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use sqlx::{PgPool, Row};
use tracing::error;

use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_retry::PermissionRetryExt;
use crate::cpb::sunbeam::kanban::v1::{
    CreateLabelRequest, CreateLabelResponse, DeleteLabelRequest, DeleteLabelResponse, Label,
    LabelService, ListLabelsRequest, ListLabelsResponse, UpdateLabelRequest, UpdateLabelResponse,
};

// ── Constants ────────────────────────────────────────────────────────────────

const PERMISSION_TYPE_PROJECT: &str = "KanbanProject";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct LabelServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
}

// ── Error helpers ────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    error!(error = %err, "{msg}");
    ConnectError::internal(msg)
}

/// Map label write failures, translating the partial unique indexes
/// (`labels_project_name_unique` / `labels_global_name_unique`) into
/// `already_exists`.
fn map_write_error(context: &str, e: sqlx::Error) -> ConnectError {
    if let sqlx::Error::Database(ref db) = e
        && db.is_unique_violation()
    {
        return ConnectError::already_exists("label with this name already exists in this scope");
    }
    internal(context, e)
}

fn subject_from_request(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing auth context"))
}

fn tenant_id_from_request(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))
}

/// Extract the caller's tenant from the auth context and derive a permission
/// client scoped to that tenant's store (see `TENANT_HEADER`).
async fn tenant_client_for(
    permission: &PermissionClient,
    ctx: &RequestContext,
) -> Result<PermissionClient, ConnectError> {
    let tenant = ctx
        .extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))?;
    permission
        .tenant_client(&tenant)
        .await
        .map_err(|e| internal("failed to build tenant permission client", e))
}

fn parse_id(s: &str, field: &str) -> Result<Id, ConnectError> {
    match s.parse::<Id>() {
        Ok(id) => Ok(id),
        Err(_) => Err(ConnectError::invalid_argument(format!("invalid {field}"))),
    }
}

// ── Project-level permission checks ──────────────────────────────────────────

async fn check_project_permission(
    permission: &PermissionClient,
    project_id: Id,
    relation: &str,
    subject: &str,
) -> Result<(), ConnectError> {
    let allowed = permission
        .check_permission_with_retry(
            PERMISSION_TYPE_PROJECT,
            &project_id.to_string(),
            relation,
            subject,
        )
        .await
        .map_err(|e| internal("failed to check project permission", e))?;

    if allowed {
        Ok(())
    } else {
        Err(ConnectError::permission_denied("permission denied"))
    }
}

/// Global-label writes require `manage` on at least one project of the
/// tenant. There is no tenant-level OpenFGA object to check against, so we
/// iterate the tenant's projects and allow on the first grant.
async fn check_global_label_permission(
    pool: &PgPool,
    permission: &PermissionClient,
    tenant_id: &str,
    subject: &str,
) -> Result<(), ConnectError> {
    let rows = sqlx::query("SELECT id FROM projects WHERE tenant_id = $1")
        .bind(tenant_id)
        .fetch_all(pool)
        .await
        .map_err(|e| internal("failed to list tenant projects", e))?;

    for row in rows {
        let project_id: Id = row.get("id");
        let allowed = permission
            .check_permission_with_retry(
                PERMISSION_TYPE_PROJECT,
                &project_id.to_string(),
                "manage",
                subject,
            )
            .await
            .map_err(|e| internal("failed to check project permission", e))?;
        if allowed {
            return Ok(());
        }
    }

    Err(ConnectError::permission_denied("permission denied"))
}

/// Gate a write on the scope of an existing label row: `manage` on its
/// project, or the any-project manage check for global rows.
async fn check_label_scope_permission(
    pool: &PgPool,
    permission: &PermissionClient,
    project_id: Option<Id>,
    tenant_id: &str,
    subject: &str,
) -> Result<(), ConnectError> {
    match project_id {
        Some(pid) => check_project_permission(permission, pid, "manage", subject).await,
        None => check_global_label_permission(pool, permission, tenant_id, subject).await,
    }
}

// ── Row → proto helpers ───────────────────────────────────────────────────────

fn label_from_row(row: &sqlx::postgres::PgRow) -> Label {
    let id: Id = row.get("id");
    let project_id: Option<Id> = row.get("project_id");

    Label {
        id: id.to_string(),
        project_id: project_id.map(|p| p.to_string()).unwrap_or_default(),
        name: row.get("name"),
        style: row.get("style"),
        ..Default::default()
    }
}

// ── Field-mask helper ────────────────────────────────────────────────────────

fn mask_contains(mask: &buffa::MessageField<FieldMask>, path: &str) -> bool {
    mask.as_option()
        .map(|m| m.paths.iter().any(|p| p == path))
        .unwrap_or(false)
}

// ── impl LabelService ────────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl LabelService for LabelServiceImpl {
    async fn list_labels(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListLabelsRequest>,
    ) -> ServiceResult<ListLabelsResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let project_id = parse_id(&req.project_id, "project_id")?;

        check_project_permission(&permission, project_id, "view", &subject).await?;

        // Global labels (project_id IS NULL) are visible in every project of
        // the tenant.
        let rows = sqlx::query(
            "SELECT id, project_id, name, style FROM labels \
             WHERE tenant_id = $1 AND (project_id = $2 OR project_id IS NULL) \
             ORDER BY name",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list labels", e))?;

        Ok(Response::new(ListLabelsResponse {
            labels: rows.iter().map(label_from_row).collect(),
            ..Default::default()
        }))
    }

    async fn create_label(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateLabelRequest>,
    ) -> ServiceResult<CreateLabelResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        if req.name.is_empty() {
            return Err(ConnectError::invalid_argument("name is required"));
        }
        if req.style.is_empty() {
            return Err(ConnectError::invalid_argument("style is required"));
        }

        let project_id: Option<Id> = if req.project_id.is_empty() {
            check_global_label_permission(&self.pool, &permission, &tenant_id, &subject).await?;
            None
        } else {
            let pid = parse_id(&req.project_id, "project_id")?;
            check_project_permission(&permission, pid, "manage", &subject).await?;
            Some(pid)
        };

        let label_id = Id::new();

        let row = sqlx::query(
            "INSERT INTO labels (id, tenant_id, project_id, name, style) \
             VALUES ($1, $2, $3, $4, $5) \
             RETURNING id, project_id, name, style",
        )
        .bind(label_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&req.style)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| map_write_error("failed to create label", e))?;

        Ok(Response::new(CreateLabelResponse {
            label: Some(label_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn update_label(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateLabelRequest>,
    ) -> ServiceResult<UpdateLabelResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let label_id = parse_id(&req.label_id, "label_id")?;

        let existing =
            sqlx::query("SELECT project_id FROM labels WHERE id = $1 AND tenant_id = $2")
                .bind(label_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch label for update", e))?
                .ok_or_else(|| ConnectError::not_found("label not found"))?;

        let project_id: Option<Id> = existing.get("project_id");
        check_label_scope_permission(&self.pool, &permission, project_id, &tenant_id, &subject)
            .await?;

        let name = if mask_contains(&req.update_mask, "name") {
            Some(req.name)
        } else {
            None
        };
        let style = if mask_contains(&req.update_mask, "style") {
            Some(req.style)
        } else {
            None
        };

        let row = sqlx::query(
            r#"
            UPDATE labels SET
                name  = COALESCE($2, name),
                style = COALESCE($3, style)
            WHERE id = $1 AND tenant_id = $4
            RETURNING id, project_id, name, style
            "#,
        )
        .bind(label_id)
        .bind(name)
        .bind(style)
        .bind(&tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| map_write_error("failed to update label", e))?;

        Ok(Response::new(UpdateLabelResponse {
            label: Some(label_from_row(&row)).into(),
            ..Default::default()
        }))
    }

    async fn delete_label(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteLabelRequest>,
    ) -> ServiceResult<DeleteLabelResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let label_id = parse_id(&req.label_id, "label_id")?;

        let existing =
            sqlx::query("SELECT project_id FROM labels WHERE id = $1 AND tenant_id = $2")
                .bind(label_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch label for delete", e))?
                .ok_or_else(|| ConnectError::not_found("label not found"))?;

        let project_id: Option<Id> = existing.get("project_id");
        check_label_scope_permission(&self.pool, &permission, project_id, &tenant_id, &subject)
            .await?;

        // Card assignments are removed via the card_labels FK cascade.
        let result = sqlx::query("DELETE FROM labels WHERE id = $1 AND tenant_id = $2")
            .bind(label_id)
            .bind(&tenant_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete label", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("label not found"));
        }

        Ok(Response::new(DeleteLabelResponse::default()))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::{connect_ctx, connect_request, containers};

    async fn make_service() -> LabelServiceImpl {
        let infra = containers::setup().await;
        LabelServiceImpl {
            pool: infra.pool,
            permission: Arc::clone(&infra.permission),
        }
    }

    /// Build a request context with an authenticated subject in its extensions.
    fn authed_ctx(subject: &str) -> RequestContext {
        connect_ctx(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            subject,
        ))
    }

    /// Create a minimal project and grant the test subject manage and view on it.
    async fn create_test_project(
        pool: &PgPool,
        permission: &PermissionClient,
        subject: &str,
    ) -> Id {
        let project_id = Id::new();
        let slug = format!("tp-{}", &project_id.to_string()[18..26]);
        let tenant_id = crate::test_support::test_tenant_id();

        sqlx::query(
            "INSERT INTO projects (id, tenant_id, name, slug, description, owner_id) VALUES ($1, $2, $3, $4, '', $5)",
        )
        .bind(project_id)
        .bind(tenant_id)
        .bind(format!("Test Project {project_id}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");

        permission
            .grant_with_retry(
                PERMISSION_TYPE_PROJECT,
                &project_id.to_string(),
                "admin",
                subject,
            )
            .await
            .expect("grant admin failed");
        permission
            .grant_with_retry(
                PERMISSION_TYPE_PROJECT,
                &project_id.to_string(),
                "viewer",
                subject,
            )
            .await
            .expect("grant viewer failed");

        project_id
    }

    async fn seed_board(pool: &PgPool, project_id: Id, tenant_id: &str) -> Id {
        let board_id = Id::new();
        let slug = format!("b-{}", &board_id.to_string()[18..26]);
        sqlx::query(
            "INSERT INTO boards (id, tenant_id, project_id, name, slug) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(board_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(format!("Board {board_id}"))
        .bind(slug)
        .execute(pool)
        .await
        .expect("seed board failed");
        board_id
    }

    async fn seed_column(pool: &PgPool, board_id: Id, tenant_id: &str) -> Id {
        let column_id = Id::new();
        sqlx::query(
            "INSERT INTO columns (id, tenant_id, board_id, title, position) VALUES ($1, $2, $3, $4, 0)",
        )
        .bind(column_id)
        .bind(tenant_id)
        .bind(board_id)
        .bind("To Do")
        .execute(pool)
        .await
        .expect("seed column failed");
        column_id
    }

    async fn seed_card(
        pool: &PgPool,
        project_id: Id,
        board_id: Id,
        column_id: Id,
        tenant_id: &str,
    ) -> Id {
        let card_id = Id::new();
        let card_ref = format!("LB-{}", &card_id.to_string()[18..26]);
        sqlx::query(
            "INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, position, created_by, revision) \
             VALUES ($1, $2, $3, $4, $5, $6, 'Label Card', 0, 'user:test', 0)",
        )
        .bind(card_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(board_id)
        .bind(column_id)
        .bind(card_ref)
        .execute(pool)
        .await
        .expect("seed card failed");
        card_id
    }

    async fn cleanup_project(pool: &PgPool, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    async fn cleanup_label(pool: &PgPool, label_id: Id) {
        let tenant_id = crate::test_support::test_tenant_id();
        let _ = sqlx::query("DELETE FROM labels WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id)
            .bind(label_id)
            .execute(pool)
            .await;
    }

    async fn create_label(
        svc: &LabelServiceImpl,
        subject: &str,
        project_id: &str,
        name: &str,
        style: &str,
    ) -> Label {
        svc.create_label(
            authed_ctx(subject),
            connect_request(&CreateLabelRequest {
                project_id: project_id.to_string(),
                name: name.to_string(),
                style: style.to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("create_label failed")
        .body
        .label
        .into_option()
        .expect("label missing")
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_list_project_label() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        // Name and style are required.
        let err = svc
            .create_label(
                authed_ctx(&subject),
                connect_request(&CreateLabelRequest {
                    project_id: project_id.to_string(),
                    name: String::new(),
                    style: "red".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("empty name must be rejected");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);

        let created = create_label(&svc, &subject, &project_id.to_string(), "bug", "red").await;
        assert!(!created.id.is_empty());
        assert_eq!(created.project_id, project_id.to_string());
        assert_eq!(created.name, "bug");
        assert_eq!(created.style, "red");

        let list = svc
            .list_labels(
                authed_ctx(&subject),
                connect_request(&ListLabelsRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_labels failed")
            .body;

        assert!(
            list.labels.iter().any(|l| l.id == created.id),
            "project label should appear in ListLabels"
        );

        cleanup_label(&svc.pool, created.id.parse::<Id>().unwrap()).await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn global_label_visible_in_other_project() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_a = create_test_project(&svc.pool, &svc.permission, &subject).await;
        let project_b = create_test_project(&svc.pool, &svc.permission, &subject).await;

        // Empty project_id creates a global (tenant-wide) label.
        let created = create_label(&svc, &subject, "", "urgent", "amber").await;
        assert!(
            created.project_id.is_empty(),
            "global label must have empty project_id on the wire"
        );

        // The global label is visible from a DIFFERENT project of the tenant.
        let list = svc
            .list_labels(
                authed_ctx(&subject),
                connect_request(&ListLabelsRequest {
                    project_id: project_b.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_labels failed")
            .body;

        let found = list
            .labels
            .iter()
            .find(|l| l.id == created.id)
            .expect("global label should appear in another project's ListLabels");
        assert!(found.project_id.is_empty());
        assert_eq!(found.name, "urgent");

        // Project A sees it too.
        let list_a = svc
            .list_labels(
                authed_ctx(&subject),
                connect_request(&ListLabelsRequest {
                    project_id: project_a.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_labels failed")
            .body;
        assert!(list_a.labels.iter().any(|l| l.id == created.id));

        cleanup_label(&svc.pool, created.id.parse::<Id>().unwrap()).await;
        cleanup_project(&svc.pool, project_a).await;
        cleanup_project(&svc.pool, project_b).await;
    }

    #[tokio::test]
    async fn duplicate_name_rejected_per_scope() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let project_label =
            create_label(&svc, &subject, &project_id.to_string(), "backend", "blue").await;

        // Same name in the same project scope conflicts.
        let err = svc
            .create_label(
                authed_ctx(&subject),
                connect_request(&CreateLabelRequest {
                    project_id: project_id.to_string(),
                    name: "backend".to_string(),
                    style: "green".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("duplicate project label must be rejected");
        assert_eq!(err.code, connectrpc::ErrorCode::AlreadyExists);

        // The same name as a global label AND a project label is allowed.
        let global_label = create_label(&svc, &subject, "", "backend", "green").await;
        assert!(global_label.project_id.is_empty());

        // But a second global label with that name conflicts.
        let err = svc
            .create_label(
                authed_ctx(&subject),
                connect_request(&CreateLabelRequest {
                    project_id: String::new(),
                    name: "backend".to_string(),
                    style: "red".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("duplicate global label must be rejected");
        assert_eq!(err.code, connectrpc::ErrorCode::AlreadyExists);

        cleanup_label(&svc.pool, project_label.id.parse::<Id>().unwrap()).await;
        cleanup_label(&svc.pool, global_label.id.parse::<Id>().unwrap()).await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn update_label_applies_mask_and_detects_conflict() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let first = create_label(&svc, &subject, &project_id.to_string(), "one", "red").await;
        let second = create_label(&svc, &subject, &project_id.to_string(), "two", "blue").await;

        // Style-only mask: name must be left untouched.
        let updated = svc
            .update_label(
                authed_ctx(&subject),
                connect_request(&UpdateLabelRequest {
                    label_id: first.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["style".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    name: "SHOULD-NOT-APPLY".to_string(),
                    style: "green".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_label failed")
            .body
            .label
            .into_option()
            .expect("label missing");

        assert_eq!(updated.name, "one");
        assert_eq!(updated.style, "green");

        // Renaming to an existing name in the same scope conflicts.
        let err = svc
            .update_label(
                authed_ctx(&subject),
                connect_request(&UpdateLabelRequest {
                    label_id: first.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["name".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    name: "two".to_string(),
                    style: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("rename to existing name must be rejected");
        assert_eq!(err.code, connectrpc::ErrorCode::AlreadyExists);

        cleanup_label(&svc.pool, first.id.parse::<Id>().unwrap()).await;
        cleanup_label(&svc.pool, second.id.parse::<Id>().unwrap()).await;
        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn delete_label_cascades_card_labels() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let tenant_id = crate::test_support::test_tenant_id();
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;
        let board_id = seed_board(&svc.pool, project_id, &tenant_id).await;
        let column_id = seed_column(&svc.pool, board_id, &tenant_id).await;
        let card_id = seed_card(&svc.pool, project_id, board_id, column_id, &tenant_id).await;

        let created = create_label(&svc, &subject, &project_id.to_string(), "ui", "violet").await;
        let label_id: Id = created.id.parse().unwrap();

        sqlx::query("INSERT INTO card_labels (card_id, label_id) VALUES ($1, $2)")
            .bind(card_id)
            .bind(label_id)
            .execute(&svc.pool)
            .await
            .expect("seed card_labels failed");

        let count: i64 = sqlx::query("SELECT COUNT(*) FROM card_labels WHERE label_id = $1")
            .bind(label_id)
            .fetch_one(&svc.pool)
            .await
            .expect("count card_labels failed")
            .get(0);
        assert_eq!(count, 1);

        svc.delete_label(
            authed_ctx(&subject),
            connect_request(&DeleteLabelRequest {
                label_id: created.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete_label failed");

        // The join row is removed via the card_labels FK cascade.
        let count: i64 = sqlx::query("SELECT COUNT(*) FROM card_labels WHERE label_id = $1")
            .bind(label_id)
            .fetch_one(&svc.pool)
            .await
            .expect("count card_labels failed")
            .get(0);
        assert_eq!(count, 0, "card_labels row must be cascaded away");

        // The label itself is gone too.
        let err = svc
            .delete_label(
                authed_ctx(&subject),
                connect_request(&DeleteLabelRequest {
                    label_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("deleting a deleted label must be not_found");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);

        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn unauthorized_subject_denied_read_and_write() {
        let svc = make_service().await;
        let owner = format!("user:test-{}", Id::new());
        let stranger = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &owner).await;

        let err = svc
            .list_labels(
                authed_ctx(&stranger),
                connect_request(&ListLabelsRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("list without view must be denied");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);

        let err = svc
            .create_label(
                authed_ctx(&stranger),
                connect_request(&CreateLabelRequest {
                    project_id: project_id.to_string(),
                    name: "nope".to_string(),
                    style: "red".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("create without manage must be denied");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);

        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn global_write_requires_manage_on_some_project() {
        let svc = make_service().await;
        // This subject holds no grants on any project of the tenant.
        let outsider = format!("user:test-{}", Id::new());

        let err = svc
            .create_label(
                authed_ctx(&outsider),
                connect_request(&CreateLabelRequest {
                    project_id: String::new(),
                    name: "global-nope".to_string(),
                    style: "red".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("global create without any project manage must be denied");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);
    }
}
