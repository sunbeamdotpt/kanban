// SPDX-License-Identifier: AGPL-3.0-or-later
//! ProjectService implementation.
//!
//! Handles project lifecycle and membership. All permission changes are
//! written to Keto first, then mirrored to the `project_members` table.
//! If the SQL write fails after Keto succeeds, we log a `mirror_drift`
//! warning so the background reconciler can catch up.
//!
// (original doc below)
//!
//! `SubscribeProject` is intentionally left unimplemented for now; it will
//! eventually merge live streams from every board visible to the caller.
//!
//! Uses the dynamic sqlx API (no macros) so the crate builds without a
//! live DATABASE_URL.

use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use sqlx::PgPool;
use sqlx::Row;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{error, warn};
use uuid::Uuid;

use sunbeam_g2v::middleware::auth::AuthContext;
use sunbeam_g2v::middleware::auth::keto::KetoClient;

use crate::auth::keto_dispatch::CheckedObjectId;
use crate::auth::keto_expand::{ExpandQuery, expand_objects};
use crate::auth::keto_retry::KetoRetryExt;
use crate::auth::logout_watermark::LogoutWatermark;
use crate::pb::project_service_server::ProjectService;
use crate::pb::{
    AddMemberRequest, BoardEventEnvelope, CreateProjectRequest, DeleteProjectRequest,
    GetProjectRequest, ListMembersRequest, ListMembersResponse, ListProjectsRequest,
    ListProjectsResponse, Project, ProjectMember, RemoveMemberRequest, SubscribeProjectRequest,
    UpdateProjectRequest,
};
use crate::realtime::registry::BoardSubscriberRegistry;

// ── Constants ────────────────────────────────────────────────────────────────

const KETO_NS: &str = "KanbanProject";
const MAX_PROJECTS: usize = 10_000;

// ── Service struct ───────────────────────────────────────────────────────────

pub struct ProjectServiceImpl {
    pub pool: PgPool,
    pub keto: Arc<KetoClient>,
    pub registry: Arc<BoardSubscriberRegistry>,
    pub watermark: Arc<LogoutWatermark>,
}

// ── Timestamp helpers (chrono ↔ prost_types) ─────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> Status {
    error!(error = %err, "{msg}");
    Status::internal(msg)
}

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

/// Convert a Postgres row into a `Project` proto.
fn project_from_row(row: &sqlx::postgres::PgRow, member_count: i32) -> Project {
    let id: Uuid = row.get("id");
    let name: String = row.get("name");
    let slug: String = row.get("slug");
    let description: Option<String> = row.get("description");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Project {
        id: id.to_string(),
        name,
        prefix: slug.to_uppercase(),
        icon: String::new(),
        color: String::new(),
        description: description.unwrap_or_default(),
        created_at: Some(to_proto_ts(created_at)),
        updated_at: Some(to_proto_ts(updated_at)),
        member_count,
    }
}

/// Convert a Postgres row into a `ProjectMember` proto.
fn member_from_row(row: &sqlx::postgres::PgRow) -> ProjectMember {
    let project_id: Uuid = row.get("project_id");
    let user_id: String = row.get("user_id");
    let role: String = row.get("role");
    let created_at: DateTime<Utc> = row.get("created_at");

    ProjectMember {
        project_id: project_id.to_string(),
        subject: user_id,
        relation: role,
        display_name: String::new(),
        email: String::new(),
        added_at: Some(to_proto_ts(created_at)),
    }
}

async fn fetch_member_count(pool: &PgPool, project_id: Uuid) -> i32 {
    sqlx::query("SELECT COUNT(*) AS cnt FROM project_members WHERE project_id = $1")
        .bind(project_id)
        .fetch_one(pool)
        .await
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt as i32
        })
        .unwrap_or(0)
}

// ── Type alias ───────────────────────────────────────────────────────────────

type SubscribeProjectStream =
    Pin<Box<dyn Stream<Item = Result<BoardEventEnvelope, Status>> + Send + 'static>>;

// ── impl ProjectService ──────────────────────────────────────────────────────

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    // ── CreateProject ────────────────────────────────────────────────────────

    async fn create_project(
        &self,
        request: Request<CreateProjectRequest>,
    ) -> Result<Response<Project>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        // Idempotency check — look up by key; if found, re-fetch the project from DB.
        if !req.idempotency_key.is_empty() {
            let cached_id: Option<Option<Uuid>> =
                sqlx::query("SELECT response_card_id FROM idempotency_keys WHERE key = $1")
                    .bind(&req.idempotency_key)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| internal("idempotency key lookup failed", e))?
                    .map(|r| r.get("response_card_id"));

            if let Some(Some(project_id)) = cached_id {
                let row = sqlx::query(
                    "SELECT id, name, slug, description, owner_id, created_at, updated_at FROM projects WHERE id = $1",
                )
                .bind(project_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached project", e))?;

                if let Some(row) = row {
                    let member_count = fetch_member_count(&self.pool, project_id).await;
                    return Ok(Response::new(project_from_row(&row, member_count)));
                }
                // Project was deleted after idempotency key was set — fall through.
            }
        }

        // Validate required fields.
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        // Derive slug from prefix or name.
        let slug = if req.prefix.is_empty() {
            req.name
                .chars()
                .filter(|c| c.is_alphanumeric())
                .take(8)
                .collect::<String>()
                .to_uppercase()
        } else {
            req.prefix.to_uppercase()
        };

        let project_id = Uuid::new_v4();

        // INSERT project.
        let row = sqlx::query(
            r#"
            INSERT INTO projects (id, name, slug, description, owner_id)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, name, slug, description, owner_id, created_at, updated_at
            "#,
        )
        .bind(project_id)
        .bind(&req.name)
        .bind(&slug)
        .bind(&req.description)
        .bind(&subject)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db) = e
                && db.constraint() == Some("projects_slug_key")
            {
                return Status::already_exists("project with that prefix already exists");
            }
            internal("failed to insert project", e)
        })?;

        // Write Keto owner tuple.
        self.keto
            .grant_with_retry(KETO_NS, &project_id.to_string(), "owner", &subject)
            .await
            .map_err(|e| internal("failed to write Keto owner tuple", e))?;

        // Also write a "view" tuple so ListProjects sees it.
        self.keto
            .grant_with_retry(KETO_NS, &project_id.to_string(), "view", &subject)
            .await
            .map_err(|e| internal("failed to write Keto view tuple", e))?;

        // Insert owner into project_members mirror (Keto is already written — best-effort SQL).
        if let Err(e) = sqlx::query(
            "INSERT INTO project_members (project_id, user_id, role) VALUES ($1, $2, 'owner') ON CONFLICT DO NOTHING",
        )
        .bind(project_id)
        .bind(&subject)
        .execute(&self.pool)
        .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                "mirror_drift: Keto owner tuple written but project_members insert failed"
            );
        }

        let member_count = fetch_member_count(&self.pool, project_id).await;
        let project = project_from_row(&row, member_count);

        // Store idempotency response — record the created project UUID.
        if !req.idempotency_key.is_empty()
            && let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (key, response_card_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(&req.idempotency_key)
            .bind(project_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }

        Ok(Response::new(project))
    }

    // ── GetProject ───────────────────────────────────────────────────────────

    async fn get_project(
        &self,
        request: Request<GetProjectRequest>,
    ) -> Result<Response<Project>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let row = sqlx::query(
            "SELECT id, name, slug, description, owner_id, created_at, updated_at FROM projects WHERE id = $1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch project", e))?
        .ok_or_else(|| Status::not_found("project not found"))?;

        let member_count = fetch_member_count(&self.pool, project_id).await;
        Ok(Response::new(project_from_row(&row, member_count)))
    }

    // ── ListProjects ─────────────────────────────────────────────────────────

    async fn list_projects(
        &self,
        request: Request<ListProjectsRequest>,
    ) -> Result<Response<ListProjectsResponse>, Status> {
        let subject = subject_from_request(&request)?;

        let visible_ids = expand_objects(
            &self.keto,
            ExpandQuery {
                namespace: KETO_NS,
                relation: "view",
                subject: &subject,
                max_page_size: 256,
            },
            MAX_PROJECTS,
        )
        .await
        .map_err(|e| internal("keto expand failed", e))?;

        if visible_ids.is_empty() {
            return Ok(Response::new(ListProjectsResponse { projects: vec![] }));
        }

        let ids: Vec<Uuid> = visible_ids
            .iter()
            .filter_map(|s| Uuid::parse_str(s).ok())
            .collect();

        let rows = sqlx::query(
            "SELECT id, name, slug, description, owner_id, created_at, updated_at FROM projects WHERE id = ANY($1)",
        )
        .bind(&ids as &[Uuid])
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list projects", e))?;

        let mut projects = Vec::with_capacity(rows.len());
        for row in &rows {
            let pid: Uuid = row.get("id");
            let member_count = fetch_member_count(&self.pool, pid).await;
            projects.push(project_from_row(row, member_count));
        }

        Ok(Response::new(ListProjectsResponse { projects }))
    }

    // ── UpdateProject ────────────────────────────────────────────────────────

    async fn update_project(
        &self,
        request: Request<UpdateProjectRequest>,
    ) -> Result<Response<Project>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let req = request.into_inner();
        let patch = req.project.unwrap_or_default();

        // Apply sparse patch — only non-empty fields are applied.
        let row = sqlx::query(
            r#"
            UPDATE projects SET
                name        = CASE WHEN $2 != '' THEN $2 ELSE name END,
                slug        = CASE WHEN $3 != '' THEN $3 ELSE slug END,
                description = CASE WHEN $4 != '' THEN $4 ELSE description END,
                updated_at  = now()
            WHERE id = $1
            RETURNING id, name, slug, description, owner_id, created_at, updated_at
            "#,
        )
        .bind(project_id)
        .bind(&patch.name)
        .bind(patch.prefix.to_uppercase())
        .bind(&patch.description)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to update project", e))?
        .ok_or_else(|| Status::not_found("project not found"))?;

        let member_count = fetch_member_count(&self.pool, project_id).await;
        Ok(Response::new(project_from_row(&row, member_count)))
    }

    // ── DeleteProject ────────────────────────────────────────────────────────

    async fn delete_project(
        &self,
        request: Request<DeleteProjectRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let result = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete project", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("project not found"));
        }

        // Best-effort: Keto tuple cleanup for this project is logged as drift.
        // delete_relation_tuples filters by subject (not object); object-scoped
        // deletion requires the Stage 7a reconciler. We log the drift so it is
        // visible in metrics and the reconciler can mop it up.
        warn!(
            project_id = %project_id,
            "delete_project: Keto tuple cleanup is best-effort; reconciler (Stage 7a) will catch any drift"
        );

        Ok(Response::new(()))
    }

    // ── AddMember ────────────────────────────────────────────────────────────

    async fn add_member(&self, request: Request<AddMemberRequest>) -> Result<Response<()>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let req = request.into_inner();

        if req.subject.is_empty() {
            return Err(Status::invalid_argument("subject is required"));
        }
        if req.relation.is_empty() {
            return Err(Status::invalid_argument("relation is required"));
        }
        // owner is set at CreateProject and is immutable.
        if req.relation == "owner" {
            return Err(Status::permission_denied(
                "owner relation cannot be granted via AddMember",
            ));
        }

        // Keto FIRST (mirror-table write order per plan / Pre-mortem 5).
        self.keto
            .grant_with_retry(
                KETO_NS,
                &project_id.to_string(),
                &req.relation,
                &req.subject,
            )
            .await
            .map_err(|e| internal("failed to write Keto member tuple", e))?;

        // SQL mirror second — best-effort; Keto is the source of truth.
        if let Err(e) = sqlx::query(
            r#"
            INSERT INTO project_members (project_id, user_id, role)
            VALUES ($1, $2, $3)
            ON CONFLICT (project_id, user_id) DO UPDATE SET role = $3
            "#,
        )
        .bind(project_id)
        .bind(&req.subject)
        .bind(&req.relation)
        .execute(&self.pool)
        .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                subject = %req.subject,
                "mirror_drift: Keto tuple written but project_members insert failed"
            );
            // Do not return error — reconciler will fix SQL drift.
        }

        Ok(Response::new(()))
    }

    // ── RemoveMember ─────────────────────────────────────────────────────────

    async fn remove_member(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let req = request.into_inner();

        if req.subject.is_empty() {
            return Err(Status::invalid_argument("subject is required"));
        }

        // Read existing relation from mirror to know which Keto tuple to delete.
        let existing_role: Option<String> =
            sqlx::query("SELECT role FROM project_members WHERE project_id = $1 AND user_id = $2")
                .bind(project_id)
                .bind(&req.subject)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to look up member", e))?
                .map(|r| r.get("role"));

        // Delete Keto tuple for the known relation.
        if let Some(ref relation) = existing_role
            && let Err(e) = crate::auth::keto_compat::delete_relation_tuples(
                &self.keto,
                KETO_NS,
                Some(relation.as_str()),
                Some(&req.subject),
            )
            .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                subject = %req.subject,
                "mirror_drift: failed to delete Keto tuple for removed member"
            );
        }

        // Delete SQL row.
        let result =
            sqlx::query("DELETE FROM project_members WHERE project_id = $1 AND user_id = $2")
                .bind(project_id)
                .bind(&req.subject)
                .execute(&self.pool)
                .await
                .map_err(|e| internal("failed to delete member row", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("member not found"));
        }

        Ok(Response::new(()))
    }

    // ── ListMembers ───────────────────────────────────────────────────────────

    async fn list_members(
        &self,
        request: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let rows = sqlx::query(
            "SELECT project_id, user_id, role, created_at FROM project_members WHERE project_id = $1 ORDER BY created_at",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list members", e))?;

        let members = rows.iter().map(member_from_row).collect();
        Ok(Response::new(ListMembersResponse { members }))
    }

    // ── SubscribeProject (Stage 4c — deferred to 4c.5) ───────────────────────
    //
    // TODO(4c.5): multi-board merge — enumerate all KanbanBoard objects visible
    // to the subject via `keto_expand_objects(KanbanBoard, view, subject)`, then
    // open one `build_subscribe_board_stream` per board and merge them via
    // `tokio_stream::StreamExt::merge` / `select_all`.

    type SubscribeProjectStream = SubscribeProjectStream;

    async fn subscribe_project(
        &self,
        _request: Request<SubscribeProjectRequest>,
    ) -> Result<Response<Self::SubscribeProjectStream>, Status> {
        Err(Status::unimplemented(
            "Stage 4c.5 — per-project multi-board merge not yet implemented",
        ))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── Setup helpers ────────────────────────────────────────────────────────

    async fn setup_pool() -> PgPool {
        crate::test_support::setup_pool().await
    }

    async fn setup_keto() -> Arc<KetoClient> {
        crate::test_support::setup_keto().await
    }

    async fn make_service(pool: PgPool, keto: Arc<KetoClient>) -> ProjectServiceImpl {
        let nats_url =
            std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
        let valkey_url =
            std::env::var("VALKEY_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
        let nats = Arc::new(
            sunbeam_g2v::mq::NatsClient::connect(&sunbeam_g2v::config::NatsConfig {
                url: nats_url,
                jetstream: true,
                lease_duration: 30,
            })
            .await
            .expect("NATS connect failed"),
        );
        let registry = Arc::new(BoardSubscriberRegistry::new(nats, "pod-test-projects"));
        let watermark = Arc::new(LogoutWatermark::new(&valkey_url).expect("LogoutWatermark::new"));
        ProjectServiceImpl {
            pool,
            keto,
            registry,
            watermark,
        }
    }

    /// Create an authenticated request that only carries the caller subject.
    fn authed_request<T>(body: T, subject: &str) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut()
            .insert(AuthContext::authenticated(subject, None));
        req
    }

    /// Create an authenticated request that also carries a `CheckedObjectId`.
    fn authed_request_with_object<T>(body: T, subject: &str, object_id: &str) -> Request<T> {
        let mut req = authed_request(body, subject);
        req.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        req
    }

    /// Remove every Keto relation tuple a test may have created for a subject.
    async fn cleanup_keto_for_subject(keto: &KetoClient, subject: &str) {
        for relation in &["owner", "view", "edit", "manage", "administer"] {
            let _ = crate::auth::keto_compat::delete_relation_tuples(
                keto,
                KETO_NS,
                Some(relation),
                Some(subject),
            )
            .await;
        }
    }

    /// Delete a project row directly during test cleanup.
    async fn cleanup_project(pool: &PgPool, project_id: Uuid) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_returns_same_project() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());

        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "Test Project Alpha".to_string(),
                    prefix: "TPA".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: "Integration test project".to_string(),
                    idempotency_key: String::new(),
                },
                &subject,
            ))
            .await
            .expect("create_project failed")
            .into_inner();

        assert!(!created.id.is_empty(), "created project must have an id");
        assert_eq!(created.name, "Test Project Alpha");
        assert_eq!(created.prefix, "TPA");
        assert_eq!(created.description, "Integration test project");
        assert!(created.created_at.is_some());
        assert!(created.updated_at.is_some());

        let project_id = Uuid::parse_str(&created.id).expect("invalid uuid from create");

        let fetched = svc
            .get_project(authed_request_with_object(
                GetProjectRequest {
                    project_id: created.id.clone(),
                },
                &subject,
                &created.id,
            ))
            .await
            .expect("get_project failed")
            .into_inner();

        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.name, created.name);
        assert_eq!(fetched.prefix, created.prefix);
        assert_eq!(fetched.description, created.description);

        // Cleanup
        cleanup_project(&pool, project_id).await;
        cleanup_keto_for_subject(&keto, &subject).await;
    }

    #[tokio::test]
    async fn list_projects_returns_only_visible_via_keto_expand() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject_a = format!("user:test-a-{}", Uuid::new_v4());
        let subject_b = format!("user:test-b-{}", Uuid::new_v4());

        // Unique prefix suffix so repeated test runs (or retries) don't collide
        // on the global shared Postgres instance.
        let suffix = Uuid::new_v4().simple().to_string()[..6].to_uppercase();
        let mut project_ids_a = Vec::new();

        // User A creates 3 projects.
        for i in 0..3_u32 {
            let p = svc
                .create_project(authed_request(
                    CreateProjectRequest {
                        name: format!("Project A-{i}"),
                        prefix: format!("A{i}{suffix}"),
                        icon: String::new(),
                        color: String::new(),
                        description: String::new(),
                        idempotency_key: String::new(),
                    },
                    &subject_a,
                ))
                .await
                .expect("create failed")
                .into_inner();
            project_ids_a.push(Uuid::parse_str(&p.id).unwrap());
        }

        // User B creates 1 project.
        let pb = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "Project B-0".to_string(),
                    prefix: format!("B0{suffix}"),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                },
                &subject_b,
            ))
            .await
            .expect("create failed")
            .into_inner();
        let project_id_b = Uuid::parse_str(&pb.id).unwrap();

        // User A sees exactly 3 projects.
        let list_a = svc
            .list_projects(authed_request(ListProjectsRequest {}, &subject_a))
            .await
            .expect("list_projects failed for user A")
            .into_inner();

        let ids_a: Vec<String> = list_a.projects.iter().map(|p| p.id.clone()).collect();
        assert_eq!(
            ids_a.len(),
            3,
            "user A should see exactly 3 projects, got: {ids_a:?}"
        );
        for pid in &project_ids_a {
            assert!(
                ids_a.contains(&pid.to_string()),
                "user A's project {pid} missing from list"
            );
        }

        // User B sees exactly 1 project.
        let list_b = svc
            .list_projects(authed_request(ListProjectsRequest {}, &subject_b))
            .await
            .expect("list_projects failed for user B")
            .into_inner();

        let ids_b: Vec<String> = list_b.projects.iter().map(|p| p.id.clone()).collect();
        assert_eq!(
            ids_b.len(),
            1,
            "user B should see exactly 1 project, got: {ids_b:?}"
        );
        assert!(
            ids_b.contains(&project_id_b.to_string()),
            "user B's project {project_id_b} missing from list"
        );

        // Cleanup
        for pid in &project_ids_a {
            cleanup_project(&pool, *pid).await;
        }
        cleanup_project(&pool, project_id_b).await;
        cleanup_keto_for_subject(&keto, &subject_a).await;
        cleanup_keto_for_subject(&keto, &subject_b).await;
    }

    #[tokio::test]
    async fn update_project_applies_patch_fields_only() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());

        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "Original Name".to_string(),
                    prefix: "ORIG".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: "original description".to_string(),
                    idempotency_key: String::new(),
                },
                &subject,
            ))
            .await
            .expect("create failed")
            .into_inner();

        let project_id = created.id.clone();

        // Update name only; description and prefix should be unchanged.
        let updated = svc
            .update_project(authed_request_with_object(
                UpdateProjectRequest {
                    project_id: project_id.clone(),
                    project: Some(Project {
                        id: String::new(),
                        name: "Updated Name".to_string(),
                        prefix: String::new(),
                        icon: String::new(),
                        color: String::new(),
                        description: String::new(),
                        created_at: None,
                        updated_at: None,
                        member_count: 0,
                    }),
                    update_mask: None,
                },
                &subject,
                &project_id,
            ))
            .await
            .expect("update_project failed")
            .into_inner();

        assert_eq!(updated.name, "Updated Name", "name should be updated");
        assert_eq!(
            updated.description, "original description",
            "description should be unchanged"
        );
        assert_eq!(updated.prefix, "ORIG", "prefix should be unchanged");

        // Cleanup
        let pid = Uuid::parse_str(&project_id).unwrap();
        cleanup_project(&pool, pid).await;
        cleanup_keto_for_subject(&keto, &subject).await;
    }

    #[tokio::test]
    async fn delete_project_cascades_and_clears_keto_tuples() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());

        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "To Delete".to_string(),
                    prefix: "DEL".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
            ))
            .await
            .expect("create failed")
            .into_inner();

        let project_id = created.id.clone();
        let pid = Uuid::parse_str(&project_id).unwrap();

        // Verify it exists before delete.
        let before: Option<Uuid> = sqlx::query("SELECT id FROM projects WHERE id = $1")
            .bind(pid)
            .fetch_optional(&pool)
            .await
            .unwrap()
            .map(|r| r.get("id"));
        assert!(before.is_some(), "project should exist before delete");

        svc.delete_project(authed_request_with_object(
            DeleteProjectRequest {
                project_id: project_id.clone(),
            },
            &subject,
            &project_id,
        ))
        .await
        .expect("delete_project failed");

        // Verify row is gone.
        let after: Option<Uuid> = sqlx::query("SELECT id FROM projects WHERE id = $1")
            .bind(pid)
            .fetch_optional(&pool)
            .await
            .unwrap()
            .map(|r| r.get("id"));
        assert!(after.is_none(), "project row should be gone after delete");

        // Cleanup residual Keto tuples (best-effort; delete already ran).
        cleanup_keto_for_subject(&keto, &subject).await;
    }

    #[tokio::test]
    async fn add_member_writes_keto_then_sql_row() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let owner = format!("user:test-owner-{}", Uuid::new_v4());
        let member = format!("user:test-member-{}", Uuid::new_v4());

        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "AddMember Test".to_string(),
                    prefix: "AMT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                },
                &owner,
            ))
            .await
            .expect("create failed")
            .into_inner();

        let project_id = created.id.clone();
        let pid = Uuid::parse_str(&project_id).unwrap();

        svc.add_member(authed_request_with_object(
            AddMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                relation: "edit".to_string(),
            },
            &owner,
            &project_id,
        ))
        .await
        .expect("add_member failed");

        // Verify Keto tuple was written.
        let keto_allowed = keto
            .check_permission_with_retry(KETO_NS, &project_id, "edit", &member)
            .await
            .expect("keto check failed");
        assert!(keto_allowed, "Keto should have 'edit' tuple for member");

        // Verify SQL mirror row.
        let sql_role: Option<String> =
            sqlx::query("SELECT role FROM project_members WHERE project_id = $1 AND user_id = $2")
                .bind(pid)
                .bind(&member)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .map(|r| r.get("role"));
        assert!(sql_role.is_some(), "project_members row should exist");
        assert_eq!(sql_role.unwrap(), "edit");

        // Cleanup
        cleanup_project(&pool, pid).await;
        cleanup_keto_for_subject(&keto, &owner).await;
        cleanup_keto_for_subject(&keto, &member).await;
    }

    #[tokio::test]
    async fn remove_member_clears_both_keto_and_sql() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let owner = format!("user:test-owner-{}", Uuid::new_v4());
        let member = format!("user:test-member-{}", Uuid::new_v4());

        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "RemoveMember Test".to_string(),
                    prefix: "RMT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                },
                &owner,
            ))
            .await
            .expect("create failed")
            .into_inner();

        let project_id = created.id.clone();
        let pid = Uuid::parse_str(&project_id).unwrap();

        // Add then remove.
        svc.add_member(authed_request_with_object(
            AddMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                relation: "view".to_string(),
            },
            &owner,
            &project_id,
        ))
        .await
        .expect("add_member failed");

        svc.remove_member(authed_request_with_object(
            RemoveMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
            },
            &owner,
            &project_id,
        ))
        .await
        .expect("remove_member failed");

        // Keto tuple should be gone.
        let keto_allowed = keto
            .check_permission_with_retry(KETO_NS, &project_id, "view", &member)
            .await
            .unwrap_or(false);
        assert!(
            !keto_allowed,
            "Keto 'view' tuple should be gone after remove"
        );

        // SQL row should be gone.
        let sql_row: Option<String> =
            sqlx::query("SELECT role FROM project_members WHERE project_id = $1 AND user_id = $2")
                .bind(pid)
                .bind(&member)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .map(|r| r.get("role"));
        assert!(
            sql_row.is_none(),
            "project_members row should be gone after remove"
        );

        // Cleanup
        cleanup_project(&pool, pid).await;
        cleanup_keto_for_subject(&keto, &owner).await;
    }

    #[tokio::test]
    async fn list_members_returns_inserted_rows() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let owner = format!("user:test-owner-{}", Uuid::new_v4());
        let viewer = format!("user:test-viewer-{}", Uuid::new_v4());

        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "ListMembers Test".to_string(),
                    prefix: "LMT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                },
                &owner,
            ))
            .await
            .expect("create failed")
            .into_inner();

        let project_id = created.id.clone();
        let pid = Uuid::parse_str(&project_id).unwrap();

        svc.add_member(authed_request_with_object(
            AddMemberRequest {
                project_id: project_id.clone(),
                subject: viewer.clone(),
                relation: "view".to_string(),
            },
            &owner,
            &project_id,
        ))
        .await
        .expect("add_member failed");

        let members = svc
            .list_members(authed_request_with_object(
                ListMembersRequest {
                    project_id: project_id.clone(),
                },
                &owner,
                &project_id,
            ))
            .await
            .expect("list_members failed")
            .into_inner()
            .members;

        // Should have at least owner + viewer.
        assert!(
            members.len() >= 2,
            "expected at least 2 members, got {}",
            members.len()
        );

        let subjects: Vec<&str> = members.iter().map(|m| m.subject.as_str()).collect();
        assert!(
            subjects.contains(&owner.as_str()),
            "owner should be in members list"
        );
        assert!(
            subjects.contains(&viewer.as_str()),
            "viewer should be in members list"
        );

        let viewer_entry = members.iter().find(|m| m.subject == viewer).unwrap();
        assert_eq!(viewer_entry.relation, "view");
        assert_eq!(viewer_entry.project_id, project_id);
        assert!(viewer_entry.added_at.is_some());

        // Cleanup
        cleanup_project(&pool, pid).await;
        cleanup_keto_for_subject(&keto, &owner).await;
        cleanup_keto_for_subject(&keto, &viewer).await;
    }

    #[tokio::test]
    async fn create_with_idempotency_key_returns_cached_response_on_replay() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let idem_key = format!("idem-test-{}", Uuid::new_v4());

        let make_req = || {
            authed_request(
                CreateProjectRequest {
                    name: "Idempotent Project".to_string(),
                    prefix: "IDP".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: "idempotency test".to_string(),
                    idempotency_key: idem_key.clone(),
                },
                &subject,
            )
        };

        let first = svc
            .create_project(make_req())
            .await
            .expect("first create failed")
            .into_inner();

        let second = svc
            .create_project(make_req())
            .await
            .expect("second create (replay) failed")
            .into_inner();

        // Both responses must carry the same project id.
        assert_eq!(
            first.id, second.id,
            "idempotent replay must return same project id"
        );
        assert_eq!(first.name, second.name);

        // Cleanup
        let pid = Uuid::parse_str(&first.id).unwrap();
        cleanup_project(&pool, pid).await;
        let _ = sqlx::query("DELETE FROM idempotency_keys WHERE key = $1")
            .bind(&idem_key)
            .execute(&pool)
            .await;
        cleanup_keto_for_subject(&keto, &subject).await;
    }

    // ── Stage 4c SubscribeProject tests ──────────────────────────────────────

    /// SubscribeProject is not implemented yet and returns `Unimplemented`.
    #[tokio::test]
    async fn subscribe_project_returns_unimplemented_pending_stage_4c5() {
        let pool = setup_pool().await;
        let keto = setup_keto().await;
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = Uuid::new_v4().to_string();

        let mut req = Request::new(SubscribeProjectRequest {
            project_id: project_id.clone(),
            since_seq: 0,
        });
        req.extensions_mut()
            .insert(AuthContext::authenticated(&subject, None));
        req.extensions_mut()
            .insert(crate::auth::keto_dispatch::CheckedObjectId(
                project_id.clone(),
            ));

        let result = svc.subscribe_project(req).await;

        assert!(
            result.is_err(),
            "SubscribeProject must return an error (unimplemented)"
        );
        let status = result.err().expect("result was Ok after is_err check");
        assert_eq!(
            status.code(),
            tonic::Code::Unimplemented,
            "SubscribeProject must return Unimplemented pending Stage 4c.5, got {status:?}"
        );
    }

    // NOTE: per-project multi-board merge test will be added together with
    // the `subscribe_project` implementation (currently returns
    // `Status::unimplemented`; covered by
    // `subscribe_project_returns_unimplemented_pending_stage_4c5`).
}
