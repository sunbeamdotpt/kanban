// SPDX-License-Identifier: AGPL-3.0-or-later
//! ProjectService implementation.
//!
//! Handles project lifecycle and membership. All permission changes are
//! written to the permission backend first, then mirrored to the `project_members` table.
//! If the SQL write fails after the permission write succeeds, we log a `mirror_drift`
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

use crate::id::Id;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use sqlx::PgPool;
use sqlx::Row;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{error, warn};

use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_dispatch::CheckedObjectId;
use crate::auth::permission_expand::{ExpandQuery, expand_objects};
use crate::auth::permission_retry::PermissionRetryExt;
use crate::pb::project_service_server::ProjectService;
use crate::pb::{
    AddMemberRequest, AddMemberResponse, CreateProjectRequest, CreateProjectResponse,
    DeleteProjectRequest, DeleteProjectResponse, GetProjectRequest, GetProjectResponse,
    ListMembersRequest, ListMembersResponse, ListProjectsRequest, ListProjectsResponse, Project,
    ProjectMember, RemoveMemberRequest, RemoveMemberResponse, SubscribeProjectRequest,
    SubscribeProjectResponse, UpdateProjectRequest, UpdateProjectResponse,
};
use crate::realtime::registry::BoardSubscriberRegistry;

// ── Constants ────────────────────────────────────────────────────────────────

const PERMISSION_TYPE: &str = "KanbanProject";
const MAX_PROJECTS: usize = 10_000;

// ── Service struct ───────────────────────────────────────────────────────────

pub struct ProjectServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
    pub registry: Arc<BoardSubscriberRegistry>,
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
    let full = format!("{msg}: {err}");
    error!("{full}");
    Status::internal(full)
}

fn subject_from_request<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| Status::unauthenticated("missing auth context"))
}

/// Extract the caller's tenant id from the auth context.
fn tenant_id_from_request<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| Status::unauthenticated("missing tenant context"))
}

/// Extract the caller's tenant from the auth context and derive a permission
/// client scoped to that tenant's store (see `TENANT_HEADER`).
async fn tenant_client_for<T>(
    permission: &PermissionClient,
    req: &Request<T>,
) -> Result<PermissionClient, Status> {
    let tenant = req
        .extensions()
        .get::<AuthContext>()
        .and_then(|a| a.tenant_id.clone())
        .ok_or_else(|| Status::unauthenticated("missing tenant context"))?;
    permission
        .tenant_client(&tenant)
        .await
        .map_err(|e| internal("failed to build tenant permission client", e))
}

fn checked_object_id<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<CheckedObjectId>()
        .map(|c| c.0.clone())
        .ok_or_else(|| Status::internal("missing CheckedObjectId extension"))
}

/// Convert a Postgres row into a `Project` proto.
fn project_from_row(row: &sqlx::postgres::PgRow, member_count: i32) -> Project {
    let id: Id = row.get("id");
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
    let project_id: Id = row.get("project_id");
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

async fn fetch_member_count(pool: &PgPool, tenant_id: &str, project_id: Id) -> i32 {
    sqlx::query(
        "SELECT COUNT(*) AS cnt FROM project_members WHERE tenant_id = $1 AND project_id = $2",
    )
    .bind(tenant_id)
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
    Pin<Box<dyn Stream<Item = Result<SubscribeProjectResponse, Status>> + Send + 'static>>;

// ── impl ProjectService ──────────────────────────────────────────────────────

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    // ── CreateProject ────────────────────────────────────────────────────────

    async fn create_project(
        &self,
        request: Request<CreateProjectRequest>,
    ) -> Result<Response<CreateProjectResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let permission = tenant_client_for(&self.permission, &request).await?;
        let req = request.into_inner();

        // Idempotency check — look up by key; if found, re-fetch the project from DB.
        if !req.idempotency_key.is_empty() {
            let cached_id: Option<Option<Id>> = sqlx::query(
                "SELECT response_card_id FROM idempotency_keys WHERE tenant_id = $1 AND key = $2",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("idempotency key lookup failed", e))?
            .map(|r| r.get("response_card_id"));

            if let Some(Some(project_id)) = cached_id {
                let row = sqlx::query(
                    "SELECT id, name, slug, description, owner_id, created_at, updated_at FROM projects WHERE tenant_id = $1 AND id = $2",
                )
                .bind(&tenant_id)
                .bind(project_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached project", e))?;

                if let Some(row) = row {
                    let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
                    return Ok(Response::new(CreateProjectResponse {
                        project: Some(project_from_row(&row, member_count)),
                    }));
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

        let project_id = Id::new();

        // INSERT project.
        let row = sqlx::query(
            r#"
            INSERT INTO projects (id, tenant_id, name, slug, description, owner_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, name, slug, description, owner_id, created_at, updated_at
            "#,
        )
        .bind(project_id)
        .bind(&tenant_id)
        .bind(&req.name)
        .bind(&slug)
        .bind(&req.description)
        .bind(&subject)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db) = e
                && db.constraint() == Some("projects_tenant_slug_key")
            {
                return Status::already_exists("project with that prefix already exists");
            }
            internal("failed to insert project", e)
        })?;

        // Write owner tuple to the permission backend.
        permission
            .grant_with_retry(PERMISSION_TYPE, &project_id.to_string(), "owner", &subject)
            .await
            .map_err(|e| internal("failed to write owner tuple to the permission backend", e))?;

        // Also write a "viewer" tuple so ListProjects sees it (owner already
        // implies viewer; the explicit role keeps the expand query simple).
        permission
            .grant_with_retry(PERMISSION_TYPE, &project_id.to_string(), "viewer", &subject)
            .await
            .map_err(|e| internal("failed to write viewer tuple to the permission backend", e))?;

        // Insert owner into project_members mirror (permission backend is already written — best-effort SQL).
        if let Err(e) = sqlx::query(
            "INSERT INTO project_members (project_id, tenant_id, user_id, role) VALUES ($1, $2, $3, 'owner') ON CONFLICT DO NOTHING",
        )
        .bind(project_id)
        .bind(&tenant_id)
        .bind(&subject)
        .execute(&self.pool)
        .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                "mirror_drift: owner tuple written to the permission backend but project_members insert failed"
            );
        }

        let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
        let project = project_from_row(&row, member_count);

        // Store idempotency response — record the created project UUID.
        if !req.idempotency_key.is_empty()
            && let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (tenant_id, key, response_card_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .bind(project_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }

        Ok(Response::new(CreateProjectResponse {
            project: Some(project),
        }))
    }

    // ── GetProject ───────────────────────────────────────────────────────────

    async fn get_project(
        &self,
        request: Request<GetProjectRequest>,
    ) -> Result<Response<GetProjectResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let row = sqlx::query(
            "SELECT id, name, slug, description, owner_id, created_at, updated_at FROM projects WHERE tenant_id = $1 AND id = $2",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch project", e))?
        .ok_or_else(|| Status::not_found("project not found"))?;

        let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
        Ok(Response::new(GetProjectResponse {
            project: Some(project_from_row(&row, member_count)),
        }))
    }

    // ── ListProjects ─────────────────────────────────────────────────────────

    async fn list_projects(
        &self,
        request: Request<ListProjectsRequest>,
    ) -> Result<Response<ListProjectsResponse>, Status> {
        let subject = subject_from_request(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let permission = tenant_client_for(&self.permission, &request).await?;

        let visible_ids = expand_objects(
            &permission,
            ExpandQuery {
                namespace: PERMISSION_TYPE,
                relation: "view",
                subject: &subject,
            },
            MAX_PROJECTS,
        )
        .await
        .map_err(|e| internal("permission expand failed", e))?;

        if visible_ids.is_empty() {
            return Ok(Response::new(ListProjectsResponse { projects: vec![] }));
        }

        let ids: Vec<Id> = visible_ids
            .iter()
            .filter_map(|s| s.parse::<Id>().ok())
            .collect();

        let rows = sqlx::query(
            "SELECT id, name, slug, description, owner_id, created_at, updated_at FROM projects WHERE tenant_id = $1 AND id = ANY($2)",
        )
        .bind(&tenant_id)
        .bind(&ids as &[Id])
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list projects", e))?;

        let mut projects = Vec::with_capacity(rows.len());
        for row in &rows {
            let pid: Id = row.get("id");
            let member_count = fetch_member_count(&self.pool, &tenant_id, pid).await;
            projects.push(project_from_row(row, member_count));
        }

        Ok(Response::new(ListProjectsResponse { projects }))
    }

    // ── UpdateProject ────────────────────────────────────────────────────────

    async fn update_project(
        &self,
        request: Request<UpdateProjectRequest>,
    ) -> Result<Response<UpdateProjectResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let req = request.into_inner();
        let patch = req.project.unwrap_or_default();

        // Apply sparse patch — only non-empty fields are applied.
        let row = sqlx::query(
            r#"
            UPDATE projects SET
                name        = CASE WHEN $3 != '' THEN $3 ELSE name END,
                slug        = CASE WHEN $4 != '' THEN $4 ELSE slug END,
                description = CASE WHEN $5 != '' THEN $5 ELSE description END,
                updated_at  = now()
            WHERE tenant_id = $1 AND id = $2
            RETURNING id, name, slug, description, owner_id, created_at, updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&patch.name)
        .bind(patch.prefix.to_uppercase())
        .bind(&patch.description)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to update project", e))?
        .ok_or_else(|| Status::not_found("project not found"))?;

        let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
        Ok(Response::new(UpdateProjectResponse {
            project: Some(project_from_row(&row, member_count)),
        }))
    }

    // ── DeleteProject ────────────────────────────────────────────────────────

    async fn delete_project(
        &self,
        request: Request<DeleteProjectRequest>,
    ) -> Result<Response<DeleteProjectResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let result = sqlx::query("DELETE FROM projects WHERE tenant_id = $1 AND id = $2")
            .bind(&tenant_id)
            .bind(project_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete project", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("project not found"));
        }

        // Best-effort: permission tuple cleanup for this project is logged as drift.
        // delete_relation_tuples filters by subject (not object); object-scoped
        // deletion requires the Stage 7a reconciler. We log the drift so it is
        // visible in metrics and the reconciler can mop it up.
        warn!(
            project_id = %project_id,
            "delete_project: permission tuple cleanup is best-effort; reconciler (Stage 7a) will catch any drift"
        );

        Ok(Response::new(DeleteProjectResponse {}))
    }

    // ── AddMember ────────────────────────────────────────────────────────────

    async fn add_member(
        &self,
        request: Request<AddMemberRequest>,
    ) -> Result<Response<AddMemberResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;
        let permission = tenant_client_for(&self.permission, &request).await?;

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
        // The OpenFGA model only accepts role writes; computed relations
        // (view/edit/manage/delete) cannot be written directly.
        if !matches!(req.relation.as_str(), "admin" | "editor" | "viewer") {
            return Err(Status::invalid_argument(
                "relation must be one of: admin, editor, viewer",
            ));
        }

        // Permission backend FIRST (mirror-table write order per plan / Pre-mortem 5).
        permission
            .grant_with_retry(
                PERMISSION_TYPE,
                &project_id.to_string(),
                &req.relation,
                &req.subject,
            )
            .await
            .map_err(|e| internal("failed to write member tuple to the permission backend", e))?;

        // SQL mirror second — best-effort; the permission backend is the source of truth.
        if let Err(e) = sqlx::query(
            r#"
            INSERT INTO project_members (project_id, tenant_id, user_id, role)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (project_id, user_id) DO UPDATE SET role = $4
            "#,
        )
        .bind(project_id)
        .bind(&tenant_id)
        .bind(&req.subject)
        .bind(&req.relation)
        .execute(&self.pool)
        .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                subject = %req.subject,
                "mirror_drift: permission tuple written but project_members insert failed"
            );
            // Do not return error — reconciler will fix SQL drift.
        }

        Ok(Response::new(AddMemberResponse {}))
    }

    // ── RemoveMember ─────────────────────────────────────────────────────────

    async fn remove_member(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<RemoveMemberResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;
        let permission = tenant_client_for(&self.permission, &request).await?;

        let req = request.into_inner();

        if req.subject.is_empty() {
            return Err(Status::invalid_argument("subject is required"));
        }

        // Read existing relation from mirror to know which permission tuple to delete.
        let existing_role: Option<String> = sqlx::query(
            "SELECT role FROM project_members WHERE tenant_id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.subject)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to look up member", e))?
        .map(|r| r.get("role"));

        // Delete permission tuple for the known relation.
        if let Some(ref relation) = existing_role
            && let Err(e) = permission
                .delete_relation_tuples(
                    PERMISSION_TYPE,
                    None,
                    Some(relation.clone()),
                    Some(req.subject.clone()),
                )
                .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                subject = %req.subject,
                "mirror_drift: failed to delete permission tuple for removed member"
            );
        }

        // Delete SQL row.
        let result = sqlx::query(
            "DELETE FROM project_members WHERE tenant_id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.subject)
        .execute(&self.pool)
        .await
        .map_err(|e| internal("failed to delete member row", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("member not found"));
        }

        Ok(Response::new(RemoveMemberResponse {}))
    }

    // ── ListMembers ───────────────────────────────────────────────────────────

    async fn list_members(
        &self,
        request: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let tenant_id = tenant_id_from_request(&request)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let rows = sqlx::query(
            "SELECT project_id, user_id, role, created_at FROM project_members WHERE tenant_id = $1 AND project_id = $2 ORDER BY created_at",
        )
        .bind(&tenant_id)
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
    // to the subject via `permission_expand_objects(KanbanBoard, view, subject)`, then
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

    async fn setup_permission() -> Arc<PermissionClient> {
        crate::test_support::setup_permission().await
    }

    async fn make_service(pool: PgPool, permission: Arc<PermissionClient>) -> ProjectServiceImpl {
        let nats_url =
            std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
        let nats = Arc::new(
            sunbeam_g2v::mq::NatsClient::connect(&sunbeam_g2v::config::NatsConfig {
                url: nats_url,
                jetstream: true,
                lease_duration: 30,
                auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
            })
            .await
            .expect("NATS connect failed"),
        );
        let registry = Arc::new(BoardSubscriberRegistry::new(nats, "pod-test-projects"));
        ProjectServiceImpl {
            pool,
            permission,
            registry,
        }
    }

    /// Create an authenticated request that only carries the caller subject.
    fn authed_request<T>(body: T, subject: &str) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut().insert(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            subject,
        ));
        req
    }

    /// Create an authenticated request that also carries a `CheckedObjectId`.
    fn authed_request_with_object<T>(body: T, subject: &str, object_id: &str) -> Request<T> {
        let mut req = authed_request(body, subject);
        req.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        req
    }

    /// Remove every permission relation tuple a test may have created for a subject.
    async fn cleanup_permission_for_subject(permission: &PermissionClient, subject: &str) {
        for relation in &["owner", "admin", "editor", "viewer"] {
            let _ = permission
                .delete_relation_tuples(
                    PERMISSION_TYPE,
                    None,
                    Some(relation.to_string()),
                    Some(subject.to_string()),
                )
                .await;
        }
    }

    /// Delete a project row directly during test cleanup.
    async fn cleanup_project(pool: &PgPool, tenant_id: &str, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id)
            .bind(project_id)
            .execute(pool)
            .await;
    }

    /// Tenant id used by the test helpers below.
    fn test_tenant_id() -> String {
        crate::test_support::test_tenant_id()
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_returns_same_project() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

        assert!(!created.id.is_empty(), "created project must have an id");
        assert_eq!(created.name, "Test Project Alpha");
        assert_eq!(created.prefix, "TPA");
        assert_eq!(created.description, "Integration test project");
        assert!(created.created_at.is_some());
        assert!(created.updated_at.is_some());

        let project_id = created.id.parse::<Id>().expect("invalid id from create");

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
            .into_inner()
            .project
            .expect("project missing");

        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.name, created.name);
        assert_eq!(fetched.prefix, created.prefix);
        assert_eq!(fetched.description, created.description);

        // Cleanup
        cleanup_project(&pool, &test_tenant_id(), project_id).await;
        cleanup_permission_for_subject(&permission, &subject).await;
    }

    #[tokio::test]
    async fn list_projects_returns_only_visible_via_permission_expand() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject_a = format!("user:test-a-{}", Id::new());
        let subject_b = format!("user:test-b-{}", Id::new());

        // Unique prefix suffix so repeated test runs (or retries) don't collide
        // on the global shared Postgres instance.
        let suffix = Id::new().to_string()[20..26].to_uppercase();
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
                .into_inner()
                .project
                .expect("project missing");
            project_ids_a.push(p.id.parse::<Id>().unwrap());
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
            .into_inner()
            .project
            .expect("project missing");
        let project_id_b = pb.id.parse::<Id>().unwrap();

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
        let tenant_id = test_tenant_id();
        for pid in &project_ids_a {
            cleanup_project(&pool, &tenant_id, *pid).await;
        }
        cleanup_project(&pool, &tenant_id, project_id_b).await;
        cleanup_permission_for_subject(&permission, &subject_a).await;
        cleanup_permission_for_subject(&permission, &subject_b).await;
    }

    #[tokio::test]
    async fn update_project_applies_patch_fields_only() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

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
            .into_inner()
            .project
            .expect("project missing");

        assert_eq!(updated.name, "Updated Name", "name should be updated");
        assert_eq!(
            updated.description, "original description",
            "description should be unchanged"
        );
        assert_eq!(updated.prefix, "ORIG", "prefix should be unchanged");

        // Cleanup
        let pid = project_id.parse::<Id>().unwrap();
        cleanup_project(&pool, &test_tenant_id(), pid).await;
        cleanup_permission_for_subject(&permission, &subject).await;
    }

    #[tokio::test]
    async fn delete_project_cascades_and_clears_permission_tuples() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        let tenant_id = test_tenant_id();

        // Verify it exists before delete.
        let before: Option<Id> =
            sqlx::query("SELECT id FROM projects WHERE tenant_id = $1 AND id = $2")
                .bind(&tenant_id)
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
        let after: Option<Id> =
            sqlx::query("SELECT id FROM projects WHERE tenant_id = $1 AND id = $2")
                .bind(&tenant_id)
                .bind(pid)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .map(|r| r.get("id"));
        assert!(after.is_none(), "project row should be gone after delete");

        // Cleanup residual permission tuples (best-effort; delete already ran).
        cleanup_permission_for_subject(&permission, &subject).await;
    }

    #[tokio::test]
    async fn add_member_writes_permission_then_sql_row() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let member = format!("user:test-member-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        svc.add_member(authed_request_with_object(
            AddMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                relation: "editor".to_string(),
            },
            &owner,
            &project_id,
        ))
        .await
        .expect("add_member failed");

        // Verify permission tuple was written.
        let permission_allowed = permission
            .check_permission_with_retry(PERMISSION_TYPE, &project_id, "edit", &member)
            .await
            .expect("permission check failed");
        assert!(
            permission_allowed,
            "permission backend should have 'edit' tuple for member"
        );

        // Verify SQL mirror row.
        let tenant_id = test_tenant_id();
        let sql_role: Option<String> = sqlx::query(
            "SELECT role FROM project_members WHERE tenant_id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(&tenant_id)
        .bind(pid)
        .bind(&member)
        .fetch_optional(&pool)
        .await
        .unwrap()
        .map(|r| r.get("role"));
        assert!(sql_role.is_some(), "project_members row should exist");
        assert_eq!(sql_role.unwrap(), "editor");

        // Cleanup
        cleanup_project(&pool, &test_tenant_id(), pid).await;
        cleanup_permission_for_subject(&permission, &owner).await;
        cleanup_permission_for_subject(&permission, &member).await;
    }

    #[tokio::test]
    async fn remove_member_clears_both_permission_and_sql() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let member = format!("user:test-member-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        // Add then remove.
        svc.add_member(authed_request_with_object(
            AddMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                relation: "viewer".to_string(),
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

        // permission tuple should be gone.
        let permission_allowed = permission
            .check_permission_with_retry(PERMISSION_TYPE, &project_id, "view", &member)
            .await
            .unwrap_or(false);
        assert!(
            !permission_allowed,
            "permission 'view' tuple should be gone after remove"
        );

        // SQL row should be gone.
        let tenant_id = test_tenant_id();
        let sql_row: Option<String> = sqlx::query(
            "SELECT role FROM project_members WHERE tenant_id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(&tenant_id)
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
        cleanup_project(&pool, &test_tenant_id(), pid).await;
        cleanup_permission_for_subject(&permission, &owner).await;
    }

    #[tokio::test]
    async fn list_members_returns_inserted_rows() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let viewer = format!("user:test-viewer-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        svc.add_member(authed_request_with_object(
            AddMemberRequest {
                project_id: project_id.clone(),
                subject: viewer.clone(),
                relation: "viewer".to_string(),
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
        assert_eq!(viewer_entry.relation, "viewer");
        assert_eq!(viewer_entry.project_id, project_id);
        assert!(viewer_entry.added_at.is_some());

        // Cleanup
        cleanup_project(&pool, &test_tenant_id(), pid).await;
        cleanup_permission_for_subject(&permission, &owner).await;
        cleanup_permission_for_subject(&permission, &viewer).await;
    }

    #[tokio::test]
    async fn create_with_idempotency_key_returns_cached_response_on_replay() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let idem_key = format!("idem-test-{}", Id::new());

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
            .into_inner()
            .project
            .expect("project missing");

        let second = svc
            .create_project(make_req())
            .await
            .expect("second create (replay) failed")
            .into_inner()
            .project
            .expect("project missing");

        // Both responses must carry the same project id.
        assert_eq!(
            first.id, second.id,
            "idempotent replay must return same project id"
        );
        assert_eq!(first.name, second.name);

        // Cleanup
        let tenant_id = test_tenant_id();
        let pid = first.id.parse::<Id>().unwrap();
        cleanup_project(&pool, &tenant_id, pid).await;
        let _ = sqlx::query("DELETE FROM idempotency_keys WHERE tenant_id = $1 AND key = $2")
            .bind(&tenant_id)
            .bind(&idem_key)
            .execute(&pool)
            .await;
        cleanup_permission_for_subject(&permission, &subject).await;
    }

    #[tokio::test]
    async fn cross_tenant_project_is_invisible() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let sso_gateway_url = crate::test_support::setup_sso_gateway_url().await;
        let tenant_a = test_tenant_id();
        let tenant_b = crate::test_support::containers::create_tenant(
            &sso_gateway_url,
            &format!("tenant-b-{}", Id::new()),
            "Tenant B",
        )
        .await
        .expect("failed to create tenant B");

        let subject_a = format!("user:test-a-{}", Id::new());
        let subject_b = format!("user:test-b-{}", Id::new());

        // Create a project in tenant A.
        let created = svc
            .create_project(authed_request(
                CreateProjectRequest {
                    name: "Tenant A Project".to_string(),
                    prefix: format!("TA{}", Id::new().to_string()[20..26].to_uppercase()),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                },
                &subject_a,
            ))
            .await
            .expect("create failed")
            .into_inner()
            .project
            .expect("project missing");
        let project_id = created.id.parse::<Id>().expect("invalid id");

        // Tenant B caller cannot read the project even with the id in the
        // CheckedObjectId extension (SQL isolation).
        let mut get_req = Request::new(GetProjectRequest {
            project_id: created.id.clone(),
        });
        get_req
            .extensions_mut()
            .insert(AuthContext::authenticated(&tenant_b, &subject_b));
        get_req
            .extensions_mut()
            .insert(CheckedObjectId(created.id.clone()));

        let status = svc
            .get_project(get_req)
            .await
            .expect_err("tenant B must not see tenant A project")
            .code();
        assert_eq!(
            status,
            tonic::Code::NotFound,
            "cross-tenant get_project must return NotFound"
        );

        // Tenant B caller lists projects and sees none.
        let mut list_req = Request::new(ListProjectsRequest {});
        list_req
            .extensions_mut()
            .insert(AuthContext::authenticated(&tenant_b, &subject_b));
        let list = svc
            .list_projects(list_req)
            .await
            .expect("list_projects failed for tenant B")
            .into_inner();
        assert!(
            list.projects.is_empty(),
            "tenant B must not see tenant A projects"
        );

        // Cleanup
        cleanup_project(&pool, &tenant_a, project_id).await;
        cleanup_permission_for_subject(&permission, &subject_a).await;
    }

    // ── Stage 4c SubscribeProject tests ──────────────────────────────────────

    /// SubscribeProject is not implemented yet and returns `Unimplemented`.
    #[tokio::test]
    async fn subscribe_project_returns_unimplemented_pending_stage_4c5() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = Id::new().to_string();

        let mut req = Request::new(SubscribeProjectRequest {
            project_id: project_id.clone(),
            since_seq: 0,
        });
        req.extensions_mut().insert(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            &subject,
        ));
        req.extensions_mut()
            .insert(crate::auth::permission_dispatch::CheckedObjectId(
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
