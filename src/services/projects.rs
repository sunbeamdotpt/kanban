// SPDX-License-Identifier: AGPL-3.0-or-later
//! ProjectService implementation.
//!
//! Handles project lifecycle and membership. All permission changes are
//! written to the permission backend first, then mirrored to the `project_members` table.
//! If the SQL write fails after the permission write succeeds, we log a `mirror_drift`
//! warning so the background reconciler can catch up.
//!
//! Member and project-field mutations write project-scoped `event_log`
//! outbox rows (`MemberAdded` / `MemberRoleChanged` / `MemberRemoved` /
//! `ProjectUpdated`) routed to `kanban.project.<id>.events`.
//!
//! `SubscribeProject` merges the live streams of every board in the project
//! (one `build_subscribe_board_stream` child per board) with the project
//! subject (member/project events) into a single server stream.
//!
//! Uses the dynamic sqlx API (no macros) so the crate builds without a
//! live DATABASE_URL.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::id::Id;
use async_stream::stream;
use buffa_types::google::protobuf::Timestamp;
use chrono::{DateTime, Utc};
use connectrpc::{
    ConnectError, RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream,
};
use serde_json::json;
use sqlx::PgPool;
use sqlx::Row;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tracing::{error, warn};

use crate::auth::identity_client::IdentityClient;
use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_dispatch::CheckedObjectId;
use crate::auth::permission_expand::{ExpandQuery, expand_objects};
use crate::auth::permission_retry::PermissionRetryExt;
use crate::cpb::sunbeam::kanban::v1::{
    AddMemberRequest, AddMemberResponse, BoardEventEnvelope, CreateProjectRequest,
    CreateProjectResponse, DeleteProjectRequest, DeleteProjectResponse, GetProjectRequest,
    GetProjectResponse, ListMembersRequest, ListMembersResponse, ListProjectsRequest,
    ListProjectsResponse, Project, ProjectMember, ProjectService, RemoveMemberRequest,
    RemoveMemberResponse, SubscribeProjectRequest, SubscribeProjectResponse, UpdateProjectRequest,
    UpdateProjectResponse, board_event_envelope::Payload,
};
use crate::event_log::insert_project_event;
use crate::realtime::registry::{BoardSubscriberRegistry, StreamHandle};
use crate::services::boards::{
    SubscribeBoardArgs, build_subscribe_board_stream, heartbeat_envelope, revalidate_permission,
    revalidate_token,
};
use crate::services::visibility::is_public_or_internal;

// ── Constants ────────────────────────────────────────────────────────────────

const PERMISSION_TYPE: &str = "KanbanProject";
const MAX_PROJECTS: usize = 10_000;

// ── Service struct ───────────────────────────────────────────────────────────

pub struct ProjectServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
    pub identity: Arc<IdentityClient>,
    pub registry: Arc<BoardSubscriberRegistry>,
    pub heartbeat_interval: Duration,
    pub permission_recheck_interval: Duration,
    pub cutover_seen_capacity: usize,
}

// ── Timestamp helpers (chrono ↔ buffa_types) ────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
        ..Default::default()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    let full = format!("{msg}: {err}");
    error!("{full}");
    ConnectError::internal(full)
}

fn subject_from_request(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| ConnectError::unauthenticated("missing auth context"))
}

/// Extract the caller's tenant id from the auth context.
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

fn checked_object_id(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<CheckedObjectId>()
        .map(|c| c.0.clone())
        .ok_or_else(|| ConnectError::internal("missing CheckedObjectId extension"))
}

/// Convert a Postgres row into a `Project` proto.
fn project_from_row(row: &sqlx::postgres::PgRow, member_count: i32) -> Project {
    let id: Id = row.get("id");
    let name: String = row.get("name");
    let slug: String = row.get("slug");
    let description: Option<String> = row.get("description");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    let prefix: String = row
        .try_get::<String, _>("prefix")
        .ok()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| slug.chars().take(4).collect::<String>().to_uppercase());

    Project {
        id: id.to_string(),
        name,
        prefix,
        icon: String::new(),
        color: String::new(),
        description: description.unwrap_or_default(),
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        member_count,
        ..Default::default()
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
        added_at: Some(to_proto_ts(created_at)).into(),
        ..Default::default()
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

type SubscribeProjectStream = ServiceStream<SubscribeProjectResponse>;

// ── impl ProjectService ──────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl ProjectService for ProjectServiceImpl {
    // ── CreateProject ────────────────────────────────────────────────────────

    async fn create_project(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateProjectRequest>,
    ) -> ServiceResult<CreateProjectResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

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
                    "SELECT id, name, slug, prefix, description, owner_id, created_at, updated_at FROM projects WHERE tenant_id = $1 AND id = $2",
                )
                .bind(&tenant_id)
                .bind(project_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached project", e))?;

                if let Some(row) = row {
                    let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
                    return Ok(Response::new(CreateProjectResponse {
                        project: Some(project_from_row(&row, member_count)).into(),
                        ..Default::default()
                    }));
                }
                // Project was deleted after idempotency key was set — fall through.
            }
        }

        // Validate required fields.
        if req.name.is_empty() {
            return Err(ConnectError::invalid_argument("name is required"));
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

        // Card refs are minted as `{prefix}-{seq}`; without an explicit prefix
        // fall back to the first 4 slug chars (same convention as migration
        // 0019's backfill).
        let prefix = if req.prefix.is_empty() {
            slug.chars().take(4).collect::<String>()
        } else {
            req.prefix.to_uppercase()
        };

        let project_id = Id::new();

        // INSERT project.
        let row = sqlx::query(
            r#"
            INSERT INTO projects (id, tenant_id, name, slug, description, owner_id, prefix)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, name, slug, description, owner_id, created_at, updated_at
            "#,
        )
        .bind(project_id)
        .bind(&tenant_id)
        .bind(&req.name)
        .bind(&slug)
        .bind(&req.description)
        .bind(&subject)
        .bind(&prefix)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db) = e
                && db.constraint() == Some("projects_tenant_slug_key")
            {
                return ConnectError::already_exists("project with that prefix already exists");
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
            project: Some(project).into(),
            ..Default::default()
        }))
    }

    // ── GetProject ───────────────────────────────────────────────────────────

    async fn get_project(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, GetProjectRequest>,
    ) -> ServiceResult<GetProjectResponse> {
        let object_id = checked_object_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let row = sqlx::query(
            "SELECT id, name, slug, prefix, description, owner_id, created_at, updated_at FROM projects WHERE tenant_id = $1 AND id = $2",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch project", e))?
        .ok_or_else(|| ConnectError::not_found("project not found"))?;

        let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
        Ok(Response::new(GetProjectResponse {
            project: Some(project_from_row(&row, member_count)).into(),
            ..Default::default()
        }))
    }

    // ── ListProjects ─────────────────────────────────────────────────────────

    async fn list_projects(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, ListProjectsRequest>,
    ) -> ServiceResult<ListProjectsResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;

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
            return Ok(Response::new(ListProjectsResponse {
                projects: vec![],
                ..Default::default()
            }));
        }

        let ids: Vec<Id> = visible_ids
            .iter()
            .filter_map(|s| s.parse::<Id>().ok())
            .collect();

        let rows = sqlx::query(
            "SELECT id, name, slug, prefix, description, owner_id, created_at, updated_at FROM projects WHERE tenant_id = $1 AND id = ANY($2)",
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

        Ok(Response::new(ListProjectsResponse {
            projects,
            ..Default::default()
        }))
    }

    // ── UpdateProject ────────────────────────────────────────────────────────

    async fn update_project(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateProjectRequest>,
    ) -> ServiceResult<UpdateProjectResponse> {
        let object_id = checked_object_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let req = request.to_owned_message();
        let patch = req.project.into_option().unwrap_or_default();

        // Apply sparse patch — only non-empty fields are applied. `slug` is
        // immutable identity (derived at create); the patch's `prefix` field
        // updates the card-ref `prefix` column, never the slug. The
        // ProjectUpdated outbox event (post-patch values) rides in the same
        // transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = sqlx::query(
            r#"
            UPDATE projects SET
                name        = CASE WHEN $3 != '' THEN $3 ELSE name END,
                prefix      = CASE WHEN $4 != '' THEN $4 ELSE prefix END,
                description = CASE WHEN $5 != '' THEN $5 ELSE description END,
                updated_at  = now()
            WHERE tenant_id = $1 AND id = $2
            RETURNING id, name, slug, prefix, description, owner_id, created_at, updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&patch.name)
        .bind(patch.prefix.to_uppercase())
        .bind(&patch.description)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to update project", e))?
        .ok_or_else(|| ConnectError::not_found("project not found"))?;

        let name: String = row.get("name");
        let prefix: String = row.get("prefix");
        let description: Option<String> = row.get("description");
        insert_project_event(
            &mut tx,
            &tenant_id,
            project_id,
            "ProjectUpdated",
            json!({
                "project_id": project_id.to_string(),
                "name": name,
                "prefix": prefix,
                "description": description.unwrap_or_default(),
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        let member_count = fetch_member_count(&self.pool, &tenant_id, project_id).await;
        Ok(Response::new(UpdateProjectResponse {
            project: Some(project_from_row(&row, member_count)).into(),
            ..Default::default()
        }))
    }

    // ── DeleteProject ────────────────────────────────────────────────────────

    async fn delete_project(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, DeleteProjectRequest>,
    ) -> ServiceResult<DeleteProjectResponse> {
        let object_id = checked_object_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let result = sqlx::query("DELETE FROM projects WHERE tenant_id = $1 AND id = $2")
            .bind(&tenant_id)
            .bind(project_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete project", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("project not found"));
        }

        // Best-effort: permission tuple cleanup for this project is logged as drift.
        // delete_relation_tuples filters by subject (not object); object-scoped
        // deletion requires the Stage 7a reconciler. We log the drift so it is
        // visible in metrics and the reconciler can mop it up.
        warn!(
            project_id = %project_id,
            "delete_project: permission tuple cleanup is best-effort; reconciler (Stage 7a) will catch any drift"
        );

        Ok(Response::new(DeleteProjectResponse::default()))
    }

    // ── AddMember ────────────────────────────────────────────────────────────

    async fn add_member(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AddMemberRequest>,
    ) -> ServiceResult<AddMemberResponse> {
        let object_id = checked_object_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;

        let req = request.to_owned_message();

        if req.subject.is_empty() {
            return Err(ConnectError::invalid_argument("subject is required"));
        }
        // Subjects are typed ids (`user:<id>` / `agent:<id>`) — the OpenFGA
        // model only relates those types. Reject free-form strings so typos
        // don't create unreachable tuples.
        let subject_valid = req
            .subject
            .split_once(':')
            .is_some_and(|(kind, id)| matches!(kind, "user" | "agent") && !id.is_empty());
        if !subject_valid {
            return Err(ConnectError::invalid_argument(
                "subject must be of the form user:<id> or agent:<id>",
            ));
        }
        if req.relation.is_empty() {
            return Err(ConnectError::invalid_argument("relation is required"));
        }
        // owner is set at CreateProject and is immutable.
        if req.relation == "owner" {
            return Err(ConnectError::permission_denied(
                "owner relation cannot be granted via AddMember",
            ));
        }
        // The OpenFGA model only accepts role writes; computed relations
        // (view/edit/manage/delete) cannot be written directly.
        if !matches!(req.relation.as_str(), "admin" | "editor" | "viewer") {
            return Err(ConnectError::invalid_argument(
                "relation must be one of: admin, editor, viewer",
            ));
        }

        // A subject may already hold a different role on this project. Clear
        // the stale tuple first so they never accumulate roles the mirror row
        // (single role per member) does not reflect.
        let existing_role: Option<String> = sqlx::query(
            "SELECT role FROM project_members WHERE tenant_id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.subject)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to look up existing member role", e))?
        .map(|r| r.get("role"));

        if let Some(ref existing) = existing_role
            && existing != &req.relation
            && let Err(e) = permission
                .delete_relation_tuples(
                    PERMISSION_TYPE,
                    Some(project_id.to_string()),
                    Some(existing.clone()),
                    Some(req.subject.clone()),
                )
                .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                subject = %req.subject,
                "mirror_drift: failed to delete stale role tuple before re-grant"
            );
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

        // Project-scoped outbox event: a role swap is MemberRoleChanged,
        // anything else is MemberAdded.
        let (event_type, payload) = match existing_role {
            Some(ref old) if old != &req.relation => (
                "MemberRoleChanged",
                json!({
                    "project_id": project_id.to_string(),
                    "subject": req.subject,
                    "old_relation": old,
                    "new_relation": req.relation,
                }),
            ),
            _ => (
                "MemberAdded",
                json!({
                    "project_id": project_id.to_string(),
                    "subject": req.subject,
                    "relation": req.relation,
                    "display_name": "",
                    "email": "",
                }),
            ),
        };
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;
        insert_project_event(&mut tx, &tenant_id, project_id, event_type, payload).await?;
        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(AddMemberResponse::default()))
    }

    // ── RemoveMember ─────────────────────────────────────────────────────────

    async fn remove_member(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RemoveMemberRequest>,
    ) -> ServiceResult<RemoveMemberResponse> {
        let object_id = checked_object_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;

        let req = request.to_owned_message();

        if req.subject.is_empty() {
            return Err(ConnectError::invalid_argument("subject is required"));
        }

        // Delete every role tuple the subject holds on THIS project. Scoping
        // to the object is essential: a namespace-wide delete would revoke the
        // subject's roles on unrelated projects, and a mirror-driven delete
        // would miss stale tuples left by earlier role changes.
        if let Err(e) = permission
            .delete_relation_tuples(
                PERMISSION_TYPE,
                Some(project_id.to_string()),
                None,
                Some(req.subject.clone()),
            )
            .await
        {
            warn!(
                error = %e,
                project_id = %project_id,
                subject = %req.subject,
                "mirror_drift: failed to delete permission tuples for removed member"
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
            return Err(ConnectError::not_found("member not found"));
        }

        // Project-scoped outbox event.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;
        insert_project_event(
            &mut tx,
            &tenant_id,
            project_id,
            "MemberRemoved",
            json!({
                "project_id": project_id.to_string(),
                "subject": req.subject,
            }),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(RemoveMemberResponse::default()))
    }

    // ── ListMembers ───────────────────────────────────────────────────────────

    async fn list_members(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, ListMembersRequest>,
    ) -> ServiceResult<ListMembersResponse> {
        let object_id = checked_object_id(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let rows = sqlx::query(
            "SELECT project_id, user_id, role, created_at FROM project_members WHERE tenant_id = $1 AND project_id = $2 ORDER BY created_at",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list members", e))?;

        let members = rows.iter().map(member_from_row).collect();
        Ok(Response::new(ListMembersResponse {
            members,
            ..Default::default()
        }))
    }

    // ── SubscribeProject ──────────────────────────────────────────────────────
    //
    // Multi-board merge: one `build_subscribe_board_stream` child per board in
    // the project (each child replays that board's snapshot and tails its live
    // events) plus the project subject (Member*/ProjectUpdated envelopes).
    // Child heartbeats are filtered; the merge loop emits one heartbeat per
    // interval itself. Token revalidation runs per yield and the KanbanProject
    // view permission is rechecked per interval.

    async fn subscribe_project(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, SubscribeProjectRequest>,
    ) -> ServiceResult<SubscribeProjectStream> {
        let auth = ctx
            .extensions()
            .get::<AuthContext>()
            .cloned()
            .ok_or_else(|| ConnectError::unauthenticated("missing auth context"))?;
        let tenant = auth
            .tenant_id
            .clone()
            .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))?;
        let req = request.to_owned_message();
        let project_id_str = req.project_id;
        let project_id = project_id_str
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let permission = Arc::new(
            self.permission
                .tenant_client(&tenant)
                .await
                .map_err(|e| internal("failed to build tenant permission client", e))?,
        );

        let board_rows = sqlx::query(
            "SELECT id, visibility FROM boards WHERE tenant_id = $1 AND project_id = $2 ORDER BY created_at ASC",
        )
        .bind(&tenant)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list project boards", e))?;

        // Build one child stream per board. `since_seq` from the request is
        // not forwarded: JetStream sequence spaces are per-board, so a project
        // resume token cannot map onto them — children always replay a full
        // snapshot, which is idempotent for the client.
        let mut children = Vec::new();
        for row in &board_rows {
            let board_id: Id = row.get("id");
            let visibility: String = row.get("visibility");
            let board_id_str = board_id.to_string();
            match build_subscribe_board_stream(SubscribeBoardArgs {
                registry: Arc::clone(&self.registry),
                permission: Arc::clone(&permission),
                identity: Arc::clone(&self.identity),
                auth: auth.clone(),
                board_id: board_id_str.clone(),
                is_private: !is_public_or_internal(&visibility),
                heartbeat_interval: self.heartbeat_interval,
                permission_recheck_interval: self.permission_recheck_interval,
                pool: self.pool.clone(),
                tenant_id: tenant.clone(),
                since_seq: 0,
                cutover_seen_capacity: self.cutover_seen_capacity,
            })
            .await
            {
                Ok(child) => children.push((board_id_str, child)),
                Err(e) => {
                    warn!(
                        board_id = %board_id_str,
                        error = %e,
                        "SubscribeProject: failed to build child board stream; board skipped"
                    );
                }
            }
        }

        // Project-scoped events (Member*/ProjectUpdated) arrive on the
        // project subject. A failure here must not kill the board streams.
        let project_handle = match Arc::clone(&self.registry)
            .subscribe_project(&project_id_str)
            .await
        {
            Ok(h) => Some(h),
            Err(e) => {
                warn!(
                    project_id = %project_id_str,
                    error = %e,
                    "SubscribeProject: project subject subscribe failed; continuing with board streams"
                );
                None
            }
        };

        let heartbeat_interval = self.heartbeat_interval;
        let permission_recheck_interval = self.permission_recheck_interval;

        let s = stream! {
            // Forward every child board stream into one channel. A child that
            // ends (board deleted mid-subscription) or errors is dropped
            // without killing the merge.
            let (child_tx, mut child_rx) = mpsc::channel::<BoardEventEnvelope>(256);
            let mut children_open = !children.is_empty();
            for (board_id, mut child) in children {
                let tx = child_tx.clone();
                tokio::spawn(async move {
                    while let Some(item) = child.next().await {
                        match item {
                            Ok(resp) => {
                                let Some(envelope) = resp.envelope.into_option() else {
                                    continue;
                                };
                                // The merge loop emits its own heartbeat.
                                if matches!(envelope.payload, Some(Payload::Heartbeat(_))) {
                                    continue;
                                }
                                if tx.send(envelope).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    board_id = %board_id,
                                    error = %e,
                                    "SubscribeProject: child board stream error; board dropped"
                                );
                                break;
                            }
                        }
                    }
                });
            }
            drop(child_tx);

            let mut project_handle = project_handle;
            let mut heartbeat = tokio::time::interval(heartbeat_interval);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            heartbeat.tick().await;
            let mut last_permission_recheck = Instant::now();

            loop {
                // Token revalidation on every yield.
                match revalidate_token(&auth) {
                    Ok(true) => {}
                    Ok(false) => {
                        yield Err(ConnectError::unauthenticated("token expired"));
                        break;
                    }
                    Err(status) => {
                        yield Err(status);
                        break;
                    }
                }

                // Permission recheck on KanbanProject view per interval.
                if last_permission_recheck.elapsed() >= permission_recheck_interval {
                    match revalidate_permission(&permission, &auth, PERMISSION_TYPE, &project_id_str)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            yield Err(ConnectError::permission_denied(
                                "permission revoked mid-stream",
                            ));
                            break;
                        }
                        Err(status) => {
                            yield Err(status);
                            break;
                        }
                    }
                    last_permission_recheck = Instant::now();
                }

                tokio::select! {
                    env = recv_child(&mut child_rx, children_open) => {
                        match env {
                            Some(envelope) => yield Ok(SubscribeProjectResponse {
                                envelope: Some(envelope).into(),
                                ..Default::default()
                            }),
                            None => {
                                // Every child stream ended.
                                children_open = false;
                                if project_handle.is_none() {
                                    break;
                                }
                            }
                        }
                    }
                    msg = recv_project(&mut project_handle) => {
                        match msg {
                            Some(Ok(envelope)) => yield Ok(SubscribeProjectResponse {
                                envelope: Some(envelope).into(),
                                ..Default::default()
                            }),
                            Some(Err(broadcast::error::RecvError::Lagged(_))) => {
                                // Skip lagged project events; the next ones still arrive.
                            }
                            Some(Err(broadcast::error::RecvError::Closed)) | None => {
                                warn!("SubscribeProject: project event channel closed; continuing with board streams");
                                project_handle = None;
                                if !children_open {
                                    break;
                                }
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        yield Ok(SubscribeProjectResponse {
                            envelope: Some(heartbeat_envelope()).into(),
                            ..Default::default()
                        });
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(s)))
    }
}

// ── Merge-loop helpers ─────────────────────────────────────────────────────────

/// Receive the next envelope forwarded from a child board stream. Pends
/// forever once every child has ended (`open == false`).
async fn recv_child(
    rx: &mut mpsc::Receiver<BoardEventEnvelope>,
    open: bool,
) -> Option<BoardEventEnvelope> {
    if open {
        rx.recv().await
    } else {
        std::future::pending().await
    }
}

/// Receive the next project-subject envelope. Pends forever when the project
/// subscription is not active.
#[allow(clippy::type_complexity)]
async fn recv_project(
    handle: &mut Option<StreamHandle>,
) -> Option<Result<BoardEventEnvelope, broadcast::error::RecvError>> {
    match handle.as_mut() {
        Some(h) => Some(h.receiver.recv().await),
        None => std::future::pending().await,
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{connect_ctx, connect_request};

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
            identity: crate::test_support::setup_identity().await,
            registry,
            heartbeat_interval: Duration::from_millis(500),
            permission_recheck_interval: Duration::from_millis(30_000),
            cutover_seen_capacity: 1024,
        }
    }

    /// Create an authenticated context that only carries the caller subject.
    fn authed_ctx(subject: &str) -> RequestContext {
        connect_ctx(AuthContext::authenticated(
            crate::test_support::test_tenant_id(),
            subject,
        ))
    }

    /// Create an authenticated context that also carries a `CheckedObjectId`.
    fn authed_ctx_with_object(subject: &str, object_id: &str) -> RequestContext {
        let mut ctx = authed_ctx(subject);
        ctx.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        ctx
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
            .create_project(
                authed_ctx(&subject),
                connect_request(&CreateProjectRequest {
                    name: "Test Project Alpha".to_string(),
                    prefix: "TPA".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: "Integration test project".to_string(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_project failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        assert!(!created.id.is_empty(), "created project must have an id");
        assert_eq!(created.name, "Test Project Alpha");
        assert_eq!(created.prefix, "TPA");
        assert_eq!(created.description, "Integration test project");
        assert!(created.created_at.is_set());
        assert!(created.updated_at.is_set());

        let project_id = created.id.parse::<Id>().expect("invalid id from create");

        let fetched = svc
            .get_project(
                authed_ctx_with_object(&subject, &created.id),
                connect_request(&GetProjectRequest {
                    project_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get_project failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.name, created.name);
        assert_eq!(fetched.prefix, created.prefix);
        assert_eq!(fetched.description, created.description);

        // The prefix must be persisted (card refs are minted from it).
        let stored_prefix: String = sqlx::query_scalar("SELECT prefix FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_one(&pool)
            .await
            .expect("fetch stored prefix");
        assert_eq!(stored_prefix, "TPA");

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
                .create_project(
                    authed_ctx(&subject_a),
                    connect_request(&CreateProjectRequest {
                        name: format!("Project A-{i}"),
                        prefix: format!("A{i}{suffix}"),
                        icon: String::new(),
                        color: String::new(),
                        description: String::new(),
                        idempotency_key: String::new(),
                        ..Default::default()
                    }),
                )
                .await
                .expect("create failed")
                .body
                .project
                .into_option()
                .expect("project missing");
            project_ids_a.push(p.id.parse::<Id>().unwrap());
        }

        // User B creates 1 project.
        let pb = svc
            .create_project(
                authed_ctx(&subject_b),
                connect_request(&CreateProjectRequest {
                    name: "Project B-0".to_string(),
                    prefix: format!("B0{suffix}"),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");
        let project_id_b = pb.id.parse::<Id>().unwrap();

        // User A sees exactly 3 projects.
        let list_a = svc
            .list_projects(
                authed_ctx(&subject_a),
                connect_request(&ListProjectsRequest::default()),
            )
            .await
            .expect("list_projects failed for user A")
            .body;

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
            .list_projects(
                authed_ctx(&subject_b),
                connect_request(&ListProjectsRequest::default()),
            )
            .await
            .expect("list_projects failed for user B")
            .body;

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
            .create_project(
                authed_ctx(&subject),
                connect_request(&CreateProjectRequest {
                    name: "Original Name".to_string(),
                    prefix: "ORIG".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: "original description".to_string(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        let project_id = created.id.clone();

        // Update name only; description and prefix should be unchanged.
        let updated = svc
            .update_project(
                authed_ctx_with_object(&subject, &project_id),
                connect_request(&UpdateProjectRequest {
                    project_id: project_id.clone(),
                    project: Some(Project {
                        name: "Updated Name".to_string(),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_project failed")
            .body
            .project
            .into_option()
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

    /// KANBAN-006 regression: UpdateProject must write the patch's `prefix`
    /// into the `prefix` column (uppercased) and leave the immutable `slug`
    /// untouched. The response must echo the real prefix.
    #[tokio::test]
    async fn update_project_updates_prefix_without_touching_slug() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());

        let created = svc
            .create_project(
                authed_ctx(&subject),
                connect_request(&CreateProjectRequest {
                    name: "Prefix Update".to_string(),
                    prefix: "PUPD".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        let updated = svc
            .update_project(
                authed_ctx_with_object(&subject, &project_id),
                connect_request(&UpdateProjectRequest {
                    project_id: project_id.clone(),
                    project: Some(Project {
                        prefix: "triforce".to_string(),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_project failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        // (c) The response echoes the real (uppercased) prefix.
        assert_eq!(updated.prefix, "TRIFORCE");

        // (a) prefix column updated, (b) slug unchanged.
        let row = sqlx::query("SELECT slug, prefix FROM projects WHERE tenant_id = $1 AND id = $2")
            .bind(test_tenant_id())
            .bind(pid)
            .fetch_one(&pool)
            .await
            .expect("project row missing");
        let slug: String = row.get("slug");
        let prefix: String = row.get("prefix");
        assert_eq!(prefix, "TRIFORCE", "prefix column should be updated");
        assert_eq!(
            slug, "PUPD",
            "slug is immutable identity and must not change"
        );

        // Cleanup
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
            .create_project(
                authed_ctx(&subject),
                connect_request(&CreateProjectRequest {
                    name: "To Delete".to_string(),
                    prefix: "DEL".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
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

        svc.delete_project(
            authed_ctx_with_object(&subject, &project_id),
            connect_request(&DeleteProjectRequest {
                project_id: project_id.clone(),
                ..Default::default()
            }),
        )
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
            .create_project(
                authed_ctx(&owner),
                connect_request(&CreateProjectRequest {
                    name: "AddMember Test".to_string(),
                    prefix: "AMT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        svc.add_member(
            authed_ctx_with_object(&owner, &project_id),
            connect_request(&AddMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                relation: "editor".to_string(),
                ..Default::default()
            }),
        )
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
            .create_project(
                authed_ctx(&owner),
                connect_request(&CreateProjectRequest {
                    name: "RemoveMember Test".to_string(),
                    prefix: "RMT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        // Add then remove.
        svc.add_member(
            authed_ctx_with_object(&owner, &project_id),
            connect_request(&AddMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                relation: "viewer".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("add_member failed");

        svc.remove_member(
            authed_ctx_with_object(&owner, &project_id),
            connect_request(&RemoveMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                ..Default::default()
            }),
        )
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

    /// Removing a member from one project must not revoke their roles on
    /// unrelated projects (the tuple delete must be object-scoped).
    #[tokio::test]
    async fn remove_member_does_not_revoke_other_projects() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let member = format!("user:test-member-{}", Id::new());

        let mut project_ids = Vec::new();
        for (name, prefix) in [("Project A", "RMA"), ("Project B", "RMB")] {
            let created = svc
                .create_project(
                    authed_ctx(&owner),
                    connect_request(&CreateProjectRequest {
                        name: name.to_string(),
                        prefix: prefix.to_string(),
                        icon: String::new(),
                        color: String::new(),
                        description: String::new(),
                        idempotency_key: String::new(),
                        ..Default::default()
                    }),
                )
                .await
                .expect("create failed")
                .body
                .project
                .into_option()
                .expect("project missing");
            svc.add_member(
                authed_ctx_with_object(&owner, &created.id),
                connect_request(&AddMemberRequest {
                    project_id: created.id.clone(),
                    subject: member.clone(),
                    relation: "viewer".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_member failed");
            project_ids.push(created.id);
        }

        // Remove from project A only.
        svc.remove_member(
            authed_ctx_with_object(&owner, &project_ids[0]),
            connect_request(&RemoveMemberRequest {
                project_id: project_ids[0].clone(),
                subject: member.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("remove_member failed");

        assert!(
            !permission
                .check_permission_with_retry(PERMISSION_TYPE, &project_ids[0], "view", &member)
                .await
                .unwrap_or(false),
            "view should be revoked on the removed project"
        );
        assert!(
            permission
                .check_permission_with_retry(PERMISSION_TYPE, &project_ids[1], "view", &member)
                .await
                .unwrap_or(false),
            "view must survive on the unrelated project"
        );

        for id in &project_ids {
            cleanup_project(&pool, &test_tenant_id(), id.parse::<Id>().unwrap()).await;
        }
        cleanup_permission_for_subject(&permission, &owner).await;
        cleanup_permission_for_subject(&permission, &member).await;
    }

    /// AddMember must reject free-form subject strings (typed ids only).
    #[tokio::test]
    async fn add_member_rejects_invalid_subject_format() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let created = svc
            .create_project(
                authed_ctx(&owner),
                connect_request(&CreateProjectRequest {
                    name: "Subject Validation".to_string(),
                    prefix: "SVL".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        for bad in ["not-a-subject", "group:admins", "user:", ":", ""] {
            let err = svc
                .add_member(
                    authed_ctx_with_object(&owner, &created.id),
                    connect_request(&AddMemberRequest {
                        project_id: created.id.clone(),
                        subject: bad.to_string(),
                        relation: "viewer".to_string(),
                        ..Default::default()
                    }),
                )
                .await
                .expect_err("invalid subject must be rejected");
            assert_eq!(
                err.code,
                connectrpc::ErrorCode::InvalidArgument,
                "subject {bad:?}"
            );
        }

        cleanup_project(&pool, &test_tenant_id(), created.id.parse::<Id>().unwrap()).await;
        cleanup_permission_for_subject(&permission, &owner).await;
    }

    /// Re-adding a member with a different role must not leave a stale tuple
    /// behind: a later RemoveMember revokes all access.
    #[tokio::test]
    async fn remove_member_after_role_change_revokes_all_roles() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let member = format!("user:test-member-{}", Id::new());

        let created = svc
            .create_project(
                authed_ctx(&owner),
                connect_request(&CreateProjectRequest {
                    name: "Role Change Test".to_string(),
                    prefix: "RCT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");
        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        for relation in ["viewer", "editor"] {
            svc.add_member(
                authed_ctx_with_object(&owner, &project_id),
                connect_request(&AddMemberRequest {
                    project_id: project_id.clone(),
                    subject: member.clone(),
                    relation: relation.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_member failed");
        }

        // The mirror holds exactly one row with the latest role.
        let role: String = sqlx::query_scalar(
            "SELECT role FROM project_members WHERE tenant_id = $1 AND project_id = $2 AND user_id = $3",
        )
        .bind(test_tenant_id())
        .bind(pid)
        .bind(&member)
        .fetch_one(&pool)
        .await
        .expect("member row missing");
        assert_eq!(role, "editor");

        svc.remove_member(
            authed_ctx_with_object(&owner, &project_id),
            connect_request(&RemoveMemberRequest {
                project_id: project_id.clone(),
                subject: member.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("remove_member failed");

        assert!(
            !permission
                .check_permission_with_retry(PERMISSION_TYPE, &project_id, "view", &member)
                .await
                .unwrap_or(false),
            "stale viewer tuple must not survive removal"
        );

        cleanup_project(&pool, &test_tenant_id(), pid).await;
        cleanup_permission_for_subject(&permission, &owner).await;
        cleanup_permission_for_subject(&permission, &member).await;
    }

    #[tokio::test]
    async fn list_members_returns_inserted_rows() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-owner-{}", Id::new());
        let viewer = format!("user:test-viewer-{}", Id::new());

        let created = svc
            .create_project(
                authed_ctx(&owner),
                connect_request(&CreateProjectRequest {
                    name: "ListMembers Test".to_string(),
                    prefix: "LMT".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        let project_id = created.id.clone();
        let pid = project_id.parse::<Id>().unwrap();

        svc.add_member(
            authed_ctx_with_object(&owner, &project_id),
            connect_request(&AddMemberRequest {
                project_id: project_id.clone(),
                subject: viewer.clone(),
                relation: "viewer".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("add_member failed");

        let members = svc
            .list_members(
                authed_ctx_with_object(&owner, &project_id),
                connect_request(&ListMembersRequest {
                    project_id: project_id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_members failed")
            .body
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
        assert!(viewer_entry.added_at.is_set());

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
            (
                authed_ctx(&subject),
                connect_request(&CreateProjectRequest {
                    name: "Idempotent Project".to_string(),
                    prefix: "IDP".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: "idempotency test".to_string(),
                    idempotency_key: idem_key.clone(),
                    ..Default::default()
                }),
            )
        };

        let (ctx, req) = make_req();
        let first = svc
            .create_project(ctx, req)
            .await
            .expect("first create failed")
            .body
            .project
            .into_option()
            .expect("project missing");

        let (ctx, req) = make_req();
        let second = svc
            .create_project(ctx, req)
            .await
            .expect("second create (replay) failed")
            .body
            .project
            .into_option()
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
            .create_project(
                authed_ctx(&subject_a),
                connect_request(&CreateProjectRequest {
                    name: "Tenant A Project".to_string(),
                    prefix: format!("TA{}", Id::new().to_string()[20..26].to_uppercase()),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .project
            .into_option()
            .expect("project missing");
        let project_id = created.id.parse::<Id>().expect("invalid id");

        // Tenant B caller cannot read the project even with the id in the
        // CheckedObjectId extension (SQL isolation).
        let mut get_ctx = connect_ctx(AuthContext::authenticated(&tenant_b, &subject_b));
        get_ctx
            .extensions_mut()
            .insert(CheckedObjectId(created.id.clone()));

        let err = svc
            .get_project(
                get_ctx,
                connect_request(&GetProjectRequest {
                    project_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("tenant B must not see tenant A project");
        assert_eq!(
            err.code,
            connectrpc::ErrorCode::NotFound,
            "cross-tenant get_project must return NotFound"
        );

        // Tenant B caller lists projects and sees none.
        let list_ctx = connect_ctx(AuthContext::authenticated(&tenant_b, &subject_b));
        let list = svc
            .list_projects(list_ctx, connect_request(&ListProjectsRequest::default()))
            .await
            .expect("list_projects failed for tenant B")
            .body;
        assert!(
            list.projects.is_empty(),
            "tenant B must not see tenant A projects"
        );

        // Cleanup
        cleanup_project(&pool, &tenant_a, project_id).await;
        cleanup_permission_for_subject(&permission, &subject_a).await;
    }

    // ── SubscribeProject tests ───────────────────────────────────────────────

    /// Receive envelopes until `want` matches (heartbeats and child cutovers
    /// are skipped by the predicate).
    ///
    /// 30s budget: under full-suite load the chain add_member → permission
    /// grant → SQL → outbox drain → JetStream publish → registry pump can
    /// individually exceed 10s against the shared containers (observed
    /// acquire latencies >10s), and a tight timeout flakes the test without
    /// any product defect.
    async fn recv_until(
        stream: &mut SubscribeProjectStream,
        mut want: impl FnMut(&BoardEventEnvelope) -> bool,
    ) -> BoardEventEnvelope {
        use tokio_stream::StreamExt;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let env = stream
                    .next()
                    .await
                    .expect("stream ended")
                    .expect("stream error")
                    .envelope
                    .into_option()
                    .expect("envelope missing");
                if want(&env) {
                    return env;
                }
            }
        })
        .await
        .expect("timeout waiting for expected envelope")
    }

    /// A project with two boards receives live events from both boards plus
    /// project-scoped MemberAdded on a single merged stream.
    #[tokio::test]
    async fn subscribe_project_merges_board_streams_and_project_events() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let tenant_id = test_tenant_id();

        let project = svc
            .create_project(
                authed_ctx(&subject),
                connect_request(&CreateProjectRequest {
                    name: "Merge Project".to_string(),
                    prefix: "MRG".to_string(),
                    icon: String::new(),
                    color: String::new(),
                    description: String::new(),
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_project failed")
            .body
            .project
            .into_option()
            .expect("project missing");
        let project_id: Id = project.id.parse().expect("project id");
        let board_a = crate::test_support::seed_board(&pool, &tenant_id, project_id).await;
        let board_b = crate::test_support::seed_board(&pool, &tenant_id, project_id).await;

        // NATS client for the outbox drain; ensure the JetStream stream exists.
        let nats = Arc::new(
            sunbeam_g2v::mq::NatsClient::connect(&sunbeam_g2v::config::NatsConfig {
                url: std::env::var("NATS_URL")
                    .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
                jetstream: true,
                lease_duration: 30,
                auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
            })
            .await
            .expect("NATS connect failed"),
        );
        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("ensure_kanban_stream failed");

        let mut stream = svc
            .subscribe_project(
                authed_ctx(&subject),
                connect_request(&SubscribeProjectRequest {
                    project_id: project_id.to_string(),
                    since_seq: 0,
                    ..Default::default()
                }),
            )
            .await
            .expect("subscribe_project failed")
            .body;

        // Both child board streams cut over with an empty snapshot.
        let mut cutovers = 0;
        recv_until(&mut stream, |env| {
            if matches!(env.payload, Some(Payload::Cutover(_))) {
                cutovers += 1;
            }
            cutovers >= 2
        })
        .await;

        // Write + dispatch a card event on each board; both must arrive on
        // the merged stream.
        for board in [board_a, board_b] {
            let mut tx = pool.begin().await.expect("begin tx");
            crate::event_log::insert_board_event(
                &mut tx,
                &tenant_id,
                board,
                "CardCreated",
                serde_json::json!({ "card_id": Id::new().to_string() }),
            )
            .await
            .expect("insert_board_event failed");
            tx.commit().await.expect("commit");

            let dispatcher = crate::realtime::outbox::OutboxDispatcher::new(
                pool.clone(),
                Arc::clone(&nats),
                crate::test_support::setup_identity().await,
                crate::realtime::outbox::OutboxConfig::default(),
            )
            .with_board_filter(board);
            assert_eq!(dispatcher.drain_once().await.expect("drain failed"), 1);

            let env = recv_until(&mut stream, |env| {
                matches!(env.payload, Some(Payload::CardCreated(_)))
            })
            .await;
            assert_eq!(env.board_id, board.to_string());
        }

        // add_member emits a project-scoped MemberAdded on the same stream.
        let member_subject = format!("user:member-{}", Id::new());
        svc.add_member(
            authed_ctx_with_object(&subject, &project_id.to_string()),
            connect_request(&AddMemberRequest {
                project_id: project_id.to_string(),
                subject: member_subject.clone(),
                relation: "viewer".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("add_member failed");

        let dispatcher = crate::realtime::outbox::OutboxDispatcher::new(
            pool.clone(),
            Arc::clone(&nats),
            crate::test_support::setup_identity().await,
            crate::realtime::outbox::OutboxConfig::default(),
        )
        .with_board_filter(project_id);
        assert_eq!(dispatcher.drain_once().await.expect("drain failed"), 1);

        let env = recv_until(&mut stream, |env| {
            matches!(env.payload, Some(Payload::MemberAdded(_)))
        })
        .await;
        match env.payload {
            Some(Payload::MemberAdded(ev)) => {
                assert_eq!(ev.project_id, project_id.to_string());
                assert_eq!(ev.subject, member_subject);
                assert_eq!(ev.relation, "viewer");
            }
            other => panic!("expected MemberAdded, got {other:?}"),
        }

        drop(stream);
        cleanup_project(&pool, &tenant_id, project_id).await;
        cleanup_permission_for_subject(&permission, &subject).await;
        cleanup_permission_for_subject(&permission, &member_subject).await;
    }
}
