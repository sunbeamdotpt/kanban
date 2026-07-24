// SPDX-License-Identifier: AGPL-3.0-or-later
//! Project-scoped milestones.
//!
//! Milestones are project-owned catalog resources within a tenant; they have
//! no OpenFGA object of their own and reuse the parent project's permissions:
//!   * `view`   → list and get
//!   * `manage` → create, update, and delete
//!
//! Cards reference a milestone via `cards.milestone_id` (no foreign key), so
//! the service computes completion stats (`total_cards` / `completed_cards`)
//! with a grouped query over `cards`, and `DeleteMilestone` clears referencing
//! cards in the same transaction. No realtime events are emitted
//! (catalog-style, like templates).
//!
//! The dynamic `sqlx` API is used throughout, so `cargo check` works without a
//! live database connection.

use std::collections::HashMap;
use std::sync::Arc;

use crate::id::Id;
use buffa_types::google::protobuf::{FieldMask, Timestamp};
use chrono::{DateTime, Utc};
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use sqlx::{PgPool, Row};
use tracing::error;

use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_retry::PermissionRetryExt;
use crate::cpb::sunbeam::kanban::v1::{
    CreateMilestoneRequest, CreateMilestoneResponse, DeleteMilestoneRequest,
    DeleteMilestoneResponse, GetMilestoneRequest, GetMilestoneResponse, ListMilestonesRequest,
    ListMilestonesResponse, Milestone, MilestoneService, UpdateMilestoneRequest,
    UpdateMilestoneResponse,
};

// ── Constants ────────────────────────────────────────────────────────────────

const PERMISSION_TYPE_PROJECT: &str = "KanbanProject";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct MilestoneServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
}

// ── Timestamp helpers (chrono ↔ buffa_types) ────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
        ..Default::default()
    }
}

fn opt_to_proto_ts(dt: Option<DateTime<Utc>>) -> buffa::MessageField<Timestamp> {
    dt.map(to_proto_ts).into()
}

fn from_proto_ts(ts: &Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
}

// ── Error helpers ────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    error!(error = %err, "{msg}");
    ConnectError::internal(msg)
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

// ── Field-mask helper ────────────────────────────────────────────────────────

fn mask_contains(mask: &buffa::MessageField<FieldMask>, path: &str) -> bool {
    mask.as_option()
        .map(|m| m.paths.iter().any(|p| p == path))
        .unwrap_or(false)
}

// ── Row → proto helpers ───────────────────────────────────────────────────────

/// Map a milestones row to the proto message, folding in pre-computed card
/// stats (`total_cards`, `completed_cards`).
fn milestone_from_row(row: &sqlx::postgres::PgRow, stats: (i32, i32)) -> Milestone {
    let id: Id = row.get("id");
    let project_id: Id = row.get("project_id");
    let title: String = row.get("title");
    let due: Option<DateTime<Utc>> = row.get("due");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Milestone {
        id: id.to_string(),
        project_id: project_id.to_string(),
        title,
        due: opt_to_proto_ts(due),
        total_cards: stats.0,
        completed_cards: stats.1,
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        ..Default::default()
    }
}

// ── Card stats ───────────────────────────────────────────────────────────────

/// Completion stats per milestone: total cards and cards with `completed_at`
/// set. Milestones without cards are absent from the map (stats default to 0).
async fn fetch_card_stats(
    pool: &PgPool,
    tenant_id: &str,
    milestone_ids: Vec<String>,
) -> Result<HashMap<String, (i32, i32)>, ConnectError> {
    let rows = sqlx::query(
        "SELECT milestone_id, count(*) AS total, \
         count(*) FILTER (WHERE completed_at IS NOT NULL) AS completed \
         FROM cards \
         WHERE tenant_id = $1 AND milestone_id = ANY($2) \
         GROUP BY milestone_id",
    )
    .bind(tenant_id)
    .bind(milestone_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch milestone card stats", e))?;

    let mut stats = HashMap::with_capacity(rows.len());
    for row in &rows {
        let milestone_id: String = row.get("milestone_id");
        let total: i64 = row.get("total");
        let completed: i64 = row.get("completed");
        stats.insert(milestone_id, (total as i32, completed as i32));
    }
    Ok(stats)
}

/// Load a milestone row by id within the caller's tenant.
async fn load_milestone_row(
    pool: &PgPool,
    tenant_id: &str,
    milestone_id: Id,
) -> Result<sqlx::postgres::PgRow, ConnectError> {
    sqlx::query(
        "SELECT id, project_id, title, due, created_at, updated_at \
         FROM milestones \
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(milestone_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| internal("failed to fetch milestone", e))?
    .ok_or_else(|| ConnectError::not_found("milestone not found"))
}

/// Attach card stats to a single milestone row.
async fn milestone_with_stats(
    pool: &PgPool,
    tenant_id: &str,
    row: &sqlx::postgres::PgRow,
) -> Result<Milestone, ConnectError> {
    let id: Id = row.get("id");
    let stats = fetch_card_stats(pool, tenant_id, vec![id.to_string()]).await?;
    let counts = stats.get(&id.to_string()).copied().unwrap_or((0, 0));
    Ok(milestone_from_row(row, counts))
}

// ── impl MilestoneService ────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl MilestoneService for MilestoneServiceImpl {
    async fn create_milestone(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateMilestoneRequest>,
    ) -> ServiceResult<CreateMilestoneResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        if req.title.is_empty() {
            return Err(ConnectError::invalid_argument("title is required"));
        }

        let project_id = parse_id(&req.project_id, "project_id")?;
        check_project_permission(&permission, project_id, "manage", &subject).await?;

        let due: Option<DateTime<Utc>> = req.due.as_option().and_then(from_proto_ts);
        let milestone_id = Id::new();

        let row = sqlx::query(
            r#"
            INSERT INTO milestones (id, tenant_id, project_id, title, due)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, project_id, title, due, created_at, updated_at
            "#,
        )
        .bind(milestone_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.title)
        .bind(due)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to create milestone", e))?;

        // A fresh milestone has no cards yet.
        Ok(Response::new(CreateMilestoneResponse {
            milestone: Some(milestone_from_row(&row, (0, 0))).into(),
            ..Default::default()
        }))
    }

    async fn list_milestones(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListMilestonesRequest>,
    ) -> ServiceResult<ListMilestonesResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();

        let project_id = parse_id(&req.project_id, "project_id")?;
        check_project_permission(&permission, project_id, "view", &subject).await?;

        let rows = sqlx::query(
            "SELECT id, project_id, title, due, created_at, updated_at \
             FROM milestones \
             WHERE tenant_id = $1 AND project_id = $2 \
             ORDER BY created_at, id",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list milestones", e))?;

        let ids: Vec<String> = rows
            .iter()
            .map(|r| r.get::<Id, _>("id").to_string())
            .collect();
        let stats = fetch_card_stats(&self.pool, &tenant_id, ids).await?;

        let milestones = rows
            .iter()
            .map(|row| {
                let id: Id = row.get("id");
                let counts = stats.get(&id.to_string()).copied().unwrap_or((0, 0));
                milestone_from_row(row, counts)
            })
            .collect();

        Ok(Response::new(ListMilestonesResponse {
            milestones,
            ..Default::default()
        }))
    }

    async fn get_milestone(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetMilestoneRequest>,
    ) -> ServiceResult<GetMilestoneResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let milestone_id = parse_id(&req.milestone_id, "milestone_id")?;

        let row = load_milestone_row(&self.pool, &tenant_id, milestone_id).await?;

        let project_id: Id = row.get("project_id");
        check_project_permission(&permission, project_id, "view", &subject).await?;

        let milestone = milestone_with_stats(&self.pool, &tenant_id, &row).await?;

        Ok(Response::new(GetMilestoneResponse {
            milestone: Some(milestone).into(),
            ..Default::default()
        }))
    }

    async fn update_milestone(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateMilestoneRequest>,
    ) -> ServiceResult<UpdateMilestoneResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let milestone_id = parse_id(&req.milestone_id, "milestone_id")?;

        let existing = load_milestone_row(&self.pool, &tenant_id, milestone_id).await?;

        let project_id: Id = existing.get("project_id");
        check_project_permission(&permission, project_id, "manage", &subject).await?;

        let title = if mask_contains(&req.update_mask, "title") {
            if req.title.is_empty() {
                return Err(ConnectError::invalid_argument("title cannot be empty"));
            }
            Some(req.title)
        } else {
            None
        };
        // Tri-state via the mask: naming "due" applies the request value, and
        // an absent `due` field clears the column.
        let update_due = mask_contains(&req.update_mask, "due");
        let due: Option<DateTime<Utc>> = req.due.as_option().and_then(from_proto_ts);

        let row = sqlx::query(
            r#"
            UPDATE milestones SET
                title      = COALESCE($2, title),
                due        = CASE WHEN $3 THEN $4 ELSE due END,
                updated_at = now()
            WHERE id = $1 AND tenant_id = $5
            RETURNING id, project_id, title, due, created_at, updated_at
            "#,
        )
        .bind(milestone_id)
        .bind(title)
        .bind(update_due)
        .bind(due)
        .bind(&tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to update milestone", e))?;

        let milestone = milestone_with_stats(&self.pool, &tenant_id, &row).await?;

        Ok(Response::new(UpdateMilestoneResponse {
            milestone: Some(milestone).into(),
            ..Default::default()
        }))
    }

    async fn delete_milestone(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteMilestoneRequest>,
    ) -> ServiceResult<DeleteMilestoneResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let milestone_id = parse_id(&req.milestone_id, "milestone_id")?;

        let existing = load_milestone_row(&self.pool, &tenant_id, milestone_id).await?;

        let project_id: Id = existing.get("project_id");
        check_project_permission(&permission, project_id, "manage", &subject).await?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // cards.milestone_id has no FK; clear referencing cards manually.
        sqlx::query(
            "UPDATE cards SET milestone_id = NULL WHERE tenant_id = $1 AND milestone_id = $2",
        )
        .bind(&tenant_id)
        .bind(milestone_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to clear milestone from cards", e))?;

        let result = sqlx::query("DELETE FROM milestones WHERE id = $1 AND tenant_id = $2")
            .bind(milestone_id)
            .bind(&tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to delete milestone", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("milestone not found"));
        }

        tx.commit()
            .await
            .map_err(|e| internal("commit tx failed", e))?;

        Ok(Response::new(DeleteMilestoneResponse::default()))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::{connect_ctx, connect_request, containers};

    async fn make_service() -> MilestoneServiceImpl {
        let infra = containers::setup().await;
        MilestoneServiceImpl {
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

    async fn cleanup_project(pool: &PgPool, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    /// Seed the board + column chain a card row needs, then insert the card.
    async fn seed_card(
        pool: &PgPool,
        project_id: Id,
        milestone_id: Option<Id>,
        completed: bool,
    ) -> Id {
        let tenant_id = crate::test_support::test_tenant_id();
        let board_id = Id::new();
        let column_id = Id::new();
        let card_id = Id::new();

        sqlx::query(
            "INSERT INTO boards (id, tenant_id, project_id, name, slug) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(board_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(format!("Board {board_id}"))
        .bind(format!("b-{}", &board_id.to_string()[18..26]))
        .execute(pool)
        .await
        .expect("seed board failed");

        sqlx::query(
            "INSERT INTO columns (id, tenant_id, board_id, title, position) VALUES ($1, $2, $3, 'To Do', 0)",
        )
        .bind(column_id)
        .bind(&tenant_id)
        .bind(board_id)
        .execute(pool)
        .await
        .expect("seed column failed");

        let completed_at: Option<DateTime<Utc>> = completed.then(Utc::now);
        sqlx::query(
            "INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, position, milestone_id, completed_at, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $9, 'user:test')",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(board_id)
        .bind(column_id)
        .bind(format!("T-{}", &card_id.to_string()[18..26]))
        .bind(format!("Card {card_id}"))
        .bind(milestone_id)
        .bind(completed_at)
        .execute(pool)
        .await
        .expect("seed card failed");

        card_id
    }

    async fn create_milestone(
        svc: &MilestoneServiceImpl,
        subject: &str,
        project_id: Id,
        title: &str,
    ) -> Milestone {
        svc.create_milestone(
            authed_ctx(subject),
            connect_request(&CreateMilestoneRequest {
                project_id: project_id.to_string(),
                title: title.to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("create_milestone failed")
        .body
        .milestone
        .into_option()
        .expect("milestone missing")
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_milestone_with_zero_stats() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_milestone(
                authed_ctx(&subject),
                connect_request(&CreateMilestoneRequest {
                    project_id: project_id.to_string(),
                    title: "v1.0".to_string(),
                    due: Some(Timestamp {
                        seconds: 1_800_000_000,
                        nanos: 0,
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_milestone failed")
            .body
            .milestone
            .into_option()
            .expect("milestone missing");

        assert!(!created.id.is_empty());
        assert_eq!(created.title, "v1.0");
        assert_eq!(created.project_id, project_id.to_string());
        assert_eq!(created.total_cards, 0);
        assert_eq!(created.completed_cards, 0);
        let due = created.due.as_option().expect("due should be set");
        assert_eq!(due.seconds, 1_800_000_000);

        let fetched = svc
            .get_milestone(
                authed_ctx(&subject),
                connect_request(&GetMilestoneRequest {
                    milestone_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get_milestone failed")
            .body
            .milestone
            .into_option()
            .expect("milestone missing");

        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.title, "v1.0");
        assert_eq!(fetched.total_cards, 0);
        assert_eq!(fetched.completed_cards, 0);
        assert!(fetched.due.as_option().is_some());

        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn list_milestones_includes_card_stats() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let ms_a = create_milestone(&svc, &subject, project_id, "Alpha").await;
        let ms_b = create_milestone(&svc, &subject, project_id, "Beta").await;
        let ms_a_id = ms_a.id.parse::<Id>().unwrap();
        let ms_b_id = ms_b.id.parse::<Id>().unwrap();

        // Alpha: two cards, one completed. Beta: one open card.
        seed_card(&svc.pool, project_id, Some(ms_a_id), true).await;
        seed_card(&svc.pool, project_id, Some(ms_a_id), false).await;
        seed_card(&svc.pool, project_id, Some(ms_b_id), false).await;
        // Unrelated card without a milestone must not affect the stats.
        seed_card(&svc.pool, project_id, None, true).await;

        let list = svc
            .list_milestones(
                authed_ctx(&subject),
                connect_request(&ListMilestonesRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_milestones failed")
            .body;

        assert_eq!(list.milestones.len(), 2);
        let alpha = list
            .milestones
            .iter()
            .find(|m| m.id == ms_a.id)
            .expect("alpha missing");
        let beta = list
            .milestones
            .iter()
            .find(|m| m.id == ms_b.id)
            .expect("beta missing");
        assert_eq!(alpha.total_cards, 2);
        assert_eq!(alpha.completed_cards, 1);
        assert_eq!(beta.total_cards, 1);
        assert_eq!(beta.completed_cards, 0);

        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn update_milestone_applies_mask() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = svc
            .create_milestone(
                authed_ctx(&subject),
                connect_request(&CreateMilestoneRequest {
                    project_id: project_id.to_string(),
                    title: "Original".to_string(),
                    due: Some(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 0,
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_milestone failed")
            .body
            .milestone
            .into_option()
            .expect("milestone missing");

        // Title-only mask: due must survive even though the field is absent.
        let renamed = svc
            .update_milestone(
                authed_ctx(&subject),
                connect_request(&UpdateMilestoneRequest {
                    milestone_id: created.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["title".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    title: "Renamed".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_milestone failed")
            .body
            .milestone
            .into_option()
            .expect("milestone missing");
        assert_eq!(renamed.title, "Renamed");
        assert_eq!(
            renamed.due.as_option().expect("due should be kept").seconds,
            1_700_000_000
        );

        // Set due via the mask; title stays untouched.
        let re_dated = svc
            .update_milestone(
                authed_ctx(&subject),
                connect_request(&UpdateMilestoneRequest {
                    milestone_id: created.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["due".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    due: Some(Timestamp {
                        seconds: 1_900_000_000,
                        nanos: 0,
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_milestone failed")
            .body
            .milestone
            .into_option()
            .expect("milestone missing");
        assert_eq!(re_dated.title, "Renamed");
        assert_eq!(
            re_dated.due.as_option().expect("due should be set").seconds,
            1_900_000_000
        );

        // Naming "due" with the field absent clears it.
        let cleared = svc
            .update_milestone(
                authed_ctx(&subject),
                connect_request(&UpdateMilestoneRequest {
                    milestone_id: created.id.clone(),
                    update_mask: Some(FieldMask {
                        paths: vec!["due".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_milestone failed")
            .body
            .milestone
            .into_option()
            .expect("milestone missing");
        assert_eq!(cleared.title, "Renamed");
        assert!(
            cleared.due.as_option().is_none(),
            "due should be cleared when the mask names it and the field is absent"
        );

        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn delete_milestone_clears_cards() {
        let svc = make_service().await;
        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &subject).await;

        let created = create_milestone(&svc, &subject, project_id, "To Delete").await;
        let milestone_id = created.id.parse::<Id>().unwrap();
        let card_id = seed_card(&svc.pool, project_id, Some(milestone_id), false).await;

        svc.delete_milestone(
            authed_ctx(&subject),
            connect_request(&DeleteMilestoneRequest {
                milestone_id: created.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete_milestone failed");

        let err = svc
            .get_milestone(
                authed_ctx(&subject),
                connect_request(&GetMilestoneRequest {
                    milestone_id: created.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("deleted milestone should not be found");
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);

        // The referencing card survives with milestone_id cleared.
        let row = sqlx::query("SELECT milestone_id FROM cards WHERE id = $1 AND tenant_id = $2")
            .bind(card_id)
            .bind(crate::test_support::test_tenant_id())
            .fetch_one(&svc.pool)
            .await
            .expect("card should survive milestone deletion");
        let card_milestone: Option<String> = row.get("milestone_id");
        assert!(
            card_milestone.is_none(),
            "card milestone_id should be cleared on milestone delete"
        );

        cleanup_project(&svc.pool, project_id).await;
    }

    #[tokio::test]
    async fn unauthorized_subject_is_denied() {
        let svc = make_service().await;
        let owner = format!("user:test-{}", Id::new());
        let intruder = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&svc.pool, &svc.permission, &owner).await;

        // Read: no view grant on the project.
        let err = svc
            .list_milestones(
                authed_ctx(&intruder),
                connect_request(&ListMilestonesRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("list without view should be denied");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);

        // Write: no manage grant on the project.
        let err = svc
            .create_milestone(
                authed_ctx(&intruder),
                connect_request(&CreateMilestoneRequest {
                    project_id: project_id.to_string(),
                    title: "Nope".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("create without manage should be denied");
        assert_eq!(err.code, connectrpc::ErrorCode::PermissionDenied);

        cleanup_project(&svc.pool, project_id).await;
    }
}
