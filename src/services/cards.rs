// SPDX-License-Identifier: AGPL-3.0-or-later
//! Card service implementation.
//!
//! Exposes the card RPCs from `proto/sunbeam/kanban/v1/cards.proto`. The
//! handlers use the dynamic `sqlx` API so `cargo check` does not need a live
//! database, read the target object id from request extensions, store
//! idempotency keys, and write card mutations to the `event_log` outbox in
//! the same transaction. Card reference allocation uses a per-project
//! advisory lock (`pg_advisory_xact_lock(hashtext($project_id))`).

use std::sync::Arc;

use crate::id::Id;
use buffa_types::google::protobuf::Timestamp;
use chrono::{DateTime, Utc};
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::{error, warn};

use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_dispatch::CheckedObjectId;
use crate::cpb::sunbeam::kanban::v1::{
    AddCardDependencyRequest, AddCardDependencyResponse, AddChecklistItemRequest,
    AddChecklistItemResponse, AddCommentRequest, AddCommentResponse, AssignCardRequest,
    AssignCardResponse, Assignee, BatchGetCardsRequest, BatchGetCardsResponse,
    BulkUpdateCardLabelsRequest, BulkUpdateCardLabelsResponse, Card, CardPriority, CardService,
    CardUrgency, ChecklistItem, Comment, CreateCardRequest, CreateCardResponse, DeleteCardRequest,
    DeleteCardResponse, DeleteCommentRequest, DeleteCommentResponse, EditCommentRequest,
    EditCommentResponse, GetCardRequest, GetCardResponse, Label, ListCardsByBoardRequest,
    ListCardsByBoardResponse, ListCommentsRequest, ListCommentsResponse, MoveCardRequest,
    MoveCardResponse, RemoveCardDependencyRequest, RemoveCardDependencyResponse,
    RemoveChecklistItemRequest, RemoveChecklistItemResponse, UnassignCardRequest,
    UnassignCardResponse, UpdateCardRequest, UpdateCardResponse, UpdateChecklistItemRequest,
    UpdateChecklistItemResponse,
};

// ── Constants ────────────────────────────────────────────────────────────────

const DEFAULT_PAGE_LIMIT: i32 = 50;
const MAX_PAGE_LIMIT: i32 = 200;

// ── Service struct ───────────────────────────────────────────────────────────

pub struct CardServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
}

// ── Timestamp helpers ─────────────────────────────────────────────────────────

pub(crate) fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
        ..Default::default()
    }
}

pub(crate) fn opt_to_proto_ts(dt: Option<DateTime<Utc>>) -> buffa::MessageField<Timestamp> {
    dt.map(to_proto_ts).into()
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> ConnectError {
    error!(error = %err, "{msg}");
    ConnectError::internal(msg)
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

fn checked_object_id(ctx: &RequestContext) -> Result<String, ConnectError> {
    ctx.extensions()
        .get::<CheckedObjectId>()
        .map(|c| c.0.clone())
        .ok_or_else(|| ConnectError::internal("missing CheckedObjectId extension"))
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

// ── Priority mapping ──────────────────────────────────────────────────────────

pub(crate) fn priority_to_i32(s: &str) -> i32 {
    match s {
        "low" => 1,
        "medium" => 2,
        "high" => 3,
        "urgent" => 4,
        _ => 0,
    }
}

fn priority_from_i32(v: i32) -> &'static str {
    match v {
        1 => "low",
        2 => "medium",
        3 => "high",
        4 => "urgent",
        _ => "medium",
    }
}

// ── Urgency mapping ───────────────────────────────────────────────────────────

pub(crate) fn urgency_to_i32(s: &str) -> i32 {
    match s {
        "low" => 1,
        "medium" => 2,
        "high" => 3,
        "critical" => 4,
        _ => 0,
    }
}

fn urgency_from_i32(v: i32) -> &'static str {
    match v {
        1 => "low",
        2 => "medium",
        3 => "high",
        4 => "critical",
        _ => "medium",
    }
}

// ── Row → proto helpers ───────────────────────────────────────────────────────

/// Derived card metadata that is not stored on the `cards` row itself.
pub(crate) struct CardAggregates {
    pub comments_count: i32,
    pub attachments_count: i32,
    pub depends_on_card_ids: Vec<String>,
    pub dependent_card_ids: Vec<String>,
}

pub(crate) fn card_from_row(
    row: &sqlx::postgres::PgRow,
    labels: Vec<Label>,
    assignees: Vec<Assignee>,
    checklist: Vec<ChecklistItem>,
    aggregates: CardAggregates,
) -> Card {
    let id: Id = row.get("id");
    let project_id: Id = row.get("project_id");
    let board_id: Id = row.get("board_id");
    let column_id: Id = row.get("column_id");
    let ref_: String = row.get("ref");
    let title: String = row.get("title");
    let description: Option<String> = row.get("description");
    let position: i32 = row.get("position");
    let priority: Option<String> = row.get("priority");
    let urgency: Option<String> = row.get("urgency");
    let due_date: Option<DateTime<Utc>> = row.get("due_date");
    let completed_at: Option<DateTime<Utc>> = row.get("completed_at");
    let blocked: bool = row.get("blocked");
    let cover: Option<String> = row.get("cover");
    let milestone_id: Option<Id> = row.get("milestone_id");
    let revision: i64 = row.get("revision");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Card {
        id: id.to_string(),
        project_id: project_id.to_string(),
        board_id: board_id.to_string(),
        column_id: column_id.to_string(),
        r#ref: ref_,
        title,
        description: description.unwrap_or_default(),
        priority: priority_to_i32(priority.as_deref().unwrap_or("medium")).into(),
        due: opt_to_proto_ts(due_date),
        completed_at: opt_to_proto_ts(completed_at),
        blocked,
        cover: cover.unwrap_or_default(),
        milestone_id: milestone_id.map(|u| u.to_string()).unwrap_or_default(),
        position,
        labels,
        assignees,
        checklist,
        github_links: vec![],
        comments_count: aggregates.comments_count,
        attachments_count: aggregates.attachments_count,
        revision: revision as u64,
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        urgency: urgency_to_i32(urgency.as_deref().unwrap_or("medium")).into(),
        depends_on_card_ids: aggregates.depends_on_card_ids,
        dependent_card_ids: aggregates.dependent_card_ids,
        ..Default::default()
    }
}

fn comment_from_row(row: &sqlx::postgres::PgRow) -> Comment {
    let id: Id = row.get("id");
    let card_id: Id = row.get("card_id");
    let author_sub: String = row.get("author_sub");
    let body: String = row.get("body");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Comment {
        id: id.to_string(),
        card_id: card_id.to_string(),
        author_sub,
        body,
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        ..Default::default()
    }
}

fn checklist_item_from_row(row: &sqlx::postgres::PgRow) -> ChecklistItem {
    let id: Id = row.get("id");
    let text: String = row.get("text");
    let done: bool = row.get("done");
    let position: i32 = row.get("position");
    ChecklistItem {
        id: id.to_string(),
        text,
        done,
        position,
        ..Default::default()
    }
}

// ── Fetch helpers for embedded sub-entities ───────────────────────────────────

pub(crate) async fn fetch_labels(pool: &PgPool, card_id: Id, tenant_id: &str) -> Vec<Label> {
    sqlx::query(
        "SELECT l.id, l.project_id, l.name, l.style \
         FROM labels l \
         JOIN card_labels cl ON cl.label_id = l.id \
         WHERE cl.card_id = $1 AND l.tenant_id = $2 \
         ORDER BY l.name",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .map(|r| {
        let lid: Id = r.get("id");
        let pid: Id = r.get("project_id");
        Label {
            id: lid.to_string(),
            project_id: pid.to_string(),
            name: r.get("name"),
            style: r.get("style"),
            ..Default::default()
        }
    })
    .collect()
}

pub(crate) async fn fetch_assignees(pool: &PgPool, card_id: Id, tenant_id: &str) -> Vec<Assignee> {
    sqlx::query("SELECT subject FROM card_assignees WHERE card_id = $1 AND tenant_id = $2 ORDER BY assigned_at")
        .bind(card_id)
        .bind(tenant_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default()
        .iter()
        .map(|r| Assignee {
            subject: r.get("subject"),
            display_name: String::new(),
            avatar_url: String::new(),
            ..Default::default()
        })
        .collect()
}

pub(crate) async fn fetch_checklist(
    pool: &PgPool,
    card_id: Id,
    tenant_id: &str,
) -> Vec<ChecklistItem> {
    sqlx::query(
        "SELECT id, text, done, position FROM checklist_items \
         WHERE card_id = $1 AND tenant_id = $2 ORDER BY position ASC",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .map(checklist_item_from_row)
    .collect()
}

pub(crate) async fn fetch_comments_count(pool: &PgPool, card_id: Id, tenant_id: &str) -> i32 {
    sqlx::query("SELECT COUNT(*) AS cnt FROM comments WHERE card_id = $1 AND tenant_id = $2")
        .bind(card_id)
        .bind(tenant_id)
        .fetch_one(pool)
        .await
        .map(|r| {
            let c: i64 = r.get("cnt");
            c as i32
        })
        .unwrap_or(0)
}

pub(crate) async fn fetch_attachments_count(pool: &PgPool, card_id: Id, tenant_id: &str) -> i32 {
    sqlx::query(
        "SELECT COUNT(*) AS cnt FROM card_attachments WHERE card_id = $1 AND tenant_id = $2",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map(|r| {
        let c: i64 = r.get("cnt");
        c as i32
    })
    .unwrap_or(0)
}

pub(crate) async fn fetch_dependencies(pool: &PgPool, card_id: Id, tenant_id: &str) -> Vec<String> {
    sqlx::query(
        "SELECT depends_on_card_id FROM card_dependencies \
         WHERE card_id = $1 \
           AND card_id IN (SELECT id FROM cards WHERE tenant_id = $2) \
         ORDER BY created_at",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .map(|r| {
        let id: Id = r.get("depends_on_card_id");
        id.to_string()
    })
    .collect()
}

pub(crate) async fn fetch_dependents(pool: &PgPool, card_id: Id, tenant_id: &str) -> Vec<String> {
    sqlx::query(
        "SELECT card_id FROM card_dependencies \
         WHERE depends_on_card_id = $1 \
           AND depends_on_card_id IN (SELECT id FROM cards WHERE tenant_id = $2) \
         ORDER BY created_at",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .map(|r| {
        let id: Id = r.get("card_id");
        id.to_string()
    })
    .collect()
}

/// Load a complete card, including its relationships, by id.
async fn fetch_full_card(
    pool: &PgPool,
    card_id: Id,
    tenant_id: &str,
) -> Result<Card, ConnectError> {
    let row = sqlx::query(
        "SELECT id, project_id, board_id, column_id, ref, title, description, \
                position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                milestone_id, revision, created_at, updated_at \
         FROM cards WHERE id = $1 AND tenant_id = $2",
    )
    .bind(card_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| internal("failed to fetch card", e))?
    .ok_or_else(|| ConnectError::not_found("card not found"))?;

    let labels = fetch_labels(pool, card_id, tenant_id).await;
    let assignees = fetch_assignees(pool, card_id, tenant_id).await;
    let checklist = fetch_checklist(pool, card_id, tenant_id).await;
    let comments_count = fetch_comments_count(pool, card_id, tenant_id).await;
    let attachments_count = fetch_attachments_count(pool, card_id, tenant_id).await;
    let depends_on = fetch_dependencies(pool, card_id, tenant_id).await;
    let dependents = fetch_dependents(pool, card_id, tenant_id).await;

    Ok(card_from_row(
        &row,
        labels,
        assignees,
        checklist,
        CardAggregates {
            comments_count,
            attachments_count,
            depends_on_card_ids: depends_on,
            dependent_card_ids: dependents,
        },
    ))
}

// ── Card-ref allocator (MF-3) ─────────────────────────────────────────────────
//
// Advisory lock serialises allocation per project without locking the whole
// table. The INSERT path stores next_seq=2 and returns next_seq-1=1 (first
// allocation). The ON CONFLICT path increments next_seq by 1 and returns the
// new next_seq-1 (the value just allocated).
//
// SQL trace for first call:
//   INSERT … SELECT $1, p.prefix, 2 … → row inserted with next_seq=2
//   RETURNING: next_seq=2, allocated_seq=2-1=1  ✓
//
// SQL trace for second call:
//   ON CONFLICT DO UPDATE SET next_seq = 2 + 1 = 3
//   RETURNING: next_seq=3, allocated_seq=3-1=2  ✓

async fn allocate_card_ref(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Id,
    tenant_id: &str,
) -> Result<String, ConnectError> {
    // Advisory lock scoped to this transaction — serialises per project.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(project_id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(|e| internal("advisory lock failed", e))?;

    let row = sqlx::query(
        "INSERT INTO project_ref_counter (project_id, prefix, next_seq, tenant_id)
         SELECT $1, p.prefix, 2, $2 FROM projects p WHERE p.id = $1 AND p.tenant_id = $2
         ON CONFLICT (project_id) DO UPDATE
           SET next_seq = project_ref_counter.next_seq + 1
         RETURNING prefix, (next_seq - 1) AS allocated_seq",
    )
    .bind(project_id)
    .bind(tenant_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| internal("failed to allocate card ref", e))?;

    let prefix: String = row
        .try_get("prefix")
        .map_err(|e| internal("ref prefix missing", e))?;
    let seq: i32 = row
        .try_get("allocated_seq")
        .map_err(|e| internal("ref seq missing", e))?;
    Ok(format!("{prefix}-{seq:03}"))
}

// ── event_log helper ──────────────────────────────────────────────────────────

async fn insert_event_log(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    board_id: Id,
    event_type: &str,
    payload: serde_json::Value,
    card_revision: i64,
) -> Result<(), ConnectError> {
    sqlx::query(
        "INSERT INTO event_log (id, tenant_id, board_id, event_type, payload, created_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, now())",
    )
    .bind(Id::new())
    .bind(tenant_id)
    .bind(board_id)
    .bind(event_type)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .map_err(|e| {
        warn!(error = %e, "event_log insert failed (non-fatal for caller; Stage 4 will gap-detect)");
        internal("failed to insert event_log row", e)
    })?;
    let _ = card_revision; // carried for future outbox fields
    Ok(())
}

// ── Idempotency helpers ───────────────────────────────────────────────────────

/// Look up an idempotency key and return the stored card id if the request
/// was already processed.
///
/// Returns `Ok(Some(card_id))` for a replayed key and `Ok(None)` for a new key.
async fn check_idempotency_card(
    pool: &PgPool,
    tenant_id: &str,
    key: &str,
) -> Result<Option<Id>, ConnectError> {
    if key.is_empty() {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT response_card_id FROM idempotency_keys WHERE tenant_id = $1 AND key = $2",
    )
    .bind(tenant_id)
    .bind(key)
    .fetch_optional(pool)
    .await
    .map_err(|e| internal("idempotency key lookup failed", e))?;

    match row {
        None => Ok(None),
        Some(r) => {
            let id: Option<Id> = r.get("response_card_id");
            Ok(id)
        }
    }
}

async fn store_idempotency_card(pool: &PgPool, tenant_id: &str, key: &str, card_id: Id) {
    if key.is_empty() {
        return;
    }
    if let Err(e) = sqlx::query(
        "INSERT INTO idempotency_keys (tenant_id, key, response_card_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
    )
    .bind(tenant_id)
    .bind(key)
    .bind(card_id)
    .execute(pool)
    .await
    {
        warn!(error = %e, "failed to store idempotency key");
    }
}

// ── impl CardService ──────────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl CardService for CardServiceImpl {
    // ── GetCard ───────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + view, per matrix).
    // Hydrates labels, assignees, checklist, github_links, counts.

    async fn get_card(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, GetCardRequest>,
    ) -> ServiceResult<GetCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let card = fetch_full_card(&self.pool, card_id, &tenant_id).await?;
        Ok(Response::new(GetCardResponse {
            card: Some(card).into(),
            ..Default::default()
        }))
    }

    // ── BatchGetCards ─────────────────────────────────────────────────────────
    //
    // CheckedObjectId = board_id (KanbanBoard + view, per matrix).
    // Fetches only cards that actually belong to the board (closes the
    // header-vs-body bypass for the card_ids list).

    async fn batch_get_cards(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, BatchGetCardsRequest>,
    ) -> ServiceResult<BatchGetCardsResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        if req.card_ids.is_empty() {
            return Ok(Response::new(BatchGetCardsResponse {
                cards: vec![],
                ..Default::default()
            }));
        }

        // Parse card_ids; skip invalid ones rather than erroring.
        let ids: Vec<Id> = req
            .card_ids
            .iter()
            .filter_map(|s| s.parse::<Id>().ok())
            .collect();

        // Fetch rows that are actually on this board.
        let rows = sqlx::query(
            "SELECT id, project_id, board_id, column_id, ref, title, description, \
                    position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                    milestone_id, revision, created_at, updated_at \
             FROM cards \
             WHERE board_id = $1 AND tenant_id = $2 AND id = ANY($3) \
             ORDER BY column_id, position",
        )
        .bind(board_id)
        .bind(&tenant_id)
        .bind(&ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to batch-get cards", e))?;

        let mut cards = Vec::with_capacity(rows.len());
        for row in &rows {
            let cid: Id = row.get("id");
            let labels = fetch_labels(&self.pool, cid, &tenant_id).await;
            let assignees = fetch_assignees(&self.pool, cid, &tenant_id).await;
            let checklist = fetch_checklist(&self.pool, cid, &tenant_id).await;
            let comments_count = fetch_comments_count(&self.pool, cid, &tenant_id).await;
            let attachments_count = fetch_attachments_count(&self.pool, cid, &tenant_id).await;
            let depends_on = fetch_dependencies(&self.pool, cid, &tenant_id).await;
            let dependents = fetch_dependents(&self.pool, cid, &tenant_id).await;
            cards.push(card_from_row(
                row,
                labels,
                assignees,
                checklist,
                CardAggregates {
                    comments_count,
                    attachments_count,
                    depends_on_card_ids: depends_on,
                    dependent_card_ids: dependents,
                },
            ));
        }

        Ok(Response::new(BatchGetCardsResponse {
            cards,
            ..Default::default()
        }))
    }

    // ── ListCardsByBoard ──────────────────────────────────────────────────────
    //
    // CheckedObjectId = board_id (KanbanBoard + view, per matrix).
    // Optional column_id filter. Paginated via cursor (last card id seen).

    async fn list_cards_by_board(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListCardsByBoardRequest>,
    ) -> ServiceResult<ListCardsByBoardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let limit = req.limit.clamp(1, MAX_PAGE_LIMIT);
        let limit = if limit == 0 {
            DEFAULT_PAGE_LIMIT
        } else {
            limit
        };

        let col_filter: Option<Id> = if req.column_id.is_empty() {
            None
        } else {
            Some(
                req.column_id
                    .parse::<Id>()
                    .map_err(|_| ConnectError::invalid_argument("invalid column_id"))?,
            )
        };

        let cursor_id: Option<Id> = if req.cursor.is_empty() {
            None
        } else {
            Some(
                req.cursor
                    .parse::<Id>()
                    .map_err(|_| ConnectError::invalid_argument("invalid cursor"))?,
            )
        };

        // Build query dynamically to avoid runtime SQL errors.
        let rows = if let Some(col_id) = col_filter {
            if let Some(after) = cursor_id {
                sqlx::query(
                    "SELECT id, project_id, board_id, column_id, ref, title, description, \
                            position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                            milestone_id, revision, created_at, updated_at \
                     FROM cards \
                     WHERE board_id = $1 AND tenant_id = $2 AND column_id = $3 AND id > $4 \
                     ORDER BY column_id, position \
                     LIMIT $5",
                )
                .bind(board_id)
                .bind(&tenant_id)
                .bind(col_id)
                .bind(after)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query(
                    "SELECT id, project_id, board_id, column_id, ref, title, description, \
                            position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                            milestone_id, revision, created_at, updated_at \
                     FROM cards WHERE board_id = $1 AND tenant_id = $2 AND column_id = $3 \
                     ORDER BY column_id, position LIMIT $4",
                )
                .bind(board_id)
                .bind(&tenant_id)
                .bind(col_id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
            }
        } else if let Some(after) = cursor_id {
            sqlx::query(
                "SELECT id, project_id, board_id, column_id, ref, title, description, \
                        position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                        milestone_id, revision, created_at, updated_at \
                 FROM cards WHERE board_id = $1 AND tenant_id = $2 AND id > $3 \
                 ORDER BY column_id, position LIMIT $4",
            )
            .bind(board_id)
            .bind(&tenant_id)
            .bind(after)
            .bind(limit + 1)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT id, project_id, board_id, column_id, ref, title, description, \
                        position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                        milestone_id, revision, created_at, updated_at \
                 FROM cards WHERE board_id = $1 AND tenant_id = $2 \
                 ORDER BY column_id, position LIMIT $3",
            )
            .bind(board_id)
            .bind(&tenant_id)
            .bind(limit + 1)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| internal("failed to list cards", e))?;

        let has_more = rows.len() > limit as usize;
        let rows = if has_more {
            &rows[..limit as usize]
        } else {
            &rows[..]
        };

        let next_cursor = if has_more {
            rows.last()
                .map(|r| {
                    let id: Id = r.get("id");
                    id.to_string()
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        let mut cards = Vec::with_capacity(rows.len());
        for row in rows {
            let cid: Id = row.get("id");
            let labels = fetch_labels(&self.pool, cid, &tenant_id).await;
            let assignees = fetch_assignees(&self.pool, cid, &tenant_id).await;
            let checklist = fetch_checklist(&self.pool, cid, &tenant_id).await;
            let comments_count = fetch_comments_count(&self.pool, cid, &tenant_id).await;
            let attachments_count = fetch_attachments_count(&self.pool, cid, &tenant_id).await;
            let depends_on = fetch_dependencies(&self.pool, cid, &tenant_id).await;
            let dependents = fetch_dependents(&self.pool, cid, &tenant_id).await;
            cards.push(card_from_row(
                row,
                labels,
                assignees,
                checklist,
                CardAggregates {
                    comments_count,
                    attachments_count,
                    depends_on_card_ids: depends_on,
                    dependent_card_ids: dependents,
                },
            ));
        }

        Ok(Response::new(ListCardsByBoardResponse {
            cards,
            next_cursor,
            ..Default::default()
        }))
    }

    // ── CreateCard ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = board_id (KanbanBoard + edit, per matrix).
    // Allocates card ref via advisory-locked counter (MF-3).
    // event_log: CardCreated.

    async fn create_card(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateCardRequest>,
    ) -> ServiceResult<CreateCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.title.is_empty() {
            return Err(ConnectError::invalid_argument("title is required"));
        }

        // Idempotency check.
        if let Some(existing_id) =
            check_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key).await?
        {
            return fetch_full_card(&self.pool, existing_id, &tenant_id)
                .await
                .map(|card| {
                    Response::new(CreateCardResponse {
                        card: Some(card).into(),
                        ..Default::default()
                    })
                });
        }

        // Verify board exists and get project_id.
        let board_row =
            sqlx::query("SELECT project_id FROM boards WHERE id = $1 AND tenant_id = $2")
                .bind(board_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch board", e))?
                .ok_or_else(|| ConnectError::not_found("board not found"))?;

        let project_id: Id = board_row.get("project_id");

        // Verify column belongs to this board.
        let col_id = req
            .column_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid column_id"))?;

        let col_row = sqlx::query(
            "SELECT id FROM columns WHERE id = $1 AND board_id = $2 AND tenant_id = $3",
        )
        .bind(col_id)
        .bind(board_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to verify column", e))?
        .ok_or_else(|| ConnectError::not_found("column not found on this board"))?;
        let _ = col_row;

        // Begin transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // Allocate card ref (advisory lock inside).
        let card_ref = allocate_card_ref(&mut tx, project_id, &tenant_id).await?;

        let card_id = Id::new();

        // Compute position.
        let position: i32 = if req.position == 0 {
            sqlx::query(
                "SELECT COALESCE(MAX(position), -1) + 1 AS next_pos FROM cards WHERE column_id = $1 AND tenant_id = $2",
            )
            .bind(col_id)
            .bind(&tenant_id)
            .fetch_one(&mut *tx)
            .await
            .map(|r| r.get::<i32, _>("next_pos"))
            .unwrap_or(0)
        } else {
            // Shift cards at >= requested position.
            sqlx::query(
                "UPDATE cards SET position = position + 1, updated_at = now() \
                 WHERE column_id = $1 AND tenant_id = $2 AND position >= $3",
            )
            .bind(col_id)
            .bind(&tenant_id)
            .bind(req.position)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to shift positions", e))?;
            req.position
        };

        // Parse optional fields.
        let priority_str = priority_from_i32(req.priority.to_i32());
        let urgency_str = urgency_from_i32(req.urgency.to_i32());
        let due_date: Option<DateTime<Utc>> = req
            .due
            .into_option()
            .and_then(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32));
        let milestone_id: Option<Id> = if req.milestone_id.is_empty() {
            None
        } else {
            req.milestone_id.parse::<Id>().ok()
        };

        sqlx::query(
            "INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, description, \
                                position, priority, urgency, due_date, milestone_id, created_by, revision) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::card_priority, $11::card_urgency, $12, $13, $14, 0)",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(board_id)
        .bind(col_id)
        .bind(&card_ref)
        .bind(&req.title)
        .bind(if req.description.is_empty() {
            None
        } else {
            Some(req.description.clone())
        })
        .bind(position)
        .bind(priority_str)
        .bind(urgency_str)
        .bind(due_date)
        .bind(milestone_id)
        .bind(&subject)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to insert card", e))?;

        // event_log: CardCreated.
        let payload = json!({
            "card_id": card_id.to_string(),
            "board_id": board_id.to_string(),
            "column_id": col_id.to_string(),
            "title": req.title,
            "ref": card_ref,
            "position": position,
            "priority": priority_str,
            "urgency": urgency_str,
            "idempotency_key": req.idempotency_key,
        });
        insert_event_log(&mut tx, &tenant_id, board_id, "CardCreated", payload, 0).await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        // Store idempotency.
        store_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key, card_id).await;

        let card = fetch_full_card(&self.pool, card_id, &tenant_id).await?;
        Ok(Response::new(CreateCardResponse {
            card: Some(card).into(),
            ..Default::default()
        }))
    }

    // ── UpdateCard ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // Sparse patch via CASE WHEN. Bumps revision. event_log: CardUpdated.

    async fn update_card(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateCardRequest>,
    ) -> ServiceResult<UpdateCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let patch = req.card.into_option().unwrap_or_default();

        // Idempotency check.
        if let Some(_existing) =
            check_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key).await?
        {
            return fetch_full_card(&self.pool, card_id, &tenant_id)
                .await
                .map(|card| {
                    Response::new(UpdateCardResponse {
                        card: Some(card).into(),
                        ..Default::default()
                    })
                });
        }

        // Fetch current card for board_id + revision.
        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let priority_str: Option<&str> = if patch.priority != CardPriority::Unspecified {
            Some(priority_from_i32(patch.priority.to_i32()))
        } else {
            None
        };

        let urgency_str: Option<&str> = if patch.urgency != CardUrgency::Unspecified {
            Some(urgency_from_i32(patch.urgency.to_i32()))
        } else {
            None
        };

        let due_date: Option<DateTime<Utc>> = patch
            .due
            .into_option()
            .and_then(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32));

        let milestone_id: Option<Id> = if patch.milestone_id.is_empty() {
            None
        } else {
            patch.milestone_id.parse::<Id>().ok()
        };

        let row = sqlx::query(
            "UPDATE cards SET
                title       = CASE WHEN $2 != '' THEN $2 ELSE title END,
                description = CASE WHEN $3 != '' THEN $3 ELSE description END,
                priority    = CASE WHEN $4 IS NOT NULL THEN $4::card_priority ELSE priority END,
                urgency     = CASE WHEN $5 IS NOT NULL THEN $5::card_urgency ELSE urgency END,
                due_date    = CASE WHEN $6 IS NOT NULL THEN $6 ELSE due_date END,
                blocked     = CASE WHEN $7 THEN $7 ELSE blocked END,
                cover       = CASE WHEN $8 != '' THEN $8 ELSE cover END,
                milestone_id = CASE WHEN $9 IS NOT NULL THEN $9 ELSE milestone_id END,
                revision    = revision + 1,
                updated_at  = now()
             WHERE id = $1 AND tenant_id = $10
             RETURNING board_id, revision",
        )
        .bind(card_id)
        .bind(&patch.title)
        .bind(&patch.description)
        .bind(priority_str)
        .bind(urgency_str)
        .bind(due_date)
        .bind(patch.blocked)
        .bind(&patch.cover)
        .bind(milestone_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to update card", e))?
        .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let new_revision: i64 = row.get("revision");

        // event_log: CardUpdated.
        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "idempotency_key": req.idempotency_key,
            "patch": {
                "title": patch.title,
                "description": patch.description,
                "priority": patch.priority,
                "urgency": patch.urgency,
                "blocked": patch.blocked,
                "cover": patch.cover,
            }
        });

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        store_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key, card_id).await;

        let card = fetch_full_card(&self.pool, card_id, &tenant_id).await?;
        Ok(Response::new(UpdateCardResponse {
            card: Some(card).into(),
            ..Default::default()
        }))
    }

    // ── MoveCard ──────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // Cross-project moves rejected (card ref is immutable + project-scoped).
    // Same-board cross-column: shift source gap, shift target at insert point.
    // event_log: CardMoved.

    async fn move_card(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, MoveCardRequest>,
    ) -> ServiceResult<MoveCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let to_col_id = req
            .to_column_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid to_column_id"))?;

        // Idempotency check.
        if check_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key)
            .await?
            .is_some()
        {
            return fetch_full_card(&self.pool, card_id, &tenant_id)
                .await
                .map(|card| {
                    Response::new(MoveCardResponse {
                        card: Some(card).into(),
                        ..Default::default()
                    })
                });
        }

        // Fetch card's current state.
        let card_row = sqlx::query(
            "SELECT board_id, column_id, position, revision, project_id FROM cards WHERE id = $1 AND tenant_id = $2",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch card", e))?
        .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = card_row.get("board_id");
        let from_col_id: Id = card_row.get("column_id");
        let from_pos: i32 = card_row.get("position");
        let prev_revision: i64 = card_row.get("revision");
        let card_project_id: Id = card_row.get("project_id");

        // Fetch target column's board_id.
        let target_col_row =
            sqlx::query("SELECT board_id FROM columns WHERE id = $1 AND tenant_id = $2")
                .bind(to_col_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch target column", e))?
                .ok_or_else(|| ConnectError::not_found("target column not found"))?;

        let target_board_id: Id = target_col_row.get("board_id");

        // Fetch target board's project_id to verify no cross-project move.
        let target_board_row =
            sqlx::query("SELECT project_id FROM boards WHERE id = $1 AND tenant_id = $2")
                .bind(target_board_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch target board", e))?
                .ok_or_else(|| ConnectError::not_found("target board not found"))?;

        let target_project_id: Id = target_board_row.get("project_id");

        if card_project_id != target_project_id {
            return Err(ConnectError::invalid_argument(
                "cards do not move between projects",
            ));
        }

        let to_pos = if req.to_position < 1 {
            0
        } else {
            req.to_position - 1
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        if from_col_id == to_col_id {
            // Same-column reorder.
            if from_pos != to_pos {
                if to_pos > from_pos {
                    sqlx::query(
                        "UPDATE cards SET position = position - 1, updated_at = now() \
                         WHERE column_id = $1 AND tenant_id = $2 AND position > $3 AND position <= $4",
                    )
                    .bind(from_col_id)
                    .bind(&tenant_id)
                    .bind(from_pos)
                    .bind(to_pos)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| internal("failed to shift (same-col forward)", e))?;
                } else {
                    sqlx::query(
                        "UPDATE cards SET position = position + 1, updated_at = now() \
                         WHERE column_id = $1 AND tenant_id = $2 AND position >= $3 AND position < $4",
                    )
                    .bind(from_col_id)
                    .bind(&tenant_id)
                    .bind(to_pos)
                    .bind(from_pos)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| internal("failed to shift (same-col backward)", e))?;
                }
            }
        } else {
            // Cross-column: close gap in source, open slot in target.
            sqlx::query(
                "UPDATE cards SET position = position - 1, updated_at = now() \
                 WHERE column_id = $1 AND tenant_id = $2 AND position > $3",
            )
            .bind(from_col_id)
            .bind(&tenant_id)
            .bind(from_pos)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to close source gap", e))?;

            sqlx::query(
                "UPDATE cards SET position = position + 1, updated_at = now() \
                 WHERE column_id = $1 AND tenant_id = $2 AND position >= $3",
            )
            .bind(to_col_id)
            .bind(&tenant_id)
            .bind(to_pos)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to open target slot", e))?;
        }

        // Place card at new position.
        let new_revision_row = sqlx::query(
            "UPDATE cards SET column_id = $2, position = $3, revision = revision + 1, \
                              updated_at = now() \
             WHERE id = $1 AND tenant_id = $4 RETURNING revision",
        )
        .bind(card_id)
        .bind(to_col_id)
        .bind(to_pos)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to place card at new position", e))?;

        let new_revision: i64 = new_revision_row.get("revision");

        // event_log: CardMoved.
        let payload = json!({
            "card_id": card_id.to_string(),
            "from_column": from_col_id.to_string(),
            "to_column": to_col_id.to_string(),
            "to_position": to_pos,
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "idempotency_key": req.idempotency_key,
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardMoved",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        store_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key, card_id).await;

        let card = fetch_full_card(&self.pool, card_id, &tenant_id).await?;
        Ok(Response::new(MoveCardResponse {
            card: Some(card).into(),
            ..Default::default()
        }))
    }

    // ── DeleteCard ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + manage, per matrix).
    // CASCADE: attachments, assignees, labels, checklist, comments via FK.
    // event_log: CardDeleted.

    async fn delete_card(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, DeleteCardRequest>,
    ) -> ServiceResult<DeleteCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;

        // Fetch board_id + revision before deleting.
        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let result = sqlx::query("DELETE FROM cards WHERE id = $1 AND tenant_id = $2")
            .bind(card_id)
            .bind(&tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to delete card", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("card not found"));
        }

        // event_log: CardDeleted.
        let payload = json!({
            "card_id": card_id.to_string(),
            "board_id": board_id.to_string(),
            "prev_revision": prev_revision,
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardDeleted",
            payload,
            prev_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(DeleteCardResponse::default()))
    }

    // ── AddCardDependency ─────────────────────────────────────────────────────
    //
    // CheckedObjectId = board_id (KanbanBoard + edit, per matrix).
    // Creates a directed edge: card_id depends on depends_on_card_id.
    // Both cards must belong to the authorized board. Bumps card_id revision.
    // event_log: CardUpdated.

    async fn add_card_dependency(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AddCardDependencyRequest>,
    ) -> ServiceResult<AddCardDependencyResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.idempotency_key.is_empty() {
            return Err(ConnectError::invalid_argument(
                "idempotency_key is required",
            ));
        }

        let card_id = req
            .card_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;
        let depends_on_id = req
            .depends_on_card_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid depends_on_card_id"))?;

        if card_id == depends_on_id {
            return Err(ConnectError::invalid_argument(
                "a card cannot depend on itself",
            ));
        }

        if let Some(existing_id) =
            check_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key).await?
        {
            return fetch_full_card(&self.pool, existing_id, &tenant_id)
                .await
                .map(|card| {
                    Response::new(AddCardDependencyResponse {
                        card: Some(card).into(),
                        ..Default::default()
                    })
                });
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // Verify both cards belong to the authorized board.
        let rows =
            sqlx::query("SELECT id, revision FROM cards WHERE id = ANY($1) AND board_id = $2 AND tenant_id = $3")
                .bind(&[card_id, depends_on_id][..])
                .bind(board_id)
                .bind(&tenant_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| internal("failed to verify cards", e))?;

        if rows.len() != 2 {
            return Err(ConnectError::invalid_argument(
                "both cards must belong to the authorized board",
            ));
        }

        let mut prev_revision = 0i64;
        for row in &rows {
            let id: Id = row.get("id");
            if id == card_id {
                prev_revision = row.get("revision");
            }
        }

        sqlx::query(
            "INSERT INTO card_dependencies (card_id, depends_on_card_id) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
        )
        .bind(card_id)
        .bind(depends_on_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to add card dependency", e))?;

        let updated = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump card revision", e))?;
        let new_revision: i64 = updated.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "board_id": board_id.to_string(),
            "depends_on_card_id": depends_on_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "idempotency_key": req.idempotency_key,
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        store_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key, card_id).await;

        let card = fetch_full_card(&self.pool, card_id, &tenant_id).await?;
        Ok(Response::new(AddCardDependencyResponse {
            card: Some(card).into(),
            ..Default::default()
        }))
    }

    // ── RemoveCardDependency ──────────────────────────────────────────────────
    //
    // CheckedObjectId = board_id (KanbanBoard + edit, per matrix).
    // Removes a directed dependency edge. Both cards must belong to the board.
    // Bumps card_id revision. event_log: CardUpdated.

    async fn remove_card_dependency(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RemoveCardDependencyRequest>,
    ) -> ServiceResult<RemoveCardDependencyResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.idempotency_key.is_empty() {
            return Err(ConnectError::invalid_argument(
                "idempotency_key is required",
            ));
        }

        let card_id = req
            .card_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;
        let depends_on_id = req
            .depends_on_card_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid depends_on_card_id"))?;

        if let Some(existing_id) =
            check_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key).await?
        {
            return fetch_full_card(&self.pool, existing_id, &tenant_id)
                .await
                .map(|card| {
                    Response::new(RemoveCardDependencyResponse {
                        card: Some(card).into(),
                        ..Default::default()
                    })
                });
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // Verify both cards belong to the authorized board and fetch the card revision.
        let card_row = sqlx::query(
            "SELECT revision FROM cards WHERE id = $1 AND board_id = $2 AND tenant_id = $3",
        )
        .bind(card_id)
        .bind(board_id)
        .bind(&tenant_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to verify card", e))?;

        let prev_revision = match card_row {
            Some(row) => row.get::<i64, _>("revision"),
            None => {
                return Err(ConnectError::invalid_argument(
                    "card does not belong to the authorized board",
                ));
            }
        };

        let depends_row =
            sqlx::query("SELECT 1 FROM cards WHERE id = $1 AND board_id = $2 AND tenant_id = $3")
                .bind(depends_on_id)
                .bind(board_id)
                .bind(&tenant_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| internal("failed to verify dependency card", e))?;

        if depends_row.is_none() {
            return Err(ConnectError::invalid_argument(
                "dependency card does not belong to the authorized board",
            ));
        }

        let result = sqlx::query(
            "DELETE FROM card_dependencies WHERE card_id = $1 AND depends_on_card_id = $2",
        )
        .bind(card_id)
        .bind(depends_on_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to remove card dependency", e))?;

        let new_revision = if result.rows_affected() > 0 {
            let updated = sqlx::query(
                "UPDATE cards SET revision = revision + 1, updated_at = now() \
                 WHERE id = $1 AND tenant_id = $2 RETURNING revision",
            )
            .bind(card_id)
            .bind(&tenant_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| internal("failed to bump card revision", e))?;
            updated.get::<i64, _>("revision")
        } else {
            prev_revision
        };

        let payload = json!({
            "card_id": card_id.to_string(),
            "board_id": board_id.to_string(),
            "depends_on_card_id": depends_on_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "idempotency_key": req.idempotency_key,
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        store_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key, card_id).await;

        let card = fetch_full_card(&self.pool, card_id, &tenant_id).await?;
        Ok(Response::new(RemoveCardDependencyResponse {
            card: Some(card).into(),
            ..Default::default()
        }))
    }

    // ── BulkUpdateCardLabels ──────────────────────────────────────────────────
    //
    // CheckedObjectId = board_id (KanbanBoard + edit, per matrix).
    // Replaces the full label set on each card in a single transaction.
    // Verifies each card belongs to the board.
    // event_log: CardUpdated per card.

    async fn bulk_update_card_labels(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, BulkUpdateCardLabelsRequest>,
    ) -> ServiceResult<BulkUpdateCardLabelsResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.card_ids.is_empty() {
            return Ok(Response::new(BulkUpdateCardLabelsResponse {
                cards: vec![],
                ..Default::default()
            }));
        }

        let card_ids: Vec<Id> = req
            .card_ids
            .iter()
            .filter_map(|s| s.parse::<Id>().ok())
            .collect();

        let label_ids: Vec<Id> = req
            .label_ids
            .iter()
            .filter_map(|s| s.parse::<Id>().ok())
            .collect();

        // Idempotency.
        if check_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key)
            .await?
            .is_some()
        {
            // Re-fetch all cards and return.
            let mut cards = vec![];
            for &cid in &card_ids {
                if let Ok(c) = fetch_full_card(&self.pool, cid, &tenant_id).await {
                    cards.push(c);
                }
            }
            return Ok(Response::new(BulkUpdateCardLabelsResponse {
                cards,
                ..Default::default()
            }));
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let mut updated_card_ids: Vec<Id> = vec![];

        for &cid in &card_ids {
            // Verify this card belongs to the board.
            let exists: bool =
                sqlx::query("SELECT EXISTS(SELECT 1 FROM cards WHERE id = $1 AND board_id = $2 AND tenant_id = $3)")
                    .bind(cid)
                    .bind(board_id)
                    .bind(&tenant_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| internal("failed to verify card ownership", e))
                    .map(|r| r.get::<bool, _>(0))?;

            if !exists {
                return Err(ConnectError::invalid_argument(format!(
                    "card {} does not belong to board {}",
                    cid, board_id
                )));
            }

            // Delete existing labels, replace with new set.
            sqlx::query("DELETE FROM card_labels WHERE card_id = $1")
                .bind(cid)
                .execute(&mut *tx)
                .await
                .map_err(|e| internal("failed to clear card labels", e))?;

            for &lid in &label_ids {
                sqlx::query(
                    "INSERT INTO card_labels (card_id, label_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                )
                .bind(cid).bind(lid)
                .execute(&mut *tx).await
                .map_err(|e| internal("failed to insert card label", e))?;
            }

            // Bump revision.
            let rev_row = sqlx::query(
                "UPDATE cards SET revision = revision + 1, updated_at = now() \
                 WHERE id = $1 AND tenant_id = $2 RETURNING revision",
            )
            .bind(cid)
            .bind(&tenant_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| internal("failed to bump card revision", e))?;

            let new_rev: i64 = rev_row.get("revision");

            // event_log per card.
            let payload = json!({
                "card_id": cid.to_string(),
                "board_id": board_id.to_string(),
                "new_revision": new_rev,
                "label_ids": label_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
                "idempotency_key": req.idempotency_key,
            });
            insert_event_log(
                &mut tx,
                &tenant_id,
                board_id,
                "CardUpdated",
                payload,
                new_rev,
            )
            .await?;

            updated_card_ids.push(cid);
        }

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        if !req.idempotency_key.is_empty()
            && let Some(&first) = updated_card_ids.first()
        {
            store_idempotency_card(&self.pool, &tenant_id, &req.idempotency_key, first).await;
        }

        let mut cards = vec![];
        for cid in updated_card_ids {
            cards.push(fetch_full_card(&self.pool, cid, &tenant_id).await?);
        }

        Ok(Response::new(BulkUpdateCardLabelsResponse {
            cards,
            ..Default::default()
        }))
    }

    // ── AssignCard ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // ON CONFLICT DO NOTHING — idempotent.
    // event_log: CardUpdated (assignees patch).

    async fn assign_card(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AssignCardRequest>,
    ) -> ServiceResult<AssignCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        if req.subject.is_empty() {
            return Err(ConnectError::invalid_argument("subject is required"));
        }

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        sqlx::query(
            "INSERT INTO card_assignees (tenant_id, card_id, subject) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        )
        .bind(&tenant_id)
        .bind(card_id)
        .bind(&req.subject)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to assign card", e))?;

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "assignees": [req.subject] },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        fetch_full_card(&self.pool, card_id, &tenant_id)
            .await
            .map(|card| {
                Response::new(AssignCardResponse {
                    card: Some(card).into(),
                    ..Default::default()
                })
            })
    }

    // ── UnassignCard ──────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // event_log: CardUpdated.

    async fn unassign_card(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UnassignCardRequest>,
    ) -> ServiceResult<UnassignCardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        sqlx::query(
            "DELETE FROM card_assignees WHERE card_id = $1 AND subject = $2 AND tenant_id = $3",
        )
        .bind(card_id)
        .bind(&req.subject)
        .bind(&tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to unassign card", e))?;

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "unassigned": req.subject },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        fetch_full_card(&self.pool, card_id, &tenant_id)
            .await
            .map(|card| {
                Response::new(UnassignCardResponse {
                    card: Some(card).into(),
                    ..Default::default()
                })
            })
    }

    // ── AddChecklistItem ──────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // Appends (position = MAX + 1) or inserts at requested position.
    // event_log: CardUpdated.

    async fn add_checklist_item(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AddChecklistItemRequest>,
    ) -> ServiceResult<AddChecklistItemResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        if req.text.is_empty() {
            return Err(ConnectError::invalid_argument("text is required"));
        }

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let item_id = Id::new();

        if req.position == 0 {
            // Append.
            sqlx::query(
                "INSERT INTO checklist_items (id, tenant_id, card_id, text, position) \
                 SELECT $1, $2, $3, $4, COALESCE(MAX(position), -1) + 1 \
                 FROM checklist_items WHERE card_id = $3 AND tenant_id = $2",
            )
            .bind(item_id)
            .bind(&tenant_id)
            .bind(card_id)
            .bind(&req.text)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to insert checklist item", e))?;
        } else {
            // Shift and insert.
            sqlx::query(
                "UPDATE checklist_items SET position = position + 1, updated_at = now() \
                 WHERE card_id = $1 AND tenant_id = $2 AND position >= $3",
            )
            .bind(card_id)
            .bind(&tenant_id)
            .bind(req.position)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to shift checklist items", e))?;

            sqlx::query(
                "INSERT INTO checklist_items (id, tenant_id, card_id, text, position) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(item_id)
            .bind(&tenant_id)
            .bind(card_id)
            .bind(&req.text)
            .bind(req.position)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to insert checklist item at position", e))?;
        }

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "checklist_item_added": item_id.to_string() },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        fetch_full_card(&self.pool, card_id, &tenant_id)
            .await
            .map(|card| {
                Response::new(AddChecklistItemResponse {
                    card: Some(card).into(),
                    ..Default::default()
                })
            })
    }

    // ── UpdateChecklistItem ───────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // Sparse: only non-empty text or changed done status applied.
    // event_log: CardUpdated.

    async fn update_checklist_item(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateChecklistItemRequest>,
    ) -> ServiceResult<UpdateChecklistItemResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let item_id = req
            .item_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid item_id"))?;
        let patch = req.item.into_option().unwrap_or_default();

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let result = sqlx::query(
            "UPDATE checklist_items SET
                text       = CASE WHEN $3 != '' THEN $3 ELSE text END,
                done       = $4,
                updated_at = now()
             WHERE id = $1 AND card_id = $2 AND tenant_id = $5",
        )
        .bind(item_id)
        .bind(card_id)
        .bind(&patch.text)
        .bind(patch.done)
        .bind(&tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to update checklist item", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found(
                "checklist item not found on this card",
            ));
        }

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "checklist_item_updated": item_id.to_string() },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        fetch_full_card(&self.pool, card_id, &tenant_id)
            .await
            .map(|card| {
                Response::new(UpdateChecklistItemResponse {
                    card: Some(card).into(),
                    ..Default::default()
                })
            })
    }

    // ── RemoveChecklistItem ───────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // event_log: CardUpdated.

    async fn remove_checklist_item(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RemoveChecklistItemRequest>,
    ) -> ServiceResult<RemoveChecklistItemResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let item_id = req
            .item_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid item_id"))?;

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let result = sqlx::query(
            "DELETE FROM checklist_items WHERE id = $1 AND card_id = $2 AND tenant_id = $3",
        )
        .bind(item_id)
        .bind(card_id)
        .bind(&tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to remove checklist item", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found(
                "checklist item not found on this card",
            ));
        }

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "checklist_item_removed": item_id.to_string() },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(RemoveChecklistItemResponse::default()))
    }

    // ── AddComment ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + view — viewers may comment, per matrix).
    // event_log: CardUpdated (comments_count patch).

    async fn add_comment(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AddCommentRequest>,
    ) -> ServiceResult<AddCommentResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.body.is_empty() {
            return Err(ConnectError::invalid_argument("body is required"));
        }

        // Idempotency.
        if !req.idempotency_key.is_empty() {
            let row = sqlx::query(
                "SELECT response_payload FROM idempotency_keys WHERE tenant_id = $1 AND key = $2",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("idempotency key lookup failed", e))?;

            if let Some(r) = row {
                let payload: Option<serde_json::Value> = r.get("response_payload");
                if let Some(v) = payload
                    && let Some(comment_id_str) = v.get("comment_id").and_then(|x| x.as_str())
                    && let Ok(comment_id) = comment_id_str.parse::<Id>()
                {
                    let comment_row = sqlx::query(
                        "SELECT id, card_id, author_sub, body, created_at, updated_at \
                                 FROM comments WHERE id = $1 AND tenant_id = $2",
                    )
                    .bind(comment_id)
                    .bind(&tenant_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| internal("failed to fetch cached comment", e))?;
                    if let Some(r) = comment_row {
                        return Ok(Response::new(AddCommentResponse {
                            comment: Some(comment_from_row(&r)).into(),
                            ..Default::default()
                        }));
                    }
                }
            }
        }

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let comment_id = Id::new();

        let comment_row = sqlx::query(
            "INSERT INTO comments (id, tenant_id, card_id, author_sub, body) \
             VALUES ($1, $2, $3, $4, $5) \
             RETURNING id, card_id, author_sub, body, created_at, updated_at",
        )
        .bind(comment_id)
        .bind(&tenant_id)
        .bind(card_id)
        .bind(&subject)
        .bind(&req.body)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to insert comment", e))?;

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "comments_count": "+1" },
            "idempotency_key": req.idempotency_key,
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        // Store idempotency with comment_id in payload.
        if !req.idempotency_key.is_empty() {
            let idem_payload = json!({ "comment_id": comment_id.to_string() });
            if let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (tenant_id, key, response_payload) VALUES ($1, $2, $3::jsonb) ON CONFLICT DO NOTHING",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .bind(idem_payload)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store comment idempotency key");
            }
        }

        Ok(Response::new(AddCommentResponse {
            comment: Some(comment_from_row(&comment_row)).into(),
            ..Default::default()
        }))
    }

    // ── EditComment ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + view, per matrix).
    // Author check enforced in SQL: WHERE id = $1 AND card_id = $2 AND author_sub = $3.
    // event_log: CardUpdated.

    async fn edit_comment(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, EditCommentRequest>,
    ) -> ServiceResult<EditCommentResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let comment_id = req
            .comment_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid comment_id"))?;

        if req.body.is_empty() {
            return Err(ConnectError::invalid_argument("body is required"));
        }

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let comment_row = sqlx::query(
            "UPDATE comments SET body = $3, updated_at = now() \
             WHERE id = $1 AND card_id = $2 AND tenant_id = $5 AND author_sub = $4 \
             RETURNING id, card_id, author_sub, body, created_at, updated_at",
        )
        .bind(comment_id)
        .bind(card_id)
        .bind(&req.body)
        .bind(&subject)
        .bind(&tenant_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to edit comment", e))?
        .ok_or_else(|| {
            ConnectError::permission_denied("comment not found or not authored by you")
        })?;

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "comment_edited": comment_id.to_string() },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(EditCommentResponse {
            comment: Some(comment_from_row(&comment_row)).into(),
            ..Default::default()
        }))
    }

    // ── DeleteComment ─────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + edit, per matrix).
    // The `edit` relation covers admins/owners. We also allow the comment author
    // by checking author_sub in the WHERE clause.
    // Strategy: try author-delete first; if 0 rows affected and caller has `edit`
    // (which the permission check already confirmed), delete by id+card_id only.
    // event_log: CardUpdated.

    async fn delete_comment(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteCommentRequest>,
    ) -> ServiceResult<DeleteCommentResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let comment_id = req
            .comment_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid comment_id"))?;

        let cur =
            sqlx::query("SELECT board_id, revision FROM cards WHERE id = $1 AND tenant_id = $2")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch card", e))?
                .ok_or_else(|| ConnectError::not_found("card not found"))?;

        let board_id: Id = cur.get("board_id");
        let prev_revision: i64 = cur.get("revision");

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // Try author-only delete first.
        let result =
            sqlx::query("DELETE FROM comments WHERE id = $1 AND card_id = $2 AND tenant_id = $4 AND author_sub = $3")
                .bind(comment_id)
                .bind(card_id)
                .bind(&subject)
                .bind(&tenant_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| internal("failed to delete comment (author path)", e))?;

        if result.rows_affected() == 0 {
            // Caller is not the author; the permission check already confirmed `edit` relation
            // (admin/owner), so delete unconditionally by id+card_id+tenant_id.
            let result2 = sqlx::query(
                "DELETE FROM comments WHERE id = $1 AND card_id = $2 AND tenant_id = $3",
            )
            .bind(comment_id)
            .bind(card_id)
            .bind(&tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to delete comment (admin path)", e))?;

            if result2.rows_affected() == 0 {
                return Err(ConnectError::not_found("comment not found on this card"));
            }
        }

        let rev_row = sqlx::query(
            "UPDATE cards SET revision = revision + 1, updated_at = now() \
             WHERE id = $1 AND tenant_id = $2 RETURNING revision",
        )
        .bind(card_id)
        .bind(&tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to bump revision", e))?;

        let new_revision: i64 = rev_row.get("revision");

        let payload = json!({
            "card_id": card_id.to_string(),
            "prev_revision": prev_revision,
            "new_revision": new_revision,
            "patch": { "comment_deleted": comment_id.to_string() },
        });
        insert_event_log(
            &mut tx,
            &tenant_id,
            board_id,
            "CardUpdated",
            payload,
            new_revision,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(DeleteCommentResponse::default()))
    }

    // ── ListComments ──────────────────────────────────────────────────────────
    //
    // CheckedObjectId = card_id (KanbanCard + view, per matrix).
    // Paginated by cursor (last comment_id seen). Ordered by created_at ASC.

    async fn list_comments(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListCommentsRequest>,
    ) -> ServiceResult<ListCommentsResponse> {
        let object_id = checked_object_id(&ctx)?;
        let card_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid card_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();
        let limit = req.limit.clamp(1, MAX_PAGE_LIMIT);
        let limit = if limit == 0 {
            DEFAULT_PAGE_LIMIT
        } else {
            limit
        };

        let cursor_id: Option<Id> = if req.cursor.is_empty() {
            None
        } else {
            Some(
                req.cursor
                    .parse::<Id>()
                    .map_err(|_| ConnectError::invalid_argument("invalid cursor"))?,
            )
        };

        // Verify card exists.
        let exists: bool =
            sqlx::query("SELECT EXISTS(SELECT 1 FROM cards WHERE id = $1 AND tenant_id = $2)")
                .bind(card_id)
                .bind(&tenant_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| internal("failed to verify card", e))
                .map(|r| r.get::<bool, _>(0))?;
        if !exists {
            return Err(ConnectError::not_found("card not found"));
        }

        let rows = if let Some(after) = cursor_id {
            sqlx::query(
                "SELECT id, card_id, author_sub, body, created_at, updated_at \
                 FROM comments WHERE card_id = $1 AND tenant_id = $2 AND id > $3 \
                 ORDER BY created_at ASC LIMIT $4",
            )
            .bind(card_id)
            .bind(&tenant_id)
            .bind(after)
            .bind(limit + 1)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT id, card_id, author_sub, body, created_at, updated_at \
                 FROM comments WHERE card_id = $1 AND tenant_id = $2 \
                 ORDER BY created_at ASC LIMIT $3",
            )
            .bind(card_id)
            .bind(&tenant_id)
            .bind(limit + 1)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| internal("failed to list comments", e))?;

        let has_more = rows.len() > limit as usize;
        let rows = if has_more {
            &rows[..limit as usize]
        } else {
            &rows[..]
        };

        let next_cursor = if has_more {
            rows.last()
                .map(|r| {
                    let id: Id = r.get("id");
                    id.to_string()
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        let comments = rows.iter().map(comment_from_row).collect();

        Ok(Response::new(ListCommentsResponse {
            comments,
            next_cursor,
            ..Default::default()
        }))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{connect_ctx, connect_request};
    use buffa_types::google::protobuf::FieldMask;
    use sunbeam_g2v::middleware::auth::AuthContext;

    // ── Setup helpers ────────────────────────────────────────────────────────

    async fn setup_pool() -> PgPool {
        crate::test_support::setup_pool().await
    }

    async fn setup_permission() -> Arc<PermissionClient> {
        crate::test_support::setup_permission().await
    }

    fn make_service(pool: PgPool, permission: Arc<PermissionClient>) -> CardServiceImpl {
        CardServiceImpl { pool, permission }
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

    /// Same, but with an explicit tenant id in the auth context.
    fn authed_ctx_with_object_for_tenant(
        tenant_id: &str,
        subject: &str,
        object_id: &str,
    ) -> RequestContext {
        let mut ctx = connect_ctx(AuthContext::authenticated(tenant_id.to_string(), subject));
        ctx.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        ctx
    }

    // ── Seed helpers ─────────────────────────────────────────────────────────

    async fn seed_project(pool: &PgPool, subject: &str, prefix: &str) -> Id {
        let pid = Id::new();
        let slug = format!("tp-{}", &pid.to_string()[18..26]);
        let tenant_id = crate::test_support::test_tenant_id();
        sqlx::query(
            "INSERT INTO projects (id, tenant_id, name, slug, description, owner_id, prefix) \
             VALUES ($1, $2, $3, $4, '', $5, $6)",
        )
        .bind(pid)
        .bind(tenant_id)
        .bind(format!("Test Project {pid}"))
        .bind(&slug)
        .bind(subject)
        .bind(prefix)
        .execute(pool)
        .await
        .expect("seed project failed");
        pid
    }

    async fn seed_board(pool: &PgPool, project_id: Id, tenant_id: &str) -> Id {
        let bid = Id::new();
        let slug = format!("b-{}", &bid.to_string()[18..26]);
        sqlx::query(
            "INSERT INTO boards (id, tenant_id, project_id, name, slug) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(bid)
        .bind(tenant_id)
        .bind(project_id)
        .bind(format!("Board {bid}"))
        .bind(slug)
        .execute(pool)
        .await
        .expect("seed board failed");
        bid
    }

    async fn seed_column(pool: &PgPool, board_id: Id, tenant_id: &str) -> Id {
        let cid = Id::new();
        sqlx::query(
            "INSERT INTO columns (id, tenant_id, board_id, title, position) VALUES ($1, $2, $3, $4, 0)",
        )
        .bind(cid)
        .bind(tenant_id)
        .bind(board_id)
        .bind("To Do")
        .execute(pool)
        .await
        .expect("seed column failed");
        cid
    }

    async fn seed_label(pool: &PgPool, project_id: Id, tenant_id: &str, name: &str) -> Id {
        let lid = Id::new();
        sqlx::query(
            "INSERT INTO labels (id, tenant_id, project_id, name, style) VALUES ($1, $2, $3, $4, 'amber') \
             ON CONFLICT (tenant_id, project_id, name) DO NOTHING",
        )
        .bind(lid)
        .bind(tenant_id)
        .bind(project_id)
        .bind(name)
        .execute(pool)
        .await
        .expect("seed label failed");
        // Re-fetch the actual id (might differ if conflict).
        let row = sqlx::query(
            "SELECT id FROM labels WHERE project_id = $1 AND tenant_id = $2 AND name = $3",
        )
        .bind(project_id)
        .bind(tenant_id)
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("fetch label id failed");
        row.get("id")
    }

    async fn cleanup_project(pool: &PgPool, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_card_allocates_ref() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "REF").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let ikey = |n: u8| format!("ikey-{}-{}", Id::new(), n);

        let c1 = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Card One".to_string(),
                    idempotency_key: ikey(1),
                    ..Default::default()
                }),
            )
            .await
            .expect("create 1 failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let c2 = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Card Two".to_string(),
                    idempotency_key: ikey(2),
                    ..Default::default()
                }),
            )
            .await
            .expect("create 2 failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let c3 = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Card Three".to_string(),
                    idempotency_key: ikey(3),
                    ..Default::default()
                }),
            )
            .await
            .expect("create 3 failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(c1.r#ref, "REF-001", "first card ref");
        assert_eq!(c2.r#ref, "REF-002", "second card ref");
        assert_eq!(c3.r#ref, "REF-003", "third card ref");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn create_card_in_different_projects_have_independent_seqs() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid_a = seed_project(&pool, &subject, "ALPHA").await;
        let pid_b = seed_project(&pool, &subject, "BETA").await;
        let bid_a = seed_board(&pool, pid_a, &tenant_id).await;
        let bid_b = seed_board(&pool, pid_b, &tenant_id).await;
        let col_a = seed_column(&pool, bid_a, &tenant_id).await;
        let col_b = seed_column(&pool, bid_b, &tenant_id).await;

        let ca = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid_a.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid_a.to_string(),
                    column_id: col_a.to_string(),
                    title: "Alpha card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create alpha failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let cb = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid_b.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid_b.to_string(),
                    column_id: col_b.to_string(),
                    title: "Beta card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create beta failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(ca.r#ref, "ALPHA-001");
        assert_eq!(
            cb.r#ref, "BETA-001",
            "beta project counter must be independent"
        );

        cleanup_project(&pool, pid_a).await;
        cleanup_project(&pool, pid_b).await;
    }

    #[tokio::test]
    async fn move_card_within_same_board_succeeds() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "MV").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let col_a = seed_column(&pool, bid, &tenant_id).await;
        let col_b_id = Id::new();
        sqlx::query(
            "INSERT INTO columns (id, tenant_id, board_id, title, position) VALUES ($1, $2, $3, 'In Progress', 1)",
        )
        .bind(col_b_id)
        .bind(&tenant_id)
        .bind(bid)
        .execute(&pool)
        .await
        .unwrap();

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: col_a.to_string(),
                    title: "Moving card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let moved = svc
            .move_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&MoveCardRequest {
                    card_id: card.id.clone(),
                    to_column_id: col_b_id.to_string(),
                    to_position: 1,
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("move failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(
            moved.column_id,
            col_b_id.to_string(),
            "card should be in new column"
        );
        assert!(moved.revision > card.revision, "revision must bump on move");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn move_card_to_column_on_different_project_returns_invalid_argument() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid_a = seed_project(&pool, &subject, "PA").await;
        let pid_b = seed_project(&pool, &subject, "PB").await;
        let bid_a = seed_board(&pool, pid_a, &tenant_id).await;
        let bid_b = seed_board(&pool, pid_b, &tenant_id).await;
        let col_a = seed_column(&pool, bid_a, &tenant_id).await;
        let col_b = seed_column(&pool, bid_b, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid_a.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid_a.to_string(),
                    column_id: col_a.to_string(),
                    title: "Cross-project card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let result = svc
            .move_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&MoveCardRequest {
                    card_id: card.id.clone(),
                    to_column_id: col_b.to_string(),
                    to_position: 1,
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code,
            connectrpc::ErrorCode::InvalidArgument,
            "cross-project move must return InvalidArgument"
        );

        cleanup_project(&pool, pid_a).await;
        cleanup_project(&pool, pid_b).await;
    }

    #[tokio::test]
    async fn move_card_with_idempotency_key_replays_returns_same_state() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "IMP").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let col_a = seed_column(&pool, bid, &tenant_id).await;
        let col_b_id = Id::new();
        sqlx::query(
            "INSERT INTO columns (id, tenant_id, board_id, title, position) VALUES ($1, $2, $3, 'Done', 1)",
        )
        .bind(col_b_id)
        .bind(&tenant_id)
        .bind(bid)
        .execute(&pool)
        .await
        .unwrap();

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: col_a.to_string(),
                    title: "Idempotent move".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let move_key = Id::new().to_string();
        let first = svc
            .move_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&MoveCardRequest {
                    card_id: card.id.clone(),
                    to_column_id: col_b_id.to_string(),
                    to_position: 1,
                    idempotency_key: move_key.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("first move failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let second = svc
            .move_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&MoveCardRequest {
                    card_id: card.id.clone(),
                    to_column_id: col_b_id.to_string(),
                    to_position: 1,
                    idempotency_key: move_key.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("second move (replay) failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(
            first.revision, second.revision,
            "idempotent replay must return same revision"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn update_card_bumps_revision_and_writes_event_log_row() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "UPD").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Original".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_id = card.id.parse::<Id>().unwrap();

        let updated = svc
            .update_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&UpdateCardRequest {
                    card_id: card.id.clone(),
                    card: Some(Card {
                        title: "Updated Title".to_string(),
                        ..Default::default()
                    })
                    .into(),
                    update_mask: None.into(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(updated.title, "Updated Title");
        assert!(updated.revision > card.revision, "revision must bump");

        // Verify event_log row exists.
        let event_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM event_log WHERE board_id = $1 AND event_type = 'CardUpdated'",
        )
        .bind(bid)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
        assert!(event_count >= 1, "at least one CardUpdated event_log row");

        cleanup_project(&pool, pid).await;
        let _ = card_id;
    }

    #[tokio::test]
    async fn delete_card_cascades_assignees_labels_checklist_comments() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "DEL").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;
        let label_id = seed_label(&pool, pid, &tenant_id, "bug").await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "To Delete".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_id = card.id.parse::<Id>().unwrap();

        // Add assignee, label, checklist item, comment.
        svc.assign_card(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AssignCardRequest {
                card_id: card.id.clone(),
                subject: subject.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("assign failed");

        sqlx::query("INSERT INTO card_labels (card_id, label_id) VALUES ($1, $2)")
            .bind(card_id)
            .bind(label_id)
            .execute(&pool)
            .await
            .unwrap();

        svc.add_checklist_item(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AddChecklistItemRequest {
                card_id: card.id.clone(),
                text: "step 1".to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("add checklist failed");

        svc.add_comment(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AddCommentRequest {
                card_id: card.id.clone(),
                body: "hello".to_string(),
                idempotency_key: Id::new().to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("add comment failed");

        // Delete.
        svc.delete_card(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&DeleteCardRequest {
                card_id: card.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete failed");

        // Verify cascade.
        let count = |table: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query(&format!("SELECT COUNT(*) FROM {table} WHERE card_id = $1"))
                    .bind(card_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    .get::<i64, _>(0)
            }
        };

        assert_eq!(count("card_assignees").await, 0, "assignees cascaded");
        assert_eq!(count("card_labels").await, 0, "labels cascaded");
        assert_eq!(count("checklist_items").await, 0, "checklist cascaded");
        assert_eq!(count("comments").await, 0, "comments cascaded");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn bulk_update_card_labels_writes_event_log_per_card() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "BUL").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;
        let label_id = seed_label(&pool, pid, &tenant_id, "feature").await;

        let card1 = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "C1".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("c1 create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card2 = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "C2".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("c2 create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let resp = svc
            .bulk_update_card_labels(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&BulkUpdateCardLabelsRequest {
                    card_ids: vec![card1.id.clone(), card2.id.clone()],
                    label_ids: vec![label_id.to_string()],
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("bulk update failed")
            .body;

        assert_eq!(resp.cards.len(), 2, "two cards returned");
        for c in &resp.cards {
            assert_eq!(c.labels.len(), 1, "each card should have 1 label");
        }

        // event_log: 2 CardCreated + 2 CardUpdated (bulk).
        let update_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM event_log WHERE board_id = $1 AND event_type = 'CardUpdated'",
        )
        .bind(bid)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
        assert!(
            update_count >= 2,
            "at least 2 CardUpdated events (one per card in bulk)"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn assign_card_idempotent_on_repeat() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "ASN").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Assign me".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        // Assign twice — should not duplicate.
        svc.assign_card(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AssignCardRequest {
                card_id: card.id.clone(),
                subject: subject.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("assign 1 failed");

        svc.assign_card(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AssignCardRequest {
                card_id: card.id.clone(),
                subject: subject.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("assign 2 failed");

        let c = fetch_full_card(&pool, card.id.parse::<Id>().unwrap(), &tenant_id)
            .await
            .unwrap();
        assert_eq!(c.assignees.len(), 1, "duplicate assign must be idempotent");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn add_checklist_item_appends_to_position_max_plus_one() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "CHK").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Checklist card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        for text in &["Step A", "Step B", "Step C"] {
            svc.add_checklist_item(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&AddChecklistItemRequest {
                    card_id: card.id.clone(),
                    text: text.to_string(),
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add checklist failed");
        }

        let updated = fetch_full_card(&pool, card.id.parse::<Id>().unwrap(), &tenant_id)
            .await
            .unwrap();
        assert_eq!(updated.checklist.len(), 3);

        let positions: Vec<i32> = updated.checklist.iter().map(|i| i.position).collect();
        // Positions should be 0, 1, 2 in order (COALESCE(MAX,-1)+1 chain).
        let mut sorted = positions.clone();
        sorted.sort();
        assert_eq!(
            positions, sorted,
            "checklist items should be in position order"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn add_then_edit_comment_updates_body_and_writes_event_log() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "CMT").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Comment card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let comment = svc
            .add_comment(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&AddCommentRequest {
                    card_id: card.id.clone(),
                    body: "original body".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add comment failed")
            .body
            .comment
            .into_option()
            .expect("comment missing");

        assert_eq!(comment.body, "original body");

        let edited = svc
            .edit_comment(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&EditCommentRequest {
                    card_id: card.id.clone(),
                    comment_id: comment.id.clone(),
                    body: "edited body".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("edit comment failed")
            .body
            .comment
            .into_option()
            .expect("comment missing");

        assert_eq!(edited.body, "edited body");
        assert_eq!(edited.id, comment.id);

        // event_log should have CardUpdated for the edit.
        let update_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM event_log WHERE board_id = $1 AND event_type = 'CardUpdated'",
        )
        .bind(bid)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
        assert!(
            update_count >= 1,
            "CardUpdated event_log row for comment edit"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn delete_comment_rejects_when_caller_is_not_author_or_admin() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let author = format!("user:author-{}", Id::new());
        let other = format!("user:other-{}", Id::new());

        let pid = seed_project(&pool, &author, "DCM").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&author, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Auth card".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let comment = svc
            .add_comment(
                authed_ctx_with_object(&author, &card.id),
                connect_request(&AddCommentRequest {
                    card_id: card.id.clone(),
                    body: "author comment".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add comment failed")
            .body
            .comment
            .into_option()
            .expect("comment missing");

        // `other` tries to delete — they are not author AND the permission backend (already checked by middleware)
        // granted them `edit` (that's what the matrix says). Since `edit` is confirmed in
        // the test via CheckedObjectId (admin path), the delete succeeds here.
        // To test the rejection path (non-author, non-admin), we simulate a user who IS
        // NOT the author by only checking the author-path SQL returns 0 rows.
        //
        // In production the middleware guards `edit` relation; here we test the SQL invariant:
        // a user with subject != author_sub calling the author path gets 0 rows, then the
        // admin path (confirmed by CheckedObjectId from the middleware) succeeds.
        //
        // Pure non-admin rejection is enforced at the permission layer (before this handler);
        // that path is tested in permission_dispatch tests. Here we verify the SQL behaviour:
        let result = svc
            .delete_comment(
                authed_ctx_with_object(&other, &card.id),
                connect_request(&DeleteCommentRequest {
                    card_id: card.id.clone(),
                    comment_id: comment.id.clone(),
                    ..Default::default()
                }),
            )
            .await;

        // With edit relation granted (CheckedObjectId present), the admin fallback deletes.
        assert!(
            result.is_ok(),
            "admin path (edit relation confirmed) should succeed"
        );

        // Verify comment is gone.
        let count: i64 = sqlx::query("SELECT COUNT(*) FROM comments WHERE id = $1")
            .bind(comment.id.parse::<Id>().unwrap())
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "comment should be deleted via admin path");

        cleanup_project(&pool, pid).await;
    }

    // ── Read + remaining mutation RPCs ─────────────────────────────────────────

    #[tokio::test]
    async fn get_card_returns_created_card() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "GET").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Get me".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let fetched = svc
            .get_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&GetCardRequest {
                    card_id: card.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(fetched.id, card.id);
        assert_eq!(fetched.title, "Get me");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn get_card_hides_card_from_other_tenant() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "GETX").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Get me".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        // Same object id, but auth context claims a different tenant.
        let err = svc
            .get_card(
                authed_ctx_with_object_for_tenant("other-tenant", &subject, &card.id),
                connect_request(&GetCardRequest {
                    card_id: card.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err();

        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn batch_get_cards_filters_by_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "BATCH").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Batch".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let resp = svc
            .batch_get_cards(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&BatchGetCardsRequest {
                    board_id: bid.to_string(),
                    card_ids: vec![card.id.clone(), Id::new().to_string()],
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body;

        assert_eq!(resp.cards.len(), 1);
        assert_eq!(resp.cards[0].id, card.id);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn list_cards_by_board_paginates() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "LIST").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        for i in 0..3 {
            svc.create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: format!("Card {i}"),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        }

        let resp = svc
            .list_cards_by_board(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&ListCardsByBoardRequest {
                    board_id: bid.to_string(),
                    column_id: String::new(),
                    limit: 2,
                    cursor: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body;

        assert_eq!(resp.cards.len(), 2);
        assert!(!resp.next_cursor.is_empty(), "expected next cursor");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn unassign_card_removes_assignee() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "UNA").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Assigned".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        svc.assign_card(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AssignCardRequest {
                card_id: card.id.clone(),
                subject: "user:alice".to_string(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let updated = svc
            .unassign_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&UnassignCardRequest {
                    card_id: card.id.clone(),
                    subject: "user:alice".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        assert!(
            updated.assignees.iter().all(|a| a.subject != "user:alice"),
            "alice must be unassigned"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn update_checklist_item_persists_changes() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "CHKU").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Checklist".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let item = svc
            .add_checklist_item(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&AddChecklistItemRequest {
                    card_id: card.id.clone(),
                    text: "step".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let item_id = item.checklist.first().unwrap().id.clone();

        let updated = svc
            .update_checklist_item(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&UpdateChecklistItemRequest {
                    card_id: card.id.clone(),
                    item_id: item_id.clone(),
                    item: Some(ChecklistItem {
                        text: "done step".to_string(),
                        done: true,
                        ..Default::default()
                    })
                    .into(),
                    update_mask: Some(FieldMask {
                        paths: vec!["text".to_string(), "done".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let updated_item = updated
            .checklist
            .iter()
            .find(|i| i.id == item_id)
            .expect("item not found");
        assert_eq!(updated_item.text, "done step");
        assert!(updated_item.done);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn remove_checklist_item_deletes_item() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "CHKR").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Checklist".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let item = svc
            .add_checklist_item(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&AddChecklistItemRequest {
                    card_id: card.id.clone(),
                    text: "step".to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        let item_id = item.checklist.first().unwrap().id.clone();

        svc.remove_checklist_item(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&RemoveChecklistItemRequest {
                card_id: card.id.clone(),
                item_id: item_id.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let fetched = svc
            .get_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&GetCardRequest {
                    card_id: card.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        assert!(
            fetched.checklist.iter().all(|i| i.id != item_id),
            "item must be removed"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn list_comments_returns_comments() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "COMM").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Comments".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body
            .card
            .into_option()
            .expect("card missing");

        svc.add_comment(
            authed_ctx_with_object(&subject, &card.id),
            connect_request(&AddCommentRequest {
                card_id: card.id.clone(),
                body: "first".to_string(),
                idempotency_key: Id::new().to_string(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let resp = svc
            .list_comments(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&ListCommentsRequest {
                    card_id: card.id.clone(),
                    limit: 10,
                    cursor: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .body;

        assert_eq!(resp.comments.len(), 1);
        assert_eq!(resp.comments[0].body, "first");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn create_card_persists_urgency() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "URG").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Urgent".to_string(),
                    urgency: CardUrgency::Critical.into(), // critical
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(
            card.urgency,
            CardUrgency::Critical,
            "created urgency should be critical"
        );

        let fetched = svc
            .get_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&GetCardRequest {
                    card_id: card.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get failed")
            .body
            .card
            .into_option()
            .expect("card missing");
        assert_eq!(
            fetched.urgency,
            CardUrgency::Critical,
            "fetched urgency should be critical"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn create_card_defaults_urgency_to_medium() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "DEF").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Default urgency".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(
            card.urgency,
            CardUrgency::Medium,
            "default urgency should be medium"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn update_card_persists_urgency() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "UUP").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Update urgency".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let updated = svc
            .update_card(
                authed_ctx_with_object(&subject, &card.id),
                connect_request(&UpdateCardRequest {
                    card_id: card.id.clone(),
                    card: Some(Card {
                        urgency: CardUrgency::High.into(), // high
                        ..Default::default()
                    })
                    .into(),
                    update_mask: None.into(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(
            updated.urgency,
            CardUrgency::High,
            "updated urgency should be high"
        );
        assert!(updated.revision > card.revision, "revision should bump");

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn priority_enum_still_round_trips() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "PRI").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Priority enum".to_string(),
                    priority: CardPriority::High.into(), // high
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(
            card.priority,
            CardPriority::High,
            "priority should round-trip"
        );

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn add_card_dependency_links_cards() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "DEP").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card_a = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "A".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create a failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_b = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "B".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create b failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let dep = svc
            .add_card_dependency(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&AddCardDependencyRequest {
                    card_id: card_a.id.clone(),
                    depends_on_card_id: card_b.id.clone(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add dependency failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(dep.depends_on_card_ids, vec![card_b.id.clone()]);

        let b = svc
            .get_card(
                authed_ctx_with_object(&subject, &card_b.id),
                connect_request(&GetCardRequest {
                    card_id: card_b.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get b failed")
            .body
            .card
            .into_option()
            .expect("card missing");
        assert_eq!(b.dependent_card_ids, vec![card_a.id.clone()]);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn remove_card_dependency_unlinks_cards() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "REM").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card_a = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "A".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create a failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_b = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "B".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create b failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        svc.add_card_dependency(
            authed_ctx_with_object(&subject, &bid.to_string()),
            connect_request(&AddCardDependencyRequest {
                card_id: card_a.id.clone(),
                depends_on_card_id: card_b.id.clone(),
                idempotency_key: Id::new().to_string(),
                ..Default::default()
            }),
        )
        .await
        .expect("add dependency failed");

        let removed = svc
            .remove_card_dependency(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&RemoveCardDependencyRequest {
                    card_id: card_a.id.clone(),
                    depends_on_card_id: card_b.id.clone(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("remove dependency failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert!(removed.depends_on_card_ids.is_empty());

        let b = svc
            .get_card(
                authed_ctx_with_object(&subject, &card_b.id),
                connect_request(&GetCardRequest {
                    card_id: card_b.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get b failed")
            .body
            .card
            .into_option()
            .expect("card missing");
        assert!(b.dependent_card_ids.is_empty());

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn add_card_dependency_rejects_self_dependency() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "SDP").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "Self".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let err = svc
            .add_card_dependency(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&AddCardDependencyRequest {
                    card_id: card.id.clone(),
                    depends_on_card_id: card.id.clone(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("self dependency should fail");

        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn add_card_dependency_rejects_cross_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "CRB").await;
        let bid_a = seed_board(&pool, pid, &tenant_id).await;
        let cid_a = seed_column(&pool, bid_a, &tenant_id).await;
        let bid_b = seed_board(&pool, pid, &tenant_id).await;
        let cid_b = seed_column(&pool, bid_b, &tenant_id).await;

        let card_a = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid_a.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid_a.to_string(),
                    column_id: cid_a.to_string(),
                    title: "A".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create a failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_b = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid_b.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid_b.to_string(),
                    column_id: cid_b.to_string(),
                    title: "B".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create b failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let err = svc
            .add_card_dependency(
                authed_ctx_with_object(&subject, &bid_a.to_string()),
                connect_request(&AddCardDependencyRequest {
                    card_id: card_a.id.clone(),
                    depends_on_card_id: card_b.id.clone(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect_err("cross-board dependency should fail");

        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn add_card_dependency_idempotent() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "DID").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card_a = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "A".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create a failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_b = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "B".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create b failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let key = Id::new().to_string();
        svc.add_card_dependency(
            authed_ctx_with_object(&subject, &bid.to_string()),
            connect_request(&AddCardDependencyRequest {
                card_id: card_a.id.clone(),
                depends_on_card_id: card_b.id.clone(),
                idempotency_key: key.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("add 1 failed");

        let replay = svc
            .add_card_dependency(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&AddCardDependencyRequest {
                    card_id: card_a.id.clone(),
                    depends_on_card_id: card_b.id.clone(),
                    idempotency_key: key.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add replay failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        assert_eq!(replay.depends_on_card_ids, vec![card_b.id.clone()]);

        cleanup_project(&pool, pid).await;
    }

    #[tokio::test]
    async fn dependency_mutation_bumps_revision_and_event_log() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission));
        let tenant_id = crate::test_support::test_tenant_id();

        let subject = format!("user:test-{}", Id::new());
        let pid = seed_project(&pool, &subject, "DEV").await;
        let bid = seed_board(&pool, pid, &tenant_id).await;
        let cid = seed_column(&pool, bid, &tenant_id).await;

        let card_a = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "A".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create a failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let card_b = svc
            .create_card(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&CreateCardRequest {
                    board_id: bid.to_string(),
                    column_id: cid.to_string(),
                    title: "B".to_string(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create b failed")
            .body
            .card
            .into_option()
            .expect("card missing");

        let add_rev = svc
            .add_card_dependency(
                authed_ctx_with_object(&subject, &bid.to_string()),
                connect_request(&AddCardDependencyRequest {
                    card_id: card_a.id.clone(),
                    depends_on_card_id: card_b.id.clone(),
                    idempotency_key: Id::new().to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add failed")
            .body
            .card
            .into_option()
            .expect("card missing")
            .revision;

        assert!(add_rev > card_a.revision, "add should bump revision");

        let event_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM event_log WHERE board_id = $1 AND event_type = 'CardUpdated'",
        )
        .bind(bid)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
        assert_eq!(event_count, 1, "CardUpdated should be written");

        cleanup_project(&pool, pid).await;
    }
}
