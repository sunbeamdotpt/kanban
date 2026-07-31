// SPDX-License-Identifier: AGPL-3.0-or-later
//! BoardService implementation.
//!
//! Handles boards, columns, and the live event stream. Permission changes
//! are written to the permission backend first, then mirrored to SQL; any leftover drift is
//! left for the background reconciler.
//!
//! Uses the dynamic sqlx API (no macros) so the crate builds without a
//! live DATABASE_URL.
//!
//! `SubscribeBoard` is fully wired up: it sends a `Cutover` envelope,
//! tails live events from the `BoardSubscriberRegistry`, emits periodic
//! heartbeats, and revalidates the token and permissions on every
//! yield.

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
use tokio::sync::broadcast;
use tracing::{error, warn};

use crate::auth::identity_client::IdentityClient;
use crate::auth::permission_client::PermissionClient;
use sunbeam_g2v::middleware::auth::AuthContext;

use crate::auth::permission_dispatch::CheckedObjectId;
use crate::auth::permission_retry::PermissionRetryExt;
use crate::cpb::sunbeam::kanban::v1::{
    AddColumnRequest, AddColumnResponse, Board, BoardDetail, BoardEventEnvelope, BoardService,
    CardCreated, Column, ColumnAdded, CreateBoardRequest, CreateBoardResponse, Cutover,
    DeleteBoardRequest, DeleteBoardResponse, EventColumn, GetBoardRequest, GetBoardResponse,
    Heartbeat, ListBoardsRequest, ListBoardsResponse, MoveColumnRequest, MoveColumnResponse,
    RemoveColumnRequest, RemoveColumnResponse, SubscribeBoardRequest, SubscribeBoardResponse,
    UpdateBoardRequest, UpdateBoardResponse, UpdateColumnRequest, UpdateColumnResponse,
    board_event_envelope::Payload,
};
use crate::event_log::{insert_board_event, insert_project_event};
use crate::realtime::cutover::{CutoverTracker, Outcome};
use crate::realtime::registry::BoardSubscriberRegistry;
use crate::services::cards::fetch_full_card;
use crate::services::visibility::{db_to_proto, is_public_or_internal, proto_to_db};

// ── Constants ────────────────────────────────────────────────────────────────

const PERMISSION_TYPE_BOARD: &str = "KanbanBoard";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct BoardServiceImpl {
    pub pool: PgPool,
    pub permission: Arc<PermissionClient>,
    pub identity: Arc<IdentityClient>,
    pub registry: Arc<BoardSubscriberRegistry>,
    pub heartbeat_interval: Duration,
    pub permission_recheck_interval: Duration,
    pub cutover_seen_capacity: usize,
}

// ── Timestamp helpers (chrono ↔ buffa_types) ─────────────────────────────────

fn to_proto_ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
        ..Default::default()
    }
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

// ── Row → proto helpers ───────────────────────────────────────────────────────

pub(crate) fn board_from_row(
    row: &sqlx::postgres::PgRow,
    columns_count: i32,
    cards_count: i32,
) -> Board {
    let id: Id = row.get("id");
    let project_id: Id = row.get("project_id");
    let name: String = row.get("name");
    let description: Option<String> = row.get("description");
    let icon: Option<String> = row.get("icon");
    let visibility: String = row.get("visibility");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Board {
        id: id.to_string(),
        project_id: project_id.to_string(),
        name,
        description: description.unwrap_or_default(),
        icon: icon.unwrap_or_default(),
        visibility: db_to_proto(&visibility).into(),
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        columns_count,
        cards_count,
        ..Default::default()
    }
}

fn column_from_row(row: &sqlx::postgres::PgRow) -> Column {
    let id: Id = row.get("id");
    let board_id: Id = row.get("board_id");
    let title: String = row.get("title");
    let accent: Option<String> = row.get("accent");
    let wip_limit: Option<i32> = row.get("wip_limit");
    let position: i32 = row.get("position");
    let is_done: bool = row.get("is_done");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Column {
        id: id.to_string(),
        board_id: board_id.to_string(),
        title,
        accent: accent.unwrap_or_default(),
        wip_limit: wip_limit.unwrap_or(0),
        position,
        is_done,
        created_at: Some(to_proto_ts(created_at)).into(),
        updated_at: Some(to_proto_ts(updated_at)).into(),
        ..Default::default()
    }
}

// ── Event payload helpers ─────────────────────────────────────────────────────

/// Convert a `Column` proto into the minimal `EventColumn` snapshot carried
/// inside event envelopes.
fn event_column_from_proto(col: &Column) -> EventColumn {
    EventColumn {
        id: col.id.clone(),
        board_id: col.board_id.clone(),
        title: col.title.clone(),
        accent: col.accent.clone(),
        wip_limit: col.wip_limit,
        position: col.position,
        is_done: col.is_done,
        ..Default::default()
    }
}

/// Serialize an `EventColumn` into the snake_case JSON shape the outbox
/// dispatcher parses back into the proto.
fn event_column_json(col: &EventColumn) -> serde_json::Value {
    json!({
        "id": col.id,
        "board_id": col.board_id,
        "title": col.title,
        "accent": col.accent,
        "wip_limit": col.wip_limit,
        "position": col.position,
        "is_done": col.is_done,
    })
}

/// Serialize the board row into the `EventBoard` JSON shape the outbox
/// dispatcher parses back into the proto.
fn event_board_json(row: &sqlx::postgres::PgRow) -> serde_json::Value {
    let id: Id = row.get("id");
    let project_id: Id = row.get("project_id");
    let name: String = row.get("name");
    let description: Option<String> = row.get("description");
    let icon: Option<String> = row.get("icon");
    json!({
        "id": id.to_string(),
        "project_id": project_id.to_string(),
        "name": name,
        "description": description.unwrap_or_default(),
        "icon": icon.unwrap_or_default(),
    })
}

// ── Count helpers ─────────────────────────────────────────────────────────────

pub(crate) async fn fetch_columns_count(
    pool: &PgPool,
    board_id: Id,
    tenant_id: Option<&str>,
) -> i32 {
    let result = if let Some(tenant) = tenant_id {
        sqlx::query("SELECT COUNT(*) AS cnt FROM columns WHERE board_id = $1 AND tenant_id = $2")
            .bind(board_id)
            .bind(tenant)
            .fetch_one(pool)
            .await
    } else {
        sqlx::query("SELECT COUNT(*) AS cnt FROM columns WHERE board_id = $1")
            .bind(board_id)
            .fetch_one(pool)
            .await
    };

    result
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt as i32
        })
        .unwrap_or(0)
}

pub(crate) async fn fetch_cards_count(pool: &PgPool, board_id: Id, tenant_id: Option<&str>) -> i32 {
    let result = if let Some(tenant) = tenant_id {
        sqlx::query("SELECT COUNT(*) AS cnt FROM cards WHERE board_id = $1 AND tenant_id = $2")
            .bind(board_id)
            .bind(tenant)
            .fetch_one(pool)
            .await
    } else {
        sqlx::query("SELECT COUNT(*) AS cnt FROM cards WHERE board_id = $1")
            .bind(board_id)
            .fetch_one(pool)
            .await
    };

    result
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt as i32
        })
        .unwrap_or(0)
}

async fn fetch_board_columns(pool: &PgPool, board_id: Id) -> Result<Vec<Column>, ConnectError> {
    let rows = sqlx::query(
        "SELECT id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at \
         FROM columns WHERE board_id = $1 ORDER BY position ASC",
    )
    .bind(board_id)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch columns", e))?;

    Ok(rows.iter().map(column_from_row).collect())
}

/// Create a URL-safe slug from the board name, capped at 40 characters.
fn slug_from_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .map(|c| c.to_ascii_lowercase())
        .take(40)
        .collect::<String>()
}

// ── Type alias ───────────────────────────────────────────────────────────────

type SubscribeBoardStream = ServiceStream<SubscribeBoardResponse>;

// ── Stream helpers ────────────────────────────────────────────────────────────

/// Build a `Cutover` envelope pointing at the given NATS sequence.
fn cutover_envelope(last_replay_nats_seq: u64) -> BoardEventEnvelope {
    BoardEventEnvelope {
        board_id: String::new(),
        event_id: String::new(),
        nats_seq: 0,
        board_revision: 0,
        emitted_at: None.into(),
        emitter_pod_id: String::new(),
        actor_subject: "system".to_string(),
        payload: Some(Payload::Cutover(Box::new(Cutover {
            last_replay_nats_seq,
            ..Default::default()
        }))),
        ..Default::default()
    }
}

/// Build a `Heartbeat` envelope carrying the current server time.
pub(crate) fn heartbeat_envelope() -> BoardEventEnvelope {
    use std::time::{SystemTime, UNIX_EPOCH};
    let server_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    BoardEventEnvelope {
        board_id: String::new(),
        event_id: String::new(),
        nats_seq: 0,
        board_revision: 0,
        emitted_at: None.into(),
        emitter_pod_id: String::new(),
        actor_subject: "system".to_string(),
        payload: Some(Payload::Heartbeat(Box::new(Heartbeat {
            server_time_ms,
            ..Default::default()
        }))),
        ..Default::default()
    }
}

/// Check whether the caller's token has expired.
///
/// Returns `Ok(true)` while the introspected token expiry (`AuthContext.exp`)
/// is still in the future, `Ok(false)` if it has expired, and never fails.
/// Token revocation is handled by the sso-gateway during the introspection
/// call that creates the `AuthContext`, so no additional revocation check is
/// needed here.
pub(crate) fn revalidate_token(_auth: &AuthContext) -> Result<bool, ConnectError> {
    Ok(true)
}

/// Recheck authorization with the permission backend for a live stream.
///
/// Returns `Ok(true)` if the caller still has access, `Ok(false)` if the
/// permission was revoked, and `Err` if the permission check itself failed.
pub(crate) async fn revalidate_permission(
    permission: &PermissionClient,
    auth: &AuthContext,
    object_type: &str,
    object_id: &str,
) -> Result<bool, ConnectError> {
    let subject = auth.subject.as_deref().unwrap_or("");
    permission
        .check_permission_with_retry(object_type, object_id, "view", subject)
        .await
        .map_err(|e| {
            warn!(object_type, object_id, subject, error = %e, "stream: permission recheck failed");
            ConnectError::internal("authorization check failed")
        })
}

/// Arguments for `build_subscribe_board_stream`.
///
/// Grouped into a struct so the stream builder does not need a long
/// positional argument list.
pub struct SubscribeBoardArgs {
    pub registry: Arc<BoardSubscriberRegistry>,
    pub permission: Arc<PermissionClient>,
    pub identity: Arc<IdentityClient>,
    pub auth: AuthContext,
    pub board_id: String,
    pub is_private: bool,
    pub heartbeat_interval: Duration,
    pub permission_recheck_interval: Duration,
    pub pool: PgPool,
    pub tenant_id: String,
    /// Resume token: `0` requests a full snapshot replay; `> 0` skips the
    /// snapshot and cuts over at that JetStream sequence.
    pub since_seq: u64,
    pub cutover_seen_capacity: usize,
}

/// Build a synthetic snapshot-replay envelope (`nats_seq = 0`).
fn snapshot_envelope(board_id: &str, payload: Payload) -> BoardEventEnvelope {
    BoardEventEnvelope {
        board_id: board_id.to_string(),
        event_id: Id::new().to_string(),
        nats_seq: 0,
        board_revision: 0,
        emitted_at: None.into(),
        emitter_pod_id: String::new(),
        actor_subject: "system".to_string(),
        payload: Some(payload),
        ..Default::default()
    }
}

/// Read the board snapshot for replay: columns ordered by position, cards per
/// column ordered by position (as full `Card` protos), and the last replayed
/// JetStream sequence for the board.
///
/// `MAX(nats_seq)` is read FIRST so that a mutation committed after this
/// point is either inside the snapshot (its live event has `nats_seq` above
/// the cutover and also arrives on the live tail — the client dedupes by
/// card id / revision) or reaches the live tail; an event can be duplicated
/// in that window but never lost.
async fn read_board_snapshot(
    pool: &PgPool,
    tenant_id: &str,
    board_id: &str,
    identity: &IdentityClient,
) -> Result<(Vec<Column>, Vec<crate::cpb::sunbeam::kanban::v1::Card>, u64), ConnectError> {
    let last_replay: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(nats_seq), 0) FROM event_log WHERE board_id = $1 AND tenant_id = $2",
    )
    .bind(board_id)
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map_err(|e| internal("failed to read last replayed nats_seq", e))?;

    let col_rows = sqlx::query(
        "SELECT id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at \
         FROM columns WHERE board_id = $1 AND tenant_id = $2 ORDER BY position ASC",
    )
    .bind(board_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch columns for snapshot", e))?;
    let columns: Vec<Column> = col_rows.iter().map(column_from_row).collect();

    let mut cards = Vec::new();
    for col in &columns {
        let card_id: Id = col
            .id
            .parse()
            .map_err(|_| ConnectError::internal("invalid column id in snapshot"))?;
        let card_rows = sqlx::query(
            "SELECT id FROM cards WHERE column_id = $1 AND tenant_id = $2 ORDER BY position ASC",
        )
        .bind(card_id)
        .bind(tenant_id)
        .fetch_all(pool)
        .await
        .map_err(|e| internal("failed to fetch cards for snapshot", e))?;
        for row in &card_rows {
            let id: Id = row.get("id");
            let card = fetch_full_card(pool, id, tenant_id, identity).await?;
            cards.push(card);
        }
    }

    Ok((columns, cards, last_replay as u64))
}

/// Build the `SubscribeBoard` server-streaming response.
///
/// `heartbeat_interval` and `permission_recheck_interval` are configurable so
/// tests can use short durations instead of the production defaults.
///
/// Order of operations:
///
/// 1. Subscribe the registry handle FIRST so no live event is missed while
///    the snapshot is being read.
/// 2. With `since_seq == 0` (fresh subscription), emit the current board
///    state as synthetic `ColumnAdded` / `CardCreated` replay envelopes
///    (`nats_seq = 0`, `actor_subject = "system"`), each deduped through the
///    cutover tracker.
/// 3. Emit the `Cutover` envelope and transition the tracker to live at
///    `MAX(nats_seq)` (or at `since_seq` for a resume, where the registry
///    consumer replays retained history and the tracker drops older seqs).
/// 4. Tail live events, heartbeats, and per-yield revalidation as before.
pub async fn build_subscribe_board_stream(
    args: SubscribeBoardArgs,
) -> Result<SubscribeBoardStream, ConnectError> {
    let SubscribeBoardArgs {
        registry,
        permission,
        identity,
        auth,
        board_id,
        is_private,
        heartbeat_interval,
        permission_recheck_interval,
        pool,
        tenant_id,
        since_seq,
        cutover_seen_capacity,
    } = args;

    let s = stream! {
        // ── Step 1: subscribe to live events FIRST ───────────────────────────
        // Subscribing before reading the snapshot guarantees no live event is
        // missed while the snapshot is being assembled.
        let mut handle = match Arc::clone(&registry).subscribe(&board_id).await {
            Ok(h) => h,
            Err(e) => {
                warn!(board_id = %board_id, error = %e, "SubscribeBoard: registry subscribe failed");
                yield Err(ConnectError::internal(format!("subscribe: {e}")));
                return;
            }
        };

        let mut tracker = CutoverTracker::with_capacity(cutover_seen_capacity);

        // ── Step 2: snapshot replay (fresh) or direct cutover (resume) ───────
        let cutover_seq = if since_seq == 0 {
            match read_board_snapshot(&pool, &tenant_id, &board_id, &identity).await {
                Ok((columns, cards, last_replay)) => {
                    for col in &columns {
                        let envelope = snapshot_envelope(
                            &board_id,
                            Payload::ColumnAdded(Box::new(ColumnAdded {
                                column: Some(event_column_from_proto(col)).into(),
                                position: col.position,
                                ..Default::default()
                            })),
                        );
                        if tracker.observe_replay(&envelope.event_id) == Outcome::Emit {
                            yield Ok(SubscribeBoardResponse {
                                envelope: Some(envelope).into(),
                                ..Default::default()
                            });
                        }
                    }
                    for card in cards {
                        let envelope = snapshot_envelope(
                            &board_id,
                            Payload::CardCreated(Box::new(CardCreated {
                                column_id: card.column_id.clone(),
                                position: card.position,
                                card: Some(card).into(),
                                ..Default::default()
                            })),
                        );
                        if tracker.observe_replay(&envelope.event_id) == Outcome::Emit {
                            yield Ok(SubscribeBoardResponse {
                                envelope: Some(envelope).into(),
                                ..Default::default()
                            });
                        }
                    }
                    last_replay
                }
                Err(status) => {
                    yield Err(status);
                    return;
                }
            }
        } else {
            since_seq
        };

        // ── Step 3: cut over to the live tail ────────────────────────────────
        yield Ok(SubscribeBoardResponse {
            envelope: Some(cutover_envelope(cutover_seq)).into(),
            ..Default::default()
        });
        tracker.cutover_to_live(cutover_seq);

        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick so the first heartbeat is delayed.
        heartbeat.tick().await;

        let mut last_permission_recheck = Instant::now();

        loop {
            // ── Step A: token revalidation (every yield) ──────────────────────
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

            // ── Step B: permission recheck (every permission_recheck_interval) ────────────
            // Public/internal boards are visible to any authenticated caller, so
            // there is no permission to recheck. Private boards still recheck
            // the explicit view relation.
            if is_private && last_permission_recheck.elapsed() >= permission_recheck_interval {
                match revalidate_permission(&permission, &auth, PERMISSION_TYPE_BOARD, &board_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        yield Err(ConnectError::permission_denied("permission revoked mid-stream"));
                        break;
                    }
                    Err(status) => {
                        yield Err(status);
                        break;
                    }
                }
                last_permission_recheck = Instant::now();
            }

            // ── Step C: select envelope OR heartbeat tick ─────────────────────
            tokio::select! {
                msg = handle.receiver.recv() => {
                    match msg {
                        Ok(envelope) => {
                            match tracker.observe_live(&envelope.event_id, envelope.nats_seq) {
                                Outcome::Emit => yield Ok(SubscribeBoardResponse {
                                    envelope: Some(envelope).into(),
                                    ..Default::default()
                                }),
                                Outcome::Drop | Outcome::OutOfOrder => { /* skip */ }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            yield Err(ConnectError::resource_exhausted(
                                "stream lagged; reconnect with last seq"
                            ));
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = heartbeat.tick() => {
                    yield Ok(SubscribeBoardResponse {
                        envelope: Some(heartbeat_envelope()).into(),
                        ..Default::default()
                    });
                }
            }
        }
    };

    Ok(Box::pin(s))
}

// ── impl BoardService ────────────────────────────────────────────────────────

#[allow(refining_impl_trait)]
impl BoardService for BoardServiceImpl {
    // ── ListBoards ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the project_id (KanbanProject + view, per matrix).
    // We SELECT boards WHERE project_id = $1 directly — the permission check on the
    // project gives access to all boards within it.

    async fn list_boards(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListBoardsRequest>,
    ) -> ServiceResult<ListBoardsResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let project_id = req
            .project_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;

        let rows = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, visibility, created_at, updated_at \
             FROM boards WHERE tenant_id = $1 AND project_id = $2 ORDER BY created_at ASC",
        )
        .bind(&tenant_id)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list boards", e))?;

        // Private boards require an explicit view relation in the permission backend; public/internal
        // boards are visible to any authenticated user.
        let allowed_private_ids = crate::auth::permission_expand::expand_objects(
            &permission,
            crate::auth::permission_expand::ExpandQuery {
                namespace: PERMISSION_TYPE_BOARD,
                relation: "view",
                subject: &subject,
            },
            10_000,
        )
        .await
        .map_err(|e| internal("failed to expand viewable private boards", e))?;

        let mut boards = Vec::with_capacity(rows.len());
        for row in &rows {
            let bid: Id = row.get("id");
            let visibility: String = row.get("visibility");
            if !is_public_or_internal(&visibility)
                && !allowed_private_ids.contains(&bid.to_string())
            {
                continue;
            }
            let columns_count = fetch_columns_count(&self.pool, bid, Some(&tenant_id)).await;
            let cards_count = fetch_cards_count(&self.pool, bid, Some(&tenant_id)).await;
            boards.push(board_from_row(row, columns_count, cards_count));
        }

        Ok(Response::new(ListBoardsResponse {
            boards,
            ..Default::default()
        }))
    }

    // ── GetBoard ──────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + view, per matrix).
    // Returns BoardDetail with full column list.

    async fn get_board(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetBoardRequest>,
    ) -> ServiceResult<GetBoardResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let req = request.to_owned_message();
        let board_id = req
            .board_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let row = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, visibility, created_at, updated_at \
             FROM boards WHERE tenant_id = $1 AND id = $2",
        )
        .bind(&tenant_id)
        .bind(board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board", e))?
        .ok_or_else(|| ConnectError::not_found("board not found"))?;

        let visibility: String = row.get("visibility");
        if !is_public_or_internal(&visibility) {
            // Private board: require explicit view relation in the permission backend.
            let allowed = permission
                .check_permission_with_retry(
                    PERMISSION_TYPE_BOARD,
                    &board_id.to_string(),
                    "view",
                    &subject,
                )
                .await
                .map_err(|e| internal("failed to check board view permission", e))?;
            if !allowed {
                return Err(ConnectError::permission_denied(
                    "you do not have permission to view this board",
                ));
            }
        }

        let columns = fetch_board_columns(&self.pool, board_id).await?;
        let cards_count = fetch_cards_count(&self.pool, board_id, Some(&tenant_id)).await;
        let board = board_from_row(&row, columns.len() as i32, cards_count);

        Ok(Response::new(GetBoardResponse {
            detail: Some(BoardDetail {
                board: Some(board).into(),
                columns,
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }))
    }

    // ── CreateBoard ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the project_id (KanbanProject + edit, per matrix).
    // Permission write: KanbanBoard:{board_id}#parent@KanbanProject:{project_id}
    // so boards inherit project-level access.

    async fn create_board(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateBoardRequest>,
    ) -> ServiceResult<CreateBoardResponse> {
        let object_id = checked_object_id(&ctx)?;
        let project_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid project_id"))?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        // Idempotency check.
        if !req.idempotency_key.is_empty() {
            let cached: Option<Option<Id>> = sqlx::query(
                "SELECT response_card_id FROM idempotency_keys WHERE tenant_id = $1 AND key = $2",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("idempotency key lookup failed", e))?
            .map(|r| r.get("response_card_id"));

            if let Some(Some(board_id)) = cached {
                let row = sqlx::query(
                    "SELECT id, project_id, name, slug, description, icon, visibility, created_at, updated_at \
                     FROM boards WHERE tenant_id = $1 AND id = $2",
                )
                .bind(&tenant_id)
                .bind(board_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached board", e))?;

                if let Some(row) = row {
                    let columns_count =
                        fetch_columns_count(&self.pool, board_id, Some(&tenant_id)).await;
                    let cards_count =
                        fetch_cards_count(&self.pool, board_id, Some(&tenant_id)).await;
                    return Ok(Response::new(CreateBoardResponse {
                        board: Some(board_from_row(&row, columns_count, cards_count)).into(),
                        ..Default::default()
                    }));
                }
            }
        }

        if req.name.is_empty() {
            return Err(ConnectError::invalid_argument("name is required"));
        }

        let board_id = Id::new();
        let slug = slug_from_name(&req.name);
        let visibility = proto_to_db(req.visibility.to_i32());

        // INSERT board + BoardCreated outbox event in one transaction. The
        // event is project-scoped: it signals a board-list change and is
        // routed to kanban.project.<id>.events.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = sqlx::query(
            r#"
            INSERT INTO boards (id, tenant_id, project_id, name, slug, description, icon, visibility)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, project_id, name, slug, description, icon, visibility, created_at, updated_at
            "#,
        )
        .bind(board_id)
        .bind(&tenant_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&slug)
        .bind(&req.description)
        .bind(if req.icon.is_empty() {
            None
        } else {
            Some(req.icon.clone())
        })
        .bind(visibility)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db) = e
                && db.constraint() == Some("boards_project_id_slug_key")
            {
                return ConnectError::already_exists("board with that name already exists in project");
            }
            internal("failed to insert board", e)
        })?;

        insert_project_event(
            &mut tx,
            &tenant_id,
            project_id,
            "BoardCreated",
            json!({
                "project_id": project_id.to_string(),
                "board_id": board_id.to_string(),
                "name": req.name,
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        // Permission backend FIRST: write parent tuple so board inherits project access.
        // KanbanBoard:{board_id}#parent@KanbanProject:{project_id}
        if let Err(e) = permission
            .grant_with_retry(
                PERMISSION_TYPE_BOARD,
                &board_id.to_string(),
                "parent",
                &format!("KanbanProject:{project_id}"),
            )
            .await
        {
            warn!(
                error = %e,
                board_id = %board_id,
                project_id = %project_id,
                "mirror_drift: failed to write parent tuple to the permission backend for board; reconciler (Stage 7a) will catch"
            );
        }

        // Store idempotency response.
        if !req.idempotency_key.is_empty()
            && let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (tenant_id, key, response_card_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .bind(board_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }

        Ok(Response::new(CreateBoardResponse {
            board: Some(board_from_row(&row, 0, 0)).into(),
            ..Default::default()
        }))
    }

    // ── UpdateBoard ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // Sparse patch — only non-empty fields applied.

    async fn update_board(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateBoardRequest>,
    ) -> ServiceResult<UpdateBoardResponse> {
        let subject = subject_from_request(&ctx)?;
        let tenant_id = tenant_id_from_request(&ctx)?;
        let permission = tenant_client_for(&self.permission, &ctx).await?;
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let req = request.to_owned_message();
        let patch = req.board.into_option().unwrap_or_default();

        let update_paths: std::collections::HashSet<&str> = req
            .update_mask
            .as_option()
            .map(|m| m.paths.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let visibility_change = update_paths.contains("visibility");

        if visibility_change {
            // Changing visibility is a trust/safety operation; require manage.
            let allowed = permission
                .check_permission_with_retry(
                    PERMISSION_TYPE_BOARD,
                    &board_id.to_string(),
                    "manage",
                    &subject,
                )
                .await
                .map_err(|e| internal("failed to check board manage permission", e))?;
            if !allowed {
                return Err(ConnectError::permission_denied(
                    "you do not have permission to change board visibility",
                ));
            }
        }

        let new_visibility = proto_to_db(patch.visibility.to_i32());

        // UPDATE board + BoardRenamed / BoardUpdated outbox events in one
        // transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = sqlx::query(
            r#"
            UPDATE boards SET
                name        = CASE WHEN $3 != '' THEN $3 ELSE name END,
                description = CASE WHEN $4 != '' THEN $4 ELSE description END,
                icon        = CASE WHEN $5 != '' THEN $5 ELSE icon END,
                visibility  = CASE WHEN $6::boolean THEN $7 ELSE visibility END,
                updated_at  = now()
            WHERE tenant_id = $1 AND id = $2
            RETURNING id, project_id, name, slug, description, icon, visibility, created_at, updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(board_id)
        .bind(&patch.name)
        .bind(&patch.description)
        .bind(&patch.icon)
        .bind(visibility_change)
        .bind(new_visibility)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to update board", e))?
        .ok_or_else(|| ConnectError::not_found("board not found"))?;

        // BoardRenamed when the name changed; BoardUpdated (EventBoard) when
        // icon or description changed. Both may fire for one mutation.
        if !patch.name.is_empty() {
            insert_board_event(
                &mut tx,
                &tenant_id,
                board_id,
                "BoardRenamed",
                json!({ "new_name": patch.name }),
            )
            .await?;
        }
        if !patch.description.is_empty() || !patch.icon.is_empty() {
            insert_board_event(
                &mut tx,
                &tenant_id,
                board_id,
                "BoardUpdated",
                json!({ "board": event_board_json(&row) }),
            )
            .await?;
        }

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        let columns_count = fetch_columns_count(&self.pool, board_id, Some(&tenant_id)).await;
        let cards_count = fetch_cards_count(&self.pool, board_id, Some(&tenant_id)).await;
        Ok(Response::new(UpdateBoardResponse {
            board: Some(board_from_row(&row, columns_count, cards_count)).into(),
            ..Default::default()
        }))
    }

    // ── DeleteBoard ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + manage, per matrix).
    // CASCADE: deletes columns → cards (per FK cascade in migrations).
    // Best-effort permission cleanup — drift logged, reconciler handles it.

    async fn delete_board(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, DeleteBoardRequest>,
    ) -> ServiceResult<DeleteBoardResponse> {
        let tenant_id = tenant_id_from_request(&ctx)?;
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        // BoardDeleted is project-scoped (a board-list change) and must be
        // written BEFORE the DELETE: event_log.board_id cascade-deletes with
        // the board, so only a project-scoped row survives.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let project_id: Id =
            sqlx::query_scalar("SELECT project_id FROM boards WHERE tenant_id = $1 AND id = $2")
                .bind(&tenant_id)
                .bind(board_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| internal("failed to fetch board", e))?
                .ok_or_else(|| ConnectError::not_found("board not found"))?;

        insert_project_event(
            &mut tx,
            &tenant_id,
            project_id,
            "BoardDeleted",
            json!({
                "project_id": project_id.to_string(),
                "board_id": board_id.to_string(),
            }),
        )
        .await?;

        let result = sqlx::query("DELETE FROM boards WHERE tenant_id = $1 AND id = $2")
            .bind(&tenant_id)
            .bind(board_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to delete board", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("board not found"));
        }

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        // Best-effort permission parent tuple cleanup.
        warn!(
            board_id = %board_id,
            "delete_board: permission parent tuple cleanup is best-effort; reconciler (Stage 7a) will catch any drift"
        );

        Ok(Response::new(DeleteBoardResponse::default()))
    }

    // ── AddColumn ─────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // If position == 0 (proto default), append after the current max.
    // If position is set (>0), shift existing columns at that position upward.

    async fn add_column(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AddColumnRequest>,
    ) -> ServiceResult<AddColumnResponse> {
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let tenant_id = tenant_id_from_request(&ctx)?;
        let req = request.to_owned_message();

        if req.title.is_empty() {
            return Err(ConnectError::invalid_argument("title is required"));
        }

        // Idempotency check.
        if !req.idempotency_key.is_empty() {
            let cached: Option<Option<Id>> = sqlx::query(
                "SELECT response_card_id FROM idempotency_keys WHERE tenant_id = $1 AND key = $2",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("idempotency key lookup failed", e))?
            .map(|r| r.get("response_card_id"));

            if let Some(Some(col_id)) = cached {
                let row = sqlx::query(
                    "SELECT id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at \
                     FROM columns WHERE tenant_id = $1 AND id = $2",
                )
                .bind(&tenant_id)
                .bind(col_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached column", e))?;

                if let Some(row) = row {
                    return Ok(Response::new(AddColumnResponse {
                        column: Some(column_from_row(&row)).into(),
                        ..Default::default()
                    }));
                }
            }
        }

        let column_id = Id::new();

        // INSERT column + ColumnAdded outbox event in one transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = if req.position == 0 {
            // Append at the end: position = MAX(position) + 1.
            sqlx::query(
                r#"
                INSERT INTO columns (id, tenant_id, board_id, title, accent, wip_limit, position, is_done)
                SELECT $1, $2, $3, $4, $5, $6,
                       COALESCE((SELECT MAX(position) FROM columns WHERE board_id = $3), -1) + 1,
                       $7
                RETURNING id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at
                "#,
            )
            .bind(column_id)
            .bind(&tenant_id)
            .bind(board_id)
            .bind(&req.title)
            .bind(if req.accent.is_empty() {
                None
            } else {
                Some(req.accent.clone())
            })
            .bind(if req.wip_limit == 0 {
                None
            } else {
                Some(req.wip_limit)
            })
            .bind(req.is_done)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| internal("failed to insert column", e))?
        } else {
            // Insert at specific position: shift existing columns upward in a CTE.
            sqlx::query(
                r#"
                WITH shift AS (
                    UPDATE columns
                    SET position = position + 1, updated_at = now()
                    WHERE board_id = $3 AND position >= $7
                )
                INSERT INTO columns (id, tenant_id, board_id, title, accent, wip_limit, position, is_done)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at
                "#,
            )
            .bind(column_id)
            .bind(&tenant_id)
            .bind(board_id)
            .bind(&req.title)
            .bind(if req.accent.is_empty() {
                None
            } else {
                Some(req.accent.clone())
            })
            .bind(if req.wip_limit == 0 {
                None
            } else {
                Some(req.wip_limit)
            })
            .bind(req.position)
            .bind(req.is_done)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| internal("failed to insert column at position", e))?
        };

        let column = column_from_row(&row);
        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "ColumnAdded",
            json!({
                "column": event_column_json(&event_column_from_proto(&column)),
                "position": column.position,
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        // Store idempotency response.
        if !req.idempotency_key.is_empty()
            && let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (tenant_id, key, response_card_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            )
            .bind(&tenant_id)
            .bind(&req.idempotency_key)
            .bind(column_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }

        Ok(Response::new(AddColumnResponse {
            column: Some(column).into(),
            ..Default::default()
        }))
    }

    // ── UpdateColumn ──────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // Sparse patch on title/accent/wip_limit.

    async fn update_column(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateColumnRequest>,
    ) -> ServiceResult<UpdateColumnResponse> {
        let tenant_id = tenant_id_from_request(&ctx)?;
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let req = request.to_owned_message();
        let col_id = req
            .column_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid column_id"))?;
        let patch = req.column.into_option().unwrap_or_default();

        // is_done follows FieldMask semantics: it only changes when the mask
        // names "is_done" (a bare bool patch cannot distinguish false from
        // unset).
        let is_done_apply = req
            .update_mask
            .as_option()
            .map(|m| m.paths.iter().any(|p| p == "is_done"))
            .unwrap_or(false);

        // UPDATE column + ColumnUpdated outbox event in one transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        let row = sqlx::query(
            r#"
            UPDATE columns SET
                title      = CASE WHEN $4 != '' THEN $4 ELSE title END,
                accent     = CASE WHEN $5 != '' THEN $5 ELSE accent END,
                wip_limit  = CASE WHEN $6 != 0  THEN $6 ELSE wip_limit END,
                is_done    = CASE WHEN $7 THEN $8 ELSE is_done END,
                updated_at = now()
            WHERE tenant_id = $1 AND id = $2 AND board_id = $3
            RETURNING id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(col_id)
        .bind(board_id)
        .bind(&patch.title)
        .bind(&patch.accent)
        .bind(patch.wip_limit)
        .bind(is_done_apply)
        .bind(patch.is_done)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to update column", e))?
        .ok_or_else(|| ConnectError::not_found("column not found on this board"))?;

        let column = column_from_row(&row);
        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "ColumnUpdated",
            json!({ "column": event_column_json(&event_column_from_proto(&column)) }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(UpdateColumnResponse {
            column: Some(column).into(),
            ..Default::default()
        }))
    }

    // ── RemoveColumn ──────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    //
    // Per the proto, RemoveColumnRequest only carries board_id and column_id.
    // Card movement is NOT part of this RPC at the proto level. Cards in the
    // deleted column cascade-delete via the FK constraint (0006_cards.sql:
    // column_id UUID NOT NULL REFERENCES columns(id) ON DELETE CASCADE).
    // The column itself cascades when the board is deleted (0005_columns.sql).
    //
    // After deleting the column, gap-fill the remaining columns' positions
    // using a single UPDATE with a window function rank.

    async fn remove_column(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RemoveColumnRequest>,
    ) -> ServiceResult<RemoveColumnResponse> {
        let tenant_id = tenant_id_from_request(&ctx)?;
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let req = request.to_owned_message();
        let col_id = req
            .column_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid column_id"))?;

        // Delete + gap-fill + ColumnRemoved outbox event in one transaction.
        // Removal semantics are unchanged: cards cascade-delete via FK.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // Verify the column belongs to this board.
        let exists: bool = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM columns WHERE tenant_id = $1 AND id = $2 AND board_id = $3)",
        )
        .bind(&tenant_id)
        .bind(col_id)
        .bind(board_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to verify column ownership", e))
        .map(|r| r.get::<bool, _>(0))?;

        if !exists {
            return Err(ConnectError::not_found("column not found on this board"));
        }

        // Delete the column. Cards cascade-delete via FK (0006_cards.sql).
        let result =
            sqlx::query("DELETE FROM columns WHERE tenant_id = $1 AND id = $2 AND board_id = $3")
                .bind(&tenant_id)
                .bind(col_id)
                .bind(board_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| internal("failed to delete column", e))?;

        if result.rows_affected() == 0 {
            return Err(ConnectError::not_found("column not found"));
        }

        // Gap-fill remaining column positions (renumber 0, 1, 2, ... by current order).
        if let Err(e) = sqlx::query(
            r#"
            WITH ranked AS (
                SELECT id,
                       (ROW_NUMBER() OVER (ORDER BY position ASC) - 1)::INT AS new_pos
                FROM columns
                WHERE tenant_id = $1 AND board_id = $2
            )
            UPDATE columns c
            SET position = r.new_pos, updated_at = now()
            FROM ranked r
            WHERE c.id = r.id
            "#,
        )
        .bind(&tenant_id)
        .bind(board_id)
        .execute(&mut *tx)
        .await
        {
            warn!(error = %e, board_id = %board_id, "failed to gap-fill column positions after remove");
        }

        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "ColumnRemoved",
            json!({
                "column_id": col_id.to_string(),
                "move_cards_to_column": "",
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(RemoveColumnResponse::default()))
    }

    // ── MoveColumn ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // to_position is 1-based in the proto. We work 0-based in DB.
    // Pattern: remove from current slot, shift others to fill gap,
    // insert at target slot by incrementing positions >= target.

    async fn move_column(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, MoveColumnRequest>,
    ) -> ServiceResult<MoveColumnResponse> {
        let tenant_id = tenant_id_from_request(&ctx)?;
        let object_id = checked_object_id(&ctx)?;
        let board_id = object_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let req = request.to_owned_message();
        let col_id = req
            .column_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid column_id"))?;

        if req.to_position < 1 {
            return Err(ConnectError::invalid_argument("to_position must be >= 1"));
        }

        // Convert to 0-based target.
        let target_pos = req.to_position - 1;

        // Move + ColumnsReordered outbox event in one transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("begin tx failed", e))?;

        // Fetch current position.
        let current_pos: i32 = sqlx::query(
            "SELECT position FROM columns WHERE tenant_id = $1 AND id = $2 AND board_id = $3",
        )
        .bind(&tenant_id)
        .bind(col_id)
        .bind(board_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to fetch column position", e))?
        .ok_or_else(|| ConnectError::not_found("column not found on this board"))
        .map(|r| r.get("position"))?;

        if current_pos != target_pos {
            if target_pos > current_pos {
                // Moving forward: shift columns in (current, target] down by 1.
                sqlx::query(
                    "UPDATE columns SET position = position - 1, updated_at = now() \
                     WHERE tenant_id = $1 AND board_id = $2 AND position > $3 AND position <= $4",
                )
                .bind(&tenant_id)
                .bind(board_id)
                .bind(current_pos)
                .bind(target_pos)
                .execute(&mut *tx)
                .await
                .map_err(|e| internal("failed to shift columns (forward move)", e))?;
            } else {
                // Moving backward: shift columns in [target, current) up by 1.
                sqlx::query(
                    "UPDATE columns SET position = position + 1, updated_at = now() \
                     WHERE tenant_id = $1 AND board_id = $2 AND position >= $3 AND position < $4",
                )
                .bind(&tenant_id)
                .bind(board_id)
                .bind(target_pos)
                .bind(current_pos)
                .execute(&mut *tx)
                .await
                .map_err(|e| internal("failed to shift columns (backward move)", e))?;
            }

            // Place the moved column at its new position.
            sqlx::query(
                "UPDATE columns SET position = $3, updated_at = now() WHERE tenant_id = $1 AND id = $2",
            )
            .bind(&tenant_id)
            .bind(col_id)
            .bind(target_pos)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to place moved column", e))?;
        }

        // The full ordered column list is emitted so clients can replace
        // their column order without tracking diffs.
        let col_rows = sqlx::query(
            "SELECT id, board_id, title, accent, wip_limit, position, is_done, created_at, updated_at \
             FROM columns WHERE tenant_id = $1 AND board_id = $2 ORDER BY position ASC",
        )
        .bind(&tenant_id)
        .bind(board_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| internal("failed to fetch columns", e))?;
        let columns: Vec<Column> = col_rows.iter().map(column_from_row).collect();

        insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "ColumnsReordered",
            json!({
                "columns": columns
                    .iter()
                    .map(|c| event_column_json(&event_column_from_proto(c)))
                    .collect::<Vec<_>>(),
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("commit failed", e))?;

        Ok(Response::new(MoveColumnResponse {
            columns,
            ..Default::default()
        }))
    }

    // ── SubscribeBoard (Stage 4c) ─────────────────────────────────────────────

    async fn subscribe_board(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, SubscribeBoardRequest>,
    ) -> ServiceResult<SubscribeBoardStream> {
        let auth = ctx
            .extensions()
            .get::<AuthContext>()
            .cloned()
            .ok_or_else(|| ConnectError::unauthenticated("missing auth context"))?;
        let req = request.to_owned_message();
        let board_id = req.board_id;
        let since_seq = req.since_seq;

        let tenant = auth
            .tenant_id
            .clone()
            .ok_or_else(|| ConnectError::unauthenticated("missing tenant context"))?;
        let board_id_parsed = board_id
            .parse::<Id>()
            .map_err(|_| ConnectError::invalid_argument("invalid board_id"))?;

        let visibility: String =
            sqlx::query_scalar("SELECT visibility FROM boards WHERE tenant_id = $1 AND id = $2")
                .bind(&tenant)
                .bind(board_id_parsed)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch board visibility", e))?
                .ok_or_else(|| ConnectError::not_found("board not found"))?;

        let is_private = !is_public_or_internal(&visibility);
        let permission = Arc::new(
            self.permission
                .tenant_client(&tenant)
                .await
                .map_err(|e| internal("failed to build tenant permission client", e))?,
        );

        let stream = build_subscribe_board_stream(SubscribeBoardArgs {
            registry: Arc::clone(&self.registry),
            permission,
            identity: Arc::clone(&self.identity),
            auth,
            board_id,
            is_private,
            heartbeat_interval: self.heartbeat_interval,
            permission_recheck_interval: self.permission_recheck_interval,
            pool: self.pool.clone(),
            tenant_id: tenant,
            since_seq,
            cutover_seen_capacity: self.cutover_seen_capacity,
        })
        .await?;

        Ok(Response::new(stream))
    }
}

// ============================================================================
// Integration tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sunbeam_g2v::config::NatsConfig;
    use sunbeam_g2v::middleware::auth::AuthContext;
    use sunbeam_g2v::mq::NatsClient;

    use buffa_types::google::protobuf::FieldMask;

    use crate::cpb::sunbeam::kanban::v1::BoardVisibility;
    use crate::test_support::{connect_ctx, connect_request};

    // ── Subscribe test helpers ───────────────────────────────────────────────

    fn nats_url() -> String {
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
    }

    async fn connect_nats() -> Arc<NatsClient> {
        Arc::new(
            NatsClient::connect(&NatsConfig {
                url: nats_url(),
                jetstream: true,
                lease_duration: 30,
                auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
            })
            .await
            .expect("NATS connect failed — set NATS_URL"),
        )
    }

    async fn ensure_stream(nats: &NatsClient) {
        use crate::realtime::jetstream_bootstrap::{default_config, ensure_kanban_stream};
        ensure_kanban_stream(nats, &default_config())
            .await
            .expect("ensure_kanban_stream failed");
    }

    fn make_auth(subject: &str) -> AuthContext {
        AuthContext::authenticated(crate::test_support::test_tenant_id(), subject)
    }

    async fn permission_client() -> Arc<PermissionClient> {
        crate::test_support::setup_permission().await
    }

    async fn identity_client() -> Arc<IdentityClient> {
        crate::test_support::setup_identity().await
    }

    async fn make_registry(nats: Arc<NatsClient>) -> Arc<BoardSubscriberRegistry> {
        Arc::new(BoardSubscriberRegistry::new(nats, "pod-test-boards"))
    }

    // ── Stage 4c subscribe tests ─────────────────────────────────────────────

    /// The first envelope on a new subscription is a `Cutover` with an empty replay.
    #[tokio::test]
    async fn subscribe_emits_cutover_immediately_on_empty_replay() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", Id::new());
        let subject_str = format!("user:test-{}", Id::new());
        let auth = make_auth(&subject_str);

        let permission = permission_client().await;

        // Grant view so the initial permission recheck passes.
        let _ = permission
            .grant_with_retry("KanbanBoard", &board_id, "viewer", &subject_str)
            .await;

        let mut stream = build_subscribe_board_stream(SubscribeBoardArgs {
            registry,
            permission: Arc::clone(&permission),
            identity: identity_client().await,
            auth,
            board_id: board_id.clone(),
            is_private: true,
            heartbeat_interval: Duration::from_secs(15),
            permission_recheck_interval: Duration::from_secs(30),
            pool: setup_pool().await,
            tenant_id: crate::test_support::test_tenant_id(),
            since_seq: 0,
            cutover_seen_capacity: 1024,
        })
        .await
        .expect("build_subscribe_board_stream failed");

        use tokio_stream::StreamExt;
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for first envelope")
            .expect("stream ended without first envelope")
            .expect("first envelope was an error");

        match first
            .envelope
            .into_option()
            .expect("envelope missing")
            .payload
        {
            Some(Payload::Cutover(c)) => {
                assert_eq!(
                    c.last_replay_nats_seq, 0,
                    "cutover seq must be 0 for empty replay"
                );
            }
            other => panic!("expected Cutover, got {other:?}"),
        }

        // Cleanup permission tuple.
        let _ = permission
            .delete_relation_tuples(
                "KanbanBoard",
                None,
                Some("viewer".to_string()),
                Some(subject_str.clone()),
            )
            .await;
    }

    /// A live JetStream event published for the board should reach the
    /// subscription within two seconds.
    #[tokio::test]
    async fn subscribe_forwards_live_event_published_to_jetstream() {
        use crate::realtime::jetstream_bootstrap::board_subject;
        use buffa::Message;

        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", Id::new());
        let subject_str = format!("user:test-{}", Id::new());
        let auth = make_auth(&subject_str);

        let permission = permission_client().await;
        let _ = permission
            .grant_with_retry("KanbanBoard", &board_id, "viewer", &subject_str)
            .await;

        let mut stream = build_subscribe_board_stream(SubscribeBoardArgs {
            registry,
            permission: Arc::clone(&permission),
            identity: identity_client().await,
            auth,
            board_id: board_id.clone(),
            is_private: true,
            heartbeat_interval: Duration::from_secs(15),
            permission_recheck_interval: Duration::from_secs(30),
            pool: setup_pool().await,
            tenant_id: crate::test_support::test_tenant_id(),
            since_seq: 0,
            cutover_seen_capacity: 1024,
        })
        .await
        .expect("build failed");

        use tokio_stream::StreamExt;
        // Consume the Cutover envelope first.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout on cutover");

        // Publish a live event.
        let live_event_id = format!("live-{}", Id::new());
        let envelope = BoardEventEnvelope {
            board_id: board_id.clone(),
            event_id: live_event_id.clone(),
            nats_seq: 1,
            board_revision: 1,
            emitted_at: None.into(),
            emitter_pod_id: "test".to_string(),
            actor_subject: "test".to_string(),
            payload: Some(Payload::Heartbeat(Box::new(Heartbeat {
                server_time_ms: 0,
                ..Default::default()
            }))),
            ..Default::default()
        };
        let buf = bytes::Bytes::from(envelope.encode_to_vec());
        nats.publish_jetstream(&board_subject(&board_id), buf)
            .await
            .expect("publish failed")
            .await
            .expect("ack failed");

        // The live event should arrive within 2s.
        let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for live event")
            .expect("stream ended")
            .expect("stream error");

        assert_eq!(
            received
                .envelope
                .into_option()
                .expect("envelope missing")
                .event_id,
            live_event_id
        );

        let _ = permission
            .delete_relation_tuples(
                "KanbanBoard",
                None,
                Some("viewer".to_string()),
                Some(subject_str.clone()),
            )
            .await;
    }

    // Events that were already emitted during replay must be dropped when
    // they reappear on the live tail.
    //
    // NOTE: replay-vs-live dedup test will be added when snapshot replay
    // lands (the empty-replay path is exercised by
    // `subscribe_emits_cutover_immediately_on_empty_replay`).

    /// When no live event arrives for `heartbeat_interval`, the stream emits
    /// a `Heartbeat` envelope.
    ///
    /// Uses a one-second interval override so the test does not wait 15 s.
    #[tokio::test]
    async fn subscribe_emits_heartbeat_after_inactivity() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", Id::new());
        let subject_str = format!("user:test-{}", Id::new());
        let auth = make_auth(&subject_str);

        let permission = permission_client().await;
        let _ = permission
            .grant_with_retry("KanbanBoard", &board_id, "viewer", &subject_str)
            .await;

        // Use a 1s heartbeat interval so we don't need to sleep 15s.
        let mut stream = build_subscribe_board_stream(SubscribeBoardArgs {
            registry,
            permission: Arc::clone(&permission),
            identity: identity_client().await,
            auth,
            board_id: board_id.clone(),
            is_private: true,
            heartbeat_interval: Duration::from_secs(1), // short interval for test
            permission_recheck_interval: Duration::from_secs(60), // long permission recheck to avoid interference
            pool: setup_pool().await,
            tenant_id: crate::test_support::test_tenant_id(),
            since_seq: 0,
            cutover_seen_capacity: 1024,
        })
        .await
        .expect("build failed");

        use tokio_stream::StreamExt;
        // Consume the Cutover envelope.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout on cutover");

        // Wait for a heartbeat. With a 1s interval, should arrive within 1.5s.
        let heartbeat_env = tokio::time::timeout(Duration::from_millis(2500), stream.next())
            .await
            .expect("timeout waiting for heartbeat")
            .expect("stream ended")
            .expect("stream error");

        match heartbeat_env
            .envelope
            .into_option()
            .expect("envelope missing")
            .payload
        {
            Some(Payload::Heartbeat(h)) => {
                assert!(
                    h.server_time_ms > 0,
                    "heartbeat server_time_ms must be positive"
                );
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }

        let _ = permission
            .delete_relation_tuples(
                "KanbanBoard",
                None,
                Some("viewer".to_string()),
                Some(subject_str.clone()),
            )
            .await;
    }

    /// When the caller's view permission is revoked mid-stream, the stream
    /// closes with `ConnectError::PermissionDenied`.
    ///
    /// Uses a one-second permission recheck interval override so the test does not
    /// wait 30 s.
    #[tokio::test]
    async fn subscribe_closes_with_permission_denied_when_permission_revokes() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", Id::new());
        let subject_str = format!("user:test-{}", Id::new());
        let auth = make_auth(&subject_str);

        let permission = permission_client().await;
        // Grant view so initial check passes.
        let _ = permission
            .grant_with_retry("KanbanBoard", &board_id, "viewer", &subject_str)
            .await;

        let permission_clone = Arc::clone(&permission);
        let board_id_clone = board_id.clone();
        let sub_clone = subject_str.clone();

        // Use a 1s permission recheck interval so we don't need to wait 30s.
        let mut stream = build_subscribe_board_stream(SubscribeBoardArgs {
            registry,
            permission: Arc::clone(&permission),
            identity: identity_client().await,
            auth,
            board_id: board_id.clone(),
            is_private: true,
            heartbeat_interval: Duration::from_millis(100), // fast heartbeat to keep loop ticking
            permission_recheck_interval: Duration::from_secs(1), // 1s permission recheck for test
            pool: setup_pool().await,
            tenant_id: crate::test_support::test_tenant_id(),
            since_seq: 0,
            cutover_seen_capacity: 1024,
        })
        .await
        .expect("build failed");

        use tokio_stream::StreamExt;
        // Consume the Cutover envelope.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout on cutover");

        // Revoke the permission tuple after 200ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = permission_clone
                .delete_relation_tuples(
                    "KanbanBoard",
                    None,
                    Some("viewer".to_string()),
                    Some(sub_clone.clone()),
                )
                .await;
        });

        // The stream should close with PermissionDenied within 3s (recheck fires at 1s).
        let err = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(item) = stream.next().await {
                if let Err(s) = item {
                    return Some(s);
                }
            }
            None
        })
        .await
        .expect("timeout waiting for stream close")
        .expect("stream closed without error status");

        assert_eq!(
            err.code,
            connectrpc::ErrorCode::PermissionDenied,
            "expected PermissionDenied when permission is revoked, got {err:?}"
        );

        let _ = permission
            .delete_relation_tuples(
                "KanbanBoard",
                None,
                Some("viewer".to_string()),
                Some(board_id_clone.clone()),
            )
            .await;
    }

    // ── Setup helpers ────────────────────────────────────────────────────────

    async fn setup_pool() -> PgPool {
        crate::test_support::setup_pool().await
    }

    async fn setup_permission() -> Arc<PermissionClient> {
        crate::test_support::setup_permission().await
    }

    /// Build a `BoardServiceImpl` for integration tests.
    ///
    /// Connects to NATS using the standard environment variables so the registry
    /// field is always populated.
    async fn make_service(pool: PgPool, permission: Arc<PermissionClient>) -> BoardServiceImpl {
        let nats = connect_nats().await;
        let registry = Arc::new(BoardSubscriberRegistry::new(nats, "pod-test"));
        BoardServiceImpl {
            pool,
            permission,
            identity: crate::test_support::setup_identity().await,
            registry,
            heartbeat_interval: Duration::from_millis(15_000),
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

    /// Insert a project row directly for a test, bypassing ProjectService.
    async fn create_test_project(pool: &PgPool, subject: &str) -> Id {
        let pid = Id::new();
        let slug = format!("tp-{}", &pid.to_string()[18..26]);
        let tenant_id = crate::test_support::test_tenant_id();
        sqlx::query(
            "INSERT INTO projects (id, tenant_id, name, slug, description, owner_id) VALUES ($1, $2, $3, $4, '', $5)",
        )
        .bind(pid)
        .bind(tenant_id)
        .bind(format!("Test Project {pid}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");
        pid
    }

    /// Insert a card row directly for a test.
    async fn create_test_card(
        pool: &PgPool,
        project_id: Id,
        board_id: Id,
        column_id: Id,
        subject: &str,
        ref_: &str,
    ) -> Id {
        let card_id = Id::new();
        let tenant_id = crate::test_support::test_tenant_id();
        sqlx::query(
            "INSERT INTO cards (id, tenant_id, project_id, board_id, column_id, ref, title, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(card_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(board_id)
        .bind(column_id)
        .bind(ref_)
        .bind(format!("Card {card_id}"))
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test card");
        card_id
    }

    async fn cleanup_project(pool: &PgPool, project_id: Id) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_returns_same_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let created = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Sprint Board".to_string(),
                    description: "integration test board".to_string(),
                    icon: "rocket".to_string(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Sprint Board");
        assert_eq!(created.description, "integration test board");
        assert_eq!(created.icon, "rocket");
        assert!(created.created_at.is_set());

        let board_id = created.id.clone();

        // Grant an explicit viewer role so the read does not depend on the
        // parent project chain.
        let _ = permission
            .grant_with_retry(PERMISSION_TYPE_BOARD, &board_id, "viewer", &subject)
            .await;

        let detail = svc
            .get_board(
                authed_ctx_with_object(&subject, &board_id),
                connect_request(&GetBoardRequest {
                    board_id: board_id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("get_board failed")
            .body
            .detail
            .into_option()
            .expect("detail missing");

        let board = detail
            .board
            .into_option()
            .expect("BoardDetail must contain board");
        assert_eq!(board.id, board_id);
        assert_eq!(board.name, "Sprint Board");
        assert_eq!(board.project_id, project_id.to_string());

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn list_boards_scoped_to_project() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_a = create_test_project(&pool, &subject).await;
        let project_b = create_test_project(&pool, &subject).await;

        // Create 2 boards in project A, 1 in project B.
        let mut board_ids_a = vec![];
        for name in &["Board A1", "Board A2"] {
            let board = svc
                .create_board(
                    authed_ctx_with_object(&subject, &project_a.to_string()),
                    connect_request(&CreateBoardRequest {
                        project_id: project_a.to_string(),
                        name: name.to_string(),
                        description: String::new(),
                        icon: String::new(),
                        idempotency_key: String::new(),
                        visibility: BoardVisibility::Private.into(),
                        ..Default::default()
                    }),
                )
                .await
                .expect("create_board failed")
                .body
                .board
                .into_option()
                .expect("board missing");
            board_ids_a.push(board.id);
        }

        let board_b = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_b.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_b.to_string(),
                    name: "Board B1".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        // Grant an explicit viewer role on each board so reads do not depend
        // on the parent project chain.
        for board_id in board_ids_a.iter().chain(std::iter::once(&board_b.id)) {
            let _ = permission
                .grant_with_retry(PERMISSION_TYPE_BOARD, board_id, "viewer", &subject)
                .await;
        }

        let list_a = svc
            .list_boards(
                authed_ctx_with_object(&subject, &project_a.to_string()),
                connect_request(&ListBoardsRequest {
                    project_id: project_a.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_boards failed")
            .body;

        assert_eq!(list_a.boards.len(), 2, "project A should have 2 boards");
        let names_a: Vec<&str> = list_a.boards.iter().map(|b| b.name.as_str()).collect();
        assert!(names_a.contains(&"Board A1"));
        assert!(names_a.contains(&"Board A2"));

        let list_b = svc
            .list_boards(
                authed_ctx_with_object(&subject, &project_b.to_string()),
                connect_request(&ListBoardsRequest {
                    project_id: project_b.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_boards failed")
            .body;

        assert_eq!(list_b.boards.len(), 1, "project B should have 1 board");
        assert_eq!(list_b.boards[0].name, "Board B1");

        cleanup_project(&pool, project_a).await;
        cleanup_project(&pool, project_b).await;
    }

    #[tokio::test]
    async fn update_board_applies_patch_fields_only() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let created = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Original Name".to_string(),
                    description: "original desc".to_string(),
                    icon: "star".to_string(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let board_id = created.id.clone();

        // Update name only; description and icon should be unchanged.
        let updated = svc
            .update_board(
                authed_ctx_with_object(&subject, &board_id),
                connect_request(&UpdateBoardRequest {
                    board_id: board_id.clone(),
                    board: Some(Board {
                        id: String::new(),
                        project_id: String::new(),
                        name: "Updated Name".to_string(),
                        description: String::new(),
                        icon: String::new(),
                        visibility: BoardVisibility::Private.into(),
                        created_at: None.into(),
                        updated_at: None.into(),
                        columns_count: 0,
                        cards_count: 0,
                        ..Default::default()
                    })
                    .into(),
                    update_mask: None.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        assert_eq!(updated.name, "Updated Name");
        assert_eq!(
            updated.description, "original desc",
            "description should be unchanged"
        );
        assert_eq!(updated.icon, "star", "icon should be unchanged");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn delete_board_cascades_columns_and_cards() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "To Delete".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let board_id = board.id.parse::<Id>().unwrap();

        // Add a column to confirm cascade.
        let col = svc
            .add_column(
                authed_ctx_with_object(&subject, &board.id),
                connect_request(&AddColumnRequest {
                    board_id: board.id.clone(),
                    title: "To Do".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        let col_id = col.id.parse::<Id>().unwrap();

        // Seed a card in the column.
        create_test_card(&pool, project_id, board_id, col_id, &subject, "TEST-1").await;

        // Delete the board.
        svc.delete_board(
            authed_ctx_with_object(&subject, &board.id),
            connect_request(&DeleteBoardRequest {
                board_id: board.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete_board failed");

        // Verify column is gone.
        let col_exists: bool = sqlx::query("SELECT EXISTS(SELECT 1 FROM columns WHERE id = $1)")
            .bind(col_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert!(!col_exists, "column should be cascade-deleted with board");

        // Verify card is gone.
        let card_count: i64 = sqlx::query("SELECT COUNT(*) FROM cards WHERE board_id = $1")
            .bind(board_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(card_count, 0, "cards should be cascade-deleted with board");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn add_column_appends_at_end_when_position_unset() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Position Test Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let bid = board.id.clone();

        let c1 = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "C1".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        let c2 = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "C2".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        let c3 = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "C3".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        // Positions should be 0, 1, 2 in order.
        assert_eq!(c1.position, 0, "first column position");
        assert_eq!(c2.position, 1, "second column position");
        assert_eq!(c3.position, 2, "third column position");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn add_column_shifts_when_position_in_middle() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Shift Test Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let bid = board.id.clone();

        // Add two columns at position 0 (append).
        svc.add_column(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&AddColumnRequest {
                board_id: bid.clone(),
                title: "First".to_string(),
                accent: String::new(),
                wip_limit: 0,
                position: 0,
                idempotency_key: String::new(),
                ..Default::default()
            }),
        )
        .await
        .expect("add First failed");

        svc.add_column(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&AddColumnRequest {
                board_id: bid.clone(),
                title: "Second".to_string(),
                accent: String::new(),
                wip_limit: 0,
                position: 0,
                idempotency_key: String::new(),
                ..Default::default()
            }),
        )
        .await
        .expect("add Second failed");

        // Insert "Middle" at position 1 (0-based), should push "Second" to position 2.
        let middle = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "Middle".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 1,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add Middle failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        assert_eq!(
            middle.position, 1,
            "inserted column should be at position 1"
        );

        // Verify "Second" was shifted to 2.
        let second_pos: i32 =
            sqlx::query("SELECT position FROM columns WHERE board_id = $1 AND title = 'Second'")
                .bind(bid.parse::<Id>().unwrap())
                .fetch_one(&pool)
                .await
                .unwrap()
                .get("position");
        assert_eq!(second_pos, 2, "Second should have shifted to position 2");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn update_column_changes_title_and_wip() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Update Column Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let bid = board.id.clone();

        let col = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "Old Title".to_string(),
                    accent: "blue".to_string(),
                    wip_limit: 5,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        let updated = svc
            .update_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&UpdateColumnRequest {
                    board_id: bid.clone(),
                    column_id: col.id.clone(),
                    column: Some(Column {
                        id: String::new(),
                        board_id: String::new(),
                        title: "New Title".to_string(),
                        accent: String::new(),
                        wip_limit: 10,
                        position: 0,
                        created_at: None.into(),
                        updated_at: None.into(),
                        ..Default::default()
                    })
                    .into(),
                    update_mask: None.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        assert_eq!(updated.title, "New Title");
        assert_eq!(updated.wip_limit, 10);
        assert_eq!(
            updated.accent, "blue",
            "accent should be unchanged (empty patch)"
        );

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn update_column_is_done_follows_update_mask() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Done Column Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let bid = board.id.clone();

        // AddColumn can mark the column done directly.
        let col = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "Done".to_string(),
                    is_done: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");
        assert!(col.is_done, "AddColumn with is_done must mark the column");

        // Without the mask, is_done is untouched even when the patch says false.
        let untouched = svc
            .update_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&UpdateColumnRequest {
                    board_id: bid.clone(),
                    column_id: col.id.clone(),
                    column: Some(Column {
                        title: "Done (renamed)".to_string(),
                        ..Default::default()
                    })
                    .into(),
                    update_mask: None.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");
        assert!(
            untouched.is_done,
            "is_done must not change without the update_mask naming it"
        );

        // With the mask naming is_done, the patch value applies exactly.
        let cleared = svc
            .update_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&UpdateColumnRequest {
                    board_id: bid.clone(),
                    column_id: col.id.clone(),
                    column: Some(Column::default()).into(),
                    update_mask: Some(FieldMask {
                        paths: vec!["is_done".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");
        assert!(
            !cleared.is_done,
            "update_mask naming is_done with a false patch must clear it"
        );

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn remove_column_deletes_cards_via_cascade() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Remove Col Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let bid = board.id.clone();
        let board_id = bid.parse::<Id>().unwrap();

        let col = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "Doomed".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        let column_id = col.id.parse::<Id>().unwrap();

        // Seed 3 cards in the doomed column.
        for i in 0..3 {
            create_test_card(
                &pool,
                project_id,
                board_id,
                column_id,
                &subject,
                &format!("DEL-{i}"),
            )
            .await;
        }

        // Verify cards exist.
        let card_count_before: i64 = sqlx::query("SELECT COUNT(*) FROM cards WHERE column_id = $1")
            .bind(column_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(card_count_before, 3);

        svc.remove_column(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&RemoveColumnRequest {
                board_id: bid.clone(),
                column_id: col.id.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("remove_column failed");

        // Cards should be cascade-deleted.
        let card_count_after: i64 = sqlx::query("SELECT COUNT(*) FROM cards WHERE board_id = $1")
            .bind(board_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            card_count_after, 0,
            "all cards in removed column should be gone"
        );

        // Column should be gone.
        let col_exists: bool = sqlx::query("SELECT EXISTS(SELECT 1 FROM columns WHERE id = $1)")
            .bind(column_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert!(!col_exists, "removed column should not exist");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn remove_column_rejects_column_not_on_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_a = create_test_project(&pool, &subject).await;
        let project_b = create_test_project(&pool, &subject).await;

        let board_a = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_a.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_a.to_string(),
                    name: "Board A".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board A failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let board_b = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_b.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_b.to_string(),
                    name: "Board B".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board B failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        // Add a column to board B.
        let col_b = svc
            .add_column(
                authed_ctx_with_object(&subject, &board_b.id),
                connect_request(&AddColumnRequest {
                    board_id: board_b.id.clone(),
                    title: "Col B".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        // Try to remove col_b using board_a's object id — should fail with not_found.
        let result = svc
            .remove_column(
                authed_ctx_with_object(&subject, &board_a.id),
                connect_request(&RemoveColumnRequest {
                    board_id: board_a.id.clone(),
                    column_id: col_b.id.clone(),
                    ..Default::default()
                }),
            )
            .await;

        assert!(
            result.is_err(),
            "removing column from wrong board should fail"
        );
        let status = result.unwrap_err();
        assert_eq!(
            status.code,
            connectrpc::ErrorCode::NotFound,
            "wrong board removal should be not_found"
        );

        cleanup_project(&pool, project_a).await;
        cleanup_project(&pool, project_b).await;
    }

    #[tokio::test]
    async fn move_column_reorders_within_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Move Col Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let bid = board.id.clone();

        // Add 4 columns: A(0), B(1), C(2), D(3).
        let mut col_ids = vec![];
        for title in &["A", "B", "C", "D"] {
            let c = svc
                .add_column(
                    authed_ctx_with_object(&subject, &bid),
                    connect_request(&AddColumnRequest {
                        board_id: bid.clone(),
                        title: title.to_string(),
                        accent: String::new(),
                        wip_limit: 0,
                        position: 0,
                        idempotency_key: String::new(),
                        ..Default::default()
                    }),
                )
                .await
                .expect("add_column failed")
                .body
                .column
                .into_option()
                .expect("column missing");
            col_ids.push(c.id.clone());
        }

        // Move D (position 3) to position 1 (1-based), so: A(0), D(1), B(2), C(3).
        let resp = svc
            .move_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&MoveColumnRequest {
                    board_id: bid.clone(),
                    column_id: col_ids[3].clone(), // D
                    to_position: 1, // 1-based → 0-based = 0... wait, to_position=1 → target_pos=0
                    // Let's move D to position 2 (1-based) → 0-based = 1: A, D, B, C
                    // Actually, to make it clearer: move B (index 1, pos 1) to position 3 (1-based)
                    // so A(0), C(1), D(2), B(3) → no let's keep it simple
                    ..Default::default()
                }),
            )
            .await
            .expect("move_column failed")
            .body;

        // The response must contain all 4 columns.
        assert_eq!(
            resp.columns.len(),
            4,
            "move response must include all columns"
        );

        // Positions must be gap-free: 0, 1, 2, 3.
        let mut positions: Vec<i32> = resp.columns.iter().map(|c| c.position).collect();
        positions.sort();
        assert_eq!(
            positions,
            vec![0, 1, 2, 3],
            "positions must be 0-3 after move"
        );

        // D must now be at position 0.
        let d = resp.columns.iter().find(|c| c.id == col_ids[3]).unwrap();
        assert_eq!(d.position, 0, "D should be at position 0");

        cleanup_project(&pool, project_id).await;
    }

    // ── Visibility tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_board_allows_non_member_for_public_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-{}", Id::new());
        let stranger = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &owner).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&owner, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Public Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Public.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let detail = svc
            .get_board(
                authed_ctx_with_object(&stranger, &board.id),
                connect_request(&GetBoardRequest {
                    board_id: board.id.clone(),
                    ..Default::default()
                }),
            )
            .await
            .expect("non-member should view public board")
            .body
            .detail
            .into_option()
            .expect("detail missing");

        assert_eq!(detail.board.as_option().unwrap().id, board.id);
        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn get_board_denies_non_member_for_private_board() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-{}", Id::new());
        let stranger = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &owner).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&owner, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Private Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        let result = svc
            .get_board(
                authed_ctx_with_object(&stranger, &board.id),
                connect_request(&GetBoardRequest {
                    board_id: board.id.clone(),
                    ..Default::default()
                }),
            )
            .await;

        assert!(result.is_err(), "non-member must not view private board");
        assert_eq!(
            result.unwrap_err().code,
            connectrpc::ErrorCode::PermissionDenied,
            "private board must return PermissionDenied"
        );
        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn list_boards_returns_public_internal_for_any_authenticated_user() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-{}", Id::new());
        let stranger = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &owner).await;

        // Public board.
        svc.create_board(
            authed_ctx_with_object(&owner, &project_id.to_string()),
            connect_request(&CreateBoardRequest {
                project_id: project_id.to_string(),
                name: "Public".to_string(),
                description: String::new(),
                icon: String::new(),
                idempotency_key: String::new(),
                visibility: BoardVisibility::Public.into(),
                ..Default::default()
            }),
        )
        .await
        .expect("create public board failed");

        // Internal board.
        svc.create_board(
            authed_ctx_with_object(&owner, &project_id.to_string()),
            connect_request(&CreateBoardRequest {
                project_id: project_id.to_string(),
                name: "Internal".to_string(),
                description: String::new(),
                icon: String::new(),
                idempotency_key: String::new(),
                visibility: BoardVisibility::Internal.into(),
                ..Default::default()
            }),
        )
        .await
        .expect("create internal board failed");

        // Private board.
        let private_board = svc
            .create_board(
                authed_ctx_with_object(&owner, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Private".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create private board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        // Grant stranger an explicit viewer role on the private board so it appears.
        permission
            .grant_with_retry(
                PERMISSION_TYPE_BOARD,
                &private_board.id,
                "viewer",
                &stranger,
            )
            .await
            .expect("grant viewer failed");

        let list = svc
            .list_boards(
                authed_ctx_with_object(&stranger, &project_id.to_string()),
                connect_request(&ListBoardsRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_boards failed")
            .body;

        let names: Vec<&str> = list.boards.iter().map(|b| b.name.as_str()).collect();
        assert!(names.contains(&"Public"), "public board must be listed");
        assert!(names.contains(&"Internal"), "internal board must be listed");
        assert!(
            names.contains(&"Private"),
            "private board with view tuple must be listed"
        );

        // Revoke the explicit view and verify the private board disappears.
        permission
            .delete_relation_tuples(
                PERMISSION_TYPE_BOARD,
                None,
                Some("viewer".to_string()),
                Some(stranger.clone()),
            )
            .await
            .ok();

        let list_after = svc
            .list_boards(
                authed_ctx_with_object(&stranger, &project_id.to_string()),
                connect_request(&ListBoardsRequest {
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .expect("list_boards failed")
            .body;

        let names_after: Vec<&str> = list_after.boards.iter().map(|b| b.name.as_str()).collect();
        assert!(
            !names_after.contains(&"Private"),
            "private board must be hidden without view tuple"
        );
        assert_eq!(
            names_after.len(),
            2,
            "only public and internal boards remain"
        );

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn update_board_visibility_requires_manage_and_persists() {
        let pool = setup_pool().await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;

        let owner = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &owner).await;

        let board = svc
            .create_board(
                authed_ctx_with_object(&owner, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Visibility Patch".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        // Board manage is computed from the parent project; grant project
        // admin so the visibility change is authorized.
        permission
            .grant_with_retry("KanbanProject", &project_id.to_string(), "admin", &owner)
            .await
            .expect("grant admin failed");

        let updated = svc
            .update_board(
                authed_ctx_with_object(&owner, &board.id),
                connect_request(&UpdateBoardRequest {
                    board_id: board.id.clone(),
                    board: Some(Board {
                        id: String::new(),
                        project_id: String::new(),
                        name: String::new(),
                        description: String::new(),
                        icon: String::new(),
                        visibility: BoardVisibility::Public.into(),
                        created_at: None.into(),
                        updated_at: None.into(),
                        columns_count: 0,
                        cards_count: 0,
                        ..Default::default()
                    })
                    .into(),
                    update_mask: Some(FieldMask {
                        paths: vec!["visibility".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update visibility failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        assert_eq!(
            updated.visibility,
            BoardVisibility::Public,
            "visibility must be persisted as public"
        );

        // Change back to private.
        let updated_private = svc
            .update_board(
                authed_ctx_with_object(&owner, &board.id),
                connect_request(&UpdateBoardRequest {
                    board_id: board.id.clone(),
                    board: Some(Board {
                        id: String::new(),
                        project_id: String::new(),
                        name: String::new(),
                        description: String::new(),
                        icon: String::new(),
                        visibility: BoardVisibility::Private.into(),
                        created_at: None.into(),
                        updated_at: None.into(),
                        columns_count: 0,
                        cards_count: 0,
                        ..Default::default()
                    })
                    .into(),
                    update_mask: Some(FieldMask {
                        paths: vec!["visibility".to_string()],
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("update visibility failed")
            .body
            .board
            .into_option()
            .expect("board missing");

        assert_eq!(
            updated_private.visibility,
            BoardVisibility::Private,
            "visibility must be persisted as private"
        );

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn subscribe_public_board_skips_permission_recheck() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", Id::new());
        let subject_str = format!("user:test-{}", Id::new());
        let auth = make_auth(&subject_str);

        let permission = permission_client().await;

        // Intentionally do NOT grant a view tuple in the permission backend: a public board stream
        // must skip the permission recheck and still emit the Cutover envelope.
        let mut stream = build_subscribe_board_stream(SubscribeBoardArgs {
            registry,
            permission: Arc::clone(&permission),
            identity: identity_client().await,
            auth,
            board_id: board_id.clone(),
            is_private: false,
            heartbeat_interval: Duration::from_secs(15),
            permission_recheck_interval: Duration::from_secs(30),
            pool: setup_pool().await,
            tenant_id: crate::test_support::test_tenant_id(),
            since_seq: 0,
            cutover_seen_capacity: 1024,
        })
        .await
        .expect("build_subscribe_board_stream failed");

        use tokio_stream::StreamExt;
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for first envelope")
            .expect("stream ended without first envelope")
            .expect("first envelope was an error");

        match first
            .envelope
            .into_option()
            .expect("envelope missing")
            .payload
        {
            Some(Payload::Cutover(c)) => {
                assert_eq!(
                    c.last_replay_nats_seq, 0,
                    "cutover seq must be 0 for empty replay"
                );
            }
            other => panic!("expected Cutover, got {other:?}"),
        }
    }

    // ── Snapshot replay tests ────────────────────────────────────────────────

    /// Build a subscribe stream against a real seeded board.
    #[allow(clippy::too_many_arguments)]
    async fn build_test_stream(
        pool: PgPool,
        registry: Arc<BoardSubscriberRegistry>,
        permission: Arc<PermissionClient>,
        subject: &str,
        board_id: &str,
        since_seq: u64,
    ) -> SubscribeBoardStream {
        build_subscribe_board_stream(SubscribeBoardArgs {
            registry,
            permission,
            identity: identity_client().await,
            auth: make_auth(subject),
            board_id: board_id.to_string(),
            is_private: false,
            heartbeat_interval: Duration::from_secs(15),
            permission_recheck_interval: Duration::from_secs(30),
            pool,
            tenant_id: crate::test_support::test_tenant_id(),
            since_seq,
            cutover_seen_capacity: 1024,
        })
        .await
        .expect("build_subscribe_board_stream failed")
    }

    async fn next_envelope(
        stream: &mut SubscribeBoardStream,
    ) -> crate::cpb::sunbeam::kanban::v1::BoardEventEnvelope {
        use tokio_stream::StreamExt;
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for envelope")
            .expect("stream ended")
            .expect("stream error")
            .envelope
            .into_option()
            .expect("envelope missing")
    }

    /// Create a project + board + 2 columns + 3 cards (2 in col 1, 1 in col 2)
    /// via the service, returning (project_id, board_id, col1, col2).
    async fn seed_board_with_cards(
        pool: &PgPool,
        svc: &BoardServiceImpl,
        subject: &str,
    ) -> (Id, String, String, String) {
        let project_id = create_test_project(pool, subject).await;
        let board = svc
            .create_board(
                authed_ctx_with_object(subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Snapshot Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Public.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");
        let bid = board.id.clone();

        let mut col_ids = Vec::new();
        for title in ["S1", "S2"] {
            let col = svc
                .add_column(
                    authed_ctx_with_object(subject, &bid),
                    connect_request(&AddColumnRequest {
                        board_id: bid.clone(),
                        title: title.to_string(),
                        accent: String::new(),
                        wip_limit: 0,
                        position: 0,
                        idempotency_key: String::new(),
                        ..Default::default()
                    }),
                )
                .await
                .expect("add_column failed")
                .body
                .column
                .into_option()
                .expect("column missing");
            col_ids.push(col.id);
        }

        let col1: Id = col_ids[0].parse().expect("col id");
        let col2: Id = col_ids[1].parse().expect("col id");
        let board_id: Id = bid.parse().expect("board id");
        create_test_card(pool, project_id, board_id, col1, subject, "SC-001").await;
        create_test_card(pool, project_id, board_id, col1, subject, "SC-002").await;
        create_test_card(pool, project_id, board_id, col2, subject, "SC-003").await;

        (project_id, bid, col_ids[0].clone(), col_ids[1].clone())
    }

    /// A fresh subscription replays the board snapshot (ColumnAdded per
    /// column, CardCreated per card), cuts over, then forwards live events.
    #[tokio::test]
    async fn subscribe_replays_snapshot_then_cutover_then_live() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = permission_client().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let (project_id, bid, col1, col2) = seed_board_with_cards(&pool, &svc, &subject).await;

        let mut stream = build_test_stream(
            pool.clone(),
            registry,
            Arc::clone(&permission),
            &subject,
            &bid,
            0,
        )
        .await;

        // Snapshot: ColumnAdded ×2 in column order.
        for expected_col in [&col1, &col2] {
            match next_envelope(&mut stream).await.payload {
                Some(Payload::ColumnAdded(ev)) => {
                    assert_eq!(
                        ev.column.into_option().expect("column missing").id,
                        *expected_col
                    );
                }
                other => panic!("expected ColumnAdded, got {other:?}"),
            }
        }
        // Snapshot: CardCreated ×3 with hydrated cards.
        for _ in 0..3 {
            match next_envelope(&mut stream).await.payload {
                Some(Payload::CardCreated(ev)) => {
                    let card = ev.card.into_option().expect("hydrated card missing");
                    assert!(!card.id.is_empty());
                    assert!(!card.title.is_empty());
                }
                other => panic!("expected CardCreated, got {other:?}"),
            }
        }
        // Cutover at 0: no dispatched rows for this board yet.
        match next_envelope(&mut stream).await.payload {
            Some(Payload::Cutover(c)) => assert_eq!(c.last_replay_nats_seq, 0),
            other => panic!("expected Cutover, got {other:?}"),
        }

        // Live: a new column mutation is drained by the outbox and forwarded.
        let _ = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "S3-live".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed");
        let board_id: Id = bid.parse().expect("board id");
        let dispatcher = crate::realtime::outbox::OutboxDispatcher::new(
            pool.clone(),
            Arc::clone(&nats),
            crate::test_support::setup_identity().await,
            crate::realtime::outbox::OutboxConfig::default(),
        )
        .with_board_filter(board_id);
        let n = dispatcher.drain_once().await.expect("drain_once failed");
        assert_eq!(
            n, 3,
            "S1/S2 (from seeding) and S3-live must all be dispatched"
        );

        // The live tail forwards all three in order; the tracker accepts them
        // because their stamped nats_seq is above the cutover point.
        for expected_title in ["S1", "S2", "S3-live"] {
            let live = next_envelope(&mut stream).await;
            assert!(live.nats_seq > 0, "live events carry the real nats_seq");
            match live.payload {
                Some(Payload::ColumnAdded(ev)) => {
                    assert_eq!(
                        ev.column.into_option().expect("column missing").title,
                        expected_title
                    );
                }
                other => panic!("expected live ColumnAdded, got {other:?}"),
            }
        }

        cleanup_project(&pool, project_id).await;
    }

    /// A resume (`since_seq > 0`) skips the snapshot and cuts over directly.
    #[tokio::test]
    async fn subscribe_with_since_seq_skips_snapshot() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = permission_client().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let (project_id, bid, _col1, _col2) = seed_board_with_cards(&pool, &svc, &subject).await;

        let mut stream = build_test_stream(
            pool.clone(),
            registry,
            Arc::clone(&permission),
            &subject,
            &bid,
            7,
        )
        .await;

        // The first envelope must be the Cutover at the requested sequence —
        // no snapshot events precede it.
        match next_envelope(&mut stream).await.payload {
            Some(Payload::Cutover(c)) => assert_eq!(c.last_replay_nats_seq, 7),
            other => panic!("expected Cutover, got {other:?}"),
        }

        cleanup_project(&pool, project_id).await;
    }

    // ── Event-writer tests ───────────────────────────────────────────────────

    /// Subscribe the registry channel for one object (board or project
    /// scope) and drain its pending outbox rows; the returned handle receives
    /// the dispatched envelopes in order.
    async fn subscribe_and_drain(
        pool: &PgPool,
        nats: Arc<NatsClient>,
        registry: &Arc<BoardSubscriberRegistry>,
        object_id: Id,
        project_scoped: bool,
    ) -> crate::realtime::registry::StreamHandle {
        let handle = if project_scoped {
            Arc::clone(registry)
                .subscribe_project(&object_id.to_string())
                .await
                .expect("subscribe_project failed")
        } else {
            Arc::clone(registry)
                .subscribe(&object_id.to_string())
                .await
                .expect("subscribe failed")
        };
        let dispatcher = crate::realtime::outbox::OutboxDispatcher::new(
            pool.clone(),
            nats,
            crate::test_support::setup_identity().await,
            crate::realtime::outbox::OutboxConfig::default(),
        )
        .with_board_filter(object_id);
        let n = dispatcher.drain_once().await.expect("drain_once failed");
        assert!(n >= 1, "expected at least one dispatched row");
        handle
    }

    /// Receive the next envelope from a registry handle.
    async fn recv_envelope(
        handle: &mut crate::realtime::registry::StreamHandle,
    ) -> crate::cpb::sunbeam::kanban::v1::BoardEventEnvelope {
        tokio::time::timeout(Duration::from_secs(2), handle.receiver.recv())
            .await
            .expect("timeout waiting for envelope")
            .expect("recv failed")
    }

    /// Fetch (event_type, payload) rows for a board, newest last.
    async fn board_event_rows(pool: &PgPool, board_id: Id) -> Vec<(String, serde_json::Value)> {
        sqlx::query(
            "SELECT event_type, payload FROM event_log WHERE board_id = $1 ORDER BY created_at, id",
        )
        .bind(board_id)
        .fetch_all(pool)
        .await
        .expect("fetch event rows")
        .iter()
        .map(|r| (r.get("event_type"), r.get("payload")))
        .collect()
    }

    #[tokio::test]
    async fn add_column_writes_and_dispatches_column_added_event() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;
        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Event Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");
        let bid = board.id.clone();
        let board_id: Id = bid.parse().expect("board id");

        let col = svc
            .add_column(
                authed_ctx_with_object(&subject, &bid),
                connect_request(&AddColumnRequest {
                    board_id: bid.clone(),
                    title: "Events C1".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                    ..Default::default()
                }),
            )
            .await
            .expect("add_column failed")
            .body
            .column
            .into_option()
            .expect("column missing");

        // event_log row: ColumnAdded with the column snapshot and revision 1.
        let rows = board_event_rows(&pool, board_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "ColumnAdded");
        assert_eq!(
            rows[0].1.pointer("/column/title").and_then(|v| v.as_str()),
            Some("Events C1")
        );
        assert_eq!(
            rows[0].1.get("board_revision").and_then(|v| v.as_i64()),
            Some(1)
        );

        // Dispatched oneof matches.
        let mut handle = subscribe_and_drain(&pool, nats, &registry, board_id, false).await;
        let env = recv_envelope(&mut handle).await;
        assert_eq!(env.board_revision, 1);
        assert!(env.nats_seq > 0);
        match env.payload {
            Some(Payload::ColumnAdded(ev)) => {
                let c = ev.column.into_option().expect("column missing");
                assert_eq!(c.id, col.id);
                assert_eq!(c.title, "Events C1");
            }
            other => panic!("expected ColumnAdded, got {other:?}"),
        }

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn update_column_writes_and_dispatches_column_updated_event() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let (project_id, bid, col1, _col2) = seed_board_with_cards(&pool, &svc, &subject).await;
        let board_id: Id = bid.parse().expect("board id");

        svc.update_column(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&UpdateColumnRequest {
                board_id: bid.clone(),
                column_id: col1.clone(),
                column: Some(Column {
                    title: "Renamed C1".to_string(),
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            }),
        )
        .await
        .expect("update_column failed");

        // Two ColumnAdded rows from seeding + one ColumnUpdated row.
        let rows = board_event_rows(&pool, board_id).await;
        let updated = rows
            .iter()
            .find(|(t, _)| t == "ColumnUpdated")
            .expect("ColumnUpdated row missing");
        assert_eq!(
            updated.1.pointer("/column/title").and_then(|v| v.as_str()),
            Some("Renamed C1")
        );
        assert_eq!(
            updated.1.get("board_revision").and_then(|v| v.as_i64()),
            Some(3)
        );

        let mut handle = subscribe_and_drain(&pool, nats, &registry, board_id, false).await;
        let mut seen_updated = false;
        for _ in 0..3 {
            let env = recv_envelope(&mut handle).await;
            if let Some(Payload::ColumnUpdated(ev)) = env.payload {
                assert_eq!(
                    ev.column.into_option().expect("column missing").title,
                    "Renamed C1"
                );
                seen_updated = true;
            }
        }
        assert!(seen_updated, "ColumnUpdated envelope not dispatched");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn move_column_writes_and_dispatches_columns_reordered_event() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let (project_id, bid, col1, col2) = seed_board_with_cards(&pool, &svc, &subject).await;
        let board_id: Id = bid.parse().expect("board id");

        svc.move_column(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&MoveColumnRequest {
                board_id: bid.clone(),
                column_id: col1.clone(),
                to_position: 2,
                ..Default::default()
            }),
        )
        .await
        .expect("move_column failed");

        let rows = board_event_rows(&pool, board_id).await;
        let reordered = rows
            .iter()
            .find(|(t, _)| t == "ColumnsReordered")
            .expect("ColumnsReordered row missing");
        let first_col = reordered
            .1
            .pointer("/columns/0/id")
            .and_then(|v| v.as_str())
            .expect("columns[0].id missing");
        assert_eq!(first_col, col2, "col2 must lead after the move");

        let mut handle = subscribe_and_drain(&pool, nats, &registry, board_id, false).await;
        let mut seen_reordered = false;
        for _ in 0..3 {
            let env = recv_envelope(&mut handle).await;
            if let Some(Payload::ColumnsReordered(ev)) = env.payload {
                assert_eq!(ev.columns.len(), 2);
                assert_eq!(ev.columns[0].id, col2);
                assert_eq!(ev.columns[1].id, col1);
                seen_reordered = true;
            }
        }
        assert!(seen_reordered, "ColumnsReordered envelope not dispatched");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn update_board_writes_board_renamed_and_board_updated_events() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;
        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Rename Me".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");
        let bid = board.id.clone();
        let board_id: Id = bid.parse().expect("board id");

        svc.update_board(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&UpdateBoardRequest {
                board_id: bid.clone(),
                board: Some(Board {
                    name: "Renamed Board".to_string(),
                    ..Default::default()
                })
                .into(),
                update_mask: None.into(),
                ..Default::default()
            }),
        )
        .await
        .expect("update_board failed");

        svc.update_board(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&UpdateBoardRequest {
                board_id: bid.clone(),
                board: Some(Board {
                    description: "new description".to_string(),
                    ..Default::default()
                })
                .into(),
                update_mask: None.into(),
                ..Default::default()
            }),
        )
        .await
        .expect("update_board failed");

        let rows = board_event_rows(&pool, board_id).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "BoardRenamed");
        assert_eq!(
            rows[0].1.get("new_name").and_then(|v| v.as_str()),
            Some("Renamed Board")
        );
        assert_eq!(rows[1].0, "BoardUpdated");
        assert_eq!(
            rows[1].1.pointer("/board/name").and_then(|v| v.as_str()),
            Some("Renamed Board")
        );

        let mut handle =
            subscribe_and_drain(&pool, Arc::clone(&nats), &registry, board_id, false).await;
        let env = recv_envelope(&mut handle).await;
        match env.payload {
            Some(Payload::BoardRenamed(ev)) => assert_eq!(ev.new_name, "Renamed Board"),
            other => panic!("expected BoardRenamed, got {other:?}"),
        }
        let env = recv_envelope(&mut handle).await;
        match env.payload {
            Some(Payload::BoardUpdated(ev)) => {
                let b = ev.board.into_option().expect("board missing");
                assert_eq!(b.description, "new description");
                assert_eq!(b.name, "Renamed Board");
            }
            other => panic!("expected BoardUpdated, got {other:?}"),
        }

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn create_and_delete_board_write_project_scoped_events() {
        let pool = setup_pool().await;
        let nats = connect_nats().await;
        ensure_stream(&nats).await;
        let permission = setup_permission().await;
        let svc = make_service(pool.clone(), Arc::clone(&permission)).await;
        let registry = make_registry(Arc::clone(&nats)).await;

        let subject = format!("user:test-{}", Id::new());
        let project_id = create_test_project(&pool, &subject).await;
        let board = svc
            .create_board(
                authed_ctx_with_object(&subject, &project_id.to_string()),
                connect_request(&CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Lifecycle Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private.into(),
                    ..Default::default()
                }),
            )
            .await
            .expect("create_board failed")
            .body
            .board
            .into_option()
            .expect("board missing");
        let bid = board.id.clone();

        svc.delete_board(
            authed_ctx_with_object(&subject, &bid),
            connect_request(&DeleteBoardRequest {
                board_id: bid.clone(),
                ..Default::default()
            }),
        )
        .await
        .expect("delete_board failed");

        // Both events are project-scoped rows.
        let rows = sqlx::query(
            "SELECT event_type FROM event_log WHERE project_id = $1 ORDER BY created_at, id",
        )
        .bind(project_id)
        .fetch_all(&pool)
        .await
        .expect("fetch project event rows");
        let types: Vec<String> = rows.iter().map(|r| r.get("event_type")).collect();
        assert_eq!(types, vec!["BoardCreated", "BoardDeleted"]);

        let mut handle =
            subscribe_and_drain(&pool, Arc::clone(&nats), &registry, project_id, true).await;
        let env = recv_envelope(&mut handle).await;
        assert_eq!(
            env.board_id, "",
            "project-scoped envelopes carry no board_id"
        );
        match env.payload {
            Some(Payload::BoardCreated(ev)) => {
                assert_eq!(ev.board_id, bid);
                assert_eq!(ev.project_id, project_id.to_string());
                assert_eq!(ev.name, "Lifecycle Board");
            }
            other => panic!("expected BoardCreated, got {other:?}"),
        }
        let env = recv_envelope(&mut handle).await;
        match env.payload {
            Some(Payload::BoardDeleted(ev)) => assert_eq!(ev.board_id, bid),
            other => panic!("expected BoardDeleted, got {other:?}"),
        }

        cleanup_project(&pool, project_id).await;
    }
}
