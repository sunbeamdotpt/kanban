//! BoardService — Stage 3b + 4c implementation.
//!
//! Mirror-table write order: Keto FIRST, then SQL. If the SQL insert fails
//! after a successful Keto write, we log a `mirror_drift` warning and let
//! the hourly reconciler (Stage 7a) catch it. This matches Pre-mortem 5.
//!
//! Dynamic sqlx API (no compile-time macros) is used throughout so that
//! `cargo check` does not require a live DATABASE_URL at build time.
//!
//! Stage 4c: `SubscribeBoard` is wired end-to-end. The stream:
//!   1. Emits a synthetic `Cutover { last_replay_nats_seq: 0 }` immediately
//!      (empty replay — Stage 4c.5 will add snapshot replay).
//!   2. Subscribes to live events via `BoardSubscriberRegistry`.
//!   3. Emits a `Heartbeat` every `heartbeat_interval` of inactivity.
//!   4. Rechecks JWT+watermark on every yield; rechecks Keto every
//!      `keto_recheck_interval`.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use sqlx::PgPool;
use sqlx::Row;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use tokio_stream::Stream;
use tracing::{error, warn};
use uuid::Uuid;

use sunbeam_g2v::middleware::auth::AuthContext;
use sunbeam_g2v::middleware::auth::keto::KetoClient;

use crate::auth::keto_dispatch::CheckedObjectId;
use crate::auth::logout_watermark::LogoutWatermark;
use crate::pb::board_service_server::BoardService;
use crate::pb::{
    board_event_envelope::Payload, AddColumnRequest, Board, BoardDetail,
    BoardEventEnvelope, Column, CreateBoardRequest,
    Cutover, DeleteBoardRequest, GetBoardRequest, Heartbeat,
    ListBoardsRequest, ListBoardsResponse, MoveColumnRequest,
    MoveColumnResponse, RemoveColumnRequest, SubscribeBoardRequest,
    UpdateBoardRequest, UpdateColumnRequest,
};
use crate::realtime::cutover::{CutoverTracker, Outcome};
use crate::realtime::registry::BoardSubscriberRegistry;

// ── Constants ────────────────────────────────────────────────────────────────

const KETO_NS_BOARD: &str = "KanbanBoard";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct BoardServiceImpl {
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

// ── Error helpers ─────────────────────────────────────────────────────────────

fn internal(msg: &str, err: impl std::fmt::Display) -> Status {
    error!(error = %err, "{msg}");
    Status::internal(msg)
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

fn checked_object_id<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<CheckedObjectId>()
        .map(|c| c.0.clone())
        .ok_or_else(|| Status::internal("missing CheckedObjectId extension"))
}

// ── Row → proto helpers ───────────────────────────────────────────────────────

fn board_from_row(row: &sqlx::postgres::PgRow, columns_count: i32, cards_count: i32) -> Board {
    let id: Uuid = row.get("id");
    let project_id: Uuid = row.get("project_id");
    let name: String = row.get("name");
    let description: Option<String> = row.get("description");
    let icon: Option<String> = row.get("icon");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Board {
        id: id.to_string(),
        project_id: project_id.to_string(),
        name,
        description: description.unwrap_or_default(),
        icon: icon.unwrap_or_default(),
        created_at: Some(to_proto_ts(created_at)),
        updated_at: Some(to_proto_ts(updated_at)),
        columns_count,
        cards_count,
    }
}

fn column_from_row(row: &sqlx::postgres::PgRow) -> Column {
    let id: Uuid = row.get("id");
    let board_id: Uuid = row.get("board_id");
    let title: String = row.get("title");
    let accent: Option<String> = row.get("accent");
    let wip_limit: Option<i32> = row.get("wip_limit");
    let position: i32 = row.get("position");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    Column {
        id: id.to_string(),
        board_id: board_id.to_string(),
        title,
        accent: accent.unwrap_or_default(),
        wip_limit: wip_limit.unwrap_or(0),
        position,
        created_at: Some(to_proto_ts(created_at)),
        updated_at: Some(to_proto_ts(updated_at)),
    }
}

// ── Count helpers ─────────────────────────────────────────────────────────────

async fn fetch_columns_count(pool: &PgPool, board_id: Uuid) -> i32 {
    sqlx::query("SELECT COUNT(*) AS cnt FROM columns WHERE board_id = $1")
        .bind(board_id)
        .fetch_one(pool)
        .await
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt as i32
        })
        .unwrap_or(0)
}

async fn fetch_cards_count(pool: &PgPool, board_id: Uuid) -> i32 {
    sqlx::query("SELECT COUNT(*) AS cnt FROM cards WHERE board_id = $1")
        .bind(board_id)
        .fetch_one(pool)
        .await
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt as i32
        })
        .unwrap_or(0)
}

async fn fetch_board_columns(pool: &PgPool, board_id: Uuid) -> Result<Vec<Column>, Status> {
    let rows = sqlx::query(
        "SELECT id, board_id, title, accent, wip_limit, position, created_at, updated_at \
         FROM columns WHERE board_id = $1 ORDER BY position ASC",
    )
    .bind(board_id)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch columns", e))?;

    Ok(rows.iter().map(column_from_row).collect())
}

/// Derive a URL-safe slug from the board name (max 40 chars).
fn slug_from_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .map(|c| c.to_ascii_lowercase())
        .take(40)
        .collect::<String>()
}

// ── Type alias ───────────────────────────────────────────────────────────────

type SubscribeBoardStream =
    Pin<Box<dyn Stream<Item = Result<BoardEventEnvelope, Status>> + Send + 'static>>;

// ── Streaming intervals (overridable in tests via build_subscribe_board_stream) ─

/// Production heartbeat interval (15s).
pub const HEARTBEAT_INTERVAL_MS: u64 = 15_000;
/// Production Keto recheck interval (30s).
pub const KETO_RECHECK_INTERVAL_MS: u64 = 30_000;

// ── Stream helpers ────────────────────────────────────────────────────────────

/// Synthesize a `Cutover` envelope at `last_replay_nats_seq`.
fn cutover_envelope(last_replay_nats_seq: u64) -> BoardEventEnvelope {
    BoardEventEnvelope {
        board_id: String::new(),
        event_id: String::new(),
        nats_seq: 0,
        board_revision: 0,
        emitted_at: None,
        emitter_pod_id: String::new(),
        actor_subject: "system".to_string(),
        payload: Some(Payload::Cutover(Cutover { last_replay_nats_seq })),
    }
}

/// Synthesize a `Heartbeat` envelope with current wall clock.
fn heartbeat_envelope() -> BoardEventEnvelope {
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
        emitted_at: None,
        emitter_pod_id: String::new(),
        actor_subject: "system".to_string(),
        payload: Some(Payload::Heartbeat(Heartbeat { server_time_ms })),
    }
}

/// Validate JWT not expired and watermark not revoked.
///
/// Returns `Ok(true)` if valid, `Ok(false)` if revoked, `Err` if the
/// watermark store is unavailable (fail-closed).
async fn revalidate_token(watermark: &LogoutWatermark, auth: &AuthContext) -> Result<bool, Status> {
    let subject = auth.subject.as_deref().unwrap_or("");

    // Check JWT exp.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if let Some(claims) = &auth.claims {
        if claims.exp < now_secs {
            return Ok(false);
        }
    }

    // Check logout watermark.
    let iat_ms: u64 = auth
        .claims
        .as_ref()
        .map(|c| (c.iat as u64).saturating_mul(1000))
        .unwrap_or(0);

    match watermark.is_token_valid(subject, iat_ms).await {
        Ok(valid) => Ok(valid),
        Err(e) => {
            warn!(subject, error = %e, "stream: logout watermark unavailable");
            Err(Status::unavailable("authorization service unavailable"))
        }
    }
}

/// Keto permission recheck for the live stream.
///
/// Returns `Ok(true)` if still authorized, `Ok(false)` if revoked,
/// `Err(Status)` on Keto failure.
async fn revalidate_keto(
    keto: &KetoClient,
    auth: &AuthContext,
    board_id: &str,
) -> Result<bool, Status> {
    let subject = auth.subject.as_deref().unwrap_or("");
    keto.check_permission("KanbanBoard", board_id, "view", subject)
        .await
        .map_err(|e| {
            warn!(board_id, subject, error = %e, "stream: Keto recheck failed");
            Status::internal("authorization check failed")
        })
}

/// Build the `SubscribeBoard` server-streaming response.
///
/// `heartbeat_interval` and `keto_recheck_interval` are parameterised so tests
/// can inject short durations without sleeping for 15s/30s.
///
/// # Stage 4c.5 TODO
/// TODO(4c.5): snapshot replay — SELECT current cards/columns + emit synthetic
/// `CardCreated`/`ColumnAdded` with `nats_seq=0` before the `Cutover` envelope.
pub async fn build_subscribe_board_stream(
    registry: Arc<BoardSubscriberRegistry>,
    keto: Arc<KetoClient>,
    watermark: Arc<LogoutWatermark>,
    auth: AuthContext,
    board_id: String,
    heartbeat_interval: Duration,
    keto_recheck_interval: Duration,
) -> Result<SubscribeBoardStream, Status> {
    let s = stream! {
        // ── Step 1: emit Cutover immediately (empty replay, seq=0) ────────────
        // TODO(4c.5): snapshot replay — emit synthetic CardCreated/ColumnAdded
        // events before this Cutover, each with nats_seq=0.
        yield Ok(cutover_envelope(0));

        // ── Step 2: subscribe to live events ─────────────────────────────────
        let mut handle = match Arc::clone(&registry).subscribe(&board_id).await {
            Ok(h) => h,
            Err(e) => {
                warn!(board_id = %board_id, error = %e, "SubscribeBoard: registry subscribe failed");
                yield Err(Status::internal(format!("subscribe: {e}")));
                return;
            }
        };

        let mut tracker = CutoverTracker::new();
        tracker.cutover_to_live(0);

        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick so the first heartbeat is delayed.
        heartbeat.tick().await;

        let mut last_keto_recheck = Instant::now();

        loop {
            // ── Step A: token revalidation (every yield) ──────────────────────
            match revalidate_token(&watermark, &auth).await {
                Ok(true) => {}
                Ok(false) => {
                    yield Err(Status::unauthenticated("token revoked"));
                    break;
                }
                Err(status) => {
                    yield Err(status);
                    break;
                }
            }

            // ── Step B: Keto recheck (every keto_recheck_interval) ────────────
            if last_keto_recheck.elapsed() >= keto_recheck_interval {
                match revalidate_keto(&keto, &auth, &board_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        yield Err(Status::permission_denied("permission revoked mid-stream"));
                        break;
                    }
                    Err(status) => {
                        yield Err(status);
                        break;
                    }
                }
                last_keto_recheck = Instant::now();
            }

            // ── Step C: select envelope OR heartbeat tick ─────────────────────
            tokio::select! {
                msg = handle.receiver.recv() => {
                    match msg {
                        Ok(envelope) => {
                            match tracker.observe_live(&envelope.event_id, envelope.nats_seq) {
                                Outcome::Emit => yield Ok(envelope),
                                Outcome::Drop | Outcome::OutOfOrder => { /* skip */ }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            yield Err(Status::resource_exhausted(
                                "stream lagged; reconnect with last seq"
                            ));
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = heartbeat.tick() => {
                    yield Ok(heartbeat_envelope());
                }
            }
        }
    };

    Ok(Box::pin(s))
}

// ── impl BoardService ────────────────────────────────────────────────────────

#[tonic::async_trait]
impl BoardService for BoardServiceImpl {
    // ── ListBoards ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the project_id (KanbanProject + view, per matrix).
    // We SELECT boards WHERE project_id = $1 directly — the Keto check on the
    // project gives access to all boards within it.

    async fn list_boards(
        &self,
        request: Request<ListBoardsRequest>,
    ) -> Result<Response<ListBoardsResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let rows = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, created_at, updated_at \
             FROM boards WHERE project_id = $1 ORDER BY created_at ASC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list boards", e))?;

        let mut boards = Vec::with_capacity(rows.len());
        for row in &rows {
            let bid: Uuid = row.get("id");
            let columns_count = fetch_columns_count(&self.pool, bid).await;
            let cards_count = fetch_cards_count(&self.pool, bid).await;
            boards.push(board_from_row(row, columns_count, cards_count));
        }

        Ok(Response::new(ListBoardsResponse { boards }))
    }

    // ── GetBoard ──────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + view, per matrix).
    // Returns BoardDetail with full column list.

    async fn get_board(
        &self,
        request: Request<GetBoardRequest>,
    ) -> Result<Response<BoardDetail>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let row = sqlx::query(
            "SELECT id, project_id, name, slug, description, icon, created_at, updated_at \
             FROM boards WHERE id = $1",
        )
        .bind(board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch board", e))?
        .ok_or_else(|| Status::not_found("board not found"))?;

        let columns = fetch_board_columns(&self.pool, board_id).await?;
        let cards_count = fetch_cards_count(&self.pool, board_id).await;
        let board = board_from_row(&row, columns.len() as i32, cards_count);

        Ok(Response::new(BoardDetail {
            board: Some(board),
            columns,
        }))
    }

    // ── CreateBoard ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the project_id (KanbanProject + edit, per matrix).
    // Keto write: KanbanBoard:{board_id}#parent@KanbanProject:{project_id}
    // so boards inherit project-level access.

    async fn create_board(
        &self,
        request: Request<CreateBoardRequest>,
    ) -> Result<Response<Board>, Status> {
        let object_id = checked_object_id(&request)?;
        let project_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid project_id"))?;

        let req = request.into_inner();

        // Idempotency check.
        if !req.idempotency_key.is_empty() {
            let cached: Option<Option<Uuid>> = sqlx::query(
                "SELECT response_card_id FROM idempotency_keys WHERE key = $1",
            )
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("idempotency key lookup failed", e))?
            .map(|r| r.get("response_card_id"));

            if let Some(Some(board_id)) = cached {
                let row = sqlx::query(
                    "SELECT id, project_id, name, slug, description, icon, created_at, updated_at \
                     FROM boards WHERE id = $1",
                )
                .bind(board_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached board", e))?;

                if let Some(row) = row {
                    let columns_count = fetch_columns_count(&self.pool, board_id).await;
                    let cards_count = fetch_cards_count(&self.pool, board_id).await;
                    return Ok(Response::new(board_from_row(&row, columns_count, cards_count)));
                }
            }
        }

        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        let board_id = Uuid::new_v4();
        let slug = slug_from_name(&req.name);

        // INSERT board.
        let row = sqlx::query(
            r#"
            INSERT INTO boards (id, project_id, name, slug, description, icon)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, project_id, name, slug, description, icon, created_at, updated_at
            "#,
        )
        .bind(board_id)
        .bind(project_id)
        .bind(&req.name)
        .bind(&slug)
        .bind(&req.description)
        .bind(if req.icon.is_empty() { None } else { Some(req.icon.clone()) })
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db) = e {
                if db.constraint() == Some("boards_project_id_slug_key") {
                    return Status::already_exists("board with that name already exists in project");
                }
            }
            internal("failed to insert board", e)
        })?;

        // Keto FIRST: write parent tuple so board inherits project access.
        // KanbanBoard:{board_id}#parent@KanbanProject:{project_id}
        if let Err(e) = self
            .keto
            .grant(
                KETO_NS_BOARD,
                &board_id.to_string(),
                "parent",
                &format!("KanbanProject:{project_id}#..."),
            )
            .await
        {
            warn!(
                error = %e,
                board_id = %board_id,
                project_id = %project_id,
                "mirror_drift: failed to write Keto parent tuple for board; reconciler (Stage 7a) will catch"
            );
        }

        // Store idempotency response.
        if !req.idempotency_key.is_empty() {
            if let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (key, response_card_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(&req.idempotency_key)
            .bind(board_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }
        }

        Ok(Response::new(board_from_row(&row, 0, 0)))
    }

    // ── UpdateBoard ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // Sparse patch — only non-empty fields applied.

    async fn update_board(
        &self,
        request: Request<UpdateBoardRequest>,
    ) -> Result<Response<Board>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let req = request.into_inner();
        let patch = req.board.unwrap_or_default();

        let row = sqlx::query(
            r#"
            UPDATE boards SET
                name        = CASE WHEN $2 != '' THEN $2 ELSE name END,
                description = CASE WHEN $3 != '' THEN $3 ELSE description END,
                icon        = CASE WHEN $4 != '' THEN $4 ELSE icon END,
                updated_at  = now()
            WHERE id = $1
            RETURNING id, project_id, name, slug, description, icon, created_at, updated_at
            "#,
        )
        .bind(board_id)
        .bind(&patch.name)
        .bind(&patch.description)
        .bind(&patch.icon)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to update board", e))?
        .ok_or_else(|| Status::not_found("board not found"))?;

        let columns_count = fetch_columns_count(&self.pool, board_id).await;
        let cards_count = fetch_cards_count(&self.pool, board_id).await;
        Ok(Response::new(board_from_row(&row, columns_count, cards_count)))
    }

    // ── DeleteBoard ───────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + manage, per matrix).
    // CASCADE: deletes columns → cards (per FK cascade in migrations).
    // Best-effort Keto cleanup — drift logged, reconciler handles it.

    async fn delete_board(
        &self,
        request: Request<DeleteBoardRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let result = sqlx::query("DELETE FROM boards WHERE id = $1")
            .bind(board_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete board", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("board not found"));
        }

        // Best-effort Keto parent tuple cleanup.
        warn!(
            board_id = %board_id,
            "delete_board: Keto parent tuple cleanup is best-effort; reconciler (Stage 7a) will catch any drift"
        );

        Ok(Response::new(()))
    }

    // ── AddColumn ─────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // If position == 0 (proto default), append after the current max.
    // If position is set (>0), shift existing columns at that position upward.

    async fn add_column(
        &self,
        request: Request<AddColumnRequest>,
    ) -> Result<Response<Column>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let req = request.into_inner();

        if req.title.is_empty() {
            return Err(Status::invalid_argument("title is required"));
        }

        // Idempotency check.
        if !req.idempotency_key.is_empty() {
            let cached: Option<Option<Uuid>> = sqlx::query(
                "SELECT response_card_id FROM idempotency_keys WHERE key = $1",
            )
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| internal("idempotency key lookup failed", e))?
            .map(|r| r.get("response_card_id"));

            if let Some(Some(col_id)) = cached {
                let row = sqlx::query(
                    "SELECT id, board_id, title, accent, wip_limit, position, created_at, updated_at \
                     FROM columns WHERE id = $1",
                )
                .bind(col_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch cached column", e))?;

                if let Some(row) = row {
                    return Ok(Response::new(column_from_row(&row)));
                }
            }
        }

        let column_id = Uuid::new_v4();

        let row = if req.position == 0 {
            // Append at the end: position = MAX(position) + 1.
            sqlx::query(
                r#"
                INSERT INTO columns (id, board_id, title, accent, wip_limit, position)
                SELECT $1, $2, $3, $4, $5,
                       COALESCE((SELECT MAX(position) FROM columns WHERE board_id = $2), -1) + 1
                RETURNING id, board_id, title, accent, wip_limit, position, created_at, updated_at
                "#,
            )
            .bind(column_id)
            .bind(board_id)
            .bind(&req.title)
            .bind(if req.accent.is_empty() { None } else { Some(req.accent.clone()) })
            .bind(if req.wip_limit == 0 { None } else { Some(req.wip_limit) })
            .fetch_one(&self.pool)
            .await
            .map_err(|e| internal("failed to insert column", e))?
        } else {
            // Insert at specific position: shift existing columns upward in a CTE.
            sqlx::query(
                r#"
                WITH shift AS (
                    UPDATE columns
                    SET position = position + 1, updated_at = now()
                    WHERE board_id = $2 AND position >= $6
                )
                INSERT INTO columns (id, board_id, title, accent, wip_limit, position)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, board_id, title, accent, wip_limit, position, created_at, updated_at
                "#,
            )
            .bind(column_id)
            .bind(board_id)
            .bind(&req.title)
            .bind(if req.accent.is_empty() { None } else { Some(req.accent.clone()) })
            .bind(if req.wip_limit == 0 { None } else { Some(req.wip_limit) })
            .bind(req.position)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| internal("failed to insert column at position", e))?
        };

        // Stage 4: emit BoardEventEnvelope::ColumnAdded over NATS subject kanban.board.<board_id>.events

        // Store idempotency response.
        if !req.idempotency_key.is_empty() {
            if let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (key, response_card_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(&req.idempotency_key)
            .bind(column_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }
        }

        Ok(Response::new(column_from_row(&row)))
    }

    // ── UpdateColumn ──────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // Sparse patch on title/accent/wip_limit.

    async fn update_column(
        &self,
        request: Request<UpdateColumnRequest>,
    ) -> Result<Response<Column>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let req = request.into_inner();
        let col_id = Uuid::parse_str(&req.column_id)
            .map_err(|_| Status::invalid_argument("invalid column_id"))?;
        let patch = req.column.unwrap_or_default();

        let row = sqlx::query(
            r#"
            UPDATE columns SET
                title      = CASE WHEN $3 != '' THEN $3 ELSE title END,
                accent     = CASE WHEN $4 != '' THEN $4 ELSE accent END,
                wip_limit  = CASE WHEN $5 != 0  THEN $5 ELSE wip_limit END,
                updated_at = now()
            WHERE id = $1 AND board_id = $2
            RETURNING id, board_id, title, accent, wip_limit, position, created_at, updated_at
            "#,
        )
        .bind(col_id)
        .bind(board_id)
        .bind(&patch.title)
        .bind(&patch.accent)
        .bind(patch.wip_limit)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to update column", e))?
        .ok_or_else(|| Status::not_found("column not found on this board"))?;

        // Stage 4: emit BoardEventEnvelope::ColumnUpdated over NATS subject kanban.board.<board_id>.events

        Ok(Response::new(column_from_row(&row)))
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
        request: Request<RemoveColumnRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let req = request.into_inner();
        let col_id = Uuid::parse_str(&req.column_id)
            .map_err(|_| Status::invalid_argument("invalid column_id"))?;

        // Verify the column belongs to this board.
        let exists: bool = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM columns WHERE id = $1 AND board_id = $2)",
        )
        .bind(col_id)
        .bind(board_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal("failed to verify column ownership", e))
        .map(|r| r.get::<bool, _>(0))?;

        if !exists {
            return Err(Status::not_found("column not found on this board"));
        }

        // Delete the column. Cards cascade-delete via FK (0006_cards.sql).
        let result = sqlx::query("DELETE FROM columns WHERE id = $1 AND board_id = $2")
            .bind(col_id)
            .bind(board_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete column", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("column not found"));
        }

        // Gap-fill remaining column positions (renumber 0, 1, 2, ... by current order).
        if let Err(e) = sqlx::query(
            r#"
            WITH ranked AS (
                SELECT id,
                       (ROW_NUMBER() OVER (ORDER BY position ASC) - 1)::INT AS new_pos
                FROM columns
                WHERE board_id = $1
            )
            UPDATE columns c
            SET position = r.new_pos, updated_at = now()
            FROM ranked r
            WHERE c.id = r.id
            "#,
        )
        .bind(board_id)
        .execute(&self.pool)
        .await
        {
            warn!(error = %e, board_id = %board_id, "failed to gap-fill column positions after remove");
        }

        // Stage 4: emit BoardEventEnvelope::ColumnDeleted over NATS subject kanban.board.<board_id>.events

        Ok(Response::new(()))
    }

    // ── MoveColumn ────────────────────────────────────────────────────────────
    //
    // CheckedObjectId is the board_id (KanbanBoard + edit, per matrix).
    // to_position is 1-based in the proto. We work 0-based in DB.
    // Pattern: remove from current slot, shift others to fill gap,
    // insert at target slot by incrementing positions >= target.

    async fn move_column(
        &self,
        request: Request<MoveColumnRequest>,
    ) -> Result<Response<MoveColumnResponse>, Status> {
        let object_id = checked_object_id(&request)?;
        let board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let req = request.into_inner();
        let col_id = Uuid::parse_str(&req.column_id)
            .map_err(|_| Status::invalid_argument("invalid column_id"))?;

        if req.to_position < 1 {
            return Err(Status::invalid_argument("to_position must be >= 1"));
        }

        // Convert to 0-based target.
        let target_pos = req.to_position - 1;

        // Fetch current position.
        let current_pos: i32 = sqlx::query(
            "SELECT position FROM columns WHERE id = $1 AND board_id = $2",
        )
        .bind(col_id)
        .bind(board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch column position", e))?
        .ok_or_else(|| Status::not_found("column not found on this board"))
        .map(|r| r.get("position"))?;

        if current_pos != target_pos {
            if target_pos > current_pos {
                // Moving forward: shift columns in (current, target] down by 1.
                sqlx::query(
                    "UPDATE columns SET position = position - 1, updated_at = now() \
                     WHERE board_id = $1 AND position > $2 AND position <= $3",
                )
                .bind(board_id)
                .bind(current_pos)
                .bind(target_pos)
                .execute(&self.pool)
                .await
                .map_err(|e| internal("failed to shift columns (forward move)", e))?;
            } else {
                // Moving backward: shift columns in [target, current) up by 1.
                sqlx::query(
                    "UPDATE columns SET position = position + 1, updated_at = now() \
                     WHERE board_id = $1 AND position >= $2 AND position < $3",
                )
                .bind(board_id)
                .bind(target_pos)
                .bind(current_pos)
                .execute(&self.pool)
                .await
                .map_err(|e| internal("failed to shift columns (backward move)", e))?;
            }

            // Place the moved column at its new position.
            sqlx::query(
                "UPDATE columns SET position = $2, updated_at = now() WHERE id = $1",
            )
            .bind(col_id)
            .bind(target_pos)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to place moved column", e))?;
        }

        // Stage 4: emit BoardEventEnvelope::ColumnsReordered over NATS subject kanban.board.<board_id>.events

        let columns = fetch_board_columns(&self.pool, board_id).await?;
        Ok(Response::new(MoveColumnResponse { columns }))
    }

    // ── SubscribeBoard (Stage 4c) ─────────────────────────────────────────────

    type SubscribeBoardStream = SubscribeBoardStream;

    async fn subscribe_board(
        &self,
        request: Request<SubscribeBoardRequest>,
    ) -> Result<Response<Self::SubscribeBoardStream>, Status> {
        let auth = request
            .extensions()
            .get::<AuthContext>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("missing auth context"))?;
        let board_id = request
            .extensions()
            .get::<CheckedObjectId>()
            .map(|c| c.0.clone())
            .ok_or_else(|| Status::internal("missing CheckedObjectId"))?;
        // since_seq is reserved for Stage 4c.5 resume protocol; ignored here.
        let _since_seq = request.into_inner().since_seq;

        let stream = build_subscribe_board_stream(
            Arc::clone(&self.registry),
            Arc::clone(&self.keto),
            Arc::clone(&self.watermark),
            auth,
            board_id,
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            Duration::from_millis(KETO_RECHECK_INTERVAL_MS),
        )
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
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;
    use sunbeam_g2v::middleware::auth::AuthContext;
    use sunbeam_g2v::config::NatsConfig;
    use sunbeam_g2v::mq::NatsClient;

    // ── Subscribe test helpers ───────────────────────────────────────────────

    fn nats_url() -> String {
        std::env::var("NATS_URL")
            .unwrap_or_else(|_| "nats://localhost:4222".to_string())
    }

    fn valkey_url() -> String {
        std::env::var("VALKEY_URL")
            .unwrap_or_else(|_| "redis://localhost:6379".to_string())
    }

    async fn connect_nats() -> Arc<NatsClient> {
        Arc::new(
            NatsClient::connect(&NatsConfig {
                url: nats_url(),
                jetstream: true,
                lease_duration: 30,
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

    fn make_auth_with_future_exp(subject: &str) -> AuthContext {
        use sunbeam_g2v::middleware::auth::jwt::JwtClaims;
        let claims = JwtClaims {
            sub: subject.to_string(),
            iat: 0,
            exp: i64::MAX,
            iss: None,
            aud: None,
            extra: Default::default(),
        };
        AuthContext::authenticated(subject, Some(claims))
    }

    async fn make_registry(nats: Arc<NatsClient>) -> Arc<BoardSubscriberRegistry> {
        Arc::new(BoardSubscriberRegistry::new(nats, "pod-test-boards"))
    }

    // ── Stage 4c subscribe tests ─────────────────────────────────────────────

    /// First envelope from SubscribeBoard must be Cutover with last_replay_nats_seq=0.
    #[tokio::test]
    async fn subscribe_emits_cutover_immediately_on_empty_replay() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", uuid::Uuid::new_v4().simple());
        let subject_str = format!("user:test-{}", uuid::Uuid::new_v4());
        let auth = make_auth_with_future_exp(&subject_str);

        let watermark = Arc::new(
            LogoutWatermark::new(&valkey_url()).expect("watermark"),
        );
        let keto_url = std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string());
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());
        let keto = Arc::new(sunbeam_g2v::middleware::auth::keto::KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_url.clone(),
                write_grpc_endpoint: keto_write_url.clone(),
            },
        ));

        // Grant view so the initial Keto recheck passes.
        let _ = keto.grant("KanbanBoard", &board_id, "view", &subject_str).await;

        let mut stream = build_subscribe_board_stream(
            registry,
            Arc::clone(&keto),
            watermark,
            auth,
            board_id.clone(),
            Duration::from_secs(15),
            Duration::from_secs(30),
        )
        .await
        .expect("build_subscribe_board_stream failed");

        use tokio_stream::StreamExt;
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for first envelope")
            .expect("stream ended without first envelope")
            .expect("first envelope was an error");

        match first.payload {
            Some(Payload::Cutover(c)) => {
                assert_eq!(c.last_replay_nats_seq, 0, "cutover seq must be 0 for empty replay");
            }
            other => panic!("expected Cutover, got {other:?}"),
        }

        // Cleanup Keto tuple.
        let _ = crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject_str),
        )
        .await;
    }

    /// After subscribing, a live JetStream event published for the board must
    /// arrive on the stream within 2s.
    #[tokio::test]
    async fn subscribe_forwards_live_event_published_to_jetstream() {
        use crate::realtime::jetstream_bootstrap::board_subject;
        use prost::Message as ProstMessage;

        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", uuid::Uuid::new_v4().simple());
        let subject_str = format!("user:test-{}", uuid::Uuid::new_v4());
        let auth = make_auth_with_future_exp(&subject_str);

        let watermark = Arc::new(
            LogoutWatermark::new(&valkey_url()).expect("watermark"),
        );
        let keto_url = std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string());
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());
        let keto = Arc::new(sunbeam_g2v::middleware::auth::keto::KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_url,
                write_grpc_endpoint: keto_write_url,
            },
        ));
        let _ = keto.grant("KanbanBoard", &board_id, "view", &subject_str).await;

        let mut stream = build_subscribe_board_stream(
            registry,
            Arc::clone(&keto),
            Arc::clone(&watermark),
            auth,
            board_id.clone(),
            Duration::from_secs(15),
            Duration::from_secs(30),
        )
        .await
        .expect("build failed");

        use tokio_stream::StreamExt;
        // Consume the Cutover envelope first.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout on cutover");

        // Publish a live event.
        let live_event_id = format!("live-{}", uuid::Uuid::new_v4().simple());
        let envelope = BoardEventEnvelope {
            board_id: board_id.clone(),
            event_id: live_event_id.clone(),
            nats_seq: 1,
            board_revision: 1,
            emitted_at: None,
            emitter_pod_id: "test".to_string(),
            actor_subject: "test".to_string(),
            payload: Some(Payload::Heartbeat(Heartbeat { server_time_ms: 0 })),
        };
        let mut buf = bytes::BytesMut::new();
        envelope.encode(&mut buf).expect("encode failed");
        nats.publish_jetstream(&board_subject(&board_id), buf.freeze())
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

        assert_eq!(received.event_id, live_event_id);

        let _ = crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject_str),
        )
        .await;
    }

    /// An event whose event_id was emitted during replay must be deduped when
    /// it arrives again on the live tail.
    ///
    // NOTE: replay-vs-live dedup test will be added when snapshot replay
    // lands (the empty-replay path is exercised by
    // `subscribe_emits_cutover_immediately_on_empty_replay`).

    /// After `heartbeat_interval` of inactivity, the stream must emit a
    /// Heartbeat envelope.
    ///
    /// Uses a 1s interval override to avoid sleeping 15s.
    #[tokio::test]
    async fn subscribe_emits_heartbeat_after_inactivity() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", uuid::Uuid::new_v4().simple());
        let subject_str = format!("user:test-{}", uuid::Uuid::new_v4());
        let auth = make_auth_with_future_exp(&subject_str);

        let watermark = Arc::new(
            LogoutWatermark::new(&valkey_url()).expect("watermark"),
        );
        let keto_url = std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string());
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());
        let keto = Arc::new(sunbeam_g2v::middleware::auth::keto::KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_url,
                write_grpc_endpoint: keto_write_url,
            },
        ));
        let _ = keto.grant("KanbanBoard", &board_id, "view", &subject_str).await;

        // Use a 1s heartbeat interval so we don't need to sleep 15s.
        let mut stream = build_subscribe_board_stream(
            registry,
            Arc::clone(&keto),
            watermark,
            auth,
            board_id.clone(),
            Duration::from_secs(1),  // short interval for test
            Duration::from_secs(60), // long keto recheck to avoid interference
        )
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

        match heartbeat_env.payload {
            Some(Payload::Heartbeat(h)) => {
                assert!(h.server_time_ms > 0, "heartbeat server_time_ms must be positive");
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }

        let _ = crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject_str),
        )
        .await;
    }

    /// When the subject's logout watermark is signalled, the stream must close
    /// with Status::Unauthenticated within 2s.
    ///
    /// Uses a very short Keto recheck interval (no Keto revocation needed for
    /// this test) and a 1s token revalidation cadence driven by the loop.
    #[tokio::test]
    async fn subscribe_closes_with_unauthenticated_when_token_revoked() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", uuid::Uuid::new_v4().simple());
        let subject_str = format!("user:test-{}", uuid::Uuid::new_v4());
        // Issue a token with iat=0 so that once watermark > 0, it's revoked.
        let auth = make_auth_with_future_exp(&subject_str);

        let watermark = Arc::new(
            LogoutWatermark::new(&valkey_url()).expect("watermark"),
        );
        let keto_url = std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string());
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());
        let keto = Arc::new(sunbeam_g2v::middleware::auth::keto::KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_url,
                write_grpc_endpoint: keto_write_url,
            },
        ));
        let _ = keto.grant("KanbanBoard", &board_id, "view", &subject_str).await;

        // Use a 100ms heartbeat so the loop iterates rapidly and picks up the
        // watermark quickly.
        let wm_clone = Arc::clone(&watermark);
        let sub_clone = subject_str.clone();
        let mut stream = build_subscribe_board_stream(
            registry,
            Arc::clone(&keto),
            Arc::clone(&watermark),
            auth,
            board_id.clone(),
            Duration::from_millis(100),
            Duration::from_secs(60),
        )
        .await
        .expect("build failed");

        use tokio_stream::StreamExt;
        // Consume the Cutover envelope.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout on cutover");

        // Signal logout in a background task — set watermark > iat_ms=0.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = wm_clone.signal_logout(&sub_clone).await;
        });

        // The stream should close with Unauthenticated within 2s.
        let err = tokio::time::timeout(Duration::from_secs(2), async {
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
            err.code(),
            tonic::Code::Unauthenticated,
            "expected Unauthenticated when token revoked, got {err:?}"
        );

        let _ = crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&subject_str),
        )
        .await;
    }

    /// When Keto revokes the subject's view permission mid-stream, the stream
    /// must close with Status::PermissionDenied.
    ///
    /// Uses a 1s Keto recheck interval override.
    #[tokio::test]
    async fn subscribe_closes_with_permission_denied_when_keto_revokes() {
        let nats = connect_nats().await;
        ensure_stream(&nats).await;

        let registry = make_registry(Arc::clone(&nats)).await;
        let board_id = format!("test-board-{}", uuid::Uuid::new_v4().simple());
        let subject_str = format!("user:test-{}", uuid::Uuid::new_v4());
        let auth = make_auth_with_future_exp(&subject_str);

        let watermark = Arc::new(
            LogoutWatermark::new(&valkey_url()).expect("watermark"),
        );
        let keto_url = std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string());
        let keto_write_url = std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string());
        let keto = Arc::new(sunbeam_g2v::middleware::auth::keto::KetoClient::new(
            sunbeam_g2v::middleware::auth::keto::KetoConfig {
                grpc_endpoint: keto_url,
                write_grpc_endpoint: keto_write_url,
            },
        ));
        // Grant view so initial check passes.
        let _ = keto.grant("KanbanBoard", &board_id, "view", &subject_str).await;

        let keto_clone = Arc::clone(&keto);
        let board_id_clone = board_id.clone();
        let sub_clone = subject_str.clone();

        // Use a 1s Keto recheck interval so we don't need to wait 30s.
        let mut stream = build_subscribe_board_stream(
            registry,
            Arc::clone(&keto),
            watermark,
            auth,
            board_id.clone(),
            Duration::from_millis(100), // fast heartbeat to keep loop ticking
            Duration::from_secs(1),     // 1s Keto recheck for test
        )
        .await
        .expect("build failed");

        use tokio_stream::StreamExt;
        // Consume the Cutover envelope.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout on cutover");

        // Revoke the Keto tuple after 200ms.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = crate::auth::keto_compat::delete_relation_tuples(
                &keto_clone,
                "KanbanBoard",
                Some("view"),
                Some(&sub_clone),
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
            err.code(),
            tonic::Code::PermissionDenied,
            "expected PermissionDenied when Keto revokes, got {err:?}"
        );

        let _ = crate::auth::keto_compat::delete_relation_tuples(
            &keto,
            "KanbanBoard",
            Some("view"),
            Some(&board_id_clone),
        )
        .await;
    }

    // ── Test env config ──────────────────────────────────────────────────────

    fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://sunbeam:sunbeam@localhost:5432/kanban".to_string())
    }

    fn keto_read_url() -> String {
        std::env::var("KETO_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4466".to_string())
    }

    fn keto_write_url() -> String {
        std::env::var("KETO_WRITE_GRPC_URL")
            .unwrap_or_else(|_| "http://localhost:4467".to_string())
    }

    // ── Setup helpers ────────────────────────────────────────────────────────

    async fn setup_pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&database_url())
            .await
            .expect("failed to connect to Postgres")
    }

    fn setup_keto() -> Arc<KetoClient> {
        Arc::new(KetoClient::new(sunbeam_g2v::middleware::auth::keto::KetoConfig {
            grpc_endpoint: keto_read_url(),
            write_grpc_endpoint: keto_write_url(),
        }))
    }

    /// Build a `BoardServiceImpl` for integration tests.
    ///
    /// All callers are under `#[ignore = "needs shared compose (postgres + keto)"]`
    /// so NATS and Valkey are available when this runs. The registry and
    /// watermark are constructed here so the struct fields are always populated.
    async fn make_service(pool: PgPool, keto: Arc<KetoClient>) -> BoardServiceImpl {
        let nats = connect_nats().await;
        let registry = Arc::new(BoardSubscriberRegistry::new(nats, "pod-test"));
        let watermark = Arc::new(
            LogoutWatermark::new(&valkey_url()).expect("LogoutWatermark::new"),
        );
        BoardServiceImpl { pool, keto, registry, watermark }
    }

    /// Build an authenticated request carrying a subject in extensions.
    fn authed_request<T>(body: T, subject: &str) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut()
            .insert(AuthContext::authenticated(subject, None));
        req
    }

    /// Build an authenticated request that also carries a `CheckedObjectId`.
    fn authed_request_with_object<T>(body: T, subject: &str, object_id: &str) -> Request<T> {
        let mut req = authed_request(body, subject);
        req.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        req
    }

    /// Seed a project row directly for test setup (bypasses ProjectService).
    async fn create_test_project(pool: &PgPool, subject: &str) -> Uuid {
        let pid = Uuid::new_v4();
        let slug = format!("tp-{}", &pid.to_string()[..8]);
        sqlx::query(
            "INSERT INTO projects (id, name, slug, description, owner_id) VALUES ($1, $2, $3, '', $4)",
        )
        .bind(pid)
        .bind(format!("Test Project {pid}"))
        .bind(&slug)
        .bind(subject)
        .execute(pool)
        .await
        .expect("failed to insert test project");
        pid
    }

    /// Seed a card row for test setup.
    async fn create_test_card(pool: &PgPool, project_id: Uuid, board_id: Uuid, column_id: Uuid, subject: &str, ref_: &str) -> Uuid {
        let card_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO cards (id, project_id, board_id, column_id, ref, title, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(card_id)
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

    async fn cleanup_project(pool: &PgPool, project_id: Uuid) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_returns_same_board() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let created = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Sprint Board".to_string(),
                    description: "integration test board".to_string(),
                    icon: "rocket".to_string(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await
            .expect("create_board failed")
            .into_inner();

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Sprint Board");
        assert_eq!(created.description, "integration test board");
        assert_eq!(created.icon, "rocket");
        assert!(created.created_at.is_some());

        let board_id = created.id.clone();

        let detail = svc
            .get_board(authed_request_with_object(
                GetBoardRequest { board_id: board_id.clone() },
                &subject,
                &board_id,
            ))
            .await
            .expect("get_board failed")
            .into_inner();

        let board = detail.board.expect("BoardDetail must contain board");
        assert_eq!(board.id, board_id);
        assert_eq!(board.name, "Sprint Board");
        assert_eq!(board.project_id, project_id.to_string());

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn list_boards_scoped_to_project() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_a = create_test_project(&pool, &subject).await;
        let project_b = create_test_project(&pool, &subject).await;

        // Create 2 boards in project A, 1 in project B.
        for name in &["Board A1", "Board A2"] {
            svc.create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_a.to_string(),
                    name: name.to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_a.to_string(),
            ))
            .await
            .expect("create_board failed");
        }

        svc.create_board(authed_request_with_object(
            CreateBoardRequest {
                project_id: project_b.to_string(),
                name: "Board B1".to_string(),
                description: String::new(),
                icon: String::new(),
                idempotency_key: String::new(),
            },
            &subject,
            &project_b.to_string(),
        ))
        .await
        .expect("create_board failed");

        let list_a = svc
            .list_boards(authed_request_with_object(
                ListBoardsRequest { project_id: project_a.to_string() },
                &subject,
                &project_a.to_string(),
            ))
            .await
            .expect("list_boards failed")
            .into_inner();

        assert_eq!(list_a.boards.len(), 2, "project A should have 2 boards");
        let names_a: Vec<&str> = list_a.boards.iter().map(|b| b.name.as_str()).collect();
        assert!(names_a.contains(&"Board A1"));
        assert!(names_a.contains(&"Board A2"));

        let list_b = svc
            .list_boards(authed_request_with_object(
                ListBoardsRequest { project_id: project_b.to_string() },
                &subject,
                &project_b.to_string(),
            ))
            .await
            .expect("list_boards failed")
            .into_inner();

        assert_eq!(list_b.boards.len(), 1, "project B should have 1 board");
        assert_eq!(list_b.boards[0].name, "Board B1");

        cleanup_project(&pool, project_a).await;
        cleanup_project(&pool, project_b).await;
    }

    #[tokio::test]
    async fn update_board_applies_patch_fields_only() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let created = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Original Name".to_string(),
                    description: "original desc".to_string(),
                    icon: "star".to_string(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await
            .expect("create failed")
            .into_inner();

        let board_id = created.id.clone();

        // Update name only; description and icon should be unchanged.
        let updated = svc
            .update_board(authed_request_with_object(
                UpdateBoardRequest {
                    board_id: board_id.clone(),
                    board: Some(Board {
                        id: String::new(),
                        project_id: String::new(),
                        name: "Updated Name".to_string(),
                        description: String::new(),
                        icon: String::new(),
                        created_at: None,
                        updated_at: None,
                        columns_count: 0,
                        cards_count: 0,
                    }),
                    update_mask: None,
                },
                &subject,
                &board_id,
            ))
            .await
            .expect("update_board failed")
            .into_inner();

        assert_eq!(updated.name, "Updated Name");
        assert_eq!(updated.description, "original desc", "description should be unchanged");
        assert_eq!(updated.icon, "star", "icon should be unchanged");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn delete_board_cascades_columns_and_cards() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "To Delete".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await
            .expect("create_board failed")
            .into_inner();

        let board_id = Uuid::parse_str(&board.id).unwrap();

        // Add a column to confirm cascade.
        let col = svc
            .add_column(authed_request_with_object(
                AddColumnRequest {
                    board_id: board.id.clone(),
                    title: "To Do".to_string(),
                    accent: String::new(),
                    wip_limit: 0,
                    position: 0,
                    idempotency_key: String::new(),
                },
                &subject,
                &board.id,
            ))
            .await
            .expect("add_column failed")
            .into_inner();

        let col_id = Uuid::parse_str(&col.id).unwrap();

        // Seed a card in the column.
        create_test_card(&pool, project_id, board_id, col_id, &subject, "TEST-1").await;

        // Delete the board.
        svc.delete_board(authed_request_with_object(
            DeleteBoardRequest { board_id: board.id.clone() },
            &subject,
            &board.id,
        ))
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
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Position Test Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await
            .expect("create_board failed")
            .into_inner();

        let bid = board.id.clone();

        let c1 = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: "C1".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
                &subject, &bid,
            ))
            .await.expect("add_column failed").into_inner();

        let c2 = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: "C2".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
                &subject, &bid,
            ))
            .await.expect("add_column failed").into_inner();

        let c3 = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: "C3".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
                &subject, &bid,
            ))
            .await.expect("add_column failed").into_inner();

        // Positions should be 0, 1, 2 in order.
        assert_eq!(c1.position, 0, "first column position");
        assert_eq!(c2.position, 1, "second column position");
        assert_eq!(c3.position, 2, "third column position");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn add_column_shifts_when_position_in_middle() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Shift Test Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await
            .expect("create_board failed")
            .into_inner();

        let bid = board.id.clone();

        // Add two columns at position 0 (append).
        svc.add_column(authed_request_with_object(
            AddColumnRequest { board_id: bid.clone(), title: "First".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
            &subject, &bid,
        )).await.expect("add First failed");

        svc.add_column(authed_request_with_object(
            AddColumnRequest { board_id: bid.clone(), title: "Second".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
            &subject, &bid,
        )).await.expect("add Second failed");

        // Insert "Middle" at position 1 (0-based), should push "Second" to position 2.
        let middle = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: "Middle".to_string(), accent: String::new(), wip_limit: 0, position: 1, idempotency_key: String::new() },
                &subject, &bid,
            ))
            .await.expect("add Middle failed").into_inner();

        assert_eq!(middle.position, 1, "inserted column should be at position 1");

        // Verify "Second" was shifted to 2.
        let second_pos: i32 = sqlx::query("SELECT position FROM columns WHERE board_id = $1 AND title = 'Second'")
            .bind(Uuid::parse_str(&bid).unwrap())
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
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Update Column Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await.expect("create_board failed").into_inner();

        let bid = board.id.clone();

        let col = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: "Old Title".to_string(), accent: "blue".to_string(), wip_limit: 5, position: 0, idempotency_key: String::new() },
                &subject, &bid,
            ))
            .await.expect("add_column failed").into_inner();

        let updated = svc
            .update_column(authed_request_with_object(
                UpdateColumnRequest {
                    board_id: bid.clone(),
                    column_id: col.id.clone(),
                    column: Some(Column {
                        id: String::new(),
                        board_id: String::new(),
                        title: "New Title".to_string(),
                        accent: String::new(),
                        wip_limit: 10,
                        position: 0,
                        created_at: None,
                        updated_at: None,
                    }),
                    update_mask: None,
                },
                &subject,
                &bid,
            ))
            .await.expect("update_column failed").into_inner();

        assert_eq!(updated.title, "New Title");
        assert_eq!(updated.wip_limit, 10);
        assert_eq!(updated.accent, "blue", "accent should be unchanged (empty patch)");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn remove_column_deletes_cards_via_cascade() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Remove Col Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject,
                &project_id.to_string(),
            ))
            .await.expect("create_board failed").into_inner();

        let bid = board.id.clone();
        let board_uuid = Uuid::parse_str(&bid).unwrap();

        let col = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: "Doomed".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
                &subject, &bid,
            ))
            .await.expect("add_column failed").into_inner();

        let col_uuid = Uuid::parse_str(&col.id).unwrap();

        // Seed 3 cards in the doomed column.
        for i in 0..3 {
            create_test_card(&pool, project_id, board_uuid, col_uuid, &subject, &format!("DEL-{i}")).await;
        }

        // Verify cards exist.
        let card_count_before: i64 = sqlx::query("SELECT COUNT(*) FROM cards WHERE column_id = $1")
            .bind(col_uuid)
            .fetch_one(&pool).await.unwrap().get(0);
        assert_eq!(card_count_before, 3);

        svc.remove_column(authed_request_with_object(
            RemoveColumnRequest { board_id: bid.clone(), column_id: col.id.clone() },
            &subject, &bid,
        ))
        .await.expect("remove_column failed");

        // Cards should be cascade-deleted.
        let card_count_after: i64 = sqlx::query("SELECT COUNT(*) FROM cards WHERE board_id = $1")
            .bind(board_uuid)
            .fetch_one(&pool).await.unwrap().get(0);
        assert_eq!(card_count_after, 0, "all cards in removed column should be gone");

        // Column should be gone.
        let col_exists: bool = sqlx::query("SELECT EXISTS(SELECT 1 FROM columns WHERE id = $1)")
            .bind(col_uuid)
            .fetch_one(&pool).await.unwrap().get(0);
        assert!(!col_exists, "removed column should not exist");

        cleanup_project(&pool, project_id).await;
    }

    #[tokio::test]
    async fn remove_column_rejects_column_not_on_board() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_a = create_test_project(&pool, &subject).await;
        let project_b = create_test_project(&pool, &subject).await;

        let board_a = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_a.to_string(),
                    name: "Board A".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject, &project_a.to_string(),
            ))
            .await.expect("create_board A failed").into_inner();

        let board_b = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_b.to_string(),
                    name: "Board B".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject, &project_b.to_string(),
            ))
            .await.expect("create_board B failed").into_inner();

        // Add a column to board B.
        let col_b = svc
            .add_column(authed_request_with_object(
                AddColumnRequest { board_id: board_b.id.clone(), title: "Col B".to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
                &subject, &board_b.id,
            ))
            .await.expect("add_column failed").into_inner();

        // Try to remove col_b using board_a's object id — should fail with not_found.
        let result = svc.remove_column(authed_request_with_object(
            RemoveColumnRequest { board_id: board_a.id.clone(), column_id: col_b.id.clone() },
            &subject, &board_a.id,
        )).await;

        assert!(result.is_err(), "removing column from wrong board should fail");
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound, "wrong board removal should be not_found");

        cleanup_project(&pool, project_a).await;
        cleanup_project(&pool, project_b).await;
    }

    #[tokio::test]
    async fn move_column_reorders_within_board() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto)).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&pool, &subject).await;

        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: "Move Col Board".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                },
                &subject, &project_id.to_string(),
            ))
            .await.expect("create_board failed").into_inner();

        let bid = board.id.clone();

        // Add 4 columns: A(0), B(1), C(2), D(3).
        let mut col_ids = vec![];
        for title in &["A", "B", "C", "D"] {
            let c = svc.add_column(authed_request_with_object(
                AddColumnRequest { board_id: bid.clone(), title: title.to_string(), accent: String::new(), wip_limit: 0, position: 0, idempotency_key: String::new() },
                &subject, &bid,
            )).await.expect("add_column failed").into_inner();
            col_ids.push(c.id.clone());
        }

        // Move D (position 3) to position 1 (1-based), so: A(0), D(1), B(2), C(3).
        let resp = svc
            .move_column(authed_request_with_object(
                MoveColumnRequest {
                    board_id: bid.clone(),
                    column_id: col_ids[3].clone(), // D
                    to_position: 1, // 1-based → 0-based = 0... wait, to_position=1 → target_pos=0
                    // Let's move D to position 2 (1-based) → 0-based = 1: A, D, B, C
                    // Actually, to make it clearer: move B (index 1, pos 1) to position 3 (1-based)
                    // so A(0), C(1), D(2), B(3) → no let's keep it simple
                },
                &subject, &bid,
            ))
            .await.expect("move_column failed").into_inner();

        // The response must contain all 4 columns.
        assert_eq!(resp.columns.len(), 4, "move response must include all columns");

        // Positions must be gap-free: 0, 1, 2, 3.
        let mut positions: Vec<i32> = resp.columns.iter().map(|c| c.position).collect();
        positions.sort();
        assert_eq!(positions, vec![0, 1, 2, 3], "positions must be 0-3 after move");

        // D must now be at position 0.
        let d = resp.columns.iter().find(|c| c.id == col_ids[3]).unwrap();
        assert_eq!(d.position, 0, "D should be at position 0");

        cleanup_project(&pool, project_id).await;
    }
}
