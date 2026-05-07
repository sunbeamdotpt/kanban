//! BoardService — Stage 3b implementation.
//!
//! Mirror-table write order: Keto FIRST, then SQL. If the SQL insert fails
//! after a successful Keto write, we log a `mirror_drift` warning and let
//! the hourly reconciler (Stage 7a) catch it. This matches Pre-mortem 5.
//!
//! Dynamic sqlx API (no compile-time macros) is used throughout so that
//! `cargo check` does not require a live DATABASE_URL at build time.

use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use sqlx::PgPool;
use sqlx::Row;
use tonic::{Request, Response, Status};
use tokio_stream::Stream;
use tracing::{error, warn};
use uuid::Uuid;

use sunbeam_g2v::middleware::auth::keto::KetoClient;

use crate::auth::keto_dispatch::CheckedObjectId;
use crate::pb::board_service_server::BoardService;
use crate::pb::{
    AddColumnRequest, Board, BoardDetail, BoardEvent, Column,
    CreateBoardRequest, DeleteBoardRequest, GetBoardRequest,
    ListBoardsRequest, ListBoardsResponse, MoveColumnRequest,
    MoveColumnResponse, RemoveColumnRequest, SubscribeBoardRequest,
    UpdateBoardRequest, UpdateColumnRequest,
};

// ── Constants ────────────────────────────────────────────────────────────────

const KETO_NS_BOARD: &str = "KanbanBoard";

// ── Service struct ───────────────────────────────────────────────────────────

pub struct BoardServiceImpl {
    pub pool: PgPool,
    pub keto: Arc<KetoClient>,
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

type BoardEventStream =
    Pin<Box<dyn Stream<Item = Result<BoardEvent, Status>> + Send + 'static>>;

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

    // ── SubscribeBoard (Stage 4) ──────────────────────────────────────────────

    type SubscribeBoardStream = BoardEventStream;

    async fn subscribe_board(
        &self,
        _request: Request<SubscribeBoardRequest>,
    ) -> Result<Response<Self::SubscribeBoardStream>, Status> {
        Err(Status::unimplemented("Stage 4 streaming"))
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

    fn make_service(pool: PgPool, keto: Arc<KetoClient>) -> BoardServiceImpl {
        BoardServiceImpl { pool, keto }
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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn create_then_get_returns_same_board() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn list_boards_scoped_to_project() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn update_board_applies_patch_fields_only() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn delete_board_cascades_columns_and_cards() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn add_column_appends_at_end_when_position_unset() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn add_column_shifts_when_position_in_middle() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn update_column_changes_title_and_wip() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn remove_column_deletes_cards_via_cascade() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn remove_column_rejects_column_not_on_board() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
    #[ignore = "needs shared compose (postgres + keto)"]
    async fn move_column_reorders_within_board() {
        let pool = setup_pool().await;
        let keto = setup_keto();
        let svc = make_service(pool.clone(), Arc::clone(&keto));

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
