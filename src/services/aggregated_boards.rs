// SPDX-License-Identifier: AGPL-3.0-or-later
//! Aggregated board service.
//!
//! An aggregated board is a read-only view over a set of source boards. It does
//! not own its own cards; any card or column changes go through the source
//! board's own service using the `board_id` carried on each item.
//!
//! `GetAggregatedBoard` streams chunks back to the client, so large aggregates
//! can be returned without pagination.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Row, Transaction};
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
use crate::pb::aggregated_board_service_server::AggregatedBoardService;
use crate::pb::{
    AddSourceBoardRequest, AggregatedBoard, AggregatedBoardChunk, AggregatedCardBatch,
    AggregatedColumn, BoardEventEnvelope, Card, CreateAggregatedBoardRequest, Cutover,
    DeleteAggregatedBoardRequest, GetAggregatedBoardRequest, Heartbeat,
    ListAggregatedBoardsRequest, ListAggregatedBoardsResponse, MoveSourceBoardRequest,
    RemoveSourceBoardRequest, SourceBoardRef, SubscribeAggregatedBoardRequest,
    UpdateAggregatedBoardRequest, aggregated_board_chunk::Payload as ChunkPayload,
    board_event_envelope::Payload as EventPayload,
};
use crate::realtime::cutover::{CutoverTracker, Outcome};
use crate::realtime::registry::BoardSubscriberRegistry;
use crate::services::cards::{
    card_from_row, fetch_assignees, fetch_attachments_count, fetch_checklist, fetch_comments_count,
    fetch_dependencies, fetch_dependents, fetch_labels, to_proto_ts,
};
use crate::services::visibility::{db_to_proto, is_public_or_internal, proto_to_db};

// ── Constants ────────────────────────────────────────────────────────────────

const KETO_NS: &str = "KanbanAggregatedBoard";
const MAX_AGGREGATES: usize = 10_000;
const CARD_BATCH_SIZE: usize = 100;

/// Heartbeat interval for live streams (15 seconds).
const HEARTBEAT_INTERVAL_MS: u64 = 15_000;
/// How often to recheck Keto permissions for live streams (30 seconds).
const KETO_RECHECK_INTERVAL_MS: u64 = 30_000;

// ── Service struct ───────────────────────────────────────────────────────────

pub struct AggregatedBoardServiceImpl {
    pub pool: PgPool,
    pub keto: Arc<KetoClient>,
    pub registry: Arc<BoardSubscriberRegistry>,
    pub watermark: Arc<LogoutWatermark>,
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

fn subject_from_request<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthContext>()
        .and_then(|a| a.subject.clone())
        .ok_or_else(|| Status::unauthenticated("missing auth context"))
}

// ── Timestamp helpers ─────────────────────────────────────────────────────────

fn aggregated_board_from_row(row: &sqlx::postgres::PgRow) -> AggregatedBoard {
    let id: Uuid = row.get("id");
    let name: String = row.get("name");
    let description: Option<String> = row.get("description");
    let icon: Option<String> = row.get("icon");
    let visibility: String = row.get("visibility");
    let created_at: DateTime<Utc> = row.get("created_at");
    let updated_at: DateTime<Utc> = row.get("updated_at");

    AggregatedBoard {
        id: id.to_string(),
        name,
        description: description.unwrap_or_default(),
        icon: icon.unwrap_or_default(),
        visibility: db_to_proto(&visibility),
        created_at: Some(to_proto_ts(created_at)),
        updated_at: Some(to_proto_ts(updated_at)),
    }
}

// ── event_log helper ──────────────────────────────────────────────────────────

async fn insert_event_log(
    tx: &mut Transaction<'_, Postgres>,
    aggregated_board_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), Status> {
    sqlx::query(
        "INSERT INTO event_log (id, aggregated_board_id, event_type, payload, created_at)
         VALUES (gen_random_uuid(), $1, $2, $3::jsonb, now())",
    )
    .bind(aggregated_board_id)
    .bind(event_type)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .map_err(|e| {
        warn!(error = %e, "event_log insert failed");
        internal("failed to insert event_log row", e)
    })?;
    Ok(())
}

// ── Source-board helpers ──────────────────────────────────────────────────────

async fn fetch_ordered_source_ids(
    tx: &mut Transaction<'_, Postgres>,
    aggregated_board_id: Uuid,
) -> Result<Vec<Uuid>, Status> {
    let rows = sqlx::query(
        "SELECT board_id FROM aggregated_board_sources \
         WHERE aggregated_board_id = $1 \
         ORDER BY position ASC, added_at ASC",
    )
    .bind(aggregated_board_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| internal("failed to fetch ordered source board ids", e))?;

    Ok(rows.iter().map(|r| r.get::<Uuid, _>("board_id")).collect())
}

async fn apply_source_order(
    tx: &mut Transaction<'_, Postgres>,
    aggregated_board_id: Uuid,
    ordered_ids: &[Uuid],
) -> Result<(), Status> {
    for (i, board_id) in ordered_ids.iter().enumerate() {
        sqlx::query(
            "UPDATE aggregated_board_sources SET position = $3 \
             WHERE aggregated_board_id = $1 AND board_id = $2",
        )
        .bind(aggregated_board_id)
        .bind(board_id)
        .bind(i as i32)
        .execute(&mut **tx)
        .await
        .map_err(|e| internal("failed to update source board position", e))?;
    }
    Ok(())
}

async fn fetch_source_boards(
    pool: &PgPool,
    aggregated_board_id: Uuid,
) -> Result<Vec<SourceBoardRef>, Status> {
    let rows = sqlx::query(
        r#"
        SELECT b.id, b.project_id, b.name, b.icon, s.position
        FROM aggregated_board_sources s
        JOIN boards b ON b.id = s.board_id
        WHERE s.aggregated_board_id = $1
        ORDER BY s.position ASC, s.added_at ASC
        "#,
    )
    .bind(aggregated_board_id)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch source boards", e))?;

    Ok(rows
        .iter()
        .map(|r| {
            let id: Uuid = r.get("id");
            let project_id: Uuid = r.get("project_id");
            let name: String = r.get("name");
            let icon: Option<String> = r.get("icon");
            let position: i32 = r.get("position");
            SourceBoardRef {
                board_id: id.to_string(),
                project_id: project_id.to_string(),
                name,
                icon: icon.unwrap_or_default(),
                position,
            }
        })
        .collect())
}

async fn fetch_source_board_ids(
    pool: &PgPool,
    aggregated_board_id: Uuid,
) -> Result<Vec<Uuid>, Status> {
    let rows = sqlx::query(
        "SELECT board_id FROM aggregated_board_sources
         WHERE aggregated_board_id = $1
         ORDER BY position ASC, added_at ASC",
    )
    .bind(aggregated_board_id)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch source board ids", e))?;

    Ok(rows.iter().map(|r| r.get::<Uuid, _>("board_id")).collect())
}

/// Keep only the board ids that are public or internal.
async fn fetch_public_internal_board_ids(
    pool: &PgPool,
    board_ids: &[Uuid],
) -> Result<std::collections::HashSet<Uuid>, Status> {
    if board_ids.is_empty() {
        return Ok(std::collections::HashSet::new());
    }

    let rows = sqlx::query(
        "SELECT id FROM boards WHERE id = ANY($1) AND visibility IN ('public', 'internal')",
    )
    .bind(board_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch source board visibilities", e))?;

    Ok(rows.iter().map(|r| r.get::<Uuid, _>("id")).collect())
}

// ── Card helpers ──────────────────────────────────────────────────────────────

async fn fetch_cards_for_boards(
    pool: &PgPool,
    board_ids: &[Uuid],
    visible_board_ids: &[Uuid],
) -> Result<Vec<Card>, Status> {
    if board_ids.is_empty() {
        return Ok(vec![]);
    }

    let visible_set: std::collections::HashSet<Uuid> = visible_board_ids.iter().copied().collect();

    let rows = sqlx::query(
        "SELECT id, project_id, board_id, column_id, ref, title, description, \
                position, priority::text, urgency::text, due_date, completed_at, blocked, cover, \
                milestone_id, revision, created_at, updated_at \
         FROM cards \
         WHERE board_id = ANY($1) \
         ORDER BY board_id, column_id, position",
    )
    .bind(board_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| internal("failed to fetch aggregate cards", e))?;

    let mut cards = Vec::with_capacity(rows.len());
    for row in &rows {
        let card_id: Uuid = row.get("id");
        let board_id: Uuid = row.get("board_id");
        if !visible_set.contains(&board_id) {
            continue;
        }

        let labels = fetch_labels(pool, card_id).await;
        let assignees = fetch_assignees(pool, card_id).await;
        let checklist = fetch_checklist(pool, card_id).await;
        let comments_count = fetch_comments_count(pool, card_id).await;
        let attachments_count = fetch_attachments_count(pool, card_id).await;
        let depends_on = fetch_dependencies(pool, card_id).await;
        let dependents = fetch_dependents(pool, card_id).await;

        cards.push(card_from_row(
            row,
            labels,
            assignees,
            checklist,
            comments_count,
            attachments_count,
            depends_on,
            dependents,
        ));
    }

    Ok(cards)
}

// ── Streaming helpers ─────────────────────────────────────────────────────────

type AggregatedBoardStream =
    Pin<Box<dyn Stream<Item = Result<AggregatedBoardChunk, Status>> + Send + 'static>>;

type SubscribeAggregatedBoardStream =
    Pin<Box<dyn Stream<Item = Result<BoardEventEnvelope, Status>> + Send + 'static>>;

fn cutover_envelope(last_replay_nats_seq: u64) -> BoardEventEnvelope {
    BoardEventEnvelope {
        board_id: String::new(),
        event_id: String::new(),
        nats_seq: 0,
        board_revision: 0,
        emitted_at: None,
        emitter_pod_id: String::new(),
        actor_subject: "system".to_string(),
        payload: Some(EventPayload::Cutover(Cutover {
            last_replay_nats_seq,
        })),
    }
}

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
        payload: Some(EventPayload::Heartbeat(Heartbeat { server_time_ms })),
    }
}

async fn revalidate_token(watermark: &LogoutWatermark, auth: &AuthContext) -> Result<bool, Status> {
    let subject = auth.subject.as_deref().unwrap_or("");
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if let Some(claims) = &auth.claims
        && claims.exp < now_secs
    {
        return Ok(false);
    }
    let iat_ms: u64 = auth
        .claims
        .as_ref()
        .map(|c| (c.iat as u64).saturating_mul(1000))
        .unwrap_or(0);
    match watermark.is_token_valid(subject, iat_ms).await {
        Ok(valid) => Ok(valid),
        Err(e) => {
            warn!(subject, error = %e, "aggregate stream: logout watermark unavailable");
            Err(Status::unavailable("authorization service unavailable"))
        }
    }
}

async fn revalidate_keto(
    keto: &KetoClient,
    auth: &AuthContext,
    aggregated_board_id: &str,
) -> Result<bool, Status> {
    let subject = auth.subject.as_deref().unwrap_or("");
    keto.check_permission_with_retry(KETO_NS, aggregated_board_id, "view", subject)
        .await
        .map_err(|e| {
            warn!(aggregated_board_id, subject, error = %e, "aggregate stream: Keto recheck failed");
            Status::internal("authorization check failed")
        })
}

// ── impl AggregatedBoardService ───────────────────────────────────────────────

#[tonic::async_trait]
impl AggregatedBoardService for AggregatedBoardServiceImpl {
    type GetAggregatedBoardStream = AggregatedBoardStream;
    type SubscribeAggregatedBoardStream = SubscribeAggregatedBoardStream;

    // ── CreateAggregatedBoard ───────────────────────────────────────────────────
    async fn create_aggregated_board(
        &self,
        request: Request<CreateAggregatedBoardRequest>,
    ) -> Result<Response<AggregatedBoard>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        let aggregated_board_id = Uuid::new_v4();
        let visibility = proto_to_db(req.visibility);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("failed to begin transaction", e))?;

        let row = sqlx::query(
            "INSERT INTO aggregated_boards (id, name, description, icon, visibility, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             RETURNING id, name, description, icon, visibility, created_at, updated_at",
        )
        .bind(aggregated_board_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&req.icon)
        .bind(visibility)
        .bind(&subject)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("failed to insert aggregated board", e))?;

        // Insert initial source boards if any.
        for (position, board_id_str) in req.source_board_ids.iter().enumerate() {
            let board_id = Uuid::parse_str(board_id_str)
                .map_err(|_| Status::invalid_argument("invalid source_board_id"))?;
            sqlx::query(
                "INSERT INTO aggregated_board_sources (aggregated_board_id, board_id, position) \
                 VALUES ($1, $2, $3)",
            )
            .bind(aggregated_board_id)
            .bind(board_id)
            .bind(position as i32)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("failed to insert source board", e))?;
        }

        // Keto FIRST: owner tuple.
        self.keto
            .grant_with_retry(KETO_NS, &aggregated_board_id.to_string(), "owner", &subject)
            .await
            .map_err(|e| internal("failed to write Keto owner tuple", e))?;

        // Also write a view tuple so ListAggregatedBoards sees it.
        self.keto
            .grant_with_retry(KETO_NS, &aggregated_board_id.to_string(), "view", &subject)
            .await
            .map_err(|e| internal("failed to write Keto view tuple", e))?;

        // Mirror member row (best-effort after Keto).
        if let Err(e) = sqlx::query(
            "INSERT INTO aggregated_board_members (aggregated_board_id, subject, relation) \
             VALUES ($1, $2, 'owner') ON CONFLICT DO NOTHING",
        )
        .bind(aggregated_board_id)
        .bind(&subject)
        .execute(&mut *tx)
        .await
        {
            warn!(
                error = %e,
                aggregated_board_id = %aggregated_board_id,
                "mirror_drift: Keto owner tuple written but aggregated_board_members insert failed"
            );
        }

        insert_event_log(
            &mut tx,
            aggregated_board_id,
            "AggregatedBoardCreated",
            serde_json::json!({ "aggregated_board_id": aggregated_board_id.to_string() }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("failed to commit transaction", e))?;

        if !req.idempotency_key.is_empty()
            && let Err(e) = sqlx::query(
                "INSERT INTO idempotency_keys (key, response_card_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(&req.idempotency_key)
            .bind(aggregated_board_id)
            .execute(&self.pool)
            .await
            {
                warn!(error = %e, "failed to store idempotency key");
            }

        Ok(Response::new(aggregated_board_from_row(&row)))
    }

    // ── GetAggregatedBoard ──────────────────────────────────────────────────────
    async fn get_aggregated_board(
        &self,
        request: Request<GetAggregatedBoardRequest>,
    ) -> Result<Response<AggregatedBoardStream>, Status> {
        let subject = subject_from_request(&request)?;
        let req = request.into_inner();
        let aggregated_board_id = Uuid::parse_str(&req.aggregated_board_id)
            .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?;

        let row = sqlx::query(
            "SELECT id, name, description, icon, visibility, created_at, updated_at \
             FROM aggregated_boards WHERE id = $1",
        )
        .bind(aggregated_board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch aggregated board", e))?
        .ok_or_else(|| Status::not_found("aggregated board not found"))?;

        let aggregate_visibility: String = row.get("visibility");
        if !is_public_or_internal(&aggregate_visibility) {
            let allowed = self
                .keto
                .check_permission_with_retry(
                    KETO_NS,
                    &aggregated_board_id.to_string(),
                    "view",
                    &subject,
                )
                .await
                .map_err(|e| internal("failed to check aggregated board view permission", e))?;
            if !allowed {
                return Err(Status::permission_denied(
                    "you do not have permission to view this aggregated board",
                ));
            }
        }

        let metadata = aggregated_board_from_row(&row);
        let source_boards = fetch_source_boards(&self.pool, aggregated_board_id).await?;
        let board_ids: Vec<Uuid> = source_boards
            .iter()
            .filter_map(|s| Uuid::parse_str(&s.board_id).ok())
            .collect();

        // Source boards are visible if public/internal or if private and the
        // caller has an explicit Keto view relation.
        let allowed_private_ids = expand_objects(
            &self.keto,
            ExpandQuery {
                namespace: "KanbanBoard",
                relation: "view",
                subject: &subject,
                max_page_size: 256,
            },
            10_000,
        )
        .await
        .map_err(|e| internal("keto expand failed", e))?;

        let public_internal_ids = fetch_public_internal_board_ids(&self.pool, &board_ids).await?;

        let allowed_source_ids: std::collections::HashSet<Uuid> = board_ids
            .iter()
            .copied()
            .filter(|id| {
                public_internal_ids.contains(id) || allowed_private_ids.contains(&id.to_string())
            })
            .collect();

        let visible_board_uuids: Vec<Uuid> = allowed_source_ids.iter().copied().collect();

        let cards = fetch_cards_for_boards(&self.pool, &board_ids, &visible_board_uuids).await?;

        let mut chunks: Vec<AggregatedBoardChunk> = Vec::new();
        chunks.push(AggregatedBoardChunk {
            payload: Some(ChunkPayload::Metadata(metadata)),
        });
        let filtered_source_boards: Vec<_> = source_boards
            .into_iter()
            .filter(|sb| {
                Uuid::parse_str(&sb.board_id)
                    .map(|id| allowed_source_ids.contains(&id))
                    .unwrap_or(false)
            })
            .collect();
        for sb in &filtered_source_boards {
            chunks.push(AggregatedBoardChunk {
                payload: Some(ChunkPayload::SourceBoard(sb.clone())),
            });
        }
        for (position, sb) in filtered_source_boards.iter().enumerate() {
            chunks.push(AggregatedBoardChunk {
                payload: Some(ChunkPayload::Column(AggregatedColumn {
                    id: format!("agg-col-{}", sb.board_id),
                    title: sb.name.clone(),
                    accent: sb.icon.clone(),
                    wip_limit: 0,
                    position: position as i32 + 1,
                    source_board_id: sb.board_id.clone(),
                })),
            });
        }

        // Card batches.
        for batch in cards.chunks(CARD_BATCH_SIZE) {
            chunks.push(AggregatedBoardChunk {
                payload: Some(ChunkPayload::CardBatch(AggregatedCardBatch {
                    cards: batch.to_vec(),
                })),
            });
        }

        let stream = stream! {
            for chunk in chunks {
                yield Ok(chunk);
            }
        };

        Ok(Response::new(Box::pin(stream) as AggregatedBoardStream))
    }

    // ── UpdateAggregatedBoard ───────────────────────────────────────────────────
    async fn update_aggregated_board(
        &self,
        request: Request<UpdateAggregatedBoardRequest>,
    ) -> Result<Response<AggregatedBoard>, Status> {
        let subject = subject_from_request(&request)?;
        let object_id = checked_object_id(&request)?;
        let aggregated_board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?;

        let req = request.into_inner();
        let patch = req.aggregated_board.unwrap_or_default();

        let update_paths: std::collections::HashSet<&str> = req
            .update_mask
            .as_ref()
            .map(|m| m.paths.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let visibility_change = update_paths.contains("visibility");

        if visibility_change {
            let allowed = self
                .keto
                .check_permission_with_retry(
                    KETO_NS,
                    &aggregated_board_id.to_string(),
                    "manage",
                    &subject,
                )
                .await
                .map_err(|e| internal("failed to check aggregated board manage permission", e))?;
            if !allowed {
                return Err(Status::permission_denied(
                    "you do not have permission to change aggregated board visibility",
                ));
            }
        }

        let new_visibility = proto_to_db(patch.visibility);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("failed to begin transaction", e))?;

        let row = sqlx::query(
            "UPDATE aggregated_boards SET \
                name        = CASE WHEN $2 != '' THEN $2 ELSE name END, \
                description = CASE WHEN $3 != '' THEN $3 ELSE description END, \
                icon        = CASE WHEN $4 != '' THEN $4 ELSE icon END, \
                visibility  = CASE WHEN $5::boolean THEN $6 ELSE visibility END, \
                updated_at  = now() \
             WHERE id = $1 \
             RETURNING id, name, description, icon, visibility, created_at, updated_at",
        )
        .bind(aggregated_board_id)
        .bind(&patch.name)
        .bind(&patch.description)
        .bind(&patch.icon)
        .bind(visibility_change)
        .bind(new_visibility)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("failed to update aggregated board", e))?
        .ok_or_else(|| Status::not_found("aggregated board not found"))?;

        insert_event_log(
            &mut tx,
            aggregated_board_id,
            "AggregatedBoardUpdated",
            serde_json::json!({ "aggregated_board_id": aggregated_board_id.to_string() }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("failed to commit transaction", e))?;

        Ok(Response::new(aggregated_board_from_row(&row)))
    }

    // ── DeleteAggregatedBoard ───────────────────────────────────────────────────
    async fn delete_aggregated_board(
        &self,
        request: Request<DeleteAggregatedBoardRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = checked_object_id(&request)?;
        let aggregated_board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?;

        let result = sqlx::query("DELETE FROM aggregated_boards WHERE id = $1")
            .bind(aggregated_board_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal("failed to delete aggregated board", e))?;

        if result.rows_affected() == 0 {
            return Err(Status::not_found("aggregated board not found"));
        }

        warn!(
            aggregated_board_id = %aggregated_board_id,
            "delete_aggregated_board: Keto tuple cleanup is best-effort; reconciler will catch any drift"
        );

        Ok(Response::new(()))
    }

    // ── ListAggregatedBoards ────────────────────────────────────────────────────
    async fn list_aggregated_boards(
        &self,
        request: Request<ListAggregatedBoardsRequest>,
    ) -> Result<Response<ListAggregatedBoardsResponse>, Status> {
        let subject = subject_from_request(&request)?;

        // Public/internal aggregates are visible to any authenticated user;
        // private aggregates require an explicit Keto view relation.
        let allowed_private_ids = expand_objects(
            &self.keto,
            ExpandQuery {
                namespace: KETO_NS,
                relation: "view",
                subject: &subject,
                max_page_size: 256,
            },
            MAX_AGGREGATES,
        )
        .await
        .map_err(|e| internal("keto expand failed", e))?;

        let rows = sqlx::query(
            "SELECT id, name, description, icon, visibility, created_at, updated_at \
             FROM aggregated_boards ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| internal("failed to list aggregated boards", e))?;

        let aggregated_boards: Vec<AggregatedBoard> = rows
            .iter()
            .filter(|row| {
                let id: Uuid = row.get("id");
                let visibility: String = row.get("visibility");
                is_public_or_internal(&visibility) || allowed_private_ids.contains(&id.to_string())
            })
            .map(aggregated_board_from_row)
            .collect();

        Ok(Response::new(ListAggregatedBoardsResponse {
            aggregated_boards,
        }))
    }

    // ── AddSourceBoard ──────────────────────────────────────────────────────────
    async fn add_source_board(
        &self,
        request: Request<AddSourceBoardRequest>,
    ) -> Result<Response<AggregatedBoard>, Status> {
        let object_id = checked_object_id(&request)?;
        let aggregated_board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?;

        let req = request.into_inner();
        let board_id = Uuid::parse_str(&req.board_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("failed to begin transaction", e))?;

        // Determine position.
        let position = if req.position > 0 {
            req.position
        } else {
            let count_row = sqlx::query(
                "SELECT COUNT(*) AS cnt FROM aggregated_board_sources WHERE aggregated_board_id = $1",
            )
            .bind(aggregated_board_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| internal("failed to count source boards", e))?;
            let count: i64 = count_row.get("cnt");
            count as i32
        };

        // Shift existing boards at or after the target position upward so the
        // new board lands exactly at `position`.
        sqlx::query(
            "UPDATE aggregated_board_sources \
             SET position = position + 1 \
             WHERE aggregated_board_id = $1 AND position >= $2",
        )
        .bind(aggregated_board_id)
        .bind(position)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to shift source board positions", e))?;

        sqlx::query(
            "INSERT INTO aggregated_board_sources (aggregated_board_id, board_id, position) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (aggregated_board_id, board_id) DO UPDATE SET position = EXCLUDED.position",
        )
        .bind(aggregated_board_id)
        .bind(board_id)
        .bind(position)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to insert source board", e))?;

        // Normalise after an upsert so positions stay contiguous and reflect
        // the requested insertion point.
        let ordered = fetch_ordered_source_ids(&mut tx, aggregated_board_id).await?;
        apply_source_order(&mut tx, aggregated_board_id, &ordered).await?;

        insert_event_log(
            &mut tx,
            aggregated_board_id,
            "SourceBoardAdded",
            serde_json::json!({
                "aggregated_board_id": aggregated_board_id.to_string(),
                "board_id": board_id.to_string(),
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("failed to commit transaction", e))?;

        self.get_aggregate_metadata(aggregated_board_id).await
    }

    // ── RemoveSourceBoard ───────────────────────────────────────────────────────
    async fn remove_source_board(
        &self,
        request: Request<RemoveSourceBoardRequest>,
    ) -> Result<Response<AggregatedBoard>, Status> {
        let object_id = checked_object_id(&request)?;
        let aggregated_board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?;

        let req = request.into_inner();
        let board_id = Uuid::parse_str(&req.board_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("failed to begin transaction", e))?;

        sqlx::query(
            "DELETE FROM aggregated_board_sources \
             WHERE aggregated_board_id = $1 AND board_id = $2",
        )
        .bind(aggregated_board_id)
        .bind(board_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("failed to remove source board", e))?;

        // Keep positions contiguous after removal.
        let ordered = fetch_ordered_source_ids(&mut tx, aggregated_board_id).await?;
        apply_source_order(&mut tx, aggregated_board_id, &ordered).await?;

        insert_event_log(
            &mut tx,
            aggregated_board_id,
            "SourceBoardRemoved",
            serde_json::json!({
                "aggregated_board_id": aggregated_board_id.to_string(),
                "board_id": board_id.to_string(),
            }),
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| internal("failed to commit transaction", e))?;

        self.get_aggregate_metadata(aggregated_board_id).await
    }

    // ── MoveSourceBoard ─────────────────────────────────────────────────────────
    async fn move_source_board(
        &self,
        request: Request<MoveSourceBoardRequest>,
    ) -> Result<Response<AggregatedBoard>, Status> {
        let object_id = checked_object_id(&request)?;
        let aggregated_board_id = Uuid::parse_str(&object_id)
            .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?;

        let req = request.into_inner();
        let board_id = Uuid::parse_str(&req.board_id)
            .map_err(|_| Status::invalid_argument("invalid board_id"))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| internal("failed to begin transaction", e))?;

        // Reorder in-memory and write contiguous positions back. For v1 we
        // accept the small race window; advisory locks are not used.
        let mut ordered = fetch_ordered_source_ids(&mut tx, aggregated_board_id).await?;
        let current_idx = ordered
            .iter()
            .position(|&id| id == board_id)
            .ok_or_else(|| Status::not_found("source board not found in aggregate"))?;
        let board_id_moved = ordered.remove(current_idx);
        let target_idx = (req.to_position as usize).min(ordered.len());
        ordered.insert(target_idx, board_id_moved);
        apply_source_order(&mut tx, aggregated_board_id, &ordered).await?;

        tx.commit()
            .await
            .map_err(|e| internal("failed to commit transaction", e))?;

        self.get_aggregate_metadata(aggregated_board_id).await
    }

    // ── SubscribeAggregatedBoard ────────────────────────────────────────────────
    async fn subscribe_aggregated_board(
        &self,
        request: Request<SubscribeAggregatedBoardRequest>,
    ) -> Result<Response<SubscribeAggregatedBoardStream>, Status> {
        let auth = request
            .extensions()
            .get::<AuthContext>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("missing auth context"))?;
        let req = request.into_inner();
        let aggregated_board_id = req.aggregated_board_id;

        let visibility: String =
            sqlx::query_scalar("SELECT visibility FROM aggregated_boards WHERE id = $1")
                .bind(
                    Uuid::parse_str(&aggregated_board_id)
                        .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?,
                )
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| internal("failed to fetch aggregated board visibility", e))?
                .ok_or_else(|| Status::not_found("aggregated board not found"))?;

        let is_private = !is_public_or_internal(&visibility);

        let source_board_ids = fetch_source_board_ids(
            &self.pool,
            Uuid::parse_str(&aggregated_board_id)
                .map_err(|_| Status::invalid_argument("invalid aggregated_board_id"))?,
        )
        .await?;

        let registry = Arc::clone(&self.registry);
        let keto = Arc::clone(&self.keto);
        let watermark = Arc::clone(&self.watermark);

        let stream = build_subscribe_aggregated_board_stream(SubscribeAggregatedBoardArgs {
            registry,
            keto,
            watermark,
            auth,
            aggregated_board_id,
            source_board_ids,
            is_private,
            heartbeat_interval: Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            keto_recheck_interval: Duration::from_millis(KETO_RECHECK_INTERVAL_MS),
        })
        .await?;

        Ok(Response::new(stream))
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

impl AggregatedBoardServiceImpl {
    async fn get_aggregate_metadata(
        &self,
        aggregated_board_id: Uuid,
    ) -> Result<Response<AggregatedBoard>, Status> {
        let row = sqlx::query(
            "SELECT id, name, description, icon, visibility, created_at, updated_at \
             FROM aggregated_boards WHERE id = $1",
        )
        .bind(aggregated_board_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal("failed to fetch aggregated board", e))?
        .ok_or_else(|| Status::not_found("aggregated board not found"))?;

        Ok(Response::new(aggregated_board_from_row(&row)))
    }
}

// ── Streaming implementation ──────────────────────────────────────────────────

/// Configuration passed to `build_subscribe_aggregated_board_stream`.
pub struct SubscribeAggregatedBoardArgs {
    pub registry: Arc<BoardSubscriberRegistry>,
    pub keto: Arc<KetoClient>,
    pub watermark: Arc<LogoutWatermark>,
    pub auth: AuthContext,
    pub aggregated_board_id: String,
    pub source_board_ids: Vec<Uuid>,
    pub is_private: bool,
    pub heartbeat_interval: Duration,
    pub keto_recheck_interval: Duration,
}

pub async fn build_subscribe_aggregated_board_stream(
    args: SubscribeAggregatedBoardArgs,
) -> Result<SubscribeAggregatedBoardStream, Status> {
    let SubscribeAggregatedBoardArgs {
        registry,
        keto,
        watermark,
        auth,
        aggregated_board_id,
        source_board_ids,
        is_private,
        heartbeat_interval,
        keto_recheck_interval,
    } = args;

    let s = stream! {
        // Emit cutover immediately (empty replay).
        yield Ok(cutover_envelope(0));

        // Subscribe to the aggregate's own event stream.
        let mut aggregate_handle = match Arc::clone(&registry).subscribe(&aggregated_board_id).await {
            Ok(h) => h,
            Err(e) => {
                warn!(aggregated_board_id = %aggregated_board_id, error = %e, "SubscribeAggregatedBoard: aggregate subscribe failed");
                yield Err(Status::internal(format!("subscribe: {e}")));
                return;
            }
        };

        // Subscribe to each source board.
        let mut source_handles = Vec::with_capacity(source_board_ids.len());
        for board_id in &source_board_ids {
            match Arc::clone(&registry).subscribe(&board_id.to_string()).await {
                Ok(h) => source_handles.push(h),
                Err(e) => {
                    warn!(board_id = %board_id, error = %e, "SubscribeAggregatedBoard: source subscribe failed");
                    yield Err(Status::internal(format!("subscribe: {e}")));
                    return;
                }
            }
        }

        let mut tracker = CutoverTracker::new();
        tracker.cutover_to_live(0);

        // Forward all source board events (and aggregate events) into a single
        // mpsc channel so we can select! over one receiver plus heartbeat.
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<BoardEventEnvelope>();

        let aggregate_tx = event_tx.clone();
        tokio::spawn(async move {
            while let Ok(envelope) = aggregate_handle.receiver.recv().await {
                if aggregate_tx.send(envelope).is_err() {
                    break;
                }
            }
        });

        for mut handle in source_handles {
            let tx = event_tx.clone();
            tokio::spawn(async move {
                while let Ok(envelope) = handle.receiver.recv().await {
                    if tx.send(envelope).is_err() {
                        break;
                    }
                }
            });
        }

        // Drop the original sender so the receiver closes when all forwarders drop.
        drop(event_tx);

        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;

        let mut last_keto_recheck = Instant::now();

        loop {
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

            if is_private && last_keto_recheck.elapsed() >= keto_recheck_interval {
                match revalidate_keto(&keto, &auth, &aggregated_board_id).await {
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

            tokio::select! {
                msg = event_rx.recv() => {
                    match msg {
                        Some(envelope) => {
                            match tracker.observe_live(&envelope.event_id, envelope.nats_seq) {
                                Outcome::Emit => yield Ok(envelope),
                                Outcome::Drop | Outcome::OutOfOrder => {}
                            }
                        }
                        None => {
                            // All forwarders have dropped; stream is done.
                            break;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    yield Ok(heartbeat_envelope());
                }
            }
        }
    };

    Ok(Box::pin(s) as SubscribeAggregatedBoardStream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    use tonic::Request;

    use crate::pb::board_service_server::BoardService;
    use crate::pb::card_service_server::CardService;
    use crate::pb::{BoardVisibility, CreateBoardRequest, CreateCardRequest, GetBoardRequest};
    use crate::services::boards::BoardServiceImpl;
    use crate::services::cards::CardServiceImpl;
    use crate::test_support::containers;
    use prost_types::FieldMask;

    fn authed_request<T>(body: T, subject: &str) -> Request<T> {
        let mut req = Request::new(body);
        req.extensions_mut()
            .insert(sunbeam_g2v::middleware::auth::AuthContext::authenticated(
                subject, None,
            ));
        req
    }

    fn authed_request_with_object<T>(body: T, subject: &str, object_id: &str) -> Request<T> {
        let mut req = authed_request(body, subject);
        req.extensions_mut()
            .insert(CheckedObjectId(object_id.to_string()));
        req
    }

    async fn make_service(infra: &containers::TestInfra) -> AggregatedBoardServiceImpl {
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&infra.nats),
            "pod-test-agg",
        ));
        AggregatedBoardServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            registry,
            watermark: Arc::clone(&infra.watermark),
        }
    }

    async fn make_board_service(infra: &containers::TestInfra) -> BoardServiceImpl {
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&infra.nats),
            "pod-test-board",
        ));
        BoardServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
            registry,
            watermark: Arc::clone(&infra.watermark),
        }
    }

    fn make_card_service(infra: &containers::TestInfra) -> CardServiceImpl {
        CardServiceImpl {
            pool: infra.pool.clone(),
            keto: Arc::clone(&infra.keto),
        }
    }

    async fn create_test_project(pool: &sqlx::PgPool, subject: &str) -> Uuid {
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

    async fn cleanup_project(pool: &sqlx::PgPool, project_id: Uuid) {
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    async fn cleanup_aggregated_board(pool: &sqlx::PgPool, aggregated_board_id: Uuid) {
        let _ = sqlx::query("DELETE FROM aggregated_boards WHERE id = $1")
            .bind(aggregated_board_id)
            .execute(pool)
            .await;
    }

    /// Create a source board and add a default column so tests can create cards right away.
    async fn create_source_board(
        svc: &BoardServiceImpl,
        project_id: Uuid,
        name: &str,
        subject: &str,
    ) -> Uuid {
        create_source_board_with_visibility(
            svc,
            project_id,
            name,
            subject,
            BoardVisibility::Private as i32,
        )
        .await
    }

    async fn create_source_board_with_visibility(
        svc: &BoardServiceImpl,
        project_id: Uuid,
        name: &str,
        subject: &str,
        visibility: i32,
    ) -> Uuid {
        let board = svc
            .create_board(authed_request_with_object(
                CreateBoardRequest {
                    project_id: project_id.to_string(),
                    name: name.to_string(),
                    description: String::new(),
                    icon: String::new(),
                    idempotency_key: String::new(),
                    visibility,
                },
                subject,
                &project_id.to_string(),
            ))
            .await
            .expect("create_board failed")
            .into_inner();
        let board_id = Uuid::parse_str(&board.id).expect("board id is uuid");

        // BoardService only writes the parent tuple; test Keto does not evaluate
        // derived permissions, so grant the creator an explicit view tuple.
        svc.keto
            .grant_with_retry("KanbanBoard", &board_id.to_string(), "view", subject)
            .await
            .expect("grant board view failed");

        // BoardService does not create a default column; add one directly so
        // card creation tests have a target column.
        sqlx::query(
            "INSERT INTO columns (id, board_id, title, position) VALUES ($1, $2, 'todo', 0)",
        )
        .bind(Uuid::new_v4())
        .bind(board_id)
        .execute(&svc.pool)
        .await
        .expect("failed to insert default column");

        board_id
    }

    /// Grant an explicit `view` tuple on a board in tests.
    ///
    /// The Keto instance used in tests does not evaluate derived permissions,
    /// so tests that read a board must grant `view` directly instead of relying
    /// on the project parent.
    async fn grant_board_view(keto: &KetoClient, board_id: Uuid, subject: &str) {
        keto.grant_with_retry("KanbanBoard", &board_id.to_string(), "view", subject)
            .await
            .expect("grant board view failed");
    }

    /// Drain a `GetAggregatedBoard` stream into a vector of chunks.
    async fn collect_aggregate_stream(
        mut stream: AggregatedBoardStream,
    ) -> Vec<AggregatedBoardChunk> {
        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("stream item failed"));
        }
        chunks
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_then_get_aggregated_board() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;

        let board_a = create_source_board(&board_svc, project_id, "Source A", &subject).await;
        let board_b = create_source_board(&board_svc, project_id, "Source B", &subject).await;

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Cross-Project View".to_string(),
                    description: "meta board".to_string(),
                    icon: "layers".to_string(),
                    source_board_ids: vec![board_a.to_string(), board_b.to_string()],
                    idempotency_key: String::new(),
                    visibility: crate::pb::BoardVisibility::Private as i32,
                },
                &subject,
            ))
            .await
            .expect("create_aggregated_board failed")
            .into_inner();

        assert!(!created.id.is_empty());
        assert_eq!(created.name, "Cross-Project View");

        let agg_id = created.id.clone();
        let stream = agg_svc
            .get_aggregated_board(authed_request_with_object(
                GetAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("get_aggregated_board failed")
            .into_inner();

        let chunks = collect_aggregate_stream(stream).await;
        let metadata = chunks
            .iter()
            .find_map(|c| match &c.payload {
                Some(ChunkPayload::Metadata(m)) => Some(m),
                _ => None,
            })
            .expect("metadata chunk missing");
        assert_eq!(metadata.id, agg_id);
        assert_eq!(metadata.name, "Cross-Project View");

        let source_ids: Vec<String> = chunks
            .iter()
            .filter_map(|c| match &c.payload {
                Some(ChunkPayload::SourceBoard(sb)) => Some(sb.board_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(source_ids.len(), 2);
        assert!(source_ids.contains(&board_a.to_string()));
        assert!(source_ids.contains(&board_b.to_string()));

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
        cleanup_project(&infra.pool, project_id).await;
    }

    #[tokio::test]
    async fn list_aggregated_boards_returns_visible_boards() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let board_id = create_source_board(&board_svc, project_id, "Source", &subject).await;

        let _ = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Visible A".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![board_id.to_string()],
                    idempotency_key: String::new(),
                    visibility: crate::pb::BoardVisibility::Private as i32,
                },
                &subject,
            ))
            .await
            .expect("create A failed")
            .into_inner();

        let _ = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Visible B".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![board_id.to_string()],
                    idempotency_key: String::new(),
                    visibility: crate::pb::BoardVisibility::Private as i32,
                },
                &subject,
            ))
            .await
            .expect("create B failed")
            .into_inner();

        let list = agg_svc
            .list_aggregated_boards(authed_request(ListAggregatedBoardsRequest {}, &subject))
            .await
            .expect("list failed")
            .into_inner();

        let names: Vec<&str> = list
            .aggregated_boards
            .iter()
            .map(|b| b.name.as_str())
            .collect();
        assert!(names.contains(&"Visible A"));
        assert!(names.contains(&"Visible B"));

        cleanup_project(&infra.pool, project_id).await;
    }

    #[tokio::test]
    async fn add_remove_and_move_source_boards() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let board_a = create_source_board(&board_svc, project_id, "A", &subject).await;
        let board_b = create_source_board(&board_svc, project_id, "B", &subject).await;
        let board_c = create_source_board(&board_svc, project_id, "C", &subject).await;

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Reorder Test".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![board_a.to_string()],
                    idempotency_key: String::new(),
                    visibility: crate::pb::BoardVisibility::Private as i32,
                },
                &subject,
            ))
            .await
            .expect("create failed")
            .into_inner();
        let agg_id = created.id;

        agg_svc
            .add_source_board(authed_request_with_object(
                AddSourceBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                    board_id: board_b.to_string(),
                    position: 1,
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("add B failed");

        agg_svc
            .add_source_board(authed_request_with_object(
                AddSourceBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                    board_id: board_c.to_string(),
                    position: 2,
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("add C failed");

        agg_svc
            .move_source_board(authed_request_with_object(
                MoveSourceBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                    board_id: board_a.to_string(),
                    to_position: 2,
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("move failed");

        agg_svc
            .remove_source_board(authed_request_with_object(
                RemoveSourceBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                    board_id: board_b.to_string(),
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("remove failed");

        let stream = agg_svc
            .get_aggregated_board(authed_request_with_object(
                GetAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("get failed")
            .into_inner();
        let chunks = collect_aggregate_stream(stream).await;

        let source_ids: Vec<String> = chunks
            .iter()
            .filter_map(|c| match &c.payload {
                Some(ChunkPayload::SourceBoard(sb)) => Some(sb.board_id.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(source_ids, vec![board_c.to_string(), board_a.to_string()]);

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
        cleanup_project(&infra.pool, project_id).await;
    }

    #[tokio::test]
    async fn get_aggregated_board_includes_cards() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;
        let card_svc = make_card_service(&infra);

        let subject = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &subject).await;
        let board_id = create_source_board(&board_svc, project_id, "Cards Board", &subject).await;
        grant_board_view(&infra.keto, board_id, &subject).await;

        // Find the default column created by BoardService.
        let board_detail = board_svc
            .get_board(authed_request_with_object(
                GetBoardRequest {
                    board_id: board_id.to_string(),
                },
                &subject,
                &board_id.to_string(),
            ))
            .await
            .expect("get board failed")
            .into_inner();
        let column_id = board_detail
            .columns
            .first()
            .expect("default column missing")
            .id
            .clone();

        card_svc
            .create_card(authed_request_with_object(
                CreateCardRequest {
                    board_id: board_id.to_string(),
                    column_id: column_id.clone(),
                    title: "Card One".to_string(),
                    idempotency_key: String::new(),
                    ..Default::default()
                },
                &subject,
                &board_id.to_string(),
            ))
            .await
            .expect("create card failed");

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Cards Aggregate".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![board_id.to_string()],
                    idempotency_key: String::new(),
                    visibility: crate::pb::BoardVisibility::Private as i32,
                },
                &subject,
            ))
            .await
            .expect("create aggregate failed")
            .into_inner();
        let agg_id = created.id;

        let stream = agg_svc
            .get_aggregated_board(authed_request_with_object(
                GetAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                },
                &subject,
                &agg_id,
            ))
            .await
            .expect("get aggregate failed")
            .into_inner();
        let chunks = collect_aggregate_stream(stream).await;

        let cards: Vec<&Card> = chunks
            .iter()
            .filter_map(|c| match &c.payload {
                Some(ChunkPayload::CardBatch(batch)) => {
                    Some(batch.cards.iter().collect::<Vec<_>>())
                }
                _ => None,
            })
            .flatten()
            .collect();

        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].title, "Card One");

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
        cleanup_project(&infra.pool, project_id).await;
    }

    #[tokio::test]
    async fn list_aggregated_boards_hides_boards_from_other_subject() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let other = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &owner).await;
        let board_id = create_source_board(&board_svc, project_id, "Source", &owner).await;

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Private Aggregate".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![board_id.to_string()],
                    idempotency_key: String::new(),
                    visibility: crate::pb::BoardVisibility::Private as i32,
                },
                &owner,
            ))
            .await
            .expect("create failed")
            .into_inner();
        let agg_id = created.id;

        // ListAggregatedBoards performs its own Keto expansion; a subject with
        // no tuples should see nothing even though the service is called
        // directly (no middleware gating this RPC in tests).
        let list = agg_svc
            .list_aggregated_boards(authed_request(ListAggregatedBoardsRequest {}, &other))
            .await
            .expect("list failed")
            .into_inner();

        assert!(
            list.aggregated_boards.iter().all(|b| b.id != agg_id),
            "other subject should not see the aggregate"
        );

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
        cleanup_project(&infra.pool, project_id).await;
    }

    // ── Visibility tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_aggregated_board_allows_non_member_for_public_aggregate() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let stranger = format!("user:test-{}", Uuid::new_v4());

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Public Aggregate".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Public as i32,
                },
                &owner,
            ))
            .await
            .expect("create aggregate failed")
            .into_inner();
        let agg_id = created.id;

        let stream = agg_svc
            .get_aggregated_board(authed_request_with_object(
                GetAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                },
                &stranger,
                &agg_id,
            ))
            .await
            .expect("non-member should view public aggregate")
            .into_inner();

        let chunks = collect_aggregate_stream(stream).await;
        let metadata = chunks
            .iter()
            .find_map(|c| match &c.payload {
                Some(ChunkPayload::Metadata(m)) => Some(m),
                _ => None,
            })
            .expect("metadata chunk missing");
        assert_eq!(metadata.id, agg_id);

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
    }

    #[tokio::test]
    async fn get_aggregated_board_denies_non_member_for_private_aggregate() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let stranger = format!("user:test-{}", Uuid::new_v4());

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Private Aggregate".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private as i32,
                },
                &owner,
            ))
            .await
            .expect("create aggregate failed")
            .into_inner();
        let agg_id = created.id;

        let result = agg_svc
            .get_aggregated_board(authed_request_with_object(
                GetAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                },
                &stranger,
                &agg_id,
            ))
            .await;

        assert!(
            result.is_err(),
            "non-member must not view private aggregate"
        );
        assert_eq!(
            result.err().unwrap().code(),
            tonic::Code::PermissionDenied,
            "private aggregate must return PermissionDenied"
        );

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
    }

    #[tokio::test]
    async fn list_aggregated_boards_returns_public_internal_for_any_authenticated_user() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let viewer = format!("user:test-{}", Uuid::new_v4());
        let non_viewer = format!("user:test-{}", Uuid::new_v4());

        let public_agg = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Public Agg".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Public as i32,
                },
                &owner,
            ))
            .await
            .expect("create public aggregate failed")
            .into_inner();

        let internal_agg = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Internal Agg".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Internal as i32,
                },
                &owner,
            ))
            .await
            .expect("create internal aggregate failed")
            .into_inner();

        let private_agg = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Private Agg".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private as i32,
                },
                &owner,
            ))
            .await
            .expect("create private aggregate failed")
            .into_inner();

        // Grant one stranger explicit view on the private aggregate.
        infra
            .keto
            .grant_with_retry(KETO_NS, &private_agg.id, "view", &viewer)
            .await
            .expect("grant view failed");

        let list = agg_svc
            .list_aggregated_boards(authed_request(ListAggregatedBoardsRequest {}, &viewer))
            .await
            .expect("list failed")
            .into_inner();

        let names: Vec<&str> = list
            .aggregated_boards
            .iter()
            .map(|b| b.name.as_str())
            .collect();
        assert!(
            names.contains(&"Public Agg"),
            "public aggregate must be listed"
        );
        assert!(
            names.contains(&"Internal Agg"),
            "internal aggregate must be listed"
        );
        assert!(
            names.contains(&"Private Agg"),
            "private aggregate with view tuple must be listed"
        );

        // A different stranger without a view tuple should see only public/internal.
        let list_after = agg_svc
            .list_aggregated_boards(authed_request(ListAggregatedBoardsRequest {}, &non_viewer))
            .await
            .expect("list failed")
            .into_inner();

        let names_after: Vec<&str> = list_after
            .aggregated_boards
            .iter()
            .map(|b| b.name.as_str())
            .collect();
        assert!(
            !names_after.contains(&"Private Agg"),
            "private aggregate must be hidden without view tuple"
        );
        assert!(
            names_after.contains(&"Public Agg"),
            "public aggregate must still be listed"
        );
        assert!(
            names_after.contains(&"Internal Agg"),
            "internal aggregate must still be listed"
        );

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&public_agg.id).unwrap()).await;
        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&internal_agg.id).unwrap()).await;
        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&private_agg.id).unwrap()).await;
    }

    #[tokio::test]
    async fn update_aggregated_board_visibility_requires_manage_and_persists() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Visibility Patch".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private as i32,
                },
                &owner,
            ))
            .await
            .expect("create aggregate failed")
            .into_inner();
        let agg_id = created.id;

        // Grant edit (middleware) and manage (handler-level visibility check).
        infra
            .keto
            .grant_with_retry(KETO_NS, &agg_id, "edit", &owner)
            .await
            .expect("grant edit failed");
        infra
            .keto
            .grant_with_retry(KETO_NS, &agg_id, "manage", &owner)
            .await
            .expect("grant manage failed");

        let updated = agg_svc
            .update_aggregated_board(authed_request_with_object(
                UpdateAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                    aggregated_board: Some(AggregatedBoard {
                        id: String::new(),
                        name: String::new(),
                        description: String::new(),
                        icon: String::new(),
                        visibility: BoardVisibility::Public as i32,
                        created_at: None,
                        updated_at: None,
                    }),
                    update_mask: Some(FieldMask {
                        paths: vec!["visibility".to_string()],
                    }),
                },
                &owner,
                &agg_id,
            ))
            .await
            .expect("update visibility failed")
            .into_inner();

        assert_eq!(
            updated.visibility,
            BoardVisibility::Public as i32,
            "visibility must be persisted as public"
        );

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
    }

    #[tokio::test]
    async fn get_aggregated_board_filters_source_boards_by_visibility() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;
        let card_svc = make_card_service(&infra);

        let owner = format!("user:test-{}", Uuid::new_v4());
        let stranger = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &owner).await;

        let public_board_id = create_source_board_with_visibility(
            &board_svc,
            project_id,
            "Public Source",
            &owner,
            BoardVisibility::Public as i32,
        )
        .await;
        let private_board_id = create_source_board_with_visibility(
            &board_svc,
            project_id,
            "Private Source",
            &owner,
            BoardVisibility::Private as i32,
        )
        .await;

        // Create one card in each source board.
        let public_column_id = board_svc
            .get_board(authed_request_with_object(
                GetBoardRequest {
                    board_id: public_board_id.to_string(),
                },
                &owner,
                &public_board_id.to_string(),
            ))
            .await
            .expect("get public board failed")
            .into_inner()
            .columns
            .first()
            .unwrap()
            .id
            .clone();
        let private_column_id = board_svc
            .get_board(authed_request_with_object(
                GetBoardRequest {
                    board_id: private_board_id.to_string(),
                },
                &owner,
                &private_board_id.to_string(),
            ))
            .await
            .expect("get private board failed")
            .into_inner()
            .columns
            .first()
            .unwrap()
            .id
            .clone();

        card_svc
            .create_card(authed_request_with_object(
                CreateCardRequest {
                    board_id: public_board_id.to_string(),
                    column_id: public_column_id,
                    title: "Public Card".to_string(),
                    idempotency_key: String::new(),
                    ..Default::default()
                },
                &owner,
                &public_board_id.to_string(),
            ))
            .await
            .expect("create public card failed");

        card_svc
            .create_card(authed_request_with_object(
                CreateCardRequest {
                    board_id: private_board_id.to_string(),
                    column_id: private_column_id,
                    title: "Private Card".to_string(),
                    idempotency_key: String::new(),
                    ..Default::default()
                },
                &owner,
                &private_board_id.to_string(),
            ))
            .await
            .expect("create private card failed");

        // Make the aggregate public so the stranger can access it.
        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Filtered Aggregate".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![
                        public_board_id.to_string(),
                        private_board_id.to_string(),
                    ],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Public as i32,
                },
                &owner,
            ))
            .await
            .expect("create aggregate failed")
            .into_inner();
        let agg_id = created.id;

        let stream = agg_svc
            .get_aggregated_board(authed_request_with_object(
                GetAggregatedBoardRequest {
                    aggregated_board_id: agg_id.clone(),
                },
                &stranger,
                &agg_id,
            ))
            .await
            .expect("stranger should view public aggregate")
            .into_inner();
        let chunks = collect_aggregate_stream(stream).await;

        let source_ids: Vec<String> = chunks
            .iter()
            .filter_map(|c| match &c.payload {
                Some(ChunkPayload::SourceBoard(sb)) => Some(sb.board_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(source_ids.len(), 1, "only public source board visible");
        assert_eq!(source_ids[0], public_board_id.to_string());

        let cards: Vec<&Card> = chunks
            .iter()
            .filter_map(|c| match &c.payload {
                Some(ChunkPayload::CardBatch(batch)) => {
                    Some(batch.cards.iter().collect::<Vec<_>>())
                }
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(cards.len(), 1, "only card from public source board visible");
        assert_eq!(cards[0].title, "Public Card");

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&agg_id).unwrap()).await;
        cleanup_project(&infra.pool, project_id).await;
    }

    // ── Delete + non-visibility update paths ───────────────────────────────────

    #[tokio::test]
    async fn delete_aggregated_board_removes_row() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "To Delete".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private as i32,
                },
                &owner,
            ))
            .await
            .unwrap()
            .into_inner();

        agg_svc
            .delete_aggregated_board(authed_request_with_object(
                DeleteAggregatedBoardRequest {
                    aggregated_board_id: created.id.clone(),
                },
                &owner,
                &created.id,
            ))
            .await
            .expect("delete failed");

        let count: i64 = sqlx::query("SELECT COUNT(*) FROM aggregated_boards WHERE id = $1")
            .bind(Uuid::parse_str(&created.id).unwrap())
            .fetch_one(&infra.pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "aggregated board must be deleted");
    }

    #[tokio::test]
    async fn delete_aggregated_board_returns_not_found_for_missing() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let missing_id = Uuid::new_v4().to_string();
        let result = agg_svc
            .delete_aggregated_board(authed_request_with_object(
                DeleteAggregatedBoardRequest {
                    aggregated_board_id: missing_id.clone(),
                },
                &owner,
                &missing_id,
            ))
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn update_aggregated_board_patches_name_and_description() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Original".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Private as i32,
                },
                &owner,
            ))
            .await
            .unwrap()
            .into_inner();

        // Grant edit so the update request passes middleware.
        infra
            .keto
            .grant_with_retry(KETO_NS, &created.id, "edit", &owner)
            .await
            .expect("grant edit failed");

        let updated = agg_svc
            .update_aggregated_board(authed_request_with_object(
                UpdateAggregatedBoardRequest {
                    aggregated_board_id: created.id.clone(),
                    aggregated_board: Some(AggregatedBoard {
                        id: String::new(),
                        name: "Renamed".to_string(),
                        description: "New desc".to_string(),
                        icon: String::new(),
                        visibility: BoardVisibility::Private as i32,
                        created_at: None,
                        updated_at: None,
                    }),
                    update_mask: Some(FieldMask {
                        paths: vec!["name".to_string(), "description".to_string()],
                    }),
                },
                &owner,
                &created.id,
            ))
            .await
            .expect("update failed")
            .into_inner();

        assert_eq!(updated.name, "Renamed");
        assert_eq!(updated.description, "New desc");

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&created.id).unwrap()).await;
    }

    #[tokio::test]
    async fn subscribe_aggregated_board_emits_cutover() {
        let infra = containers::setup().await;
        let agg_svc = make_service(&infra).await;
        let board_svc = make_board_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &owner).await;
        let board_id = create_source_board(&board_svc, project_id, "Source", &owner).await;

        let created = agg_svc
            .create_aggregated_board(authed_request(
                CreateAggregatedBoardRequest {
                    name: "Agg".to_string(),
                    description: String::new(),
                    icon: String::new(),
                    source_board_ids: vec![board_id.to_string()],
                    idempotency_key: String::new(),
                    visibility: BoardVisibility::Public as i32,
                },
                &owner,
            ))
            .await
            .expect("create aggregated board")
            .into_inner();

        // Grant view so the subscribe request passes the middleware check.
        infra
            .keto
            .grant_with_retry(KETO_NS, &created.id, "view", &owner)
            .await
            .expect("grant view failed");

        let mut req = Request::new(SubscribeAggregatedBoardRequest {
            aggregated_board_id: created.id.clone(),
            since_seq: 0,
        });
        req.extensions_mut()
            .insert(sunbeam_g2v::middleware::auth::AuthContext::authenticated(
                &owner, None,
            ));

        let mut stream = agg_svc
            .subscribe_aggregated_board(req)
            .await
            .expect("subscribe should succeed")
            .into_inner();

        let first = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("stream timed out before cutover")
            .expect("stream ended before cutover")
            .expect("cutover envelope errored");

        assert!(
            matches!(first.payload, Some(EventPayload::Cutover(_))),
            "first payload should be a cutover"
        );

        cleanup_aggregated_board(&infra.pool, Uuid::parse_str(&created.id).unwrap()).await;
        cleanup_project(&infra.pool, project_id).await;
    }

    #[tokio::test]
    async fn build_subscribe_stream_emits_heartbeat_with_short_interval() {
        let infra = containers::setup().await;
        let board_svc = make_board_service(&infra).await;

        let owner = format!("user:test-{}", Uuid::new_v4());
        let project_id = create_test_project(&infra.pool, &owner).await;
        let board_id = create_source_board(&board_svc, project_id, "Source", &owner).await;

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &infra.nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("ensure kanban stream");

        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&infra.nats),
            "pod-test-stream",
        ));

        let stream = build_subscribe_aggregated_board_stream(SubscribeAggregatedBoardArgs {
            registry,
            keto: Arc::clone(&infra.keto),
            watermark: Arc::clone(&infra.watermark),
            auth: sunbeam_g2v::middleware::auth::AuthContext::authenticated(&owner, None),
            aggregated_board_id: Uuid::new_v4().to_string(),
            source_board_ids: vec![board_id],
            is_private: false,
            heartbeat_interval: Duration::from_millis(10),
            keto_recheck_interval: Duration::from_millis(100),
        })
        .await
        .expect("build stream should succeed");

        let mut stream = stream;
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for first item")
            .expect("stream ended before first item")
            .expect("first item errored");
        assert!(
            matches!(first.payload, Some(EventPayload::Cutover(_))),
            "first item should be cutover"
        );

        let second = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for heartbeat")
            .expect("stream ended before heartbeat")
            .expect("heartbeat item errored");
        assert!(
            matches!(second.payload, Some(EventPayload::Heartbeat(_))),
            "second item should be a heartbeat"
        );

        cleanup_project(&infra.pool, project_id).await;
    }
}
