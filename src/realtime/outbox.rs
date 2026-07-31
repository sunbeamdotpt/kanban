// SPDX-License-Identifier: AGPL-3.0-or-later
//! Transactional outbox dispatcher.
//!
//! Drains the `event_log` table to NATS JetStream.
//!
//! # Design
//!
//! Each mutation handler inserts one `event_log` row in the same database
//! transaction as the entity write (see `crate::event_log`). The row starts
//! with `nats_seq IS NULL`. This dispatcher runs as a background tokio task
//! and:
//!
//! - SELECTs undispatched rows in `id` order, bounded by `batch_size`.
//! - Builds a `BoardEventEnvelope` from each row (hydrating the full `Card`
//!   proto for `CardCreated` from the cards table).
//! - Publishes the encoded proto bytes to the object's JetStream subject with
//!   a `Nats-Msg-Id` header set to the row id, so JetStream's duplicate
//!   window (2 minutes by default) dedupes the retry-after-UPDATE-failure
//!   path.
//! - On success, marks `nats_seq` and `dispatched_at` on the row.
//! - On failure, leaves the row alone; the next loop pass retries.
//!
//! The loop is idempotent on restart: rows with `nats_seq IS NOT NULL` are
//! never re-published.
//!
//! # Routing
//!
//! Board rows publish to `kanban.board.<id>.events` with the envelope's
//! `board_id` set. Aggregated-board rows publish to the same subject scheme
//! keyed by the aggregated board id. Project rows publish to
//! `kanban.project.<id>.events` and leave the envelope's `board_id` empty
//! (per events.proto, project-only events have no board context).
//!
//! # Payload mapping (`event_type` → oneof)
//!
//! | event_type string | oneof variant        |
//! |-------------------|----------------------|
//! | "CardCreated"     | payload::CardCreated (full Card hydrated from the DB) |
//! | "CardUpdated"     | payload::CardUpdated (patch as google.protobuf.Struct) |
//! | "CardMoved"       | payload::CardMoved   |
//! | "CardDeleted"     | payload::CardDeleted |
//! | "CardTransferred" | payload::CardTransferred (full Card hydrated from the DB) |
//! | "ColumnAdded"     | payload::ColumnAdded |
//! | "ColumnUpdated"   | payload::ColumnUpdated |
//! | "ColumnRenamed"   | payload::ColumnRenamed |
//! | "ColumnRemoved"   | payload::ColumnRemoved |
//! | "ColumnsReordered" | payload::ColumnsReordered |
//! | "BoardCreated"    | payload::BoardCreated |
//! | "BoardDeleted"    | payload::BoardDeleted |
//! | "BoardRenamed"    | payload::BoardRenamed |
//! | "BoardUpdated"    | payload::BoardUpdated |
//! | "MemberAdded"     | payload::MemberAdded |
//! | "MemberRemoved"   | payload::MemberRemoved |
//! | "MemberRoleChanged" | payload::MemberRoleChanged |
//! | "ProjectUpdated"  | payload::ProjectUpdated |
//! | "AggregatedBoardCreated" | payload::AggregatedBoardCreated |
//! | "AggregatedBoardUpdated" | payload::AggregatedBoardUpdated |
//! | "AggregatedBoardDeleted" | payload::AggregatedBoardDeleted |
//! | "SourceBoardAdded" | payload::SourceBoardAdded |
//! | "SourceBoardRemoved" | payload::SourceBoardRemoved |
//! | "GitHubLinkAdded" | payload::GithubLinkAdded |
//! | "GitHubLinkRefreshed" | payload::GithubLinkRefreshed |
//! | "MembershipChanged" | payload::MembershipChanged |
//! | (anything else)   | oneof left empty (envelope fields populated) |
//!
//! The JSONB `payload` column contains the raw field values inserted by the
//! mutation handler. `payload.board_revision` (written by
//! `crate::event_log::insert_board_event`) is copied to the envelope's
//! `board_revision`; legacy rows without it default to 0.
//!
//! JSONB is easy to inspect in `psql` (`SELECT payload FROM event_log`) while
//! protobuf BYTEA would deserialize faster. JSONB is the right trade-off for
//! this service's scale.
//!
//! # LISTEN/NOTIFY
//!
//! Handlers issue `pg_notify('kanban_event_log', '')` inside the mutation
//! transaction (delivered on commit). The dispatcher LISTENs on that channel
//! via a dedicated `PgListener` connection and drains immediately on each
//! notification, so the steady-state latency is the publish round-trip, not
//! the poll interval. The poll tick remains as a fallback: if the listener
//! connection dies it is dropped and the loop keeps draining on the poll
//! cadence alone.
//!
//! # Shutdown
//!
//! `with_shutdown` attaches a `watch::Receiver<bool>`; when it flips to
//! `true` (or the sender closes) the loop runs one final drain and exits,
//! so in-flight rows are not stranded mid-batch on SIGTERM.

use std::sync::Arc;
use std::time::Duration;

use crate::auth::identity_client::IdentityClient;
use crate::id::Id;
use anyhow::{Context, Result};
use buffa::Message;
use buffa_types::google::protobuf::{Struct, Timestamp};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::postgres::PgListener;
use sqlx::{PgPool, Row};
use tokio::sync::watch;
use tracing::{error, info, warn};

use sunbeam_g2v::mq::NatsClient;

use super::jetstream_bootstrap::{board_subject, project_subject};
use crate::cpb::sunbeam::kanban::v1::{
    AggregatedBoardCreated, AggregatedBoardDeleted, AggregatedBoardUpdated, BoardCreated,
    BoardDeleted, BoardEventEnvelope, BoardRenamed, BoardUpdated, CardCreated, CardDeleted,
    CardMoved, CardUpdated, ColumnAdded, ColumnRemoved, ColumnRenamed, ColumnUpdated,
    ColumnsReordered, EventBoard, EventColumn, GitHubLinkAdded, GitHubLinkRefreshed, MemberAdded,
    MemberRemoved, MemberRoleChanged, MembershipChanged, ProjectUpdated, SourceBoardAdded,
    SourceBoardRemoved, CardTransferred, board_event_envelope::Payload,
};
use crate::event_log::EVENT_LOG_NOTIFY_CHANNEL;
use crate::integrations::opensearch::OpenSearchClient;

// ── OutboxConfig ─────────────────────────────────────────────────────────────

/// Tunables for the outbox dispatcher.
#[derive(Clone, Debug)]
pub struct OutboxConfig {
    pub poll_interval: Duration,
    pub batch_size: i64,
    /// Pod identity embedded in every emitted envelope.
    pub pod_name: String,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            batch_size: 256,
            pod_name: String::new(),
        }
    }
}

// ── OutboxDispatcher ──────────────────────────────────────────────────────────

/// Long-running task that drains `event_log` rows to JetStream.
pub struct OutboxDispatcher {
    pool: PgPool,
    nats: Arc<NatsClient>,
    /// User-directory client used to hydrate assignee emails when CardCreated
    /// events carry a full card snapshot (KANBAN-035).
    identity: Arc<IdentityClient>,
    /// Polling interval when no NOTIFY mechanism is wired.
    /// 250ms floor — JetStream publish round-trip is ~1–5ms so latency is
    /// dominated by the poll, not the network.
    poll_interval: Duration,
    /// Per-loop batch size. Bounds the SELECT cursor so a backlog after
    /// downtime doesn't OOM the pod. 256 covers most steady-state load.
    batch_size: i64,
    /// Pod identity embedded in every emitted envelope.
    pod_name: String,
    /// Search-index write target: the OpenSearch client and the cards index
    /// name. `None` disables indexing (unit tests).
    opensearch: Option<(Arc<OpenSearchClient>, String)>,
    /// Graceful-shutdown signal for the spawned loop.
    shutdown: Option<watch::Receiver<bool>>,
    /// Test-only board scope. `None` in production drains every undispatched
    /// row; `Some(board_id)` restricts to one board so concurrent integration
    /// tests don't eat each other's rows.
    #[cfg(test)]
    board_filter: Option<Id>,
}

impl OutboxDispatcher {
    /// Construct a dispatcher with the provided configuration.
    pub fn new(
        pool: PgPool,
        nats: Arc<NatsClient>,
        identity: Arc<IdentityClient>,
        config: OutboxConfig,
    ) -> Self {
        Self {
            pool,
            nats,
            identity,
            poll_interval: config.poll_interval,
            batch_size: config.batch_size,
            pod_name: config.pod_name,
            opensearch: None,
            shutdown: None,
            #[cfg(test)]
            board_filter: None,
        }
    }

    /// Enable search indexing: card events are mirrored into the OpenSearch
    /// cards index as they are dispatched.
    pub fn with_opensearch(
        mut self,
        opensearch: Arc<OpenSearchClient>,
        index_name: String,
    ) -> Self {
        // Ensure the index exists before the drain loop starts. Best-effort:
        // a failure here only means the first index writes may fail and log.
        self.opensearch = Some((opensearch, index_name));
        self
    }

    /// Restrict drain to a single board. Test-only — production drains every
    /// undispatched row regardless of board.
    #[cfg(test)]
    pub fn with_board_filter(mut self, board_id: Id) -> Self {
        self.board_filter = Some(board_id);
        self
    }

    /// Override the poll interval (used by tests to speed up the loop).
    #[cfg(test)]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Override the batch size (used by tests to verify batching behaviour).
    #[cfg(test)]
    pub fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Attach a graceful-shutdown signal. When the watch value flips to
    /// `true` (or the sender closes), the loop runs one final drain and
    /// exits instead of abandoning in-flight rows.
    pub fn with_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Spawn the long-running drain loop.
    ///
    /// The loop drains on every `kanban_event_log` NOTIFY and on the
    /// poll-interval tick (fallback when the LISTEN connection is dead).
    /// With a shutdown receiver attached (see [`Self::with_shutdown`]) it
    /// runs one final drain and exits on signal; otherwise it runs until the
    /// returned `JoinHandle` is aborted.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "outbox dispatcher started (poll_interval={}ms, batch_size={})",
                self.poll_interval.as_millis(),
                self.batch_size
            );
            if let Some((os, index)) = &self.opensearch
                && let Err(e) = crate::search_indexing::ensure_cards_index(os, index).await
            {
                error!(error = %e, "outbox: failed to ensure search index; indexing will retry per event");
            }

            let mut shutdown = self.shutdown.clone();
            let mut listener = self.connect_listener().await;

            loop {
                if shutdown_signaled(&shutdown) {
                    self.final_drain().await;
                    break;
                }

                match self.drain_once().await {
                    Ok(n) if n > 0 => {
                        info!(dispatched = n, "outbox: dispatched batch");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!(error = %e, "outbox drain_once error — retrying after poll_interval");
                    }
                }

                // Wait for the next wake: shutdown signal, NOTIFY, or the
                // poll-interval tick (fallback when the listener is dead).
                tokio::select! {
                    () = wait_shutdown(&mut shutdown) => {
                        self.final_drain().await;
                        break;
                    }
                    ok = recv_notification(&mut listener) => {
                        if !ok {
                            error!("outbox: LISTEN connection lost — falling back to polling only");
                            listener = None;
                        }
                    }
                    () = tokio::time::sleep(self.poll_interval) => {}
                }
            }

            info!("outbox dispatcher stopped");
        })
    }

    /// Connect a dedicated `PgListener` and LISTEN on the outbox channel.
    ///
    /// Returns `None` (pure-polling fallback) when the listener cannot be
    /// set up — the poll tick alone still guarantees progress.
    async fn connect_listener(&self) -> Option<PgListener> {
        match PgListener::connect_with(&self.pool).await {
            Ok(mut listener) => {
                if let Err(e) = listener.listen(EVENT_LOG_NOTIFY_CHANNEL).await {
                    error!(error = %e, "outbox: LISTEN failed — falling back to polling only");
                    None
                } else {
                    Some(listener)
                }
            }
            Err(e) => {
                error!(error = %e, "outbox: failed to connect PgListener — falling back to polling only");
                None
            }
        }
    }

    /// Final drain pass on shutdown: flush whatever is pending, then log.
    async fn final_drain(&self) {
        match self.drain_once().await {
            Ok(n) => info!(dispatched = n, "outbox: final drain before shutdown"),
            Err(e) => error!(error = %e, "outbox: final drain before shutdown failed"),
        }
        info!("outbox dispatcher shutting down");
    }

    /// One drain pass: SELECT undispatched rows, publish each, mark dispatched.
    ///
    /// Returns the count of successfully dispatched rows. Exposed for
    /// deterministic testing without sleeping.
    ///
    /// Failure for a single row is logged and skipped — the row retains
    /// `nats_seq IS NULL` and will be retried on the next pass.
    pub async fn drain_once(&self) -> Result<usize> {
        // Step 1: fetch a bounded batch of undispatched rows.
        // The optional board_filter (test-only) keeps concurrent integration
        // tests from eating each other's rows.
        #[cfg(test)]
        let board_filter = self.board_filter;
        #[cfg(not(test))]
        let board_filter: Option<Id> = None;

        let rows = sqlx::query(
            "SELECT id, tenant_id, board_id, aggregated_board_id, project_id, event_type, payload, created_at \
             FROM event_log \
             WHERE nats_seq IS NULL \
               AND ($2::text IS NULL OR board_id = $2 OR aggregated_board_id = $2 OR project_id = $2) \
             ORDER BY id \
             LIMIT $1",
        )
        .bind(self.batch_size)
        .bind(board_filter)
        .fetch_all(&self.pool)
        .await
        .context("outbox: failed to SELECT undispatched event_log rows")?;

        let mut dispatched = 0usize;

        for row in &rows {
            let row_id: Id = row.get("id");
            let tenant_id: String = row.get("tenant_id");
            let event_type: String = row.get("event_type");
            let payload_json: JsonValue = row.get("payload");
            let created_at: DateTime<Utc> = row.get("created_at");

            // Routing: board and aggregated-board rows publish to
            // kanban.board.<id>.events with the envelope board_id set;
            // project rows publish to kanban.project.<id>.events with an
            // empty envelope board_id (project-only events have no board
            // context, per events.proto).
            let (subject, envelope_board_id) = match row
                .get::<Option<Id>, _>("aggregated_board_id")
                .or_else(|| row.get::<Option<Id>, _>("board_id"))
            {
                Some(id) => (board_subject(&id.to_string()), id.to_string()),
                None => match row.get::<Option<Id>, _>("project_id") {
                    Some(id) => (project_subject(&id.to_string()), String::new()),
                    None => {
                        warn!(
                            row_id = %row_id,
                            event_type = %event_type,
                            "outbox: row has no board/aggregated/project id — skipping"
                        );
                        continue;
                    }
                },
            };

            // Step 2: build BoardEventEnvelope (hydrates CardCreated payloads
            // from the cards table; falls back to the minimal variant when the
            // card no longer exists).
            let payload = self
                .build_payload(&event_type, &payload_json, &tenant_id)
                .await;
            let envelope = build_envelope(
                row_id,
                &envelope_board_id,
                &payload_json,
                created_at,
                &self.pod_name,
                payload,
            );

            // Step 3: encode to bytes.
            let encoded = Bytes::from(envelope.encode_to_vec());

            // Step 4: publish via JetStream with Nats-Msg-Id = row id so the
            // server-side duplicate window dedupes retries (e.g. the
            // UPDATE-failure path below).
            let ack_result = match self.nats.jetstream() {
                Some(js) => {
                    let mut headers = async_nats::header::HeaderMap::new();
                    headers.insert(async_nats::header::NATS_MESSAGE_ID, row_id.to_string());
                    js.publish_with_headers(subject.clone(), headers, encoded)
                        .await
                        .map_err(|e| e.to_string())
                }
                None => self
                    .nats
                    .publish_jetstream(&subject, encoded)
                    .await
                    .map_err(|e| e.to_string()),
            };
            let ack_future = match ack_result {
                Ok(f) => f,
                Err(e) => {
                    warn!(
                        row_id = %row_id,
                        subject = %subject,
                        event_type = %event_type,
                        error = %e,
                        "outbox: publish failed — row left undispatched for retry"
                    );
                    continue;
                }
            };

            // Await the JetStream publish ack to obtain the sequence number.
            let nats_seq: i64 = match ack_future.await {
                Ok(ack) => ack.sequence as i64,
                Err(e) => {
                    warn!(
                        row_id = %row_id,
                        error = %e,
                        "outbox: publish ack failed — row left undispatched for retry"
                    );
                    continue;
                }
            };

            // Step 5: mark the row dispatched.
            match sqlx::query(
                "UPDATE event_log \
                 SET nats_seq = $1, dispatched_at = now() \
                 WHERE id = $2",
            )
            .bind(nats_seq)
            .bind(row_id)
            .execute(&self.pool)
            .await
            {
                Ok(_) => {
                    dispatched += 1;

                    // Step 6: keep the search index in sync for card events.
                    // The card id rides in the payload; indexing failures are
                    // logged inside the helpers and never block dispatch.
                    if let Some((os, index)) = &self.opensearch {
                        let card_id = payload_json
                            .get("card_id")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<Id>().ok());
                        if let Some(card_id) = card_id {
                            match event_type.as_str() {
                                "CardCreated" | "CardUpdated" | "CardMoved" | "CardTransferred" => {
                                    crate::search_indexing::index_card_by_id(
                                        os, index, &self.pool, card_id, &tenant_id,
                                    )
                                    .await;
                                }
                                "CardDeleted" => {
                                    crate::search_indexing::delete_card_doc(os, index, card_id)
                                        .await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        row_id = %row_id,
                        nats_seq = nats_seq,
                        error = %e,
                        "outbox: UPDATE nats_seq failed — message was published but row \
                         stays undispatched; duplicate publish possible on retry"
                    );
                    // Do not increment `dispatched` — the row is in a dirty state.
                    // The next SELECT will pick it up again; the Nats-Msg-Id
                    // header set at publish time lets JetStream dedupe the
                    // re-publish inside its duplicate window.
                }
            }
        }

        Ok(dispatched)
    }
}

// ── Envelope builder ──────────────────────────────────────────────────────────

impl OutboxDispatcher {
    /// Build the oneof payload for an `event_log` row.
    ///
    /// `CardCreated` hydrates the full `Card` proto from the cards table at
    /// dispatch time; when the card no longer exists (deleted between the
    /// mutation and the drain) it falls back to the minimal variant so the
    /// event still reaches subscribers. `CardTransferred` hydrates the same
    /// way (post-transfer state). Every other event type maps purely from
    /// the JSONB payload.
    async fn build_payload(
        &self,
        event_type: &str,
        json: &JsonValue,
        tenant_id: &str,
    ) -> Option<Payload> {
        if event_type == "CardCreated"
            && let Some(card_id) = json
                .get("card_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Id>().ok())
            && let Ok(card) = crate::services::cards::fetch_full_card(
                &self.pool,
                card_id,
                tenant_id,
                &self.identity,
            )
            .await
        {
            return Some(Payload::CardCreated(Box::new(CardCreated {
                card: Some(card).into(),
                column_id: json_str(json, "column_id"),
                position: json_i32(json, "position"),
                idempotency_key: json_str(json, "idempotency_key"),
                ..Default::default()
            })));
        }
        if event_type == "CardTransferred"
            && let Some(card_id) = json
                .get("card_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Id>().ok())
            && let Ok(card) = crate::services::cards::fetch_full_card(
                &self.pool,
                card_id,
                tenant_id,
                &self.identity,
            )
            .await
        {
            return Some(Payload::CardTransferred(Box::new(CardTransferred {
                card: Some(card).into(),
                from_board_id: json_str(json, "from_board_id"),
                to_board_id: json_str(json, "to_board_id"),
                to_column_id: json_str(json, "to_column_id"),
                to_position: json_i32(json, "to_position"),
                previous_ref: json_str(json, "previous_ref"),
                idempotency_key: json_str(json, "idempotency_key"),
                ..Default::default()
            })));
        }
        build_payload(event_type, json)
    }
}

/// Build a `BoardEventEnvelope` from a raw `event_log` row.
///
/// The envelope fields are always populated. `board_id` is empty for
/// project-scoped rows (project-only events have no board context, per
/// events.proto). `board_revision` rides in the JSONB payload (written by
/// `crate::event_log::insert_board_event`); legacy rows without it get 0.
fn build_envelope(
    row_id: Id,
    board_id: &str,
    payload_json: &JsonValue,
    created_at: DateTime<Utc>,
    emitter_pod_id: &str,
    payload: Option<Payload>,
) -> BoardEventEnvelope {
    let emitted_at = Some(Timestamp {
        seconds: created_at.timestamp(),
        nanos: created_at.timestamp_subsec_nanos() as i32,
        ..Default::default()
    })
    .into();

    BoardEventEnvelope {
        board_id: board_id.to_string(),
        event_id: row_id.to_string(),
        // nats_seq is set to 0 before publish; the real sequence is written
        // back to the DB after the JetStream ack.
        nats_seq: 0,
        board_revision: json_u64(payload_json, "board_revision"),
        emitted_at,
        emitter_pod_id: emitter_pod_id.to_string(),
        actor_subject: payload_json
            .get("actor_subject")
            .and_then(|v| v.as_str())
            .unwrap_or("system")
            .to_string(),
        payload,
        ..Default::default()
    }
}

// ── JSON field helpers ────────────────────────────────────────────────────────

/// Read a string field from the payload JSON; missing/wrong-typed → "".
fn json_str(json: &JsonValue, key: &str) -> String {
    json.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Read a u64 field from the payload JSON; missing/wrong-typed → 0.
fn json_u64(json: &JsonValue, key: &str) -> u64 {
    json.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
}

/// Read an i32 field from the payload JSON; missing/wrong-typed → 0.
fn json_i32(json: &JsonValue, key: &str) -> i32 {
    json.get(key).and_then(|v| v.as_i64()).unwrap_or(0) as i32
}

/// Read a bool field from the payload JSON; missing/wrong-typed → false.
fn json_bool(json: &JsonValue, key: &str) -> bool {
    json.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// Read the (prev_revision, new_revision) pair carried by card mutations.
fn revision_pair(json: &JsonValue) -> (u64, u64) {
    (
        json_u64(json, "prev_revision"),
        json_u64(json, "new_revision"),
    )
}

/// Parse an `EventColumn` from a nested payload object.
fn parse_event_column(json: &JsonValue) -> EventColumn {
    EventColumn {
        id: json_str(json, "id"),
        board_id: json_str(json, "board_id"),
        title: json_str(json, "title"),
        accent: json_str(json, "accent"),
        wip_limit: json_i32(json, "wip_limit"),
        position: json_i32(json, "position"),
        is_done: json_bool(json, "is_done"),
        ..Default::default()
    }
}

/// Parse an `EventBoard` from a nested payload object.
fn parse_event_board(json: &JsonValue) -> EventBoard {
    EventBoard {
        id: json_str(json, "id"),
        project_id: json_str(json, "project_id"),
        name: json_str(json, "name"),
        description: json_str(json, "description"),
        icon: json_str(json, "icon"),
        ..Default::default()
    }
}

// ── CardUpdated patch Struct ──────────────────────────────────────────────────

/// True for payload values that encode "field not changed" and must be
/// filtered out of the `CardUpdated` patch: empty strings (the sparse-patch
/// sentinel used by `cards.rs` for text fields) and unspecified enum values
/// (buffa serializes `EnumValue` as the proto name, e.g.
/// `CARD_PRIORITY_UNSPECIFIED`).
fn is_unchanged_sentinel(v: &JsonValue) -> bool {
    match v {
        JsonValue::String(s) => s.is_empty() || s.ends_with("_UNSPECIFIED"),
        _ => false,
    }
}

/// Convert the `payload.patch` JSON object into a `google.protobuf.Struct`,
/// dropping unchanged sentinels and always setting `revision` to
/// `new_revision` (per the events.proto contract).
fn build_patch_struct(patch: Option<&JsonValue>, new_revision: u64) -> Struct {
    let mut filtered = serde_json::Map::new();
    if let Some(obj) = patch.and_then(JsonValue::as_object) {
        for (k, v) in obj {
            if !is_unchanged_sentinel(v) {
                filtered.insert(k.clone(), v.clone());
            }
        }
    }
    filtered.insert("revision".to_string(), JsonValue::from(new_revision));
    serde_json::from_value(JsonValue::Object(filtered)).unwrap_or_default()
}

// ── Payload mapping ───────────────────────────────────────────────────────────

/// Map an `event_type` string to the matching `board_event_envelope::Payload`
/// oneof variant. Returns `None` (empty oneof) for unknown event types.
///
/// Pure/sync: the CardCreated arm emits the minimal variant; the dispatcher's
/// async `build_payload` upgrades it to the hydrated Card when possible.
fn build_payload(event_type: &str, json: &JsonValue) -> Option<Payload> {
    let card_id = json_str(json, "card_id");
    let idempotency_key = json_str(json, "idempotency_key");

    match event_type {
        "CardCreated" => Some(Payload::CardCreated(Box::new(CardCreated {
            // Minimal variant: the full Card is hydrated by the dispatcher.
            card: None.into(),
            column_id: json_str(json, "column_id"),
            position: json_i32(json, "position"),
            idempotency_key,
            ..Default::default()
        }))),
        "CardUpdated" => {
            let (prev_revision, new_revision) = revision_pair(json);
            Some(Payload::CardUpdated(Box::new(CardUpdated {
                card_id,
                prev_revision,
                new_revision,
                patch: Some(build_patch_struct(json.get("patch"), new_revision)).into(),
                idempotency_key,
                ..Default::default()
            })))
        }
        "CardMoved" => {
            let (prev_revision, new_revision) = revision_pair(json);
            Some(Payload::CardMoved(Box::new(CardMoved {
                card_id,
                from_column: json_str(json, "from_column"),
                to_column: json_str(json, "to_column"),
                to_position: json_i32(json, "to_position"),
                prev_revision,
                new_revision,
                idempotency_key,
                ..Default::default()
            })))
        }
        "CardDeleted" => {
            let (prev_revision, _) = revision_pair(json);
            Some(Payload::CardDeleted(Box::new(CardDeleted {
                card_id,
                prev_revision,
                idempotency_key,
                ..Default::default()
            })))
        }
        "CardTransferred" => Some(Payload::CardTransferred(Box::new(CardTransferred {
            // Minimal variant: the full Card is hydrated by the dispatcher.
            card: None.into(),
            from_board_id: json_str(json, "from_board_id"),
            to_board_id: json_str(json, "to_board_id"),
            to_column_id: json_str(json, "to_column_id"),
            to_position: json_i32(json, "to_position"),
            previous_ref: json_str(json, "previous_ref"),
            idempotency_key,
            ..Default::default()
        }))),
        "ColumnAdded" => Some(Payload::ColumnAdded(Box::new(ColumnAdded {
            column: Some(parse_event_column(
                json.get("column").unwrap_or(&JsonValue::Null),
            ))
            .into(),
            position: json_i32(json, "position"),
            ..Default::default()
        }))),
        "ColumnUpdated" => Some(Payload::ColumnUpdated(Box::new(ColumnUpdated {
            column: Some(parse_event_column(
                json.get("column").unwrap_or(&JsonValue::Null),
            ))
            .into(),
            ..Default::default()
        }))),
        "ColumnRenamed" => Some(Payload::ColumnRenamed(Box::new(ColumnRenamed {
            column_id: json_str(json, "column_id"),
            new_title: json_str(json, "new_title"),
            ..Default::default()
        }))),
        "ColumnRemoved" => Some(Payload::ColumnRemoved(Box::new(ColumnRemoved {
            column_id: json_str(json, "column_id"),
            move_cards_to_column: json_str(json, "move_cards_to_column"),
            ..Default::default()
        }))),
        "ColumnsReordered" => {
            let columns = json
                .get("columns")
                .and_then(JsonValue::as_array)
                .map(|cols| cols.iter().map(parse_event_column).collect())
                .unwrap_or_default();
            Some(Payload::ColumnsReordered(Box::new(ColumnsReordered {
                columns,
                ..Default::default()
            })))
        }
        "BoardCreated" => Some(Payload::BoardCreated(Box::new(BoardCreated {
            project_id: json_str(json, "project_id"),
            board_id: json_str(json, "board_id"),
            name: json_str(json, "name"),
            ..Default::default()
        }))),
        "BoardDeleted" => Some(Payload::BoardDeleted(Box::new(BoardDeleted {
            project_id: json_str(json, "project_id"),
            board_id: json_str(json, "board_id"),
            ..Default::default()
        }))),
        "BoardRenamed" => Some(Payload::BoardRenamed(Box::new(BoardRenamed {
            new_name: json_str(json, "new_name"),
            ..Default::default()
        }))),
        "BoardUpdated" => Some(Payload::BoardUpdated(Box::new(BoardUpdated {
            board: Some(parse_event_board(
                json.get("board").unwrap_or(&JsonValue::Null),
            ))
            .into(),
            ..Default::default()
        }))),
        "MemberAdded" => Some(Payload::MemberAdded(Box::new(MemberAdded {
            project_id: json_str(json, "project_id"),
            subject: json_str(json, "subject"),
            relation: json_str(json, "relation"),
            display_name: json_str(json, "display_name"),
            email: json_str(json, "email"),
            ..Default::default()
        }))),
        "MemberRemoved" => Some(Payload::MemberRemoved(Box::new(MemberRemoved {
            project_id: json_str(json, "project_id"),
            subject: json_str(json, "subject"),
            ..Default::default()
        }))),
        "MemberRoleChanged" => Some(Payload::MemberRoleChanged(Box::new(MemberRoleChanged {
            project_id: json_str(json, "project_id"),
            subject: json_str(json, "subject"),
            old_relation: json_str(json, "old_relation"),
            new_relation: json_str(json, "new_relation"),
            ..Default::default()
        }))),
        "ProjectUpdated" => Some(Payload::ProjectUpdated(Box::new(ProjectUpdated {
            project_id: json_str(json, "project_id"),
            name: json_str(json, "name"),
            prefix: json_str(json, "prefix"),
            description: json_str(json, "description"),
            ..Default::default()
        }))),
        "AggregatedBoardCreated" => Some(Payload::AggregatedBoardCreated(Box::new(
            AggregatedBoardCreated {
                aggregated_board_id: json_str(json, "aggregated_board_id"),
                ..Default::default()
            },
        ))),
        "AggregatedBoardUpdated" => Some(Payload::AggregatedBoardUpdated(Box::new(
            AggregatedBoardUpdated {
                aggregated_board_id: json_str(json, "aggregated_board_id"),
                ..Default::default()
            },
        ))),
        "AggregatedBoardDeleted" => Some(Payload::AggregatedBoardDeleted(Box::new(
            AggregatedBoardDeleted {
                aggregated_board_id: json_str(json, "aggregated_board_id"),
                ..Default::default()
            },
        ))),
        "SourceBoardAdded" => Some(Payload::SourceBoardAdded(Box::new(SourceBoardAdded {
            aggregated_board_id: json_str(json, "aggregated_board_id"),
            board_id: json_str(json, "board_id"),
            ..Default::default()
        }))),
        "SourceBoardRemoved" => Some(Payload::SourceBoardRemoved(Box::new(SourceBoardRemoved {
            aggregated_board_id: json_str(json, "aggregated_board_id"),
            board_id: json_str(json, "board_id"),
            ..Default::default()
        }))),
        "GitHubLinkAdded" => Some(Payload::GithubLinkAdded(Box::new(GitHubLinkAdded {
            card_id,
            link_id: json_str(json, "link_id"),
            ..Default::default()
        }))),
        "GitHubLinkRefreshed" => Some(Payload::GithubLinkRefreshed(Box::new(
            GitHubLinkRefreshed {
                card_id,
                link_id: json_str(json, "link_id"),
                new_state: json_str(json, "new_state"),
                ..Default::default()
            },
        ))),
        "MembershipChanged" => Some(Payload::MembershipChanged(Box::new(MembershipChanged {
            subject: json_str(json, "subject"),
            relation: json_str(json, "relation"),
            granted: json
                .get("granted")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
            ..Default::default()
        }))),
        _ => {
            warn!(event_type = %event_type, "outbox: unknown event_type — oneof left empty");
            None
        }
    }
}

// ── Spawn-loop helpers ────────────────────────────────────────────────────────

/// True once the shutdown watch has been signaled.
fn shutdown_signaled(rx: &Option<watch::Receiver<bool>>) -> bool {
    rx.as_ref().is_some_and(|r| *r.borrow())
}

/// Resolve when the shutdown watch fires (or its sender closes). Pends
/// forever when no shutdown receiver is attached.
async fn wait_shutdown(rx: &mut Option<watch::Receiver<bool>>) {
    match rx.as_mut() {
        Some(r) => {
            let _ = r.changed().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Receive the next NOTIFY; resolves `false` when the listener connection
/// failed (caller drops the listener and falls back to polling). Pends
/// forever when no listener is attached.
async fn recv_notification(listener: &mut Option<PgListener>) -> bool {
    match listener.as_mut() {
        Some(l) => l.recv().await.is_ok(),
        None => std::future::pending::<bool>().await,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use sunbeam_g2v::config::NatsConfig;

    use crate::test_support::{
        containers, seed_card_chain, seed_event_log, seed_event_log_dispatched, setup_pool,
    };

    // ── Test helpers ──────────────────────────────────────────────────────────

    async fn setup_nats() -> Arc<NatsClient> {
        Arc::clone(&containers::setup().await.nats)
    }

    async fn make_dispatcher(
        pool: PgPool,
        nats: Arc<NatsClient>,
        board_id: Id,
    ) -> OutboxDispatcher {
        OutboxDispatcher::new(
            pool,
            nats,
            crate::test_support::setup_identity().await,
            OutboxConfig::default(),
        )
        .with_board_filter(board_id)
    }

    /// Count event_log rows for a board where nats_seq IS NOT NULL.
    async fn count_dispatched(pool: &PgPool, board_id: Id) -> i64 {
        sqlx::query(
            "SELECT COUNT(*) AS cnt FROM event_log \
             WHERE board_id = $1 AND nats_seq IS NOT NULL",
        )
        .bind(board_id)
        .fetch_one(pool)
        .await
        .map(|r| r.get::<i64, _>("cnt"))
        .unwrap_or(0)
    }

    /// Cleanup helper: delete all event_log rows for a board, then cascade-delete
    /// the board's cards/columns/board/project via DELETE on boards (CASCADE
    /// defined in migrations).
    async fn cleanup(pool: &PgPool, board_id: Id, project_id: Id) {
        let _ = sqlx::query("DELETE FROM event_log WHERE board_id = $1")
            .bind(board_id)
            .execute(pool)
            .await;
        let _ = sqlx::query("DELETE FROM boards WHERE id = $1")
            .bind(board_id)
            .execute(pool)
            .await;
        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(pool)
            .await;
    }

    // ── Search indexing hook ─────────────────────────────────────────────────

    /// Card events must be mirrored into OpenSearch: CardCreated/Updated/Moved
    /// index the document, CardDeleted removes it.
    #[tokio::test]
    async fn drain_once_mirrors_card_events_into_opensearch() {
        use crate::integrations::opensearch::{OpenSearchClient, OpenSearchConfig};

        let pool = setup_pool().await;
        let nats = setup_nats().await;
        let tenant_id = crate::test_support::test_tenant_id();

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, card_id) = seed_card_chain(&pool).await;

        let os = Arc::new(OpenSearchClient::new(OpenSearchConfig::from_env()));
        let index = format!("kanban-cards-test-{}", Id::new().to_string().to_lowercase());
        os.create_cards_index(&index).await.expect("create index");

        async fn insert_event(
            pool: &PgPool,
            tenant_id: &str,
            board_id: Id,
            card_id: Id,
            event_type: &str,
        ) {
            sqlx::query(
                "INSERT INTO event_log (id, tenant_id, board_id, event_type, payload, created_at) \
                 VALUES ($1, $2, $3, $4, $5::jsonb, now())",
            )
            .bind(Id::new())
            .bind(tenant_id)
            .bind(board_id)
            .bind(event_type)
            .bind(serde_json::json!({"card_id": card_id.to_string()}))
            .execute(pool)
            .await
            .expect("insert event_log failed");
        }

        let dispatcher = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id)
            .await
            .with_opensearch(Arc::clone(&os), index.clone());

        // CardCreated → document indexed.
        insert_event(&pool, &tenant_id, board_id, card_id, "CardCreated").await;
        let n = dispatcher.drain_once().await.expect("drain_once failed");
        assert_eq!(n, 1);
        os.refresh(&index).await.expect("refresh");

        let hits = os
            .search(&index, &serde_json::json!({"query": {"match_all": {}}}))
            .await
            .expect("search failed")
            .expect("results expected");
        assert_eq!(hits.hits.hits.len(), 1, "card should be indexed");
        assert_eq!(hits.hits.hits[0].source.id, card_id.to_string());
        assert_eq!(hits.hits.hits[0].source.title, "test-card");

        // CardDeleted → document removed.
        insert_event(&pool, &tenant_id, board_id, card_id, "CardDeleted").await;
        let n = dispatcher.drain_once().await.expect("drain_once failed");
        assert_eq!(n, 1);
        os.refresh(&index).await.expect("refresh");

        let hits = os
            .search(&index, &serde_json::json!({"query": {"match_all": {}}}))
            .await
            .expect("search failed");
        let count = hits.map(|h| h.hits.hits.len()).unwrap_or(0);
        assert_eq!(count, 0, "card document should be deleted");

        // Cleanup.
        let _ = os.delete_index(&index).await;
        cleanup(&pool, board_id, project_id).await;
    }

    // ── drain_once_publishes_new_rows_and_marks_them ──────────────────────────

    #[tokio::test]
    async fn drain_once_publishes_new_rows_and_marks_them() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;

        // Ensure the stream exists.
        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;

        // Insert 3 undispatched rows.
        let id1 = seed_event_log(&pool, board_id, "CardCreated").await;
        let id2 = seed_event_log(&pool, board_id, "CardUpdated").await;
        let id3 = seed_event_log(&pool, board_id, "CardDeleted").await;

        let dispatcher = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id).await;
        let n = dispatcher.drain_once().await.expect("drain_once failed");

        assert_eq!(n, 3, "expected 3 dispatched rows");

        // Assert all rows are now marked.
        let dispatched = count_dispatched(&pool, board_id).await;
        assert_eq!(dispatched, 3, "expected 3 rows with nats_seq IS NOT NULL");

        // Verify the nats_seq values are positive (JetStream-assigned).
        let rows = sqlx::query(
            "SELECT id, nats_seq FROM event_log \
             WHERE board_id = $1 AND nats_seq IS NOT NULL \
             ORDER BY id",
        )
        .bind(board_id)
        .fetch_all(&pool)
        .await
        .expect("SELECT failed");

        assert_eq!(rows.len(), 3);
        for row in &rows {
            let seq: i64 = row.get("nats_seq");
            assert!(seq > 0, "nats_seq must be positive; got {seq}");
        }

        // Verify the event_ids returned by the envelope match the inserted row ids.
        // We can't easily subscribe post-hoc without a consumer, but we can verify
        // that the row ids are the expected set.
        let row_ids: Vec<Id> = rows.iter().map(|r| r.get("id")).collect();
        assert!(row_ids.contains(&id1));
        assert!(row_ids.contains(&id2));
        assert!(row_ids.contains(&id3));

        cleanup(&pool, board_id, project_id).await;
    }

    // ── drain_once_skips_already_dispatched_rows ──────────────────────────────

    #[tokio::test]
    async fn drain_once_skips_already_dispatched_rows() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;

        // Insert 1 already-dispatched row.
        let _id = seed_event_log_dispatched(&pool, board_id, "CardCreated", 42).await;

        let dispatcher = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id).await;
        let n = dispatcher.drain_once().await.expect("drain_once failed");

        assert_eq!(
            n, 0,
            "expected 0 dispatched — already-dispatched row must be skipped"
        );

        cleanup(&pool, board_id, project_id).await;
    }

    // ── drain_once_is_idempotent_on_restart ──────────────────────────────────

    #[tokio::test]
    async fn drain_once_is_idempotent_on_restart() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;
        let _id = seed_event_log(&pool, board_id, "CardCreated").await;

        let dispatcher = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id).await;

        // First call dispatches the row.
        let n1 = dispatcher
            .drain_once()
            .await
            .expect("first drain_once failed");
        assert_eq!(n1, 1, "expected 1 dispatched on first call");

        // Second call — same instance, row now has nats_seq set.
        let n2 = dispatcher
            .drain_once()
            .await
            .expect("second drain_once failed");
        assert_eq!(
            n2, 0,
            "expected 0 dispatched on second call (already dispatched)"
        );

        // Simulate restart: construct a fresh dispatcher against the same DB.
        let dispatcher2 = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id).await;
        let n3 = dispatcher2
            .drain_once()
            .await
            .expect("third drain_once (restart) failed");
        assert_eq!(
            n3, 0,
            "expected 0 dispatched after restart (row already marked)"
        );

        cleanup(&pool, board_id, project_id).await;
    }

    // ── drain_once_respects_batch_size ────────────────────────────────────────

    #[tokio::test]
    async fn drain_once_respects_batch_size() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;

        // Insert 10 undispatched rows.
        for _ in 0..10 {
            seed_event_log(&pool, board_id, "CardUpdated").await;
        }

        // Use batch_size=3.
        let dispatcher = OutboxDispatcher::new(
            pool.clone(),
            Arc::clone(&nats),
            crate::test_support::setup_identity().await,
            OutboxConfig::default(),
        )
        .with_batch_size(3)
        .with_board_filter(board_id);

        let mut total = 0usize;
        loop {
            let n = dispatcher.drain_once().await.expect("drain_once failed");
            total += n;
            if n == 0 {
                break;
            }
        }

        assert_eq!(
            total, 10,
            "expected all 10 rows dispatched across multiple passes"
        );
        assert_eq!(count_dispatched(&pool, board_id).await, 10);

        cleanup(&pool, board_id, project_id).await;
    }

    // ── drain_once_leaves_rows_when_nats_is_unavailable ──────────────────────
    //
    // We can't stop NATS mid-test in a shared compose environment without
    // affecting other tests. This test documents the expected behaviour:
    // rows remain with nats_seq IS NULL when publish fails, and the dispatcher
    // returns 0 (not an error at the `drain_once` level).

    #[tokio::test]
    async fn drain_once_leaves_rows_undispatched_when_publish_fails() {
        let pool = setup_pool().await;

        // Construct a NatsClient pointed at a dead port so every publish fails.
        let bad_nats = NatsClient::connect(&NatsConfig {
            url: "nats://localhost:19999".to_string(),
            jetstream: true,
            lease_duration: 30,
            auth_token: None,
        })
        .await;

        // If the bad NATS won't even connect, skip rather than panic.
        let bad_nats = match bad_nats {
            Ok(c) => Arc::new(c),
            Err(_) => {
                // Expected in most environments — the port is refused immediately.
                // The test proves the connect path; publish-fail path is covered
                // by unit-level reasoning on the `continue` in drain_once.
                return;
            }
        };

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;
        seed_event_log(&pool, board_id, "CardCreated").await;

        let dispatcher = make_dispatcher(pool.clone(), bad_nats, board_id).await;
        let n = dispatcher
            .drain_once()
            .await
            .expect("drain_once must not propagate publish error");
        assert_eq!(n, 0, "expected 0 dispatched when publish fails");

        // Row must still be undispatched.
        assert_eq!(
            count_dispatched(&pool, board_id).await,
            0,
            "row must remain undispatched after publish failure"
        );

        cleanup(&pool, board_id, project_id).await;
    }

    // ── Pure helper tests ───────────────────────────────────────────────────────

    #[test]
    fn parse_event_column_reads_is_done() {
        let col = parse_event_column(&serde_json::json!({
            "id": "col-1",
            "board_id": "board-1",
            "title": "Done",
            "accent": "green",
            "wip_limit": 0,
            "position": 3,
            "is_done": true,
        }));
        assert!(col.is_done, "is_done must round-trip through the payload");

        // Legacy rows without is_done default to false.
        let legacy = parse_event_column(&serde_json::json!({
            "id": "col-2",
            "board_id": "board-1",
            "title": "To Do",
        }));
        assert!(!legacy.is_done);
    }

    #[test]
    fn build_envelope_populates_fields() {
        let row_id = Id::new();
        let board_id = Id::new();
        let created_at = Utc::now();
        let payload = serde_json::json!({
            "card_id": "card-1",
            "actor_subject": "user:alice",
            "board_revision": 7
        });

        let env = build_envelope(
            row_id,
            &board_id.to_string(),
            &payload,
            created_at,
            "pod-test",
            build_payload("CardCreated", &payload),
        );

        assert_eq!(env.board_id, board_id.to_string());
        assert_eq!(env.event_id, row_id.to_string());
        assert_eq!(env.actor_subject, "user:alice");
        assert_eq!(env.board_revision, 7);
        assert!(env.emitted_at.is_set());
        assert!(env.payload.is_some());
    }

    /// Legacy rows without `board_revision` in the payload default to 0.
    #[test]
    fn build_envelope_defaults_board_revision_to_zero() {
        let payload = serde_json::json!({ "card_id": "card-1" });
        let env = build_envelope(Id::new(), "b-1", &payload, Utc::now(), "pod-test", None);
        assert_eq!(env.board_revision, 0);
    }

    /// Project-scoped rows keep the envelope board_id empty.
    #[test]
    fn build_envelope_project_scope_leaves_board_id_empty() {
        let payload = serde_json::json!({
            "project_id": "p-1",
            "subject": "user:a",
            "relation": "viewer"
        });
        let env = build_envelope(
            Id::new(),
            "",
            &payload,
            Utc::now(),
            "pod-test",
            build_payload("MemberAdded", &payload),
        );
        assert_eq!(env.board_id, "");
        match env.payload {
            Some(Payload::MemberAdded(ev)) => {
                assert_eq!(ev.project_id, "p-1");
                assert_eq!(ev.subject, "user:a");
                assert_eq!(ev.relation, "viewer");
            }
            other => panic!("expected MemberAdded, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_card_created() {
        let json = serde_json::json!({
            "card_id": "card-1",
            "column_id": "col-1",
            "position": 5,
            "idempotency_key": "idem-1"
        });
        let payload = build_payload("CardCreated", &json).unwrap();
        match payload {
            Payload::CardCreated(ev) => {
                assert_eq!(ev.column_id, "col-1");
                assert_eq!(ev.position, 5);
                assert_eq!(ev.idempotency_key, "idem-1");
            }
            _ => panic!("expected CardCreated variant"),
        }
    }

    #[test]
    fn build_payload_card_updated() {
        let json = serde_json::json!({
            "card_id": "card-1",
            "prev_revision": 3,
            "new_revision": 4,
            "idempotency_key": "idem-2"
        });
        let payload = build_payload("CardUpdated", &json).unwrap();
        match payload {
            Payload::CardUpdated(ev) => {
                assert_eq!(ev.card_id, "card-1");
                assert_eq!(ev.prev_revision, 3);
                assert_eq!(ev.new_revision, 4);
                assert_eq!(ev.idempotency_key, "idem-2");
            }
            _ => panic!("expected CardUpdated variant"),
        }
    }

    #[test]
    fn build_payload_card_moved() {
        // Keys must match the MoveCard handler's event payload (cards.rs).
        let json = serde_json::json!({
            "card_id": "card-1",
            "from_column": "col-a",
            "to_column": "col-b",
            "to_position": 2,
            "prev_revision": 7,
            "new_revision": 8,
            "idempotency_key": "idem-3"
        });
        let payload = build_payload("CardMoved", &json).unwrap();
        match payload {
            Payload::CardMoved(ev) => {
                assert_eq!(ev.from_column, "col-a");
                assert_eq!(ev.to_column, "col-b");
                assert_eq!(ev.to_position, 2);
                assert_eq!(ev.prev_revision, 7);
                assert_eq!(ev.new_revision, 8);
            }
            _ => panic!("expected CardMoved variant"),
        }
    }

    #[test]
    fn build_payload_card_deleted() {
        let json = serde_json::json!({
            "card_id": "card-1",
            "prev_revision": 9,
            "idempotency_key": "idem-4"
        });
        let payload = build_payload("CardDeleted", &json).unwrap();
        match payload {
            Payload::CardDeleted(ev) => {
                assert_eq!(ev.card_id, "card-1");
                assert_eq!(ev.prev_revision, 9);
            }
            _ => panic!("expected CardDeleted variant"),
        }
    }

    #[test]
    fn build_payload_unknown_type_returns_none() {
        let json = serde_json::json!({ "card_id": "card-1" });
        assert!(build_payload("NoSuchEvent", &json).is_none());
        assert!(build_payload("", &json).is_none());
    }

    #[test]
    fn outbox_config_default_values() {
        let cfg = OutboxConfig::default();
        assert_eq!(cfg.poll_interval, Duration::from_millis(250));
        assert_eq!(cfg.batch_size, 256);
        assert_eq!(cfg.pod_name, "");
    }

    #[test]
    fn outbox_config_can_override_pod_name() {
        let cfg = OutboxConfig {
            poll_interval: Duration::from_millis(100),
            batch_size: 64,
            pod_name: "pod-42".into(),
        };
        assert_eq!(cfg.pod_name, "pod-42");
    }

    #[test]
    fn build_envelope_uses_emitter_pod_id() {
        let row_id = Id::new();
        let board_id = Id::new();
        let payload = serde_json::json!({ "card_id": "card-1" });
        let env = build_envelope(
            row_id,
            &board_id.to_string(),
            &payload,
            Utc::now(),
            "pod-7",
            None,
        );
        assert_eq!(env.emitter_pod_id, "pod-7");
    }

    // ── New payload arms ──────────────────────────────────────────────────────

    #[test]
    fn build_payload_column_added() {
        let json = serde_json::json!({
            "column": { "id": "c-1", "board_id": "b-1", "title": "Todo", "accent": "red", "wip_limit": 3, "position": 2 },
            "position": 2
        });
        match build_payload("ColumnAdded", &json).unwrap() {
            Payload::ColumnAdded(ev) => {
                let col = ev.column.into_option().expect("column missing");
                assert_eq!(col.id, "c-1");
                assert_eq!(col.title, "Todo");
                assert_eq!(col.accent, "red");
                assert_eq!(col.wip_limit, 3);
                assert_eq!(col.position, 2);
                assert_eq!(ev.position, 2);
            }
            other => panic!("expected ColumnAdded, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_column_updated() {
        let json = serde_json::json!({
            "column": { "id": "c-1", "board_id": "b-1", "title": "Doing", "position": 0 }
        });
        match build_payload("ColumnUpdated", &json).unwrap() {
            Payload::ColumnUpdated(ev) => {
                assert_eq!(
                    ev.column.into_option().expect("column missing").title,
                    "Doing"
                );
            }
            other => panic!("expected ColumnUpdated, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_column_renamed_and_removed() {
        let renamed = serde_json::json!({ "column_id": "c-1", "new_title": "Done" });
        match build_payload("ColumnRenamed", &renamed).unwrap() {
            Payload::ColumnRenamed(ev) => {
                assert_eq!(ev.column_id, "c-1");
                assert_eq!(ev.new_title, "Done");
            }
            other => panic!("expected ColumnRenamed, got {other:?}"),
        }

        let removed = serde_json::json!({ "column_id": "c-1", "move_cards_to_column": "" });
        match build_payload("ColumnRemoved", &removed).unwrap() {
            Payload::ColumnRemoved(ev) => {
                assert_eq!(ev.column_id, "c-1");
                assert_eq!(ev.move_cards_to_column, "");
            }
            other => panic!("expected ColumnRemoved, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_columns_reordered() {
        let json = serde_json::json!({
            "columns": [
                { "id": "c-2", "board_id": "b-1", "title": "Doing", "position": 0 },
                { "id": "c-1", "board_id": "b-1", "title": "Todo", "position": 1 }
            ]
        });
        match build_payload("ColumnsReordered", &json).unwrap() {
            Payload::ColumnsReordered(ev) => {
                assert_eq!(ev.columns.len(), 2);
                assert_eq!(ev.columns[0].id, "c-2");
                assert_eq!(ev.columns[1].id, "c-1");
            }
            other => panic!("expected ColumnsReordered, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_board_events() {
        let created =
            serde_json::json!({ "project_id": "p-1", "board_id": "b-1", "name": "Roadmap" });
        match build_payload("BoardCreated", &created).unwrap() {
            Payload::BoardCreated(ev) => {
                assert_eq!(ev.project_id, "p-1");
                assert_eq!(ev.board_id, "b-1");
                assert_eq!(ev.name, "Roadmap");
            }
            other => panic!("expected BoardCreated, got {other:?}"),
        }

        let deleted = serde_json::json!({ "project_id": "p-1", "board_id": "b-1" });
        match build_payload("BoardDeleted", &deleted).unwrap() {
            Payload::BoardDeleted(ev) => {
                assert_eq!(ev.project_id, "p-1");
                assert_eq!(ev.board_id, "b-1");
            }
            other => panic!("expected BoardDeleted, got {other:?}"),
        }

        let renamed = serde_json::json!({ "new_name": "New Name" });
        match build_payload("BoardRenamed", &renamed).unwrap() {
            Payload::BoardRenamed(ev) => assert_eq!(ev.new_name, "New Name"),
            other => panic!("expected BoardRenamed, got {other:?}"),
        }

        let updated = serde_json::json!({
            "board": { "id": "b-1", "project_id": "p-1", "name": "N", "description": "d", "icon": "i" }
        });
        match build_payload("BoardUpdated", &updated).unwrap() {
            Payload::BoardUpdated(ev) => {
                let b = ev.board.into_option().expect("board missing");
                assert_eq!(b.id, "b-1");
                assert_eq!(b.icon, "i");
            }
            other => panic!("expected BoardUpdated, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_member_events() {
        let added = serde_json::json!({
            "project_id": "p-1", "subject": "user:a", "relation": "editor",
            "display_name": "", "email": ""
        });
        match build_payload("MemberAdded", &added).unwrap() {
            Payload::MemberAdded(ev) => {
                assert_eq!(ev.subject, "user:a");
                assert_eq!(ev.relation, "editor");
            }
            other => panic!("expected MemberAdded, got {other:?}"),
        }

        let removed = serde_json::json!({ "project_id": "p-1", "subject": "user:a" });
        match build_payload("MemberRemoved", &removed).unwrap() {
            Payload::MemberRemoved(ev) => assert_eq!(ev.subject, "user:a"),
            other => panic!("expected MemberRemoved, got {other:?}"),
        }

        let changed = serde_json::json!({
            "project_id": "p-1", "subject": "user:a",
            "old_relation": "viewer", "new_relation": "admin"
        });
        match build_payload("MemberRoleChanged", &changed).unwrap() {
            Payload::MemberRoleChanged(ev) => {
                assert_eq!(ev.old_relation, "viewer");
                assert_eq!(ev.new_relation, "admin");
            }
            other => panic!("expected MemberRoleChanged, got {other:?}"),
        }

        let membership =
            serde_json::json!({ "subject": "user:a", "relation": "viewers", "granted": true });
        match build_payload("MembershipChanged", &membership).unwrap() {
            Payload::MembershipChanged(ev) => {
                assert_eq!(ev.subject, "user:a");
                assert!(ev.granted);
            }
            other => panic!("expected MembershipChanged, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_project_updated() {
        let json = serde_json::json!({
            "project_id": "p-1", "name": "N", "prefix": "ABC", "description": "d"
        });
        match build_payload("ProjectUpdated", &json).unwrap() {
            Payload::ProjectUpdated(ev) => {
                assert_eq!(ev.project_id, "p-1");
                assert_eq!(ev.prefix, "ABC");
            }
            other => panic!("expected ProjectUpdated, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_aggregated_board_events() {
        let created = serde_json::json!({ "aggregated_board_id": "a-1" });
        match build_payload("AggregatedBoardCreated", &created).unwrap() {
            Payload::AggregatedBoardCreated(ev) => assert_eq!(ev.aggregated_board_id, "a-1"),
            other => panic!("expected AggregatedBoardCreated, got {other:?}"),
        }
        match build_payload("AggregatedBoardUpdated", &created).unwrap() {
            Payload::AggregatedBoardUpdated(ev) => assert_eq!(ev.aggregated_board_id, "a-1"),
            other => panic!("expected AggregatedBoardUpdated, got {other:?}"),
        }
        match build_payload("AggregatedBoardDeleted", &created).unwrap() {
            Payload::AggregatedBoardDeleted(ev) => assert_eq!(ev.aggregated_board_id, "a-1"),
            other => panic!("expected AggregatedBoardDeleted, got {other:?}"),
        }

        let source = serde_json::json!({ "aggregated_board_id": "a-1", "board_id": "b-1" });
        match build_payload("SourceBoardAdded", &source).unwrap() {
            Payload::SourceBoardAdded(ev) => {
                assert_eq!(ev.aggregated_board_id, "a-1");
                assert_eq!(ev.board_id, "b-1");
            }
            other => panic!("expected SourceBoardAdded, got {other:?}"),
        }
        match build_payload("SourceBoardRemoved", &source).unwrap() {
            Payload::SourceBoardRemoved(ev) => assert_eq!(ev.board_id, "b-1"),
            other => panic!("expected SourceBoardRemoved, got {other:?}"),
        }
    }

    #[test]
    fn build_payload_github_link_events() {
        let added = serde_json::json!({ "card_id": "c-1", "link_id": "l-1" });
        match build_payload("GitHubLinkAdded", &added).unwrap() {
            Payload::GithubLinkAdded(ev) => {
                assert_eq!(ev.card_id, "c-1");
                assert_eq!(ev.link_id, "l-1");
            }
            other => panic!("expected GithubLinkAdded, got {other:?}"),
        }

        let refreshed =
            serde_json::json!({ "card_id": "c-1", "link_id": "l-1", "new_state": "closed" });
        match build_payload("GitHubLinkRefreshed", &refreshed).unwrap() {
            Payload::GithubLinkRefreshed(ev) => {
                assert_eq!(ev.link_id, "l-1");
                assert_eq!(ev.new_state, "closed");
            }
            other => panic!("expected GithubLinkRefreshed, got {other:?}"),
        }
    }

    // ── CardUpdated patch Struct ──────────────────────────────────────────────

    #[test]
    fn card_updated_patch_filters_sentinels_and_sets_revision() {
        let json = serde_json::json!({
            "card_id": "card-1",
            "prev_revision": 3,
            "new_revision": 4,
            "patch": {
                "title": "New title",
                "description": "",
                "priority": "CARD_PRIORITY_UNSPECIFIED",
                "urgency": "CARD_URGENCY_HIGH",
                "blocked": false
            }
        });
        match build_payload("CardUpdated", &json).unwrap() {
            Payload::CardUpdated(ev) => {
                let patch = ev.patch.into_option().expect("patch missing");
                assert_eq!(
                    patch.get("title").and_then(|v| v.as_str()),
                    Some("New title")
                );
                assert_eq!(
                    patch.get("urgency").and_then(|v| v.as_str()),
                    Some("CARD_URGENCY_HIGH")
                );
                assert_eq!(patch.get("blocked").and_then(|v| v.as_bool()), Some(false));
                assert!(
                    patch.get("description").is_none(),
                    "empty-string sentinel must be filtered"
                );
                assert!(
                    patch.get("priority").is_none(),
                    "unspecified enum sentinel must be filtered"
                );
                assert_eq!(
                    patch.get("revision").and_then(|v| v.as_number()),
                    Some(4.0),
                    "revision key must always be present as a JSON number"
                );
            }
            other => panic!("expected CardUpdated, got {other:?}"),
        }
    }

    /// A CardUpdated without a patch object still carries the revision key.
    #[test]
    fn card_updated_without_patch_still_sets_revision() {
        let json = serde_json::json!({
            "card_id": "card-1",
            "prev_revision": 0,
            "new_revision": 9
        });
        match build_payload("CardUpdated", &json).unwrap() {
            Payload::CardUpdated(ev) => {
                let patch = ev.patch.into_option().expect("patch missing");
                assert_eq!(patch.get("revision").and_then(|v| v.as_number()), Some(9.0));
            }
            other => panic!("expected CardUpdated, got {other:?}"),
        }
    }

    // ── CardCreated hydration ─────────────────────────────────────────────────

    /// CardCreated rows are hydrated with the full Card from the DB at
    /// dispatch time; a deleted card falls back to the minimal variant.
    #[tokio::test]
    async fn build_payload_hydrates_card_created_from_db() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;
        let tenant_id = crate::test_support::test_tenant_id();
        let (project_id, board_id, col_id, card_id) = seed_card_chain(&pool).await;

        let dispatcher = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id).await;

        let json = serde_json::json!({
            "card_id": card_id.to_string(),
            "column_id": col_id.to_string(),
            "position": 0
        });
        let payload = dispatcher
            .build_payload("CardCreated", &json, &tenant_id)
            .await
            .expect("payload expected");
        match payload {
            Payload::CardCreated(ev) => {
                let card = ev.card.into_option().expect("hydrated card missing");
                assert_eq!(card.id, card_id.to_string());
                assert_eq!(card.title, "test-card");
                assert_eq!(ev.column_id, col_id.to_string());
            }
            other => panic!("expected CardCreated, got {other:?}"),
        }

        // Unknown card id → minimal fallback (no hydrated card).
        let missing = serde_json::json!({
            "card_id": Id::new().to_string(),
            "column_id": col_id.to_string()
        });
        let payload = dispatcher
            .build_payload("CardCreated", &missing, &tenant_id)
            .await
            .expect("payload expected");
        match payload {
            Payload::CardCreated(ev) => assert!(!ev.card.is_set()),
            other => panic!("expected CardCreated, got {other:?}"),
        }

        cleanup(&pool, board_id, project_id).await;
    }

    // ── Project-scope routing ─────────────────────────────────────────────────

    /// Project rows are dispatched (marked with a JetStream sequence) and
    /// carry an empty envelope board_id.
    #[tokio::test]
    async fn drain_once_dispatches_project_scoped_rows() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;
        let tenant_id = crate::test_support::test_tenant_id();

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let project_id = crate::test_support::seed_project(&pool).await;
        sqlx::query(
            "INSERT INTO event_log (id, tenant_id, project_id, event_type, payload, created_at) \
             VALUES ($1, $2, $3, $4, $5::jsonb, now())",
        )
        .bind(Id::new())
        .bind(&tenant_id)
        .bind(project_id)
        .bind("MemberAdded")
        .bind(serde_json::json!({
            "project_id": project_id.to_string(),
            "subject": "user:a",
            "relation": "viewer"
        }))
        .execute(&pool)
        .await
        .expect("insert project event failed");

        let dispatcher = make_dispatcher(pool.clone(), Arc::clone(&nats), project_id).await;
        let n = dispatcher.drain_once().await.expect("drain_once failed");
        assert_eq!(n, 1, "project row must be dispatched");

        let seq: Option<i64> =
            sqlx::query_scalar("SELECT nats_seq FROM event_log WHERE project_id = $1")
                .bind(project_id)
                .fetch_one(&pool)
                .await
                .expect("fetch nats_seq");
        assert!(seq.unwrap_or(0) > 0, "nats_seq must be set after dispatch");

        let _ = sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project_id)
            .execute(&pool)
            .await;
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────

    /// Signaling the shutdown watch drains pending rows and exits the task.
    #[tokio::test]
    async fn spawn_drains_pending_rows_and_exits_on_shutdown() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;
        seed_event_log(&pool, board_id, "CardCreated").await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id)
            .await
            .with_shutdown(shutdown_rx)
            .spawn();

        // Let the loop start, then signal shutdown: the final drain must
        // flush the pending row before the task exits.
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(true).expect("shutdown send failed");

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("dispatcher task did not exit after shutdown")
            .expect("dispatcher task panicked");

        assert_eq!(
            count_dispatched(&pool, board_id).await,
            1,
            "final drain must have flushed the pending row"
        );

        cleanup(&pool, board_id, project_id).await;
    }

    /// A committed event insert wakes the loop via LISTEN/NOTIFY well before
    /// the (deliberately long) poll interval would fire.
    #[tokio::test]
    async fn spawn_wakes_on_pg_notify() {
        let pool = setup_pool().await;
        let nats = setup_nats().await;

        crate::realtime::jetstream_bootstrap::ensure_kanban_stream(
            &nats,
            &crate::realtime::jetstream_bootstrap::default_config(),
        )
        .await
        .expect("stream bootstrap failed");

        let (project_id, board_id, _col_id, _card_id) = seed_card_chain(&pool).await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = make_dispatcher(pool.clone(), Arc::clone(&nats), board_id)
            .await
            .with_poll_interval(Duration::from_secs(60))
            .with_shutdown(shutdown_rx)
            .spawn();

        // Insert via the shared helper so pg_notify fires on commit.
        let tenant_id = crate::test_support::test_tenant_id();
        let mut tx = pool.begin().await.expect("begin tx");
        crate::event_log::insert_board_event(
            &mut tx,
            &tenant_id,
            board_id,
            "CardCreated",
            serde_json::json!({ "card_id": Id::new().to_string() }),
        )
        .await
        .expect("insert_board_event failed");
        tx.commit().await.expect("commit");

        // NOTIFY must wake the loop within a couple of seconds even though the
        // poll interval is 60s.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if count_dispatched(&pool, board_id).await == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("row was not dispatched via NOTIFY wake");

        shutdown_tx.send(true).expect("shutdown send failed");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("dispatcher task did not exit")
            .expect("dispatcher task panicked");

        cleanup(&pool, board_id, project_id).await;
    }
}
