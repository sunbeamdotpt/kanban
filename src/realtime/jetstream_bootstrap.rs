// SPDX-License-Identifier: AGPL-3.0-or-later
//! JetStream stream bootstrap for the Kanban realtime spine.
//!
//! Single source of truth for stream and consumer naming and configuration.
//! Called at service startup (`server.rs`) with `?`, so a failure is fatal.
//!
//! # Stream layout
//!
//! One stream, `KANBAN_BOARD_EVENTS`, covers all boards via the wildcard
//! subject `kanban.board.>`. Each board publishes to
//! `kanban.board.<board_id>.events`.
//!
//! # Consumer naming (MF-6)
//!
//! Live-tail consumers are ephemeral; NATS GCs them on disconnect.
//! Format: `kanban-board-{board_id}-{pod_id}-{stream_id}`.
//!
//! Replay consumers (replay-from-resume-token) are also ephemeral per RPC.
//! Format: `kanban-replay-{board_id}-{replay_id}`.

use std::time::Duration;

use anyhow::Result;
use sunbeam_g2v::mq::NatsClient;

// ── Stable names ─────────────────────────────────────────────────────────────

/// JetStream stream name for all board events.
pub const STREAM_NAME: &str = "KANBAN_BOARD_EVENTS";

/// Subject prefix for per-board event subjects. Append `{board_id}.events`.
pub const STREAM_SUBJECT_PREFIX: &str = "kanban.board.";

/// Wildcard subject that covers every board; used in stream config.
pub const STREAM_WILDCARD_SUBJECT: &str = "kanban.board.>";

// ── Subject helpers ───────────────────────────────────────────────────────────

/// Subject for events on a single board: `kanban.board.<board_id>.events`.
pub fn board_subject(board_id: &str) -> String {
    format!("{STREAM_SUBJECT_PREFIX}{board_id}.events")
}

/// Ephemeral consumer name for live-tail per MF-6.
///
/// Format: `kanban-board-{board_id}-{pod_id}-{stream_id}`.
/// NATS GCs the consumer when the subscribing connection closes.
pub fn live_tail_consumer_name(board_id: &str, pod_id: &str, stream_id: &str) -> String {
    format!("kanban-board-{board_id}-{pod_id}-{stream_id}")
}

/// Ephemeral consumer name for replay-from-resume-token (one per RPC).
///
/// Format: `kanban-replay-{board_id}-{replay_id}`.
pub fn replay_consumer_name(board_id: &str, replay_id: &str) -> String {
    format!("kanban-replay-{board_id}-{replay_id}")
}

// ── StreamConfig ──────────────────────────────────────────────────────────────

/// Configuration for the `KANBAN_BOARD_EVENTS` JetStream stream.
///
/// Maps to `async_nats::jetstream::stream::Config` via
/// `From<&StreamConfig>`. Fields not surfaced by g2v's `ensure_stream`
/// (which accepts the native config directly) map 1:1.
pub struct StreamConfig {
    /// Stream name.
    pub name: &'static str,
    /// Subjects covered by this stream.
    pub subjects: &'static [&'static str],
    /// Retention policy.
    pub retention: Retention,
    /// Maximum stream age in seconds (default: 86 400 = 24 h).
    pub max_age_secs: u64,
    /// Maximum messages retained per subject (-1 = unlimited).
    pub max_msgs_per_subject: i64,
    /// Number of stream replicas (1 in dev, 3 in prod).
    pub replicas: i32,
    /// Storage backend (File in dev/prod, Memory only for tests).
    pub storage: Storage,
}

/// Retention policy variants mirroring `async_nats::jetstream::stream::RetentionPolicy`.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Retention {
    /// Retain until size/age/count limit — suitable for replay.
    Limits,
    /// Retain until all consumers ack.
    Interest,
    /// Retain until the first consumer acks (work-queue semantics).
    WorkQueue,
}

/// Storage backend variants mirroring `async_nats::jetstream::stream::StorageType`.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Storage {
    /// Persist to disk.
    File,
    /// In-memory only (tests only — no durability).
    Memory,
}

/// Default stream configuration.
///
/// - 24-hour retention, 10 000 messages per board (prevents runaway growth).
/// - Limits retention so replay consumers can seek to any offset within the window.
/// - 1 replica; set `KANBAN_NATS_REPLICAS=3` in production.
/// - File storage; override to Memory in tests via `StreamConfig { storage: Storage::Memory, .. }`.
impl std::str::FromStr for Retention {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "limits" => Ok(Retention::Limits),
            "interest" => Ok(Retention::Interest),
            "work_queue" | "workqueue" | "work-queue" => Ok(Retention::WorkQueue),
            _ => Err(format!("unknown retention policy: {s}")),
        }
    }
}

impl std::str::FromStr for Storage {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "file" => Ok(Storage::File),
            "memory" => Ok(Storage::Memory),
            _ => Err(format!("unknown storage type: {s}")),
        }
    }
}

pub fn default_config() -> StreamConfig {
    StreamConfig {
        name: STREAM_NAME,
        subjects: &[STREAM_WILDCARD_SUBJECT],
        retention: Retention::Limits,
        max_age_secs: 86_400,
        max_msgs_per_subject: 10_000,
        replicas: 1,
        storage: Storage::File,
    }
}

// ── async_nats mapping ────────────────────────────────────────────────────────

impl From<&StreamConfig> for async_nats::jetstream::stream::Config {
    fn from(cfg: &StreamConfig) -> Self {
        async_nats::jetstream::stream::Config {
            name: cfg.name.to_string(),
            subjects: cfg.subjects.iter().map(|s| s.to_string()).collect(),
            retention: match cfg.retention {
                Retention::Limits => async_nats::jetstream::stream::RetentionPolicy::Limits,
                Retention::Interest => async_nats::jetstream::stream::RetentionPolicy::Interest,
                Retention::WorkQueue => async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            },
            max_age: Duration::from_secs(cfg.max_age_secs),
            max_messages_per_subject: cfg.max_msgs_per_subject,
            num_replicas: cfg.replicas as usize,
            storage: match cfg.storage {
                Storage::File => async_nats::jetstream::stream::StorageType::File,
                Storage::Memory => async_nats::jetstream::stream::StorageType::Memory,
            },
            // TODO(g2v): expose retention + max_msgs_per_subject on NatsClient::ensure_stream
            // so callers don't need to reach into async_nats types directly.
            // For now, From<&StreamConfig> produces the native config which is passed through.
            ..Default::default()
        }
    }
}

// ── Bootstrap entry point ─────────────────────────────────────────────────────

/// Idempotently ensure the `KANBAN_BOARD_EVENTS` JetStream stream exists.
///
/// Uses `NatsClient::ensure_stream` (g2v wrapper around
/// `async_nats::jetstream::Context::get_or_create_stream`).
/// A second call with the same config is a no-op.
///
/// # Errors
///
/// Returns `Err` on any NATS/JetStream failure. The caller in `server.rs`
/// propagates this with `?` — the process panics, which is correct (the stream
/// is a hard dependency at startup).
pub async fn ensure_kanban_stream(nats: &NatsClient, cfg: &StreamConfig) -> Result<()> {
    let nats_cfg = async_nats::jetstream::stream::Config::from(cfg);
    nats.ensure_stream(nats_cfg).await.map_err(|e| {
        anyhow::anyhow!(
            "fatal: failed to bootstrap {stream}: {e}",
            stream = cfg.name
        )
    })?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Calling `ensure_kanban_stream` twice against a live NATS server must be
    /// idempotent — second call returns Ok without error or panic.
    ///
    /// Requires `NATS_URL` to point at a running NATS server with JetStream
    /// enabled. Does **not** tear down the stream after the test (the stream
    /// is shared / idempotent).
    #[tokio::test]
    async fn ensure_kanban_stream_is_idempotent() {
        let nats_url =
            std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

        let nats = NatsClient::connect(&sunbeam_g2v::config::NatsConfig {
            url: nats_url,
            jetstream: true,
            lease_duration: 30,
            auth_token: std::env::var("NATS_AUTH_TOKEN").ok(),
        })
        .await
        .expect("NATS connect failed — is NATS_URL set and the server running?");

        let cfg = default_config();

        // First call — creates the stream.
        ensure_kanban_stream(&nats, &cfg)
            .await
            .expect("first ensure_kanban_stream failed");

        // Second call — idempotent no-op.
        ensure_kanban_stream(&nats, &cfg)
            .await
            .expect("second ensure_kanban_stream failed (not idempotent)");

        // TODO(g2v): when NatsClient exposes stream_info(), assert returned
        // config matches default_config() fields (retention, max_age, etc.).
    }

    #[test]
    fn retention_from_str_parses_variants() {
        assert_eq!("limits".parse::<Retention>().unwrap(), Retention::Limits);
        assert_eq!(
            "Interest".parse::<Retention>().unwrap(),
            Retention::Interest
        );
        assert_eq!(
            "work_queue".parse::<Retention>().unwrap(),
            Retention::WorkQueue
        );
        assert!("unknown".parse::<Retention>().is_err());
    }

    #[test]
    fn storage_from_str_parses_variants() {
        assert_eq!("file".parse::<Storage>().unwrap(), Storage::File);
        assert_eq!("Memory".parse::<Storage>().unwrap(), Storage::Memory);
        assert!("tape".parse::<Storage>().is_err());
    }

    #[test]
    fn default_config_matches_documented_defaults() {
        let cfg = default_config();
        assert_eq!(cfg.name, STREAM_NAME);
        assert_eq!(cfg.retention, Retention::Limits);
        assert_eq!(cfg.max_age_secs, 86_400);
        assert_eq!(cfg.max_msgs_per_subject, 10_000);
        assert_eq!(cfg.replicas, 1);
        assert_eq!(cfg.storage, Storage::File);
    }

    #[test]
    fn stream_config_can_be_overridden() {
        let cfg = StreamConfig {
            name: STREAM_NAME,
            subjects: &[STREAM_WILDCARD_SUBJECT],
            retention: Retention::Interest,
            max_age_secs: 600,
            max_msgs_per_subject: 500,
            replicas: 3,
            storage: Storage::Memory,
        };
        assert_eq!(cfg.retention, Retention::Interest);
        assert_eq!(cfg.max_age_secs, 600);
        assert_eq!(cfg.max_msgs_per_subject, 500);
        assert_eq!(cfg.replicas, 3);
        assert_eq!(cfg.storage, Storage::Memory);
    }
}
