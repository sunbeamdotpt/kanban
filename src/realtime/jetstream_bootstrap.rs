// SPDX-License-Identifier: AGPL-3.0-or-later
//! JetStream stream bootstrap for the Kanban realtime spine.
//!
//! Single source of truth for stream and consumer naming and configuration.
//! Called at service startup (`server.rs`) with `?`, so a failure is fatal.
//!
//! # Stream layout
//!
//! One stream, `KANBAN_BOARD_EVENTS`, covers all boards and projects via the
//! wildcard subjects `kanban.board.>` and `kanban.project.>`. Each board
//! publishes to `kanban.board.<board_id>.events`; each project publishes to
//! `kanban.project.<project_id>.events`.
//!
//! `ensure_kanban_stream` is drift-correcting: after the get-or-create it
//! compares the live stream config against the desired one and issues an
//! `update_stream` when subjects, retention, or limits differ.
//!
//! # Consumer naming (MF-6)
//!
//! Live-tail consumers are ephemeral; NATS GCs them on disconnect.
//! Format: `kanban-board-{board_id}-{pod_id}-{stream_id}` for boards and
//! `kanban-project-{project_id}-{pod_id}-{stream_id}` for projects.
//!
//! Replay consumers (replay-from-resume-token) are also ephemeral per RPC.
//! Format: `kanban-replay-{board_id}-{replay_id}`.

use std::time::Duration;

use anyhow::Result;
use sunbeam_g2v::mq::NatsClient;
use tracing::info;

// ── Stable names ─────────────────────────────────────────────────────────────

/// JetStream stream name for all board events.
pub const STREAM_NAME: &str = "KANBAN_BOARD_EVENTS";

/// Subject prefix for per-board event subjects. Append `{board_id}.events`.
pub const STREAM_SUBJECT_PREFIX: &str = "kanban.board.";

/// Wildcard subject that covers every board; used in stream config.
pub const STREAM_WILDCARD_SUBJECT: &str = "kanban.board.>";

/// Subject prefix for per-project event subjects. Append `{project_id}.events`.
pub const STREAM_SUBJECT_PREFIX_PROJECT: &str = "kanban.project.";

/// Wildcard subject that covers every project; used in stream config.
pub const STREAM_WILDCARD_SUBJECT_PROJECT: &str = "kanban.project.>";

// ── Subject helpers ───────────────────────────────────────────────────────────

/// Subject for events on a single board: `kanban.board.<board_id>.events`.
pub fn board_subject(board_id: &str) -> String {
    format!("{STREAM_SUBJECT_PREFIX}{board_id}.events")
}

/// Subject for events on a single project: `kanban.project.<project_id>.events`.
pub fn project_subject(project_id: &str) -> String {
    format!("{STREAM_SUBJECT_PREFIX_PROJECT}{project_id}.events")
}

/// Ephemeral consumer name for live-tail per MF-6.
///
/// Format: `kanban-board-{board_id}-{pod_id}-{stream_id}`.
/// NATS GCs the consumer when the subscribing connection closes.
pub fn live_tail_consumer_name(board_id: &str, pod_id: &str, stream_id: &str) -> String {
    format!("kanban-board-{board_id}-{pod_id}-{stream_id}")
}

/// Ephemeral consumer name for project live-tail.
///
/// Format: `kanban-project-{project_id}-{pod_id}-{stream_id}`.
pub fn project_live_tail_consumer_name(project_id: &str, pod_id: &str, stream_id: &str) -> String {
    format!("kanban-project-{project_id}-{pod_id}-{stream_id}")
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

/// Test helper: returns a default `StreamConfig` for tests that need to
/// idempotently ensure the `KANBAN_BOARD_EVENTS` stream exists.
#[cfg(test)]
pub fn default_config() -> StreamConfig {
    StreamConfig {
        name: STREAM_NAME,
        subjects: &[STREAM_WILDCARD_SUBJECT, STREAM_WILDCARD_SUBJECT_PROJECT],
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
            ..Default::default()
        }
    }
}

// ── Bootstrap entry point ─────────────────────────────────────────────────────

/// Idempotently ensure the `KANBAN_BOARD_EVENTS` JetStream stream exists and
/// matches `cfg`.
///
/// Looks the stream up by name and corrects drift (subjects, retention,
/// limits) via `update_stream`; creates it when missing. `get_or_create_stream`
/// is deliberately *not* used: it errors with "stream name already in use
/// with a different configuration" (10058) instead of returning the drifted
/// stream, which would make config upgrades impossible without manual stream
/// surgery.
///
/// # Errors
///
/// Returns `Err` on any NATS/JetStream failure. The caller in `server.rs`
/// propagates this with `?` — the process panics, which is correct (the stream
/// is a hard dependency at startup).
pub async fn ensure_kanban_stream(nats: &NatsClient, cfg: &StreamConfig) -> Result<()> {
    let js = nats
        .jetstream()
        .ok_or_else(|| anyhow::anyhow!("JetStream not enabled on NatsClient"))?;

    match js.get_stream(cfg.name).await {
        Ok(mut stream) => {
            let current = stream
                .info()
                .await
                .map_err(|e| {
                    anyhow::anyhow!("fatal: failed to fetch {} stream info: {e}", cfg.name)
                })?
                .config
                .clone();
            correct_drift(js, cfg, &current).await
        }
        Err(_) => {
            // Not found (or a lookup error treated as such) — create. On a
            // create collision with another pod, fall back to the drift path.
            let desired = async_nats::jetstream::stream::Config::from(cfg);
            match js.create_stream(desired).await {
                Ok(_) => Ok(()),
                Err(_) => {
                    let mut stream = js.get_stream(cfg.name).await.map_err(|e| {
                        anyhow::anyhow!("fatal: failed to bootstrap {}: {e}", cfg.name)
                    })?;
                    let current = stream
                        .info()
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("fatal: failed to fetch {} stream info: {e}", cfg.name)
                        })?
                        .config
                        .clone();
                    correct_drift(js, cfg, &current).await
                }
            }
        }
    }
}

/// Update the stream when the live config has drifted from the desired one.
async fn correct_drift(
    js: &async_nats::jetstream::Context,
    cfg: &StreamConfig,
    current: &async_nats::jetstream::stream::Config,
) -> Result<()> {
    let desired = async_nats::jetstream::stream::Config::from(cfg);
    let drifted = current.subjects != desired.subjects
        || current.retention != desired.retention
        || current.max_age != desired.max_age
        || current.max_messages_per_subject != desired.max_messages_per_subject
        || current.num_replicas != desired.num_replicas
        || current.storage != desired.storage;

    if drifted {
        js.update_stream(&desired).await.map_err(|e| {
            anyhow::anyhow!(
                "fatal: failed to correct drifted {} stream config: {e}",
                cfg.name
            )
        })?;
        info!(
            stream = cfg.name,
            "JetStream stream config drifted; updated to desired config"
        );
    }

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Calling `ensure_kanban_stream` twice against a live NATS server must be
    /// idempotent — second call returns Ok without error or panic.
    ///
    /// Requires `NATS_URL` to point at a running NATS server with JetStream
    /// enabled. Does **not** tear down the stream after the test (the stream
    /// is shared / idempotent).
    #[tokio::test]
    async fn ensure_kanban_stream_is_idempotent() {
        let nats = Arc::clone(&crate::test_support::containers::setup().await.nats);

        let cfg = default_config();

        // First call — creates the stream.
        ensure_kanban_stream(&nats, &cfg)
            .await
            .expect("first ensure_kanban_stream failed");

        // Second call — idempotent no-op.
        ensure_kanban_stream(&nats, &cfg)
            .await
            .expect("second ensure_kanban_stream failed (not idempotent)");

        // The live stream config must match the desired one.
        let js = nats.jetstream().expect("jetstream enabled");
        let mut stream = js.get_stream(STREAM_NAME).await.expect("get_stream failed");
        let info = stream.info().await.expect("stream info failed");
        assert_eq!(info.config.subjects, cfg.subjects);
        assert_eq!(
            info.config.retention,
            async_nats::jetstream::stream::RetentionPolicy::Limits
        );
        assert_eq!(
            info.config.max_age,
            std::time::Duration::from_secs(cfg.max_age_secs)
        );
        assert_eq!(
            info.config.max_messages_per_subject,
            cfg.max_msgs_per_subject
        );
    }

    /// A drifted stream (missing the project wildcard subject, different
    /// limits) must be corrected back to the desired config.
    ///
    /// Uses a dedicated stream name: mutating the shared `KANBAN_BOARD_EVENTS`
    /// stream would race with parallel tests that ensure/subscribe to it.
    #[tokio::test]
    async fn ensure_kanban_stream_corrects_drift() {
        const DRIFT_STREAM: &str = "KANBAN_BOARD_EVENTS_DRIFT_TEST";
        // Dedicated subject namespace: reusing the production wildcards would
        // overlap with the real KANBAN_BOARD_EVENTS stream (NATS error 10065)
        // when tests run in parallel against the shared container.
        const DRIFT_SUBJECT: &str = "kanban.drift-test.board.>";
        const DRIFT_SUBJECT_PROJECT: &str = "kanban.drift-test.project.>";

        let nats = Arc::clone(&crate::test_support::containers::setup().await.nats);

        // Bootstrap a drifted config: board-only subjects, smaller limits.
        let drifted = StreamConfig {
            name: DRIFT_STREAM,
            subjects: &[DRIFT_SUBJECT],
            retention: Retention::Limits,
            max_age_secs: 3_600,
            max_msgs_per_subject: 100,
            replicas: 1,
            storage: Storage::File,
        };
        ensure_kanban_stream(&nats, &drifted)
            .await
            .expect("drifted bootstrap failed");

        let cfg = StreamConfig {
            name: DRIFT_STREAM,
            subjects: &[DRIFT_SUBJECT, DRIFT_SUBJECT_PROJECT],
            ..default_config()
        };
        ensure_kanban_stream(&nats, &cfg)
            .await
            .expect("drift-correcting ensure_kanban_stream failed");

        let js = nats.jetstream().expect("jetstream enabled");
        let mut stream = js
            .get_stream(DRIFT_STREAM)
            .await
            .expect("get_stream failed");
        let info = stream.info().await.expect("stream info failed");
        assert_eq!(
            info.config.subjects, cfg.subjects,
            "project wildcard subject must have been added"
        );
        assert_eq!(
            info.config.max_messages_per_subject,
            cfg.max_msgs_per_subject
        );
        assert_eq!(
            info.config.max_age,
            std::time::Duration::from_secs(cfg.max_age_secs)
        );

        // Do not leak the scratch stream into other tests.
        js.delete_stream(DRIFT_STREAM)
            .await
            .expect("delete_stream failed");
    }

    #[test]
    fn project_subject_formats_events_subject() {
        assert_eq!(project_subject("p-1"), "kanban.project.p-1.events");
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
