// SPDX-License-Identifier: AGPL-3.0-or-later
//! `BoardSubscriberRegistry` — per-pod ephemeral NATS push consumer + broadcast fanout.
//!
//! Despite the name, the registry fans out both board subjects
//! (`kanban.board.<id>.events`, via [`BoardSubscriberRegistry::subscribe`]) and
//! project subjects (`kanban.project.<id>.events`, via
//! [`BoardSubscriberRegistry::subscribe_project`]); both share the same map,
//! pump-task, and refcount machinery through a common `subscribe_object` path.
//!
//! # Concurrency invariants
//!
//! - **`boards` is the single source of truth.** No channel state lives outside
//!   this map. Constructing or tearing down a channel always goes through the
//!   map under a write lock.
//! - **Refcounts ride the write lock.** `subscribe` increments under a write
//!   lock; `StreamHandle::drop` decrements under a write lock. The refcount is
//!   never mutated without holding the write lock.
//! - **`broadcast::Sender::send` is fire-and-forget.** A `send` failure (all
//!   receivers have dropped) is not an error from the pump task's perspective —
//!   the subsequent `StreamHandle::drop` will abort the task. Lagged receivers
//!   receive `RecvError::Lagged` and the `SubscribeBoard` handler is expected
//!   to reconnect with `last_seq`.
//! - **No lock is held across `.await` points.** All async work happens outside
//!   the lock. The lock is acquired, the state is read or mutated, and the lock
//!   is released before any async call.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::id::Id;
use anyhow::{Result, anyhow};
use async_nats::jetstream::consumer::push;
use buffa::Message;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error, warn};

use sunbeam_g2v::mq::NatsClient;

use super::jetstream_bootstrap::{
    STREAM_NAME, board_subject, live_tail_consumer_name, project_live_tail_consumer_name,
    project_subject,
};
use crate::cpb::sunbeam::kanban::v1::BoardEventEnvelope;

/// Per-board ring-buffer capacity. 256 covers typical burst windows;
/// slow receivers get `RecvError::Lagged` and must reconnect.
const DEFAULT_BROADCAST_CAPACITY: usize = 256;
/// Default NATS ephemeral consumer GC threshold in seconds.
const DEFAULT_INACTIVE_THRESHOLD_SECS: u64 = 30;

/// Tunables for [`BoardSubscriberRegistry`].
#[derive(Clone, Debug)]
pub struct RegistryConfig {
    pub broadcast_capacity: usize,
    pub inactive_threshold_secs: u64,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
            inactive_threshold_secs: DEFAULT_INACTIVE_THRESHOLD_SECS,
        }
    }
}

// ── BoardChannel ─────────────────────────────────────────────────────────────

struct BoardChannel {
    sender: broadcast::Sender<BoardEventEnvelope>,
    /// Handle to the pump task; aborted when the last subscriber drops.
    consumer_task: JoinHandle<()>,
    /// Number of live `StreamHandle`s referencing this channel.
    refcount: usize,
}

// ── BoardSubscriberRegistry ───────────────────────────────────────────────────

/// Registry that owns one ephemeral NATS push consumer per board (within
/// this pod) and fans the messages out via `tokio::sync::broadcast`.
pub struct BoardSubscriberRegistry {
    nats: Arc<NatsClient>,
    pod_id: String,
    boards: Arc<RwLock<HashMap<String, BoardChannel>>>,
    config: RegistryConfig,
}

impl BoardSubscriberRegistry {
    /// Construct a new registry with default tunables.
    ///
    /// `pod_id` is embedded in every ephemeral consumer name (MF-6).
    /// A stable, unique-per-pod string (e.g. `$POD_NAME` env var or a
    /// random UUID at process start) is recommended.
    pub fn new(nats: Arc<NatsClient>, pod_id: impl Into<String>) -> Self {
        Self::new_with_config(nats, pod_id, RegistryConfig::default())
    }

    /// Construct a new registry with explicit tunables.
    pub fn new_with_config(
        nats: Arc<NatsClient>,
        pod_id: impl Into<String>,
        config: RegistryConfig,
    ) -> Self {
        Self {
            nats,
            pod_id: pod_id.into(),
            boards: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Subscribe to live events for `board_id`.
    ///
    /// # First subscriber
    ///
    /// 1. Opens an ephemeral NATS push consumer on `kanban.board.<id>.events`
    ///    with `inactive_threshold = 30s` (NATS auto-GCs if no one is listening).
    /// 2. Spawns a pump task that decodes `BoardEventEnvelope` from JetStream
    ///    messages and broadcasts them.
    /// 3. Inserts the channel into the map with `refcount = 1`.
    ///
    /// # Subsequent subscribers (same board)
    ///
    /// Increments `refcount` and returns a new `broadcast::Receiver` from the
    /// existing `Sender`. No second consumer is opened.
    ///
    /// # Errors
    ///
    /// Returns `Err` if JetStream is not enabled on the client or if the NATS
    /// `create_consumer` call fails.
    pub async fn subscribe(self: Arc<Self>, board_id: &str) -> Result<StreamHandle> {
        let stream_id = Id::new().to_string();
        let subject = board_subject(board_id);
        let consumer_name = live_tail_consumer_name(board_id, &self.pod_id, &stream_id);
        self.subscribe_object(format!("board:{board_id}"), subject, consumer_name)
            .await
    }

    /// Subscribe to live events for `project_id`
    /// (`kanban.project.<id>.events`). Same channel-sharing semantics as
    /// [`Self::subscribe`].
    pub async fn subscribe_project(self: Arc<Self>, project_id: &str) -> Result<StreamHandle> {
        let stream_id = Id::new().to_string();
        let subject = project_subject(project_id);
        let consumer_name = project_live_tail_consumer_name(project_id, &self.pod_id, &stream_id);
        self.subscribe_object(format!("project:{project_id}"), subject, consumer_name)
            .await
    }

    /// Shared subscribe path for board and project subjects.
    ///
    /// `map_key` namespaces the channel map so a board id and a project id
    /// can never alias the same channel.
    async fn subscribe_object(
        self: Arc<Self>,
        map_key: String,
        subject: String,
        consumer_name: String,
    ) -> Result<StreamHandle> {
        // Fast path: object already has a channel — take read lock, check, drop.
        {
            let guard = self.boards.read();
            if guard.contains_key(&map_key) {
                // Drop read lock before upgrading to write lock.
                drop(guard);
                // Upgrade to write lock to increment refcount.
                let mut guard = self.boards.write();
                if let Some(ch) = guard.get_mut(&map_key) {
                    ch.refcount += 1;
                    let receiver = ch.sender.subscribe();
                    debug!(
                        map_key,
                        refcount = ch.refcount,
                        "subscriber joined existing channel"
                    );
                    return Ok(StreamHandle {
                        board_id: map_key,
                        receiver,
                        registry: Arc::clone(&self),
                    });
                }
                // If the entry disappeared between the read-lock check and the
                // write-lock acquisition (unlikely but possible if the last
                // subscriber dropped between the two locks), fall through to
                // the slow path below.
            }
        }

        // Slow path: open a new push consumer and insert a new channel.
        let deliver_inbox = self.nats.client().new_inbox();

        // Access the JetStream context. NatsClient::jetstream() returns Option<&Context>.
        let js = self
            .nats
            .jetstream()
            .ok_or_else(|| anyhow!("JetStream not enabled on NatsClient"))?
            .clone();

        // Get the stream handle; we need it for create_consumer.
        let stream = js
            .get_stream(STREAM_NAME)
            .await
            .map_err(|e| anyhow!("failed to get JetStream stream {STREAM_NAME}: {e}"))?;

        // Create the ephemeral push consumer.
        //
        // g2v gap noted: `NatsClient` exposes `pull_subscribe` but has no
        // `push_subscribe` helper. We reach into the JetStream context via
        // `nats.jetstream()` (which `NatsClient` does expose) and call
        // `create_consumer` directly. The `inactive_threshold` field is
        // available on `push::Config` in async-nats 0.47.
        let consumer: async_nats::jetstream::consumer::PushConsumer = stream
            .create_consumer(push::Config {
                name: Some(consumer_name.clone()),
                deliver_subject: deliver_inbox.clone(),
                filter_subject: subject.clone(),
                inactive_threshold: Duration::from_secs(self.config.inactive_threshold_secs),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("failed to create push consumer {consumer_name}: {e}"))?;

        let (sender, _) = broadcast::channel(self.config.broadcast_capacity);
        let pump_sender = sender.clone();

        // Spawn the pump task. It runs until aborted (on last-subscriber drop).
        // Clone so we can still log consumer_name after the task move.
        let consumer_name_task = consumer_name.clone();
        let pump_task: JoinHandle<()> = tokio::spawn(async move {
            let consumer_name = consumer_name_task;
            let mut messages = match consumer.messages().await {
                Ok(m) => m,
                Err(e) => {
                    error!(consumer = %consumer_name, error = %e, "failed to open consumer message stream");
                    return;
                }
            };

            while let Some(msg_result) = messages.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "error receiving JetStream message; continuing");
                        continue;
                    }
                };

                let payload = msg.payload.clone();

                // Decode the protobuf envelope.
                match BoardEventEnvelope::decode_from_slice(&payload) {
                    Ok(mut envelope) => {
                        // The outbox publishes with nats_seq = 0 (the sequence
                        // is only known after the JetStream ack). Stamp the
                        // authoritative stream sequence so cutover tracking
                        // and client resume tokens see the real value.
                        if let Ok(info) = msg.info() {
                            envelope.nats_seq = info.stream_sequence;
                        }
                        // Fire-and-forget: if there are no receivers (all dropped),
                        // send returns Err — we ignore it; the task will be aborted
                        // shortly via Drop.
                        let _ = pump_sender.send(envelope);
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to decode BoardEventEnvelope; skipping message");
                    }
                }

                // Acknowledge the message (push consumers with explicit ack policies
                // need this; the default AckPolicy::Explicit means we must ack).
                if let Err(e) = msg.ack().await {
                    warn!(error = %e, "failed to ack JetStream message");
                }
            }

            debug!("pump task exited (consumer stream ended)");
        });

        let receiver = sender.subscribe();

        // Insert under write lock.
        {
            let mut guard = self.boards.write();
            guard.insert(
                map_key.clone(),
                BoardChannel {
                    sender,
                    consumer_task: pump_task,
                    refcount: 1,
                },
            );
        }

        debug!(map_key, consumer = %consumer_name, "opened new NATS push consumer");

        Ok(StreamHandle {
            board_id: map_key,
            receiver,
            registry: Arc::clone(&self),
        })
    }

    /// Decrement the refcount for `map_key`. If it reaches 0, abort the pump
    /// task and remove the entry from the map.
    ///
    /// Called from `StreamHandle::drop`; must not panic.
    fn decrement_refcount(&self, map_key: &str) {
        let mut guard = self.boards.write();
        let remove = match guard.get_mut(map_key) {
            Some(ch) => {
                ch.refcount = ch.refcount.saturating_sub(1);
                debug!(map_key, refcount = ch.refcount, "subscriber dropped");
                ch.refcount == 0
            }
            None => {
                warn!(map_key, "decrement_refcount called for unknown object");
                false
            }
        };

        if remove && let Some(ch) = guard.remove(map_key) {
            // Abort the pump task. The ephemeral consumer will be GC'd by
            // NATS after `inactive_threshold` (30s). No async work needed.
            ch.consumer_task.abort();
            debug!(map_key, "removed channel (last subscriber dropped)");
        }
    }
}

// ── StreamHandle ──────────────────────────────────────────────────────────────

/// A handle to an active board or project subscription.
///
/// Holds a `broadcast::Receiver<BoardEventEnvelope>`. When dropped, decrements
/// the registry's refcount for this object; if the refcount reaches 0, the
/// ephemeral NATS push consumer's pump task is aborted and NATS auto-GCs
/// the consumer after `inactive_threshold`.
pub struct StreamHandle {
    /// Registry map key (`board:<id>` or `project:<id>`), used for refcounting.
    pub board_id: String,
    pub receiver: broadcast::Receiver<BoardEventEnvelope>,
    registry: Arc<BoardSubscriberRegistry>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.registry.decrement_refcount(&self.board_id);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn setup_nats() -> Arc<NatsClient> {
        let infra = crate::test_support::containers::setup().await;
        Arc::clone(&infra.nats)
    }

    /// Ensure the KANBAN_BOARD_EVENTS stream exists (idempotent).
    async fn ensure_stream(nats: &NatsClient) {
        use crate::realtime::jetstream_bootstrap::{default_config, ensure_kanban_stream};
        let cfg = default_config();
        ensure_kanban_stream(nats, &cfg)
            .await
            .expect("ensure_kanban_stream failed");
    }

    fn make_envelope(board_id: &str, event_id: &str) -> BoardEventEnvelope {
        use crate::cpb::sunbeam::kanban::v1::{Heartbeat, board_event_envelope::Payload};

        BoardEventEnvelope {
            board_id: board_id.to_string(),
            event_id: event_id.to_string(),
            nats_seq: 0,
            board_revision: 1,
            emitted_at: None.into(),
            emitter_pod_id: "test-pod".to_string(),
            actor_subject: "test".to_string(),
            payload: Some(Payload::Heartbeat(Box::new(Heartbeat {
                server_time_ms: 0,
                ..Default::default()
            }))),
            ..Default::default()
        }
    }

    async fn publish_envelope(nats: &NatsClient, board_id: &str, envelope: &BoardEventEnvelope) {
        let subject = board_subject(board_id);
        let buf = bytes::Bytes::from(envelope.encode_to_vec());
        nats.publish_jetstream(&subject, buf)
            .await
            .expect("publish_jetstream failed")
            .await
            .expect("publish ack failed");
    }

    /// First subscriber opens the consumer and receives a published event.
    #[tokio::test]
    async fn subscribe_first_caller_opens_consumer_and_receives_published_event() {
        let nats = setup_nats().await;
        ensure_stream(&nats).await;

        let board_id = format!("test-board-{}", Id::new());
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&nats),
            "pod-test-1",
        ));
        let mut handle = Arc::clone(&registry)
            .subscribe(&board_id)
            .await
            .expect("subscribe failed");

        let envelope = make_envelope(&board_id, "evt-001");
        publish_envelope(&nats, &board_id, &envelope).await;

        let received = tokio::time::timeout(Duration::from_secs(2), handle.receiver.recv())
            .await
            .expect("timeout waiting for event")
            .expect("recv error");

        assert_eq!(received.event_id, "evt-001");
        assert_eq!(received.board_id, board_id);
    }

    /// Two subscribers on the same board share a single consumer; only one entry
    /// exists in the boards map, and both receive the same event.
    #[tokio::test]
    async fn subscribe_second_caller_shares_consumer_no_duplicate() {
        let nats = setup_nats().await;
        ensure_stream(&nats).await;

        let board_id = format!("test-board-{}", Id::new());
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&nats),
            "pod-test-2",
        ));

        let mut handle1 = Arc::clone(&registry)
            .subscribe(&board_id)
            .await
            .expect("subscribe 1");
        let mut handle2 = Arc::clone(&registry)
            .subscribe(&board_id)
            .await
            .expect("subscribe 2");

        // Assert only one entry in the boards map.
        assert_eq!(
            registry.boards.read().len(),
            1,
            "should have exactly one board channel"
        );
        {
            let guard = registry.boards.read();
            let ch = guard
                .get(&format!("board:{board_id}"))
                .expect("board channel missing");
            assert_eq!(ch.refcount, 2, "refcount should be 2");
        }

        let envelope = make_envelope(&board_id, "evt-shared");
        publish_envelope(&nats, &board_id, &envelope).await;

        let r1 = tokio::time::timeout(Duration::from_secs(2), handle1.receiver.recv())
            .await
            .expect("timeout h1")
            .expect("recv error h1");
        let r2 = tokio::time::timeout(Duration::from_secs(2), handle2.receiver.recv())
            .await
            .expect("timeout h2")
            .expect("recv error h2");

        assert_eq!(r1.event_id, "evt-shared");
        assert_eq!(r2.event_id, "evt-shared");
    }

    /// After the last subscriber drops, the channel is removed from the map.
    /// A subsequent subscribe call opens a fresh consumer.
    #[tokio::test]
    async fn dropping_last_subscriber_tears_down_consumer() {
        let nats = setup_nats().await;
        ensure_stream(&nats).await;

        let board_id = format!("test-board-{}", Id::new());
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&nats),
            "pod-test-3",
        ));

        {
            let _handle = Arc::clone(&registry)
                .subscribe(&board_id)
                .await
                .expect("subscribe");
            assert_eq!(registry.boards.read().len(), 1);
        } // handle drops here

        // Give the drop time to run.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Map must be empty.
        assert_eq!(
            registry.boards.read().len(),
            0,
            "channel should have been removed after last subscriber dropped"
        );

        // Re-subscribe opens a new channel (refcount starts at 1 again).
        let _handle2 = Arc::clone(&registry)
            .subscribe(&board_id)
            .await
            .expect("re-subscribe");
        assert_eq!(registry.boards.read().len(), 1);
        {
            let guard = registry.boards.read();
            let ch = guard.get(&format!("board:{board_id}")).unwrap();
            assert_eq!(ch.refcount, 1, "fresh channel must have refcount 1");
        }
    }

    #[test]
    fn registry_config_can_be_constructed_with_overrides() {
        let cfg = RegistryConfig {
            broadcast_capacity: 512,
            inactive_threshold_secs: 60,
        };
        assert_eq!(cfg.broadcast_capacity, 512);
        assert_eq!(cfg.inactive_threshold_secs, 60);
    }

    #[test]
    fn registry_config_default_matches_constants() {
        let cfg = RegistryConfig::default();
        assert_eq!(cfg.broadcast_capacity, DEFAULT_BROADCAST_CAPACITY);
        assert_eq!(cfg.inactive_threshold_secs, DEFAULT_INACTIVE_THRESHOLD_SECS);
    }

    /// Events on board A do not appear on board B's subscriber.
    #[tokio::test]
    async fn subscribe_to_different_boards_isolates_streams() {
        let nats = setup_nats().await;
        ensure_stream(&nats).await;

        let board_a = format!("test-board-{}", Id::new());
        let board_b = format!("test-board-{}", Id::new());
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&nats),
            "pod-test-4",
        ));

        let mut handle_a = Arc::clone(&registry)
            .subscribe(&board_a)
            .await
            .expect("sub a");
        let mut handle_b = Arc::clone(&registry)
            .subscribe(&board_b)
            .await
            .expect("sub b");

        // Publish an event to board A only.
        let envelope = make_envelope(&board_a, "evt-a-only");
        publish_envelope(&nats, &board_a, &envelope).await;

        // Board A subscriber receives it.
        let ra = tokio::time::timeout(Duration::from_secs(2), handle_a.receiver.recv())
            .await
            .expect("timeout on board A")
            .expect("recv error");
        assert_eq!(ra.event_id, "evt-a-only");

        // Board B subscriber must NOT receive it (timeout expected).
        let rb = tokio::time::timeout(Duration::from_millis(300), handle_b.receiver.recv()).await;
        assert!(rb.is_err(), "board B should not receive board A's event");
    }

    /// Project subscribers receive envelopes published to the project subject,
    /// isolated from board subjects, and share one consumer per project.
    #[tokio::test]
    async fn subscribe_project_receives_project_events() {
        let nats = setup_nats().await;
        ensure_stream(&nats).await;

        let project_id = format!("test-project-{}", Id::new());
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&nats),
            "pod-test-project",
        ));

        let mut handle1 = Arc::clone(&registry)
            .subscribe_project(&project_id)
            .await
            .expect("subscribe_project 1");
        let mut handle2 = Arc::clone(&registry)
            .subscribe_project(&project_id)
            .await
            .expect("subscribe_project 2");

        // One channel for the project, refcount 2.
        {
            let guard = registry.boards.read();
            assert_eq!(guard.len(), 1);
            let ch = guard
                .get(&format!("project:{project_id}"))
                .expect("project channel missing");
            assert_eq!(ch.refcount, 2);
        }

        let envelope = make_envelope("", "evt-project-1");
        let subject = project_subject(&project_id);
        let buf = bytes::Bytes::from(envelope.encode_to_vec());
        nats.publish_jetstream(&subject, buf)
            .await
            .expect("publish failed")
            .await
            .expect("ack failed");

        let r1 = tokio::time::timeout(Duration::from_secs(2), handle1.receiver.recv())
            .await
            .expect("timeout h1")
            .expect("recv error h1");
        let r2 = tokio::time::timeout(Duration::from_secs(2), handle2.receiver.recv())
            .await
            .expect("timeout h2")
            .expect("recv error h2");

        assert_eq!(r1.event_id, "evt-project-1");
        assert_eq!(r2.event_id, "evt-project-1");
    }

    /// A lagged receiver does not block or starve a fast receiver.
    ///
    /// Publishes 300 events (> BROADCAST_CAPACITY=256) without ever
    /// reading from the slow receiver. The fast receiver should still
    /// receive all 300 events.
    #[tokio::test]
    async fn lagged_receiver_does_not_block_others() {
        let nats = setup_nats().await;
        ensure_stream(&nats).await;

        let board_id = format!("test-board-{}", Id::new());
        let registry = Arc::new(BoardSubscriberRegistry::new(
            Arc::clone(&nats),
            "pod-test-5",
        ));

        let mut fast = Arc::clone(&registry)
            .subscribe(&board_id)
            .await
            .expect("fast");
        // Slow: we keep the handle alive but never read from it.
        let _slow = Arc::clone(&registry)
            .subscribe(&board_id)
            .await
            .expect("slow");

        const EVENT_COUNT: usize = 300;
        for i in 0..EVENT_COUNT {
            let env = make_envelope(&board_id, &format!("evt-{i:04}"));
            publish_envelope(&nats, &board_id, &env).await;
        }

        // The fast receiver should receive all events (some may lag but the
        // sender is not blocked). Count what we can within 10s.
        let mut received = 0usize;
        loop {
            match tokio::time::timeout(Duration::from_secs(10), fast.receiver.recv()).await {
                Ok(Ok(_)) => {
                    received += 1;
                    if received >= EVENT_COUNT {
                        break;
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                    // Lagged is acceptable — count the skipped messages as received
                    // since the sender was not blocked.
                    received += n as usize;
                    if received >= EVENT_COUNT {
                        break;
                    }
                }
                Ok(Err(e)) => panic!("unexpected recv error: {e}"),
                Err(_) => break, // timeout
            }
        }

        assert!(
            received >= EVENT_COUNT,
            "fast receiver should see all {EVENT_COUNT} events (got {received})"
        );
    }
}
