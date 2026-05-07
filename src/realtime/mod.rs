//! Realtime spine — Stage 4.
//!
//! 4a: jetstream_bootstrap (this module) + cutover dedupe primitives.
//! 4b: subscriber registry (NATS push consumer + per-pod fanout).
//! 4c: SubscribeBoard / SubscribeProject server-streaming RPCs.
//! 4d: outbox dispatcher (event_log → JetStream).

pub mod cutover;
pub mod jetstream_bootstrap;
pub mod outbox;
pub mod registry;
