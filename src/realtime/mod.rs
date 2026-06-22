// SPDX-License-Identifier: AGPL-3.0-or-later
//! Realtime subsystem.
//!
//! Groups the modules that move board events from Postgres into per-board
//! broadcast channels:
//!
//! - `cutover` — replay-to-live cutover state machine and deduplication.
//! - `jetstream_bootstrap` — JetStream stream and subject naming.
//! - `outbox` — transactional outbox dispatcher that drains `event_log`.
//! - `registry` — per-pod ephemeral consumer registry and fan-out.

pub mod cutover;
pub mod jetstream_bootstrap;
pub mod outbox;
pub mod registry;
