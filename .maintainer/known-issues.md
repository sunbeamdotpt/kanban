---
type: KnownIssue
title: Known issues and doc drift
description: Current discrepancies between docs and reality, TODO clusters, and small debt. Remove entries as fixed.
tags: [known-issue, docs, tech-debt]
timestamp: 2026-07-20T00:00:00Z
---

# Known issues

Active discrepancies and debt. When you fix one, delete the entry here and note
it in [log.md](log.md). Verify against the repo before trusting an entry.

## Stale docs

- `docs/development/testing.md` + `docs/development/architecture.md` claim
  fmt/clippy/test/permission-coverage **CI gates** — no such workflows exist
  (only buf lint + release). See [testing.md](testing.md).

## TODO clusters in `src/` — cleared in v2026.07.5

All clusters below were implemented in v2026.07.5 (see CHANGELOG):

- ~~Graceful shutdown~~ — SIGTERM/SIGINT → axum graceful shutdown + bounded
  final outbox drain (`server.rs`, `outbox.rs` `with_shutdown`).
- ~~Outbox gaps~~ — `Nats-Msg-Id` dedup header, `board_revision` counters
  (migration 0030), full Card hydration + patch Struct, all event arms,
  LISTEN/NOTIFY wake, project-scope routing (`kanban.project.{id}.events`,
  migration 0031).
- ~~Unimplemented features~~ — snapshot replay + `since_seq` resume
  (`boards.rs`), multi-board merge `SubscribeProject` (`projects.rs`), GitHub
  issue-linking RPCs (`services/github.rs`, no longer stubbed).
- ~~JetStream g2v API limit~~ — bootstrap uses `nats.jetstream()` directly and
  is drift-correcting; g2v changes not needed.

Remaining gaps (no writer yet, dispatcher arms exist): `ColumnRenamed`,
`MembershipChanged`, `AggregatedBoardDeleted` (undeliverable — event row
cascade-deletes with the aggregate).

## Test harness

- **testcontainers leak** — the shared stack lives in a `static OnceCell` that
  is never dropped, so every `cargo test` process leaks ~9 containers (plus
  Docker networks). Repeated runs exhaust Docker's address pools
  ("all predefined address pools have been fully subnetted") and slow the
  gateway enough to flake permission tests. Workaround: `docker rm -f` stale
  containers + `docker network prune -f`. Proper fix: explicit teardown at
  process exit.

## Cascades skip event_log

- `RemoveColumn` (and `DeleteBoard`) cascade-delete cards via FK without
  writing `CardDeleted` rows to `event_log`. Consequences: the outbox never
  publishes deletes for those cards, their `KanbanCard` permission tuples
  linger, and their OpenSearch documents stay in the index (visible in search
  until the tuple/doc drift is reconciled). The planned reconciler (Stage 7a)
  is the intended fix; alternatively issue per-card deletes in the handler.

## Small debt

- Untracked coverage artifacts at repo root (`coverage.lcov`,
  `coverage2.lcov`, `build_rs_cov.profraw`) — gitignored clutter, safe to
  delete, harmless to leave.
