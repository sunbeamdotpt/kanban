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

## TODO clusters in `src/` (~15)

- **Graceful shutdown missing** — `server.rs:600`, `realtime/outbox.rs` (×3).
- **Outbox gaps** — no `Nats-Msg-Id` dedup header, `board_revision` hardcoded
  to 0, event hydration incomplete (`realtime/outbox.rs`).
- **Unimplemented features** — snapshot-replay and multi-board merge
  (`boards.rs`, `projects.rs`); GitHub issue-linking RPCs stubbed
  (`services/github.rs`).
- JetStream stream config limited by g2v API (`realtime/jetstream_bootstrap.rs`).

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
