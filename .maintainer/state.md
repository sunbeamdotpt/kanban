---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-24T00:00:00Z
---

# State — 2026-07-24

## In flight

- **v2026.07.5 released** (tag pushed, release workflow building): realtime
  spine completed (snapshot replay, `since_seq` resume, multi-board
  `SubscribeProject`, full outbox event coverage, `Nats-Msg-Id` dedup,
  `board_revision` counters, LISTEN/NOTIFY wake, drift-correcting JetStream
  bootstrap, graceful shutdown), GithubLinkService fully implemented,
  KANBAN-006 + KANBAN-012 fixed. Migrations 0028–0031. Suite green,
  llvm-cov 90.5% lines / 92.8% functions.
- **Deploy tasks sent to sbbb**: optional `KANBAN_GITHUB_TOKEN` /
  `KANBAN_GITHUB_API_BASE_URL`; post-deploy verification of KANBAN-006/012.
- **Task sent to nats-callout**: new `kanban.project.>` subject family needs
  NATS subject grants before `SubscribeProject` works in prod.

## Board state (kanban/dev)

- Done this session: KANBAN-006, KANBAN-007, KANBAN-012 (moved to done).
- Open, deferred (design-review gaps per sunbeam): KANBAN-001..005, KANBAN-013.
- Open, owned by the CLI repo: KANBAN-008..011.

## Blocked / waiting

- Nothing blocked. `AggregatedBoardDeleted` is undeliverable via the outbox
  (event row cascade-deletes with the aggregate) — documented known issue.

## Pick up first

- Check for open cards on the `kanban` boards (`sunbeam kanban board list
  kanban`, then `sunbeam kanban card list <board-id>`).
- Verify the v2026.07.5 release workflow published the GHCR image.
- Fix the testcontainers leak (see [known-issues.md](known-issues.md)) — it
  flakes permission tests and exhausts the VM ("too many open files",
  Docker network pools) on repeated full-suite runs. Cleanup: remove leaked
  harness containers + `docker network prune -f`.
- Remaining stale-doc item: `docs/development/testing.md` +
  `docs/development/architecture.md` claim CI gates that do not exist.
- Full-suite flakes: gateway permission tests (load) and, historically,
  `subscribe_project_merges_board_streams_and_project_events` (timeout
  bumped to 30s — watch whether that fully settles it).
