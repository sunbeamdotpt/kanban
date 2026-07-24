---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-24T00:00:00Z
---

# State — 2026-07-24

## In flight

- **KANBAN-017 (MilestoneService) + KANBAN-018 (label CRUD) implemented** on
  the working tree, unreleased: new `milestones.proto`/`labels.proto` (pushed
  to buf.build/sunbeamdotpt/kanban), `src/services/milestones.rs` +
  `labels.rs` (None-source dispatch, handler-side KanbanProject view/manage,
  templates pattern; matrix 68→77), migration `0032_global_labels.sql`
  (nullable project_id = global + partial unique indexes), UpdateCard
  milestone set/clear/validate, CHANGELOG staged under [Unreleased].
  **Release cut (2026.07.7 bump + tag) is the human's per charter.**
- **v2026.07.6 deployed and verified** by sbbb (#105): cross-board/project
  card dependencies live in prod. Both queued edges wired:
  `KANBAN-009 depends_on CLI-004`, `CLI-005 depends_on KANBAN-001`.
- **Process change (sbbb, #105):** cross-repo tasks to sbbb now go as cards
  on sbbb/dev, NOT agent-mail (their outbound mail watcher is retired).
- **v2026.07.5 deployed and verified** by sbbb (#101): realtime spine,
  GithubLinkService, KANBAN-006/012 fixes confirmed in prod.

## Board state (kanban/dev)

- Done this session: KANBAN-017, KANBAN-018 (pending move to done once tests
  verified).
- Done previously: KANBAN-006, KANBAN-007, KANBAN-011, KANBAN-012, KANBAN-014.
- Open, deferred (design-review gaps per sunbeam): KANBAN-001..005, KANBAN-013.
- Open, owned by the CLI repo: KANBAN-008..010.
- Open, unblocked by this session: CLI-007/CLI-009 (label CRUD shipped),
  cli "3.2" milestone grouping (MilestoneService shipped) — the CLI side can
  proceed once the release is cut and deployed.
- KANBAN-016 (UpdateCard blocked-bool one-way patch) still open, low.
- Filed this session (GitHub auto-sync scope): KANBAN-019 (webhook receiver,
  high), KANBAN-020 (reconciliation worker, medium), KANBAN-021 (auto-status,
  medium; depends_on 019+020, product questions need in-session escalation
  before implementation). Deployment wiring card filed on sbbb/dev
  (KANBAN_GITHUB_WEBHOOK_SECRET + ingress headers).

## Blocked / waiting

- Nothing blocked. `AggregatedBoardDeleted` is undeliverable via the outbox
  (event row cascade-deletes with the aggregate) — documented known issue.

## Pick up first

- Cut release 2026.07.7 (human): bump, CHANGELOG [Unreleased] → versioned,
  tag, push; verify the GHCR image workflow.
- Tell the CLI repo (card on cli/dev) that MilestoneService + LabelService
  are live in the API so CLI-007/CLI-009 and the 3.2 milestone grouping can
  proceed.
- Fix the testcontainers leak (see [known-issues.md](known-issues.md)) — it
  flakes permission tests and exhausts the VM ("too many open files",
  Docker network pools) on repeated full-suite runs. Cleanup: remove leaked
  harness containers + `docker network prune -f`.
- Remaining stale-doc item: `docs/development/testing.md` +
  `docs/development/architecture.md` claim CI gates that do not exist.
- Full-suite flakes: gateway permission tests (load) and, historically,
  `subscribe_project_merges_board_streams_and_project_events` (timeout
  bumped to 30s — watch whether that fully settles it).
- Global-label permission semantics (manage-on-any-project) are a stopgap
  worth revisiting if a tenant-level OpenFGA object ever lands.
