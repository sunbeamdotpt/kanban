---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-24T00:00:00Z
---

# State — 2026-07-24

## In flight

- **v2026.07.6 released** (tag pushed, workflow building): cross-board and
  cross-project card dependencies (KANBAN-014) — target card may live on any
  board/project in the tenant; both cards bump revision and both boards get
  `CardUpdated`. No migrations, no new env vars. Deploy task #104 sent to
  sbbb; once deployed, wire `KANBAN-009 depends_on CLI-004` (the motivating
  case) and verify.
- **v2026.07.5 deployed and verified** by sbbb (#101): realtime spine,
  GithubLinkService, KANBAN-006/012 fixes confirmed in prod.
- **Cross-tenant dependencies** filed as low-priority KANBAN-015 (design
  question, no use case yet).
- **CLI gotcha cards** CLI-001/002 filed on cli/dev + mail #103; KANBAN-009's
  CLI half is CLI-004 on cli/dev (the human is doing the CLI work).
- Note: `.maintainer/charter.md` was modified outside this session (sbbb
  routing: board card instead of agent-mail; human escalation: in-session)
  and rode along in the v2026.07.6 commit unreviewed — confirm with the
  human it was their edit.

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
