---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-27T00:00:00Z
---

# State — 2026-07-27

## In flight

- **Committed on mainline (4 conventional commits, NOT pushed, NOT
  released)**: 56b834c9c1 fix(cards) KANBAN-016, 2c60b87899
  fix(templates) KANBAN-026+022, ec1c33f21f feat(boards) KANBAN-023,
  a0a1b02e3a docs. The four cards are in **review** pending release
  (sienna's call); CHANGELOG staged under [Unreleased]. Verified: cards
  44/44, boards 27/27, templates 10/10, realtime 59/59, clippy/fmt/
  permission-coverage clean.
- **CLI-014 filed on cli/dev**: unblock flag + column is_done CLI support,
  blocked on this server changeset being released + deployed.
- v2026.07.7 released previously (MilestoneService + label CRUD).

## Board state (kanban/dev)

- Fixed this session (pending commit → done): KANBAN-016, KANBAN-022,
  KANBAN-023, KANBAN-026.
- Deferred with triage comments on each card: KANBAN-001..005, KANBAN-013
  (2026-07-24 design-review deferral stands), KANBAN-015 (by design),
  KANBAN-019/020 (scoped, ready; not started), KANBAN-021 (product decisions
  escalated to sienna in-session; depends_on 019+020), KANBAN-024 (fix sketch
  posted; needs IdentityService wiring + gateway scope + design pick),
  KANBAN-025 (icebox).
- Owned by the CLI repo: KANBAN-008, KANBAN-010.
- KANBAN-003: same-project cross-board moves ARE implemented (MoveCard
  re-homes board_id + parent tuples); proposed closing as implemented in a
  card comment — human to confirm.
- **All 18 open cards are assigned to sienna.**

## Blocked / waiting

- KANBAN-021 blocked on product decisions (state→card mapping, per-board
  opt-in, user-override rule) — needs sienna's call in-session.
- `AggregatedBoardDeleted` remains undeliverable via the outbox (documented
  known issue).

## Pick up first

- Commit the KANBAN-016/022/023/026 changeset (human confirmed in-session),
  move the four cards to done, then release per the human's call.
- After release + deploy: prod-verify `card-template get <global-id>` and
  milestone stats after marking Done columns is_done (CLI-014).
- Testcontainers leak is still real: full-suite runs exhaust Docker ports
  ("address already in use" / sso-gateway bootstrap port race). Cleanup loop
  that works: `docker rm -f $(docker ps -aq --filter
  label=org.testcontainers.managed-by=testcontainers)` +
  `docker network prune -f`, then retry — suites pass on a clean Docker.
- Dependabot: 1 moderate vulnerability on the default branch
  (github.com/sunbeamdotpt/kanban/security/dependabot/20) — still untriaged.
- Remaining stale-doc item: `docs/development/testing.md` +
  `docs/development/architecture.md` claim CI gates that do not exist.
- Global-label permission semantics (manage-on-any-project) remain a stopgap.
