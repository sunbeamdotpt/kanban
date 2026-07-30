---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-30T00:00:00Z
---

# State — 2026-07-30

## In flight

- **v2026.07.9 released** (84ab8a43af tagged + pushed): KANBAN-027 regression
  test + deployment of the already-landed handler fixes for `completed_at` on
  done-column moves and timestamps in `ListCardsByBoard`. Release workflow
  triggered by tag `v2026.07.9`; **SBBB-013** filed on sbbb/dev for production
  deployment.
- **v2026.07.8 released** (b316af1bc4): KANBAN-016 (blocked clear via mask),
  KANBAN-023 (Column.is_done completion lanes, migration 0034), KANBAN-026 +
  KANBAN-022 (template ID canonicalization, migrations 0033). All four cards
  moved to done. CLI-014 (unblock flag + column is_done CLI support) is
  unblocked once sbbb deploys v2026.07.8/v2026.07.9.
- **KANBAN-027 done**: symptoms were already fixed on mainline by KANBAN-023;
  regression test added in `src/services/cards.rs` and card moved to done.
- v2026.07.7 released previously (MilestoneService + label CRUD).

## Board state (kanban/dev)

- Done this session: KANBAN-016, KANBAN-022, KANBAN-023, KANBAN-026,
  KANBAN-027.
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
- **All open cards are assigned to sienna.**

## Blocked / waiting

- KANBAN-021 blocked on product decisions (state→card mapping, per-board
  opt-in, user-override rule) — needs sienna's call in-session.
- `AggregatedBoardDeleted` remains undeliverable via the outbox (documented
  known issue).

## Pick up first

- Track SBBB-013 deployment of kanban v2026.07.9; prod-verify KANBAN-027 with
  `sunbeam kanban card get <done-card-ref> -o json` (completed_at populated)
  and `sunbeam kanban card list <board-id> -o json` (timestamps present).
- Verify release workflow for v2026.07.9 published the GHCR image, then
  prod-verify CLI-014 items after sbbb deploys: `card-template get <global-id>`,
  unblock via mask, and milestone stats once Done columns are marked is_done.
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
