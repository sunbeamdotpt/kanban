---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-31T15:00:00Z
---

# State — 2026-07-31

## In flight

- **Unreleased on mainline** (3 commits, awaiting a release decision from
  sienna — releases escalate per charter):
  - `4394353ab9` fix(auth): KANBAN-031 kanban-side — session client WARN-logs
    authn rejections with failure class + latency; new
    `KANBAN_SSO_INTROSPECTION_TIMEOUT_SECS` (default 10).
  - `046ad2c590` feat(templates): KANBAN-033 — `TemplateColumn.is_done`
    (additive proto tag 4) + migration 0035 flagging Done in the four seeded
    global templates.
  - `a21571d0b2` feat(cards): KANBAN-035 — `Assignee.email` (additive proto
    tag 4) hydrated read-time from the directory (5-min TTL cache);
    Board/Project/AggregatedBoard services and the outbox dispatcher now
    hold the identity client.
  - Proto pushed: `buf.build/sunbeamdotpt/kanban:7933c4d8da8043888567c2127d23dc7b`
    (buf lint + breaking clean, additive only). sdk SDK-012 unblocked
    proto-side.
- **v2026.07.11 released and image LIVE** (8189150ade; run 30590298809;
  GHCR multi-arch): Dockerfile buf fix. **v2026.07.10's image build FAILED —
  do not deploy.** SBBB-017 (prod deploy, tony) depends_on SBBB-016
  (identity:read — hard dependency, assigns 403 without it).

## Board state (kanban/dev)

- In review this session: KANBAN-031 (kanban-side shipped; blocked on
  **G2V-001** for the middleware-layer detail/logging — filed on g2v/dev,
  depends_on link set), KANBAN-033, KANBAN-035.
- Done this session: KANBAN-002 + KANBAN-032 (duplicates of KANBAN-034,
  which stays in todo as the canonical cross-project TransferCard ticket
  with design questions noted), KANBAN-030 (completed_at backfill — executed
  by tony overnight as two one-off psql runs: 41 done-titled columns flagged
  is_done, 67 cards stamped from event_log; full trail on the card).
- Deferred with triage comments (unchanged): KANBAN-001/004/005, KANBAN-013,
  KANBAN-015, KANBAN-019/020, KANBAN-021, KANBAN-024, KANBAN-025 (icebox).
- Owned by the CLI repo: KANBAN-008, KANBAN-010, CLI-018.
- KANBAN-003: cross-board same-project moves ARE implemented; proposed close
  pending human confirmation.
- All open cards assigned to sienna.

## Blocked / waiting

- KANBAN-031 in review, depends_on G2V-001 (g2v auth_middleware must emit
  Connect-shaped error detail + WARN-log with method/path).
- KANBAN-021 blocked on product decisions (state→card mapping, per-board
  opt-in, user-override rule) — needs sienna's call in-session.
- `AggregatedBoardDeleted` remains undeliverable via the outbox (documented
  known issue).

## Pick up first

- Cut release v2026.07.12 with sienna's approval: the three mainline
  commits + CHANGELOG [Unreleased]. After release: file sbbb deploy card
  (v2026.07.12) and move 033/035 (and 031 if G2V-001 landed) to done.
- Track SBBB-017 (prod deploy of v2026.07.11, tony) — SBBB-016
  (identity:read) must be applied with it. Prod-verify assign-by-email.
- KANBAN-035 prod note: email hydration needs `identity:read` (same SBBB-016
  scope); without it resolve_email logs a warning and emails arrive empty —
  reads do NOT fail.
- Testcontainers leak is still real: full-suite runs flake with
  PoolTimedOut / permission-expand errors / NATS "connection refused" when
  Docker is exhausted, and subscribe tests need the harness env (run the
  FULL suite, not subscribe-only subsets — child tests read NATS_URL from
  the shared harness bootstrap). Cleanup loop: `docker rm -f $(docker ps -aq
  --filter label=org.testcontainers.managed-by=testcontainers)` +
  `docker network prune -f`, then rerun; the 2026-07-31 full suite passed
  334/336 with the 2 failures passing in isolation.
- Dependabot: 1 moderate vulnerability on the default branch
  (github.com/sunbeamdotpt/kanban/security/dependabot/20) — still untriaged.
- Remaining stale-doc item: `docs/development/testing.md` +
  `docs/development/architecture.md` claim CI gates that do not exist.
- Global-label permission semantics (manage-on-any-project) remain a stopgap.
