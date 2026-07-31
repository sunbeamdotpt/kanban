---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-31T15:00:00Z
---

# State — 2026-07-31

## In flight

- **v2026.07.12 RELEASED 2026-07-31** (sienna approved in-session):
  TransferCard, Assignee email + display_name hydration, template is_done,
  authn logging + introspection timeout knob, harness reaper/retries/
  self-bootstrap, fetch_labels global-label fix, CardMoved key fix,
  smart-commits RFC (also PR #3 to tony). See CHANGELOG. Deployment card:
  SBBB-036 (tony, depends_on SBBB-016 identity:read — hard dep).
- Previously unreleased, now shipped in v2026.07.12:
  - `6ab6901f66` feat(cards): KANBAN-034 TransferCard (relocate-in-place).
  - `e3f67c83cf` fix(realtime): CardMoved event payload keys.
  - `093678758f` fix(cards): fetch_labels global-label panic.
  - `295544220b` test(harness): KANBAN-038 self-bootstrap helpers.
  - `e42a3ee77f` test(harness): KANBAN-036 reaper + KANBAN-037 retries.
  - Proto pushed: `buf.build/sunbeamdotpt/kanban:096a3791210a45a89ad2d820c7c028c5`
    (TransferCard + CardTransferred, buf breaking clean).
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

- Full triage pass 2026-07-31 (details in log.md): KANBAN-003 closed as
  implemented (test-covered since v2026.07.3); KANBAN-039 root-caused to a
  CLI header bug → CLI-023 (high), depends_on set; KANBAN-021 marked
  blocked (product decision pending); KANBAN-024 narrowed to
  display_name/avatar after 035 shipped the email tier, then BLOCKED
  upstream: the Kratos identity schema allows only traits.email, so
  SSO-028 (display-name trait) was filed and set as a dependency;
  KANBAN-008/010
  confirmed canonical (no cli/dev equivalents). Priorities reviewed; only
  pre-existing sane values kept.
- In review: KANBAN-031 (blocked on **G2V-001**), KANBAN-033, KANBAN-035.
- Done this session: KANBAN-002 + KANBAN-032 (duplicates of KANBAN-034,
  which stays in todo as the canonical cross-project TransferCard ticket
  with design questions noted), KANBAN-030 (completed_at backfill — executed
  by tony overnight as two one-off psql runs: 41 done-titled columns flagged
  is_done, 67 cards stamped from event_log; full trail on the card).
- Deferred with triage comments (unchanged): KANBAN-001/004/005, KANBAN-013,
  KANBAN-015, KANBAN-019/020, KANBAN-025 (icebox).
- Owned by the CLI repo: transferred to cli/dev 2026-07-31 — KANBAN-008 →
  CLI-024, KANBAN-010 → CLI-025, KANBAN-039 → CLI-023 (all closed here as
  transferred, assigned to sienna).
- **KANBAN-034 design decided (sienna): relocate-in-place**, card keeps all
  attachments/history; bumped to high, unblocked, ready for implementation
  (implications on the card).
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
- Testcontainers leak FIXED on mainline (KANBAN-036/037/038, in review):
  startup reaper (>10 min old stacks, safe for concurrent runs), gateway
  bootstrap retries + readiness settle, self-bootstrapping test helpers.
  Full suite after integration: 336 passed, 3 port/timing flakes passing
  in isolation. If flakes resurface, the manual cleanup loop still works.
- Dependabot: 1 moderate vulnerability on the default branch
  (github.com/sunbeamdotpt/kanban/security/dependabot/20) — still untriaged.
- Remaining stale-doc item: `docs/development/testing.md` +
  `docs/development/architecture.md` claim CI gates that do not exist.
- Global-label permission semantics (manage-on-any-project) remain a stopgap.
