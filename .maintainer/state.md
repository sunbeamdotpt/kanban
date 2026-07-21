---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-20T00:00:00Z
---

# State — 2026-07-20

## In flight

- **v2026.07.3 release** — card permission tuples (+backfill migration),
  card ref prefix fix (+migration 0027), OpenSearch indexing pipeline
  (outbox hook + backfill migration), AddMember subject validation.
  Full suite green (249 lib tests).
- **Maintainer system bootstrap**: bundle created 2026-07-20; kanban is the
  second repo enrolled in agent-mail (after sbbb).

## Blocked / waiting

- GitHub issue-linking RPCs are stubbed (`src/services/github.rs`) — waiting on
  product direction, not on code.

## Pick up first

- Check for open mail: `agent-mail inbox`.
- Fix the testcontainers leak (see [known-issues.md](known-issues.md)) — it
  flakes permission tests and exhausts Docker networks on repeated runs.
- Remaining stale-doc item: `docs/development/testing.md` +
  `docs/development/architecture.md` claim CI gates that do not exist.
