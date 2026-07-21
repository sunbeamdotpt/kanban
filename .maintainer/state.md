---
type: State
title: Current state of kanban
description: What is in flight, what is blocked, what the next session should pick up first.
tags: [state]
timestamp: 2026-07-20T00:00:00Z
---

# State — 2026-07-20

## In flight

- **v2026.07.4 release** — ref-counter prefix healing, S3_PUBLIC_ENDPOINT
  for presigned URLs (sbbb wires the prod value). Full suite green
  (252 lib tests).
- **Gateway flake pattern** — full-suite runs intermittently fail 1–4
  permission/OpenFGA tests with "error sending request" to the testcontainer
  gateway; they always pass in isolation. Compounds with the testcontainers
  leak (see known-issues).
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
