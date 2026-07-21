---
type: Overview
title: Service architecture
description: What kanban is, its realtime pipeline, and its authorization model.
tags: [architecture, rust, realtime, authz]
timestamp: 2026-07-20T00:00:00Z
---

# Architecture

Single-binary Rust service (edition 2024): Axum 0.8 + Tonic 0.14, Connect-RPC
over h2 with SSE fallback, sqlx 0.8 against Postgres 16+, async-nats 0.49
(JetStream), `sunbeam-g2v` for auth, OpenSearch for card search, S3 presigned
URLs for attachments, OTel + Prometheus. AGPL-3.0-or-later. Headless since
v1.0.0-rc.0 — the frontend is beam-ui.

## Realtime pipeline

```
mutation → Postgres event_log (same tx) → outbox dispatcher (250ms poll)
        → JetStream kanban.board.{id}.events → per-pod BoardSubscriberRegistry fanout
```

Details in `docs/development/architecture.md`. The outbox and registry have
known gaps — see [known-issues.md](known-issues.md) and
[fragile-areas.md](fragile-areas.md).

## Authorization

Authz lives in sso-gateway's `PermissionService` (OpenFGA per-tenant stores).
The **authoritative model** is `.integration/openfga-model.json` (types
`KanbanProject/Board/Card/AggregatedBoard`), embedded via `include_str!` and
registered with sso-gateway at boot. Evolution is dual-write only, never
in-place deletion (`.integration/README.md`). RPC→permission mapping is the
`permission_dispatch` MATRIX in `src/auth/`; `src/bin/permission-coverage.rs`
checks MATRIX against protos and must exit 0.

## Layout

- `src/services/` — 10 RPC handlers; `src/auth/` — MATRIX + middleware + clients
- `src/realtime/` — outbox, registry, cutover, JetStream bootstrap
- `src/system_migrations/` — post-sqlx migration framework with a ledger table
- `migrations/` — 26 one-way SQL files ([policy](migrations-policy.md))
- `proto/sunbeam/kanban/v1/` — public API; `proto/iam/` — vendored, read-only

## Related

- Verify changes: [testing.md](testing.md)
- Consumers and dependencies: [interfaces.md](interfaces.md)
