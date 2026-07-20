---
license: AGPL-3.0-or-later
title: Architecture
description: Technical architecture of the Kanban backend.
category: development
order: 1
nav_order: 1
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Architecture

## Request path

```
Frontend (React/TS)
  │ Connect-Web (h2 or SSE) + Bearer OAuth2 token
  ▼
kanban-server (Rust Axum) :8080
  │
  ├─ TraceLayer → propagates OpenTelemetry context
  ├─ Prometheus middleware → records RPC metrics
  ├─ IntrospectionLayer → introspects token with the sso-gateway, inserts AuthContext
  ├─ permission_dispatch → checks MATRIX, calls the permission backend, inserts CheckedObjectId
  └─ Handler → reads checked ID, mutates Postgres, emits event_log
       │
       ├─ Mutates: SQL transaction → event_log INSERT → commit
       ├─ Reads: SQL + permission expand post-filter
       └─ Streams: BoardSubscriberRegistry fanout (local broadcast)
            │
            └─ NATS JetStream ← outbox dispatcher drains event_log
```

## Real-time flow

1. A handler mutates Postgres and inserts an `event_log` row in the same transaction.
2. The `OutboxDispatcher` polls undispatched rows every 250ms and publishes to `kanban.board.{id}.events`.
3. The `BoardSubscriberRegistry` maintains one ephemeral NATS push consumer per board per pod.
4. The consumer pumps messages into a `tokio::sync::broadcast` channel with capacity 256.
5. The `SubscribeBoard` handler receives from the broadcast channel and yields `BoardEventEnvelope` to the client.
6. The client-side `useBoardSubscription` applies events to the TanStack Query cache.

## Service decomposition

| File | Responsibility |
| --- | --- |
| `src/server.rs` | Bootstrap: config, OTel, Postgres, NATS, sso-gateway, Axum router. |
| `src/auth/permission_dispatch.rs` | Static dispatch matrix and middleware. |
| `src/auth/permission_expand.rs` | Expand subjects/objects for list post-filtering. |
| `src/realtime/outbox.rs` | Polls `event_log` and publishes to JetStream. |
| `src/realtime/registry.rs` | Per-pod consumer registry and broadcast fanout. |
| `src/realtime/cutover.rs` | Resume-token deduplication for subscribers. |
| `src/integrations/opensearch.rs` | OpenSearch client and index management. |
| `src/integrations/s3.rs` | Presigned URL client. |
| `src/services/*.rs` | Tonic service handlers. |

## Key invariants

- `MATRIX` must contain exactly one entry per proto RPC. `permission-coverage` enforces this in CI.
- `CheckedObjectId` must be used by handlers, never the request body ID.
- `project_members` SQL table mirrors permission tuples. Drift is logged via `mirror_drift` warnings; the permission backend is the source of truth.
- The event log is append-only; the outbox marks `nats_seq` on successful publish.
