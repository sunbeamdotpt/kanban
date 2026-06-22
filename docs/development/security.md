---
license: AGPL-3.0-or-later
title: Security
description: Security model and operational security notes for Kanban.
category: development
order: 4
nav_order: 4
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Security

## Authentication and authorization

Every RPC is gated. The middleware stack is:

1. `TraceLayer` — propagates OpenTelemetry context.
2. `JwtLayer` — validates the Bearer JWT from Hydra and inserts `Extension<AuthContext>`.
3. `keto_dispatch` — looks the RPC up in `MATRIX`, calls Keto, and inserts `Extension<CheckedObjectId>`.
4. Handler.

## Object IDs come from headers

The frontend sets `x-sunbeam-object-id: <id>` per RPC. Handlers must read `CheckedObjectId` from request extensions, not from the protobuf body. This prevents body-forgery bypasses on server-streaming RPCs where the body ID could otherwise be manipulated.

## Logout watermark

`SignalLogout` writes a timestamp to Valkey. `keto_dispatch` compares the token's `iat_ms` against the watermark on every request. Revoked tokens receive `401 Unauthorized`.

## Data integrity

- **Mirror-table write order:** Keto FIRST, then SQL. If SQL fails after Keto succeeds, the handler logs a `mirror_drift` warning and the hourly reconciler rewrites SQL to match Keto. Keto is the source of truth.
- **Idempotency keys:** Mutations store idempotency keys in the `idempotency_keys` table to guard against retries.
- **Advisory locks:** Card ref allocation uses `pg_advisory_xact_lock(hashtext($project_id))` to prevent duplicate refs under concurrency.

## Keto namespace evolution

To add or rename a permission without downtime:

1. Deploy the new relation alongside the old one in `.integration/keto-namespaces.config.ts`.
2. Update handlers to dual-write both relations.
3. Switch `MATRIX` entries to the new relation and deploy.
4. Later, remove the old relation and stop dual-writing.

## Deployment security

- The readiness probe (`/healthz/ready`) returns 200 only after the synthetic `_kanban_health` Keto tuple check passes. If Keto namespaces are broken, pods fail readiness and are removed from load balancing.
- Schema migrations are one-way. Rolling back code without downgrading the database will crash the service. Contact the on-call DBA if a rollback is needed.
