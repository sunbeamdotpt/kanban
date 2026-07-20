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
2. `IntrospectionLayer` — introspects the Bearer token against the sso-gateway's `/oauth2/introspect` and inserts `Extension<AuthContext>`.
3. `permission_dispatch` — looks the RPC up in `MATRIX`, calls the gateway's `PermissionService`, and inserts `Extension<CheckedObjectId>`.
4. Handler.

## Object IDs come from headers

The frontend sets `x-sunbeam-object-id: <id>` per RPC. Handlers must read `CheckedObjectId` from request extensions, not from the protobuf body. This prevents body-forgery bypasses on server-streaming RPCs where the body ID could otherwise be manipulated.

## Multitenancy

Every tenant's permissions live in a dedicated OpenFGA store managed by the sso-gateway. End-user tokens resolve their tenant from token introspection; kanban's service client pins the tenant explicitly via the `x-tenant-id` header on every permission call. Handlers must never accept a tenant id from the request body.

## Token revocation

Logout is handled by the sso-gateway. Because `IntrospectionLayer` introspects every Bearer token on every request, a revoked or expired token is rejected immediately with `401 Unauthorized`.

## Data integrity

- **Mirror-table write order:** permission backend FIRST, then SQL. If SQL fails after the permission write succeeds, the handler logs a `mirror_drift` warning; the permission backend is the source of truth and SQL is rewritten to match it.
- **Idempotency keys:** Mutations store idempotency keys in the `idempotency_keys` table to guard against retries.
- **Advisory locks:** Card ref allocation uses `pg_advisory_xact_lock(hashtext($project_id))` to prevent duplicate refs under concurrency.

## Permission model evolution

The Kanban OpenFGA authorization model lives in `.integration/openfga-model.json` and is embedded into the binary. Server boot registers it via `EnsurePermissionNamespace`: an identical model is a no-op, a changed model publishes a new model version into the existing per-tenant stores without touching relation tuples. Namespaces for new tenants are provisioned lazily on their first permission call.

To add or rename a permission without downtime:

1. Add the new relation alongside the old one in `.integration/openfga-model.json` and deploy (boot publishes the new model version).
2. Update handlers to dual-write both relations.
3. Switch `MATRIX` entries to the new relation and deploy.
4. Later, remove the old relation from the model and stop dual-writing.

## Deployment security

- The readiness probe (`/healthz/ready`) proxies the sso-gateway's `/health/ready`. If the gateway or its OpenFGA backend is unhealthy, pods fail readiness and are removed from load balancing. Separately, server boot fails fast when the Kanban permission namespace cannot be provisioned.
- Schema migrations are one-way. Rolling back code without downgrading the database will crash the service. Contact the on-call DBA if a rollback is needed.
