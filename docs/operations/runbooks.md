---
license: AGPL-3.0-or-later
title: Runbooks
description: Operational runbooks for the Kanban backend.
category: operations
order: 4
nav_order: 4
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Runbooks

## Pod not ready

Symptom: `kubectl get pods -n kanban` shows `0/1 Ready`.

1. Check the readiness probe:
   ```sh
   kubectl describe pod -n kanban <pod>
   ```
2. Look for sso-gateway errors in logs:
   ```sh
   kubectl logs -n kanban <pod>
   ```
3. The readiness probe proxies the sso-gateway's `/health/ready`. Verify the gateway is reachable and healthy, and that kanban provisioned its permission namespace at boot (look for `Kanban permission namespace ensured` in the boot logs).
4. If the gateway is unhealthy, see [sso-gateway failure](#sso-gateway-failure).

## sso-gateway failure

Symptom: All Kanban pods are `NotReady`; sso-gateway logs show OpenFGA or configuration errors.

1. Check gateway logs:
   ```sh
   kubectl logs -n <sso-namespace> deployment/sso-gateway
   ```
2. Validate the Kanban authorization model locally:
   ```sh
   jq . .integration/openfga-model.json
   ```
3. Fix the model file if it is malformed and redeploy kanban; boot re-registers the model via `EnsurePermissionNamespace` (a changed model publishes a new version, tuples are untouched).
4. Restart the sso-gateway if its OpenFGA backend is wedged.

## Migration failure

Symptom: Pods crash with `sqlx migrations failed`.

1. Check the exact error in pod logs.
2. If the database is newer than the code, the binary is rolled back relative to the schema. **Do not roll back migrations.** Deploy a forward fix as a new migration.
3. If a migration is partially applied, consult the on-call DBA before manually editing `_sqlx_migrations`.

## NATS consumer lag

Symptom: Real-time updates are delayed; `kanban_jet_stream_lag_seconds` is high.

1. Check NATS JetStream status:
   ```sh
   nats stream info KANBAN_BOARD_EVENTS
   nats consumer info KANBAN_BOARD_EVENTS <consumer>
   ```
2. Scale the Kanban Deployment horizontally if CPU is saturated.
3. If a single board has many subscribers, check for lagged receivers blocking the broadcast channel. The registry drops lagged receivers to protect others.

## Rollback

Symptom: A deploy introduced a regression.

1. Roll back the Deployment:
   ```sh
   kubectl rollout undo deployment/kanban -n kanban
   ```
2. Confirm pods are healthy:
   ```sh
   kubectl rollout status deployment/kanban -n kanban
   ```
3. Remember: schema migrations are one-way. If the regression was caused by a migration, a code rollback alone will not help. Plan a forward migration or contact the on-call DBA.

## High error rate

Symptom: Alert fires on `kanban_rpc_total{status=~"5.."}`.

1. Check recent logs for the error target and span.
2. Identify if the error is correlated with a downstream dependency (Postgres, sso-gateway, NATS, OpenSearch).
3. Scale or restart the failing dependency. If the issue is in code, roll back or deploy a hotfix.

## Unauthorized access report

Symptom: A user can see or mutate something they should not.

1. Check the relation tuple for the object and subject via the sso-gateway `PermissionService` `ListRelationTuples` API (namespace `KanbanBoard`, object `<board-id>`), e.g. with `grpcurl` or the gateway admin console.
2. Verify the `x-sunbeam-object-id` header was set correctly by the frontend and matched the `CheckedObjectId` used by the handler.
3. If the permission backend and SQL are out of sync, the `mirror_drift` warnings in the logs point at the affected objects. SQL converges to the permission backend, which is the source of truth.
