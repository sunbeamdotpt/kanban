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
2. Look for Keto errors in logs:
   ```sh
   kubectl logs -n kanban <pod>
   ```
3. The readiness probe fails if the `_kanban_health` tuple cannot be read. Verify Keto is reachable and the Kanban namespaces are loaded.
4. If Keto namespaces are broken, see [Keto namespace failure](#keto-namespace-failure).

## Keto namespace failure

Symptom: All Kanban pods are `NotReady`; Keto logs show namespace parse errors.

1. Check Keto logs:
   ```sh
   kubectl logs -n ory deployment/keto
   ```
2. Validate the namespace config locally:
   ```sh
   cd .integration
   npm install
   npm run validate
   ```
3. Fix the namespace file and reapply it to the Keto ConfigMap mount at `/etc/namespaces/`.
4. Restart Keto to pick up the corrected namespaces.

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
2. Identify if the error is correlated with a downstream dependency (Postgres, Keto, NATS, OpenSearch).
3. Scale or restart the failing dependency. If the issue is in code, roll back or deploy a hotfix.

## Unauthorized access report

Symptom: A user can see or mutate something they should not.

1. Check the Keto tuple for the object and subject:
   ```sh
   keto relation-tuple get --namespace KanbanBoard --object <board-id>
   ```
2. Verify the `x-sunbeam-object-id` header was set correctly by the frontend and matched the `CheckedObjectId` used by the handler.
3. If Keto and SQL are out of sync, run the mirror reconciler or wait for the hourly reconciler to converge.
