<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
# `apps/kanban/.integration/`

Authoritative source for kanban's OpenFGA authorization model.

## Files

- `openfga-model.json` — the Kanban authorization model (object types
  `KanbanProject`, `KanbanBoard`, `KanbanCard`, `KanbanAggregatedBoard` with
  their relations and computed permissions, plus the `user` and `agent`
  subject types). This is the single source of truth: the kanban service
  embeds it at compile time (`include_str!` in
  `src/auth/permission_client.rs`) and registers it with the sso-gateway
  `PermissionService` via `EnsurePermissionNamespace` at boot. The gateway
  resolves each object type to the `kanban` permission namespace's OpenFGA
  store.

## Changing the model

Edit `openfga-model.json` and redeploy kanban. Re-ensuring an identical model
is a no-op on the gateway; a changed model publishes a new model version into
the existing store without touching relation tuples. Evolve relations by
dual-writing (add `relation_v2`, dual-write for a release, switch checks, then
drop `relation_v1`) — never delete a relation in place.
