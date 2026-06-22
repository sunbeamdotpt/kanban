<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
# `apps/kanban/.integration/`

Authoritative source for kanban's Ory Keto OPL (Ory Permission Language)
namespaces and the small validator that gates them.

## Files

- `keto-namespaces.config.ts` — namespace + permission definitions for
  `KanbanProject`, `KanbanBoard`, `KanbanCard`, plus the synthetic
  `_kanban_health` namespace used by the kanban service's readiness probe.
- `validate.ts` — type-checks the config against `@ory/keto-namespace-types`
  (the same toolchain Keto's directory-watcher applies before reload) and
  renders the relation graph in plain English.
- `package.json` — minimal deps for the validator (no workspace impact).

## Validate locally

```sh
cd apps/kanban/.integration
npm install   # first time only; pulls tsx + typescript + the local types
npm run validate
```

Exit 0 means the config compiles cleanly and all four required namespaces
parse. CI runs this same command as a PR gate (Stage 1a / Pre-mortem 1
mitigation): a syntax-broken namespace file would otherwise crash Keto on
reload and lock every user out.

## How this gets into Keto

Stage 1d (forward-reference) wires this file into the workspace Keto
deployment via the directory-mount pattern documented in
`project_keto_namespaces_directory_mode.md`: every project owns a single
`*.config.ts` under its `.integration/` and Keto v26 reads them all from
`/etc/namespaces/` (directory mode), merging without conflict. This file
will land at `/etc/namespaces/kanban.config.ts` in the Keto pod once the
kustomization in `infra/sbbb/apps/kanban/` references it as a configMap
volume mount.
