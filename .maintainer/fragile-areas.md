---
type: KnownIssue
title: Fragile areas
description: Zones with incident history or active migration risk — touch with extra care.
tags: [fragile, auth, realtime, migrations]
timestamp: 2026-07-20T00:00:00Z
---

# Fragile areas

Areas that have bitten before or are mid-transition. Slow down here; prefer
small, verified steps.

- **Auth / identity (mid-migration)** — v2026.07.0 moved authz Keto →
  sso-gateway `PermissionService`; v2026.07.1 moved introspection Hydra →
  sso-gateway and renamed env vars (`HYDRA_*` now *silently ignored* — configs
  that look fine can be dead). Stalwart's mail SSO login already broke once in
  this platform-wide migration (sbbb postmortem COE-2026-004). Any change in
  `src/auth/` needs the full test suite, and env-var changes escalate.
- **Realtime outbox** — mutation → event_log → JetStream → fanout has known
  gaps (no dedup header, hardcoded revision, incomplete hydration — see
  [known-issues.md](known-issues.md)). sbbb postmortem COE-2026-003
  (replicaset explosion) involved this service's rollout behavior. Changes to
  outbox/registry semantics affect every connected client; test against the
  testcontainers stack, not just unit tests.
- **Migrations** — one-way, boot-time, and once rewritten in place
  ([migrations-policy.md](migrations-policy.md)). Hard rule: never edit an
  existing file.
- **Authorization model** — `.integration/openfga-model.json` is registered
  with sso-gateway at boot; a bad model change can lock out every tenant at
  once. Dual-write evolution only, escalate.

## Historical context

- Multitenancy cutover (v2026.07.0): `tenant_id` everywhere, protos rewritten
  to buf STANDARD with per-RPC wrappers, all mutating RPCs require the
  `x-sunbeam-object-id` header (object IDs come from the `CheckedObjectId`
  extension, never the proto body).
