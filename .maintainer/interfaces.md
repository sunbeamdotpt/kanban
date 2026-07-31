---
type: Reference
title: Interfaces with other systems
description: Who consumes kanban's API, what kanban depends on, and the cross-repo rules.
tags: [interfaces, cross-repo, proto]
timestamp: 2026-07-20T00:00:00Z
---

# Interfaces

## Consumed by

- **beam-ui** — the frontend, via Connect-Web over h2/SSE with Bearer OAuth2.
  Primary consumer of `proto/sunbeam/kanban/v1/` (published as
  `buf.build/sunbeamdotpt/kanban`). Breaking proto changes need a task to the
  `beam-ui` identity *and* human sign-off — see [charter.md](charter.md).
- **Unauthenticated public board views** — `public_boards.proto`, read-only.

## Depends on

- **PostgreSQL 16+** — single DB, migrations run at boot (`sqlx::migrate!`).
  One-way chain: [migrations-policy.md](migrations-policy.md).
- **sso-gateway** — OAuth2 introspection (`SSO_GATEWAY_*` env vars since
  v2026.07.1; the old `HYDRA_*` names are silently ignored), `PermissionService`,
  readiness proxy. Service app needs `tenant:admin` + `permission:admin` scopes
  (+ `identity:read` for assignee validation/hydration — SBBB-016 in prod).

### sso-gateway identity traits contract

Kanban reads these traits from identity objects (`IdentityService`,
via `src/auth/identity_client.rs`):

| Trait | Used for | Availability |
|-------|----------|--------------|
| `email` | `Assignee.email`, email→identity resolution | required on every schema |
| `given_name` | `Assignee.display_name` (first part) | deployed `employee` schema |
| `family_name` | `Assignee.display_name` (last part) | deployed `employee` schema |

Display name hydrates as `"given_name family_name"` (either part alone if
the other is missing). **No avatar trait exists anywhere** (SSO-028
narrowed to that gap). **Do not trust `deploy/kratos-identity.schema.json`
in the sso-gateway repo** — it documents an email-only base schema and
lags the deployed schemas (prod uses `employee` with name traits; verified
2026-07-31 via `sunbeam user get sienna@sunbeam.pt`). Always check the live
directory (`sunbeam user get <email>` / `sunbeam user list`) before
reasoning about traits. The test harness schema mirrors the deployed
`employee` shape (optional `given_name`/`family_name`) so hydration is
testable.
- **NATS JetStream** — subjects `kanban.board.{id}.events`; auth via callout
  (grants live in the `nats-callout` repo / sbbb docs).
- **OpenSearch** — index `sunbeam-kanban-cards-v1`.
- **S3** — bucket `sunbeam-kanban`, presigned PUT 900s / GET 300s.

## Deployment (owned by sbbb)

Image `ghcr.io/sunbeamdotpt/kanban`, pinned by tag+digest in
`sbbb/overlays/kustomization.yaml`; base manifest `sbbb/base/kanban/`.
Production secrets are OpenBao-backed VSO manifests in sbbb. New env vars,
secrets, ports, or resources are a **task to the `sbbb` identity**, not a
local edit.

## Local dev

Deps via the workspace compose stack: `sunbeam ops compose up kanban` from
the workspace root (all repos checked out as siblings). `sunbeam.yaml`
declares postgres, nats, opensearch, sso-gateway as service deps.
