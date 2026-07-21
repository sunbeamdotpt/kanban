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
  readiness proxy. Service app needs `tenant:admin` + `permission:admin` scopes.
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
