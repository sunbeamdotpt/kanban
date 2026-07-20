---
license: AGPL-3.0-or-later
title: Sunbeam Kanban
description: Real-time collaborative board management for Sunbeam Studios.
category: product
order: 0
nav_order: 0
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Sunbeam Kanban

Real-time collaborative board management for Sunbeam Studios. Organize cards across projects, share with teammates, link to GitHub issues, search globally, and see changes sync live across tabs and devices.

## Quick links

- **[Product overview](docs/product/index.md)** — what Kanban is and who it is for.
- **[Features](docs/product/features.md)** — boards, cards, templates, search, GitHub linking, attachments.
- **[Templates](docs/product/templates.md)** — board and card templates.
- **[Deployment](docs/operations/deployment.md)** — Kubernetes deployment, image build, rollout/rollback.
- **[Configuration](docs/operations/configuration.md)** — environment variables and Secrets.
- **[Observability](docs/operations/observability.md)** — metrics, traces, logs, alerts.
- **[Runbooks](docs/operations/runbooks.md)** — operational playbooks.
- **[Architecture](docs/development/architecture.md)** — request path, real-time flow, service map.
- **[Testing](docs/development/testing.md)** — how to run backend and frontend tests.
- **[Adding an RPC](docs/development/adding-an-rpc.md)** — recipe for extending the API.
- **[Security](docs/development/security.md)** — authz/authn, header checks, namespace evolution.
- **[AGENTS.md](AGENTS.md)** — AI agent guide for this repo.

## Development stack

- **Backend:** Rust (Axum, SQLx, NATS, Sunbeam Studios' SSO Gateway).
- **Transport:** Connect-RPC over h2 with SSE fallback.
- **Realtime:** NATS JetStream fanout and replay.
- **Auth:** Sunbeam Studios' SSO Gateway (unified OAuth2 + OpenFGA permissions).
- **Database:** PostgreSQL.
- **Search:** OpenSearch.
- **Object storage:** S3-compatible store for attachments.

## Run locally

Start the shared services from the workspace root:

```sh
cd /Users/sienna/Development/sunbeam
sunbeam ops compose up kanban
```

Then run the backend:

```sh
cargo run --bin kanban      # :8080
```

For test commands and local setup, see [Testing](docs/development/testing.md).

## Deploy

The service is stateless and runs as a Kubernetes Deployment. The container image supports `linux/amd64` and `linux/arm64`. See [Deployment](docs/operations/deployment.md) for the full guide.

## Where to report issues

Report bugs on **[Gitea](https://src.sunbeam.pt/sunbeam/kanban)**. For security issues, email `security@sunbeam.pt` instead of opening a public issue.
