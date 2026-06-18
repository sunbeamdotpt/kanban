# Sunbeam Kanban

Real-time collaborative board management for Sunbeam Studios. Organize cards across projects, share with teammates, link to GitHub issues, search globally, and see changes sync live across tabs and devices.

## What It Does

- **Boards & Cards:** Create projects with boards; add/drag/edit cards. Optimistic UI, server-truth reconciliation via event stream.
- **Real-time Sync:** All changes stream to connected clients via NATS JetStream. Two users on different pods see each other's edits in ≤30ms.
- **Access Control:** Keto-gated read/write. Share boards with team members; private boards visible only to you. Permission checks gate every RPC.
- **Global Search:** Search card titles, descriptions, and GitHub issue titles across all accessible projects. Results streamed and paginated.
- **GitHub Integration:** Link cards to GitHub issues. Auto-fetch issue title, assignee, labels. Manual refresh for state sync (auto-sync is v2).
- **Dark Mode:** Styled with `@sunbeam/beam-ui` dark-mode tokens from beam-ui package.

## How to Run Locally

### 1. Start compose services

```sh
cd /Users/sienna/Development/sunbeam
sunbeam ops compose up kanban
```

Waits for postgres, valkey, nats, keto, kratos, opensearch, seaweedfs to be healthy.

### 2. Initialize the database

```sh
cd apps/kanban
cargo run --bin kanban-db-init
```

Runs migrations and seeds dev fixtures.

### 3. Start the dev servers

In one terminal:

```sh
cd apps/kanban
cargo run --bin kanban
# Listens on :8080
```

In another:

```sh
cd apps/kanban/ui
npm run dev
# Listens on http://localhost:47823
```

Open `http://localhost:47823` in your browser. Sign in with Kratos (dev user: `demo@sunbeam.local` / `password`).

## How to Deploy

**Do not deploy directly.** The deployment is gated by a human review step. See the deploy gate at [Stage 7e in the plan](/.omc/plans/kanban-plan-v2.md) for the full approval workflow.

When approved:

```sh
sunbeam apply kanban
```

This applies the Kustomize overlay at `infra/sbbb/apps/kanban/` (Rust service Deployment + TypeScript SPA Deployment). After apply:

1. Watch the kanban pods roll out: `sunbeam ops compose ps kanban`.
2. Tail logs for ≥30 seconds: `sunbeam ops logs kanban -f`.
3. Check Alertmanager for new firing alerts.
4. If anything is wrong, roll back: `sunbeam apply kanban --image-tag <previous_sha>`.

For detailed rollback instructions, see [AGENTS.md / Rollback Recipe](AGENTS.md#rollback-recipe).

## Where to Report Issues

Report bugs on **[Gitea](https://src.sunbeam.pt/sunbeam/kanban)** (not GitHub). Include:
- What you were doing when the bug happened.
- Browser console errors (F12 → Console tab).
- Kanban server logs: `sunbeam ops logs kanban -f`.

**For security issues:** Do not open a public issue. Email `security@sunbeam.pt` or ping the on-call security engineer.

## Contributing

Read [AGENTS.md](AGENTS.md) for:
- How to add a new RPC (proto → codegen → dispatch → handler → tests).
- How to evolve Keto namespaces without downtime.
- Architecture overview and request path.

Run tests before pushing:

```sh
cd apps/kanban

# Unit + integration tests
cargo test

# Lint
cargo clippy -- -D warnings
cargo fmt

# Type-check the frontend
cd ui && npm run type-check
```

## Development Stack

- **Backend:** Rust (Axum, SQLx, NATS, Keto).
- **Frontend:** TypeScript (React, Vite, TanStack Query, `@sunbeam/beam-ui`).
- **Transport:** Connect-RPC over h2 (proto-based gRPC-compatible).
- **Realtime:** NATS JetStream (fanout + replay).
- **Auth:** Hydra (JWT) + Keto (permission checks).
- **Database:** PostgreSQL (event sourcing + projections).

See [AGENTS.md](AGENTS.md) for the full architecture sketch and per-RPC dispatch flow.
