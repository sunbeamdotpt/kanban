# AGENTS.md — Sunbeam Kanban

> AI coding agent guide for the Kanban project. Read this first before modifying code.

## Project Overview

Sunbeam Kanban is a real-time collaborative board management service. It consists of:

- **Backend:** Rust service (`kanban`) built on Axum + Tonic (Connect-RPC/gRPC), talking to PostgreSQL, NATS JetStream, Keto (permissions), Valkey (logout watermarks), OpenSearch (search), and S3 (attachments).
- **Frontend:** TypeScript React SPA (`kanban-ui`) built with Vite, TanStack Query, Connect-Web, and `@sunbeam/beam-ui` design system.
- **Protocol:** Connect-RPC over h2 with SSE fallback. All RPCs are defined in Protobuf at `proto/sunbeam/kanban/v1/`.
- **Auth:** Hydra issues JWTs; Keto checks per-object permissions; `x-sunbeam-object-id` header gates every mutating RPC.
- **Realtime:** Mutations write to Postgres `event_log` → outbox dispatcher publishes to NATS JetStream `kanban.board.{id}.events` → per-pod `BoardSubscriberRegistry` fans out via `tokio::sync::broadcast` to all connected clients.

---

## Semantic Memory Search (Optional)

If a `sunbeam-memory` MCP server is available in your environment, use it for codebase search instead of `grep` or `rg`.

1. **Initialize the repository first.** Before searching, ensure this codebase is indexed:
   - Call `add_watch_target` with the absolute path to this repository.
   - Wait for indexing to complete, then search.
2. **Prefer semantic search.** Use `search_facts` with natural-language queries about behavior, design decisions, known issues, and prior changes.
3. **Store useful findings.** If you discover something future agents should remember (a gotcha, invariant, or decision), call `store_fact` with a concise note and a source URN when possible.

`sunbeam-memory` is **optional**. If the server is not available, skip these steps and use `grep` / `rg` / `Read` as usual. Do not fail, stall, or ask the user to install it.

---

## Technology Stack

### Backend (`/src/`)

| Concern | Technology |
|---------|------------|
| HTTP / RPC framework | Axum 0.8 + Tonic 0.14 (Connect-RPC compatible) |
| Async runtime | Tokio |
| Database | PostgreSQL 16+ via `sqlx` 0.8 (dynamic API, **no compile-time macros**) |
| Migrations | `sqlx::migrate!("./migrations")` embedded at compile time |
| Message queue | NATS JetStream (`async-nats` 0.47) |
| Permissions | Ory Keto (gRPC read 4466 / write 4467) |
| Auth middleware | `sunbeam-g2v` JwtLayer + local `keto_dispatch` middleware |
| Cache / watermark | Valkey (Redis protocol) via `redis` crate |
| Search | OpenSearch |
| Object storage | S3 (presigned URLs) |
| Observability | OpenTelemetry OTLP + Prometheus metrics + `tracing` |
| Build | Cargo; `tonic-prost-build` generates Rust proto code at build time |

### Frontend (`/ui/src/`)

| Concern | Technology |
|---------|------------|
| Framework | React 19 + React Router 7 |
| Build tool | Vite 6 |
| Styling | Panda CSS via `@sunbeam/beam-ui` preset (`panda.config.ts`) |
| State (server) | TanStack Query 5 (`useRpcQuery`, `useRpcMutation`, `useRpcStream` from `@sunbeam/g2v`) |
| State (client) | `@legendapp/state` |
| RPC transport | Connect-Web 2 (`@connectrpc/connect-web`) |
| Proto codegen | `buf generate` → TypeScript (`@bufbuild/protobuf` v2) |
| Unit tests | Vitest 2 + `@testing-library/react` + jsdom |
| E2E tests | Playwright 1.48 |

## Project Structure

```
kanban/
├── Cargo.toml                  # Rust package manifest
├── build.rs                    # tonic-prost-build proto compilation
├── sunbeam.yaml                # Sunbeam workspace target definitions
├── Dockerfile                  # Multi-stage Rust build → distroless
├── migrations/                 # sqlx migration scripts (one-way, no down)
│   ├── 0001_projects.sql
│   ├── ...
│   └── seeds/dev_fixtures.sql  # Dev seed data (idempotent)
├── src/
│   ├── main.rs                 # tokio::main → server::run()
│   ├── lib.rs                  # Module declarations + codegen smoke test
│   ├── server.rs               # Bootstrap: Postgres, NATS, Valkey, Keto, Axum router
│   ├── pb.rs                   # Re-exports of tonic-generated proto modules
│   ├── test_support.rs         # Shared test seeders (project/board/column/card chain)
│   ├── bin/keto-coverage.rs    # CI binary: verifies MATRIX covers all proto RPCs
│   ├── auth/
│   │   ├── keto_dispatch.rs    # Static 49-entry dispatch matrix + middleware
│   │   ├── keto_expand.rs      # `expand_objects` helper for list post-filtering
│   │   └── logout_watermark.rs # Valkey-backed token revocation
│   ├── realtime/
│   │   ├── jetstream_bootstrap.rs  # Ensures KANBAN_BOARD_EVENTS stream exists
│   │   ├── outbox.rs               # Polls event_log → publishes to JetStream
│   │   ├── registry.rs             # Per-pod NATS push consumer + broadcast fanout
│   │   └── cutover.rs              # Resume-token deduplication for subscribers
│   ├── integrations/
│   │   ├── opensearch.rs       # OpenSearch client
│   │   └── s3.rs               # S3 presigned URL client
│   └── services/
│       ├── mod.rs              # Tonic service registration
│       ├── auth.rs             # WhoAmI, SignalLogout
│       ├── projects.rs         # Project CRUD + SubscribeProject
│       ├── boards.rs           # Board CRUD + column ops + SubscribeBoard
│       ├── cards.rs            # 17-card RPCs (the largest service)
│       ├── attachments.rs      # Presigned upload/download/confirm/delete
│       ├── forgejo.rs          # Forgejo issue linking
│       └── search.rs           # Global card search (post-filtered via Keto)
├── ui/
│   ├── package.json
│   ├── vite.config.ts          # Dev server on :47823, React dedupe aliases
│   ├── vitest.config.ts        # jsdom, single-react plugin, inline Ark UI
│   ├── playwright.config.ts    # E2E against localhost:47823
│   ├── panda.config.ts         # beam-ui preset
│   ├── buf.gen.yaml            # TypeScript proto codegen config
│   └── src/
│       ├── main.tsx            # React root + FrameworkProvider + BrowserRouter
│       ├── App.tsx             # useRoutes(appRoutes)
│       ├── test-setup.ts       # matchMedia polyfill for jsdom
│       ├── styles.css          # Panda layers + Material Symbols + dialog fixes
│       ├── routes/index.tsx    # Route map: auth → shell → pages
│       ├── auth/               # OIDC, PKCE, transport, require-auth, state-bridge
│       ├── shell/              # KanbanShell, layout, subheader, breadcrumbs
│       ├── chrome/             # Sidebar, topbar, user-menu
│       ├── board/              # useBoard, useBoardSubscription, board-view, pending-mutations
│       ├── list-view/          # List page rendering
│       ├── card-drawer/        # Card detail form
│       ├── project-wizard/     # Create-project flow
│       ├── preview/            # Dev-only design preview harness
│       ├── tweaks/             # Dev tweaks panel
│       └── gen/                # Generated protobuf TypeScript (committed)
└── .integration/
    ├── keto-namespaces.config.ts   # Ory Keto OPL namespace definitions
    ├── validate.ts                 # Type-checks namespace config
    └── README.md
```

## Build & Run Commands

### Prerequisites

All services run via the Sunbeam compose stack:

```sh
cd /Users/sienna/Development/sunbeam
sunbeam ops compose up kanban   # Starts postgres, valkey, nats, keto, kratos, opensearch, seaweedfs
sunbeam ops compose ps          # Verify health
```

### Backend

```sh
# Dev server (listens on :8080)
cargo run --bin kanban

# Build release binary
cargo build --release --bin kanban

# Lint (deny warnings)
cargo clippy -- -D warnings

# Format
cargo fmt

# Run all tests (requires shared Postgres + Valkey + Keto)
cargo test

# Or with nextest (preferred)
cargo nextest run

# Verify Keto dispatch matrix covers all proto RPCs
cargo run --bin keto-coverage
```

**Important:** `cargo check` must **not** require a live `DATABASE_URL`. All SQL is written with the dynamic `sqlx` API (e.g., `sqlx::query(...)`) — never `sqlx::query!` or `query_as!` macros.

### Frontend

```sh
cd ui

# Install deps & generate Panda CSS
npm install
npm run prepare   # panda codegen

# Dev server (listens on http://localhost:47823)
npm run dev

# Type-check
npm run typecheck

# Unit tests (Vitest + jsdom)
npm run test
npm run test:watch

# E2E tests (Playwright; requires backend running)
npm run test:e2e
npm run test:e2e:ui      # Interactive UI mode
npm run test:e2e:headed  # Headed browser

# Proto generation (run from ui/)
npm run proto:gen
```

## Testing Strategy

### Backend Tests

Rust tests are **inline** inside `#[cfg(test)]` modules at the bottom of each source file. There are **no separate `*_tests.rs` files**.

- **Unit tests:** No external dependencies (e.g., `matrix_covers_all_rpcs`, `hash_subject_prefix_is_stable`).
- **Integration tests:** Require shared Postgres, Valkey, and Keto. They run against the real database using the seeders in `test_support.rs`. Each test uses fresh UUIDs so parallel `nextest` tasks do not collide.
- **No `#[ignore]` attributes:** All tests run by default. If a test requires external infra, it fails clearly rather than being skipped.

Key test modules:
- `src/auth/keto_dispatch.rs` — Matrix shape tests + middleware behavior tests (mocked watermark/Keto) + integration tests (live Valkey + Keto).
- `src/services/boards.rs`, `projects.rs`, `cards.rs` — Service handler integration tests (DB round-trips).
- `src/realtime/outbox.rs` — Outbox dispatcher tests (event_log → NATS).
- `src/realtime/registry.rs` — Broadcast fanout tests.

### Frontend Tests

- **Unit tests:** Colocated as `*.test.tsx` (or `*.test.ts`) next to the source file. Run with Vitest + jsdom.
- **Mock transport:** `createMockTransport` from `@sunbeam/g2v/testing` wires in-process Connect-RPC handlers for hooks/components.
- **E2E tests:** Playwright specs in `ui/e2e/`. Fixtures (`e2e/fixtures/seed.ts`) create real project/board/card data via the API before each test. Tests run against an already-deployed stack (no `webServer` in Playwright config).

## Code Style Guidelines

### Rust

- **Formatting:** `cargo fmt` (enforced in CI).
- **Clippy:** `cargo clippy -- -D warnings` (zero warnings policy).
- **SQL style:** Dynamic `sqlx` API only — no compile-time macros. Parameters bound with `.bind()`.
- **Error handling:** Use `anyhow::Result` in bootstrap / async tasks; use `tonic::Status` in gRPC handlers. Log errors with `tracing::error!` before returning `Status::internal(...)`.
- **Doc comments:** Module-level `//!` comments explain stage/purpose. `// ── Section ──` dividers for visual grouping.
- **Constants:** `SCREAMING_SNAKE_CASE` for module-level constants (e.g., `KETO_NS_BOARD`).
- **Timestamp conversion:** Use `to_proto_ts` / `from_proto_ts` helpers (chrono ↔ prost_types).

### TypeScript / React

- **Formatting:** Implied by project conventions (no explicit formatter configured; rely on IDE defaults).
- **Imports:** Grouped: React/external → `@sunbeam/*` internal → relative `./`.
- **File naming:** `kebab-case.ts`, `kebab-case.tsx` for components.
- **Component exports:** Named exports preferred over default exports.
- **Styling:** Panda CSS utilities via `styled-system` imports. Do not write raw CSS except in `styles.css`.
- **React hooks:** Colocated in `use-*.ts` files next to consumers. RPC hooks use `useRpcQuery` / `useRpcMutation` / `useRpcStream` from `@sunbeam/g2v/hooks`.
- **Query keys:** Factory functions like `boardQueryKey(boardId)` to avoid magic strings.

## Security Considerations

### Authentication & Authorization

- **Every RPC is gated.** There are no unguarded handlers. The middleware stack (outer → inner) is:
  1. `TraceLayer` (OTel propagation)
  2. `JwtLayer` (validates Bearer JWT from Hydra, inserts `Extension<AuthContext>`)
  3. `keto_dispatch` (looks up RPC in `MATRIX`, checks Keto permission, inserts `Extension<CheckedObjectId>`)
  4. Handler
- **Object IDs come from headers, never the body.** The frontend sets `x-sunbeam-object-id: <id>` per RPC. Handlers must read `CheckedObjectId` from request extensions, not from the protobuf body. This prevents body-forgery bypasses on server-streaming RPCs.
- **Logout watermark:** `SignalLogout` writes a timestamp to Valkey. `keto_dispatch` compares `iat_ms` against the watermark on every request. Revoked tokens get `401 Unauthorized`.
- **Token revocation window:** After a Keto tuple is deleted, a user may retain access for up to 30 seconds until the next Keto recheck (or 15s on heartbeat). This is an accepted v1 property.

### Data Integrity

- **Mirror-table write order:** Keto FIRST, then SQL. If SQL fails after Keto succeeds, log a `mirror_drift` warning and let the hourly reconciler fix it. Keto is the source of truth; SQL is rewritten to match.
- **Idempotency keys:** Mutations store idempotency keys in `idempotency_keys` table to guard against retries.
- **Advisory locks:** Card ref allocation uses `pg_advisory_xact_lock(hashtext($project_id))` to prevent duplicate refs under concurrency.

### Deployment Security

- **Readiness probe:** `/healthz/ready` returns 200 only after the synthetic `_kanban_health` Keto tuple check passes. If Keto namespaces are broken, the pod fails readiness and is removed from load balancing.
- **Rollback safety:** Schema migrations are one-way. Rolling back code to an older commit without downgrading the database will crash the service. For v1, there are no down scripts — contact on-call DBA if a rollback is needed.

## Development Conventions

### Adding a New RPC

See the detailed recipe in **Architecture → Add a New RPC** below. In short:

1. Define in `proto/sunbeam/kanban/v1/*.proto`.
2. Generate code (`buf generate` for Rust + TypeScript).
3. Add `DispatchEntry` to `src/auth/keto_dispatch.rs::MATRIX`.
4. Run `cargo run --bin keto-coverage` (must exit 0).
5. Implement handler in `src/services/{domain}.rs`.
6. Wire in `src/services/mod.rs`.
7. Write tests (authorized, unauthorized, persistence, events).
8. Wire frontend hook in `ui/src/hooks` or local component file.

### Database Migrations

- Migration files live in `migrations/` and are applied by `sqlx::migrate!("./migrations")` at server boot.
- **One-way only:** No down scripts are shipped. Plan schema changes carefully.
- **Seeds:** `migrations/seeds/dev_fixtures.sql` provides idempotent dev data.

### Proto Codegen

- **Rust:** `tonic-prost-build` in `build.rs` compiles protos at Cargo build time. Protos are referenced from `../../proto/sunbeam/kanban/v1/`.
- **TypeScript:** Run `npm run proto:gen` from `ui/`. Generated code is committed to `ui/src/gen/`.

### Keto Namespace Evolution

To add or rename a permission without downtime, follow the 3-step dual-write pattern documented in **Architecture → Keto Namespace Evolution Recipe** below.

## Architecture

### Request Path

```
Frontend (React/TS)
  │ Connect-Web (h2 or SSE) + Bearer JWT
  ▼
kanban-server (Rust Axum) :8080
  │
  ├─ JwtLayer → validates JWT, inserts AuthContext
  ├─ keto_dispatch → checks MATRIX, calls Keto, inserts CheckedObjectId
  └─ Handler → reads checked ID, mutates Postgres, emits event_log
       │
       ├─ Mutates: SQL transaction → event_log INSERT → commit
       ├─ Reads:  SQL + keto_expand_objects() post-filter
       └─ Streams: BoardSubscriberRegistry fanout (local broadcast)
            │
            └─ NATS JetStream ← outbox dispatcher drains event_log
```

### Realtime Flow

1. Handler mutates Postgres and inserts an `event_log` row in the same transaction.
2. `OutboxDispatcher` polls undispatched rows every 250ms, publishes to `kanban.board.{id}.events`.
3. `BoardSubscriberRegistry` maintains one ephemeral NATS push consumer per board per pod.
4. Consumer pumps messages into a `tokio::sync::broadcast` channel (capacity 256).
5. `SubscribeBoard` handler receives from the broadcast channel and yields `BoardEventEnvelope` to the client.
6. Client-side `useBoardSubscription` applies events to the TanStack Query cache.

### Key Invariants

- `MATRIX` must contain exactly one entry per proto RPC. `keto-coverage` enforces this in CI.
- `CheckedObjectId` must be used by handlers, never the request body ID.
- `project_members` SQL table mirrors Keto tuples. Drift is reconciled hourly (Keto wins).
- Event log is append-only; outbox marks `nats_seq` on success.

---

## Recipes

### Add a New RPC

Suppose you're adding `UpdateCardColor(card_id, color) -> Card`.

**1. Define the RPC in proto**

```proto
// proto/sunbeam/kanban/v1/cards.proto
message UpdateCardColorRequest {
  string card_id = 1;
  string color   = 2;
}

service CardsService {
  rpc UpdateCardColor(UpdateCardColorRequest) returns (Card);
  // ... existing RPCs ...
}
```

**2. Generate code**

```sh
cd /Users/sienna/Development/sunbeam
buf lint proto && buf build proto
buf generate proto --template proto/buf.gen.kanban.yaml  # Rust

cd apps/kanban/ui
npm run proto:gen  # TypeScript
```

**3. Add dispatch entry**

Edit `src/auth/keto_dispatch.rs`. Find `MATRIX` and add:

```rust
DispatchEntry {
    method: "/sunbeam.kanban.v1.CardsService/UpdateCardColor",
    namespace: "KanbanCard",
    relation: "edit",
    object_id_source: ObjectIdSource::Header,
},
```

**4. Verify coverage**

```sh
cargo run --bin keto-coverage
# Must exit with code 0
```

**5. Implement handler**

In `src/services/cards.rs`:

```rust
pub async fn update_card_color(
    State(AppState { db, .. }): State<AppState>,
    Extension(checked_id): Extension<CheckedObjectId>,
    req: UpdateCardColorRequest,
) -> Result<Card, ApiError> {
    let card_id = checked_id.0; // ALWAYS use checked_id, never req.card_id

    let card = sqlx::query_as::<_, Card>(
        "UPDATE cards SET color = $1, revision = revision + 1 WHERE id = $2 RETURNING *"
    )
    .bind(&req.color)
    .bind(&card_id)
    .fetch_one(&db)
    .await?;

    // Publish to NATS outbox
    outbox_dispatcher.publish_event(
        &format!("kanban.board.{}", card.board_id),
        BoardEvent {
            card_updated: Some(CardUpdated { card: Some(card.clone()) }),
            ..Default::default()
        },
    )
    .await?;

    Ok(card)
}
```

Wire in `src/services/mod.rs` via Tonic `Routes::add_service`.

**6. Write tests**

Add `#[cfg(test)]` module at the bottom of `src/services/cards.rs`. Test:
- Authorized user can update.
- Unauthorized user gets 403.
- Color is persisted.
- Event fires to NATS.

Run all tests:
```sh
cargo test
```

**7. Wire frontend consumer**

```typescript
import { useRpcMutation } from "@sunbeam/g2v/hooks";
import { CardsService } from "../gen/sunbeam/kanban/v1/cards_pb";

export const useUpdateCardColor = () => {
  return useRpcMutation(CardsService, "updateCardColor", {
    onSuccess: (card) => {
      queryClient.setQueryData(["card", card.id], card);
    },
  });
};
```

### Keto Namespace Evolution Recipe (Zero-Downtime)

You want to add a new permission (e.g., rename `view` → `view_v2`) without locking anyone out.

**Step 1: Deploy new relation**

Edit `.integration/keto-namespaces.config.ts` to add the new relation alongside the old one. Deploy with `sunbeam apply kanban`.

**Step 2: Dual-write both relations**

In every handler that grants the relation:

```rust
keto.write_relation("KanbanCard", &card_id, "view", &user_subject).await?;
keto.write_relation("KanbanCard", &card_id, "view_v2", &user_subject).await?;
```

Deploy again. Now old code (still running) checks `view`; new code writes both.

**Step 3: Switch all checks to v2**

Update `MATRIX` entries to use `relation: "view_v2"`. Run `cargo run --bin keto-coverage`. Deploy.

**Step 4: Drop old relation (later)**

Remove `view` from the namespace config and stop dual-writing. Deploy.

**Why three steps?** Old pods still running at Step 2 need `view` to exist. By Step 3, all pods check `view_v2` while both relations are still written, so there is no universal-deny window.

### Rollback Recipe

```sh
sunbeam apply kanban --image-tag <previous_sha>
sunbeam ops compose ps kanban
sunbeam ops logs kanban -f
```

**Schema migrations are one-way.** If the DB schema is newer than the rolled-back code, the service will fail to boot. For v1, contact the on-call DBA — there are no down scripts.

### Mirror Reconciliation

The `project_members` table mirrors Keto tuples. Hourly reconciliation (CronJob) queries Keto and rewrites SQL to match. Keto is the source of truth.

**Drift metric:** `kanban_mirror_drift_ratio` — pages on-call if > 0.1% for ≥1 hour.

```sh
sunbeam ops reconcile kanban-membership
```

## Common Gotchas

### `node_modules/react` duplication

When `@sunbeam/g2v` is installed via JSR, it may bring its own `react`. Symptom: two React instances, broken hooks.

**Fix:**
```sh
cd ui
rm -rf node_modules/@sunbeam/g2v/node_modules/react*
npm install
```

Vite and Vitest configs already dedupe `react`, `react-dom`, `@tanstack/react-query`, and `@legendapp/state`.

### `kanban-db-init` must run before server boots

Migrations are embedded in the binary but the database must be initialized. In compose this is managed by `depends_on` + `condition: service_healthy`. In Kubernetes, the `kanban-db-init` Job must complete before the Deployment starts.

### Keto namespace mount failures

Keto reads namespace configs from `/etc/namespaces/` (directory mode). If a deploy breaks the syntax, Keto fails to reload and the readiness probe (`_kanban_health` tuple check) fails, removing pods from service.

**Fix:**
```sh
sunbeam ops logs keto -f
sunbeam ops restart keto
```

### Frontend streaming tests

`useRpcStream` tests require a custom mock transport that yields async generators. See `ui/src/board/board-view.test.tsx` for the pattern using `makeTransport` from `@sunbeam/g2v/testing`.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `KANBAN_PORT` | `8080` | HTTP/gRPC listen port |
| `DATABASE_URL` | *required* | Postgres connection string |
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `VALKEY_URL` | `redis://localhost:6379` | Valkey (logout watermarks) |
| `KETO_READ_ADDR` | `http://localhost:4466` | Keto read endpoint |
| `KETO_WRITE_ADDR` | `http://localhost:4467` | Keto write endpoint |
| `JWT_SECRET` | `change-me` | JWT validation secret |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *unset* | OpenTelemetry OTLP endpoint |
| `S3_ENDPOINT` | *unset* | S3-compatible endpoint |
| `OPENSEARCH_URL` | `http://localhost:9200` | OpenSearch endpoint |
| `POD_NAME` | random UUID | Pod identity for NATS consumer naming |

## Pointers

- **Workspace rules:** `/Users/sienna/Development/sunbeam/CLAUDE.md` (git hosting, deploy gates, testing policy).
- **beam-ui:** `https://src.sunbeam.pt/sunbeam/beam-ui` (JSR `@sunbeam/beam-ui`).
- **g2v:** `https://src.sunbeam.pt/sunbeam/sunbeam-g2v` (Cargo + npm; auth, NATS, OTel primitives).
