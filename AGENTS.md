<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
# AGENTS.md — Sunbeam Kanban

> AI coding agent guide for the Kanban project. Read this first before modifying code. For product and operations documentation, see `docs/`; this file is focused on the conventions agents must follow.

## Project Overview

Sunbeam Kanban is a real-time collaborative board-management backend. It is a Rust service built on Axum + Tonic (Connect-RPC/gRPC), talking to PostgreSQL, NATS JetStream, the sso-gateway (unified auth + permissions, OpenFGA-backed), OpenSearch (search), and S3 (attachments). All RPCs are defined in Protobuf at `proto/sunbeam/kanban/v1/`.

- **Protocol:** Connect-RPC over h2 with SSE fallback.
- **Auth & permissions:** The sso-gateway is the unified auth stack. It issues opaque OAuth2 tokens; `sunbeam-g2v`'s `IntrospectionLayer` validates every request against the gateway's `/oauth2/introspect`, and per-object permission checks go through the gateway's `PermissionService` (OpenFGA, one store per tenant). The tenant is resolved from the introspected token; trusted service-to-service calls carry `x-tenant-id`. The `x-sunbeam-object-id` header gates every mutating RPC.
- **Realtime:** Mutations write to Postgres `event_log` → outbox dispatcher publishes to NATS JetStream `kanban.board.{id}.events` → per-pod `BoardSubscriberRegistry` fans out via `tokio::sync::broadcast`.

---

## Semantic Memory Search (Optional)

If a `sunbeam-memory` MCP server is available in your environment, use it for codebase search instead of `grep` or `rg`.

1. **Initialize the repository first.** Before searching, ensure this codebase is indexed:
   - Call `add_watch_target` with the absolute path to this repository.
   - Wait for indexing to complete, then search.
2. **Prefer semantic search.** Use `search_facts` with natural-language queries about behavior, design decisions, known issues, and prior changes.
3. **Store useful findings.** If you discover something future agents should remember, call `store_fact` with a concise note and a source URN when possible.

`sunbeam-memory` is **optional**. If the server is not available, skip these steps and use `grep` / `rg` / `Read` as usual.

---

## Technology Stack

| Concern | Technology |
|---------|------------|
| HTTP / RPC framework | Axum 0.8 + Tonic 0.14 (Connect-RPC compatible) |
| Async runtime | Tokio |
| Database | PostgreSQL 16+ via `sqlx` 0.8 (dynamic API, **no compile-time macros**) |
| Migrations | `sqlx::migrate!("./migrations")` embedded at compile time, applied on boot |
| Message queue | NATS JetStream (`async-nats` 0.47) |
| Permissions | sso-gateway `PermissionService` (OpenFGA, per-tenant stores) |
| Auth middleware | `sunbeam-g2v` `IntrospectionLayer` + local `permission_dispatch` middleware |
| Search | OpenSearch |
| Object storage | S3 (presigned URLs) |
| Observability | OpenTelemetry OTLP + Prometheus metrics + `tracing` |
| Build | Cargo; `tonic-prost-build` generates Rust proto code at build time |

For the project structure, see `docs/development/architecture.md` and `README.md`.

---

## Build & Run Commands

### Prerequisites

All services run via the Sunbeam compose stack from the workspace root:

```sh
cd /Users/sienna/Development/sunbeam
sunbeam ops compose up kanban
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

# Run all tests
cargo test

# Verify permission dispatch matrix covers all proto RPCs
cargo run --bin permission-coverage
```

**Integration tests** use testcontainers to start Postgres 16, NATS (JetStream), the sso-gateway, MinIO, and OpenSearch automatically, run migrations, bootstrap a test tenant plus service application, provision the Kanban permission namespace, create the MinIO bucket, and export the standard env vars. Run them directly with Cargo:

```sh
cargo test                    # full suite
cargo test services::boards   # run a subset
cargo llvm-cov test           # coverage (requires cargo-llvm-cov)
```

The harness auto-detects common Docker-compatible sockets (socktainer, lima-docker, Docker Desktop, and `/var/run/docker.sock`). If auto-detection fails, set `DOCKER_HOST` explicitly:

```sh
export DOCKER_HOST="unix://${HOME}/.lima/docker/sock/docker.sock"
cargo test
```

Override container images via environment variables when needed:

```sh
KANBAN_TEST_POSTGRES_IMAGE=postgres:16-alpine cargo test
```

**Important:** `cargo check` must **not** require a live `DATABASE_URL`. All SQL is written with the dynamic `sqlx` API (e.g., `sqlx::query(...)`) — never `sqlx::query!` or `query_as!` macros.

### Frontend

See `docs/development/testing.md`.

---

## Testing Strategy

Backend Rust tests are **inline** inside `#[cfg(test)]` modules at the bottom of each source file. There are **no separate `*_tests.rs` files**.

- **Unit tests:** No external dependencies.
- **Integration tests:** Require Postgres and the sso-gateway, and sometimes NATS/OpenSearch/MinIO. Each test uses fresh UUIDs so parallel runs do not collide.
- **No `#[ignore]` attributes:** All tests run by default.

For details and examples, see `docs/development/testing.md`.

---

## Code Style Guidelines

### Rust

- **Formatting:** `cargo fmt` (enforced in CI).
- **Clippy:** `cargo clippy -- -D warnings` (zero warnings policy).
- **SQL style:** Dynamic `sqlx` API only — no compile-time macros. Parameters bound with `.bind()`.
- **Error handling:** Use `anyhow::Result` in bootstrap / async tasks; use `tonic::Status` in gRPC handlers. Log errors with `tracing::error!` before returning `Status::internal(...)`.
- **Doc comments:** Module-level `//!` comments explain stage/purpose. `// ── Section ──` dividers for visual grouping.
- **Constants:** `SCREAMING_SNAKE_CASE` for module-level constants (e.g., `PERMISSION_TYPE_BOARD`).
- **Timestamp conversion:** Use `to_proto_ts` / `from_proto_ts` helpers (chrono ↔ prost_types).

---

## Security Considerations

### Authentication & Authorization

- **Every RPC is gated.** The middleware stack is:
  1. `TraceLayer`
  2. `IntrospectionLayer`
  3. `permission_dispatch`
  4. Handler
- **Object IDs come from headers, never the body.** Handlers must read `CheckedObjectId` from request extensions, not from the protobuf body.
- **Token revocation:** Logout is handled by the sso-gateway. Because every request is introspected, revoked tokens are rejected immediately.

For the full security model, see `docs/development/security.md`.

### Data Integrity

- **Mirror-table write order:** permission backend FIRST, then SQL. If SQL fails after the permission write succeeds, log a `mirror_drift` warning.
- **Idempotency keys:** Mutations store idempotency keys in `idempotency_keys`.
- **Advisory locks:** Card ref allocation uses `pg_advisory_xact_lock(hashtext($project_id))`.

### Deployment Security

- **Readiness probe:** `/healthz/ready` proxies the sso-gateway's `/health/ready`; it returns 200 only when the gateway (and with it the permission backend) is ready. Separately, server boot fails fast if the Kanban permission namespace cannot be provisioned.
- **Rollback safety:** Schema migrations are one-way. Rolling back code without downgrading the database will crash the service.

---

## Development Conventions

### Adding a New RPC

The full recipe is in `docs/development/adding-an-rpc.md`. In short:

1. Define in `proto/sunbeam/kanban/v1/*.proto`.
2. Generate Rust code with `buf generate` (or rely on `build.rs` which runs `tonic-prost-build`).
3. Add `DispatchEntry` to `src/auth/permission_dispatch.rs::MATRIX`.
4. Run `cargo run --bin permission-coverage` (must exit 0).
5. Implement handler in `src/services/{domain}.rs`.
6. Wire in `src/services/mod.rs`.
7. Write tests (authorized, unauthorized, persistence, events).

### Database Migrations

- Migration files live in `migrations/` and are applied by `sqlx::migrate!("./migrations")` at server boot.
- **One-way only:** No down scripts are shipped.
- **Seeds:** `migrations/seeds/dev_fixtures.sql` provides idempotent dev data.

### Proto Codegen

`tonic-prost-build` in `build.rs` compiles protos at Cargo build time.

### Permission Model Evolution

See the zero-downtime recipe in `docs/development/security.md`.

---

## Common Gotchas

### Permission namespace provisioning failures

The Kanban permission namespace and OpenFGA model are provisioned at server boot via `EnsurePermissionNamespace` (idempotent), and readiness proxies the sso-gateway. If the gateway is down or provisioning fails, the server fails to boot and pods are removed from service.

**Fix:**
```sh
sunbeam ops logs sso-gateway -f
sunbeam ops restart sso-gateway
```

---

## Environment Variables

All configuration is centralized in `src/server.rs` via `clap` derive flags. Every flag below can also be passed as a command-line argument (`--kebab-case`), with CLI values taking precedence over environment variables.

### Core

| Variable | Default | Purpose |
|----------|---------|---------|
| `KANBAN_HOST` | `0.0.0.0` | Bind host |
| `KANBAN_PORT` | `8080` | HTTP/gRPC listen port |
| `DATABASE_URL` | *required* | Postgres connection string |
| `KANBAN_DATABASE_MAX_CONNECTIONS` | `20` | Postgres pool size |
| `KANBAN_DATABASE_ACQUIRE_TIMEOUT_SECS` | `10` | Connection acquire timeout |
| `POD_NAME` | random UUID | Pod identity for NATS consumers and event envelopes |

### sso-gateway / OAuth2 introspection

| Variable | Default | Purpose |
|----------|---------|---------|
| `HYDRA_INTROSPECTION_URL` | `http://localhost:4445/oauth2/introspect` | sso-gateway OAuth2 introspection endpoint (historical env name). The gateway base URL — permission API, token endpoint, readiness proxy — is derived from it. |
| `HYDRA_CLIENT_ID` | `''` | OAuth2 client ID for introspection Basic auth and service-to-service permission calls (app must hold `permission:admin`) |
| `HYDRA_CLIENT_SECRET` | `''` | OAuth2 client secret for the above |

### Dependencies

| Variable | Default | Purpose |
|----------|---------|---------|
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `NATS_AUTH_TOKEN` | — | NATS auth callout token |
| `SSO_GATEWAY_URL` | — | Test-only explicit gateway base URL (production derives it from `HYDRA_INTROSPECTION_URL`) |
| `OPENSEARCH_URL` | `http://localhost:9200` | OpenSearch endpoint |
| `KANBAN_OPENSEARCH_INDEX` | `sunbeam-kanban-cards-v1` | OpenSearch card index |

### S3 / attachments

| Variable | Default | Purpose |
|----------|---------|---------|
| `S3_ENDPOINT` | — | S3-compatible endpoint |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_ACCESS_KEY` | — | S3 access key |
| `S3_SECRET_KEY` | — | S3 secret key |
| `S3_BUCKET` | `sunbeam-kanban` | S3 bucket name |
| `KANBAN_UPLOAD_EXPIRES_SECS` | `900` | Presigned PUT URL lifetime |
| `KANBAN_DOWNLOAD_EXPIRES_SECS` | `300` | Presigned GET URL lifetime |

### NATS / JetStream

| Variable | Default | Purpose |
|----------|---------|---------|
| `KANBAN_NATS_LEASE_DURATION_SECS` | `30` | NATS consumer lease duration |
| `KANBAN_NATS_REPLICAS` | `1` | JetStream stream replicas |
| `KANBAN_STREAM_MAX_AGE_SECS` | `86400` | Stream max age |
| `KANBAN_STREAM_MAX_MSGS_PER_SUBJECT` | `10000` | Max messages per subject |
| `KANBAN_STREAM_RETENTION` | `limits` | `limits`, `interest`, or `work_queue` |
| `KANBAN_STREAM_STORAGE` | `file` | `file` or `memory` |

### Realtime

| Variable | Default | Purpose |
|----------|---------|---------|
| `KANBAN_OUTBOX_POLL_INTERVAL_MS` | `250` | Outbox poll interval |
| `KANBAN_OUTBOX_BATCH_SIZE` | `256` | Outbox drain batch size |
| `KANBAN_REGISTRY_BROADCAST_CAPACITY` | `256` | Per-board broadcast capacity |
| `KANBAN_REGISTRY_INACTIVE_THRESHOLD_SECS` | `30` | Ephemeral consumer GC threshold |
| `KANBAN_HEARTBEAT_INTERVAL_MS` | `15000` | Live stream heartbeat interval |
| `KANBAN_PERMISSION_RECHECK_INTERVAL_MS` | `30000` | Live stream permission recheck interval |
| `KANBAN_CUTOVER_SEEN_CAPACITY` | `1024` | Replay/live dedup capacity |

### Observability

| Variable | Default | Purpose |
|----------|---------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OpenTelemetry OTLP endpoint |
| `RUST_LOG` | `info` | `tracing-subscriber` log filter |
| `KANBAN_RPC_DURATION_BUCKETS_SECS` | `0.005,0.01,...` | Prometheus RPC duration buckets |

For Kubernetes-specific guidance, see `docs/operations/configuration.md`.

---

## Pointers

- **Product & operations docs:** `docs/`
- **Workspace rules:** `/Users/sienna/Development/sunbeam-split/CLAUDE.md`
- **g2v:** `https://src.sunbeam.pt/sunbeam/sunbeam-g2v`
