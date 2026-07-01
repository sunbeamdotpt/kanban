---
license: AGPL-3.0-or-later
title: Testing
description: How to run and write tests for the Kanban backend.
category: development
order: 2
nav_order: 2
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Testing

## Backend tests

Rust tests are inline inside `#[cfg(test)]` modules at the bottom of each source file. There are no separate `*_tests.rs` files.

### Run the suite

Integration tests use `testcontainers` to start Postgres 16, NATS (JetStream), Ory Keto, MinIO, and OpenSearch automatically, run migrations, and export the standard env vars. Run them directly with Cargo:

```sh
cargo test                    # full suite
cargo test services::boards   # filter by module path
cargo llvm-cov test           # coverage (requires cargo-llvm-cov)
```

The harness auto-detects common Docker-compatible sockets (socktainer, lima-docker, Docker Desktop, and `/var/run/docker.sock`). If auto-detection fails, set `DOCKER_HOST` manually:

```sh
export DOCKER_HOST="unix://${HOME}/.lima/docker/sock/docker.sock"
cargo test
```

Override container images via environment variables when needed:

```sh
KANBAN_TEST_POSTGRES_IMAGE=postgres:16-alpine cargo test
```

### Reusing an existing stack

If you already have services running, set the standard env vars instead:

```sh
export DATABASE_URL='postgres://sunbeam:sunbeam@localhost:5432/kanban'
export NATS_URL='nats://localhost:4222'
export KETO_READ_ADDR='http://localhost:4466'
export KETO_WRITE_ADDR='http://localhost:4467'
export OPENSEARCH_URL='http://localhost:9200'
export S3_ENDPOINT='http://localhost:9000'
export S3_ACCESS_KEY='minioadmin'
export S3_SECRET_KEY='minioadmin'
export S3_BUCKET='sunbeam-kanban'
cargo test
```

### Test categories

- **Unit tests** — no external dependencies (e.g., `matrix_covers_all_rpcs`).
- **Integration tests** — require Postgres, Keto, and sometimes NATS/OpenSearch/MinIO. Each test uses fresh ULIDs so parallel runs do not collide.
- **No `#[ignore]` attributes** — all tests run by default.

### Writing a service integration test

Use the shared harness:

```rust
#[tokio::test]
async fn create_then_get_returns_same_board() {
    let infra = crate::test_support::containers::setup().await;
    let svc = BoardServiceImpl { pool: infra.pool.clone() };
    // ... exercise handler and assert
}
```

The harness returns fresh `TestInfra` per call but reuses the same set of containers for the process.

## CI gates

- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test`
- `cargo run --bin keto-coverage`
