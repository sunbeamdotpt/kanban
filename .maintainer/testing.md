---
type: Runbook
title: Verifying a change
description: Build, lint, and test commands — and which documented CI gates do not actually exist.
tags: [testing, ci, runbook]
timestamp: 2026-07-20T00:00:00Z
---

# Verifying a change

```bash
cargo build --release --bin kanban
cargo clippy --all-targets -- -D warnings   # zero-warning policy
cargo fmt
cargo run --bin permission-coverage         # must exit 0 after any RPC change
cargo test                                  # the full suite
```

## What `cargo test` actually does

The harness (`src/test_support.rs`, `sdk::testing` from git sdk v3.0.0) boots
**testcontainers**: Postgres 16, NATS, sso-gateway, MinIO, OpenSearch. It
applies migrations and bootstraps a tenant + service app. Consequences:

- It is slow and needs a working Docker socket (socktainer/lima/Docker Desktop
  auto-detected, or `DOCKER_HOST`).
- Do not treat a failure to find a Docker socket as a code failure — say so
  and run what you can (`cargo clippy`, targeted tests that don't need
  containers, `permission-coverage`).

## CI reality — trust this, not the docs

Only two workflows exist: `.github/workflows/buf.yml` (buf lint on proto
paths) and `.github/workflows/release.yml` (tag `v*` → multi-arch GHCR image).
**There is no fmt/clippy/test/permission-coverage CI**, despite
`docs/development/testing.md` and `docs/development/architecture.md` claiming
those gates exist. Until that changes, local verification is the *only* gate —
never skip clippy/test on the argument that CI will catch it.

## Conventions that bite

- Tests are inline `#[cfg(test)]` modules at the bottom of each source file —
  no separate `*_tests.rs`, no `#[ignore]`. Fresh ULIDs per test.
- Dynamic sqlx only, `.bind()` params; `cargo check` must pass without
  `DATABASE_URL`.
- Errors: `anyhow` in bootstrap/tasks, `tonic::Status` in handlers (log first).
- Mirror-table writes: permission backend first, then SQL; drift logged as
  `mirror_drift`.
- Adding an RPC: 7-step recipe in `docs/development/adding-an-rpc.md`.
