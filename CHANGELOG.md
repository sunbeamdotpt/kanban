<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
# Changelog

All notable changes to the Kanban backend will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-rc4] - 2026-06-26

### Changed

- Replaced local JWT validation with Hydra opaque-token introspection using `sunbeam-g2v` 0.3.3's `IntrospectionLayer`. Every request is now introspected against Hydra's `/oauth2/introspect` endpoint.
- Removed the `AuthService`, `WhoAmI`, and `SignalLogout` RPCs and the `auth.proto` definition. Logout revocation is now handled by Hydra.
- Removed the Valkey-backed logout watermark subsystem, the `redis` dependency, and all related configuration and metrics.

## [1.0.0-rc3] - 2026-06-25

### Changed

- Upgraded `sunbeam-g2v` from 0.3.1 to 0.3.2.

## [1.0.0-rc2] - 2026-06-25

### Added

- Centralized all service configuration in a `clap` derive parser in `src/server.rs`, with every setting available as both an environment variable and a command-line flag.
- NATS auth callout support via the `NATS_AUTH_TOKEN` env var / `--nats-auth-token` flag, using `sunbeam-g2v` 0.3.1.
- New tunables exposed through the central config: Postgres pool size, JetStream retention/storage/replicas, outbox poll/batch, board subscriber registry capacity/inactive threshold, live-stream heartbeat and Keto recheck intervals, cutover dedupe capacity, logout watermark TTLs, and attachment presigned-URL lifetimes.
- S3 region, access key, secret key, and bucket are now first-class config fields instead of being read ad-hoc from the environment.
- Pod identity (`POD_NAME`) is now threaded into the outbox dispatcher and embedded in every emitted event envelope.

### Changed

- Upgraded `sunbeam-g2v` to 0.3.1.
- Removed the unused `figment` and `metrics-exporter-prometheus` dependencies.
- Refactored `card_from_row` to take a `CardAggregates` struct, eliminating the clippy `too_many_arguments` warning.
- `OutboxConfig` now implements the `Default` trait instead of defining a conflicting `default` method.

### Fixed

- Removed all production `.unwrap()` and `.expect()` calls that triggered `clippy::unwrap_used` / `clippy::expect_used`.
- Allowed `clippy::question_mark_used` in generated proto modules and removed the global lint.
- Hardened `check_permission_with_retry` against the in-memory Keto image's read-after-write lag in integration tests, eliminating the flaky `services::templates` permission-denied failure under full-suite runs.
- Integration tests now consistently use the shared testcontainers setup; CLI config unit tests are isolated from leaked service environment variables.
- Updated `docs/operations/configuration.md` and `AGENTS.md` to document the new centralized config surface.

## [1.0.0-rc1] - 2026-06-24

### Added

- Card urgency: added `CardUrgency` enum and persisted urgency on card create, update, list, and get.
- Card dependencies: added `card_dependencies` table and `AddCardDependency` / `RemoveCardDependency` RPCs.
- Exposed `depends_on_card_ids` and reverse `dependent_card_ids` on the `Card` proto.
- Enum-based card priority: migrated `cards.priority` from `TEXT` to a native `card_priority` enum.
- Expanded the Keto dispatch matrix to authorize the new dependency RPCs at the board level.

### Changed

- Replaced the `#[ctor]` / `#[dtor]` test lifecycle harness with lazy async `setup()` calls per test module.
- Tests now connect to testcontainers via published host ports, supporting `DOCKER_HOST` for remote runtimes.
- Dropped the `ctor` dev-dependency.

### Added

- `.dockerignore` to exclude build artifacts, git metadata, docs, test scripts, coverage output, editor files, and local env files from the Docker build context.

## [1.0.0-rc.0] - 2026-06-22

### Added

- Project lifecycle: create, update, delete, list, and member management with Keto-backed authorization.
- Board lifecycle: public, internal, and private boards; columns; WIP limits; and realtime subscriptions via NATS JetStream.
- Card lifecycle: create, move, update, delete, assignees, labels, comments, and checklist items.
- Card reference allocation with per-project advisory locks to prevent duplicate refs under concurrency.
- Aggregated boards that roll up cards from multiple source boards.
- Public board endpoints that expose read-only views without authentication.
- Global card search backed by OpenSearch and post-filtered through Keto expand.
- Board and card templates, including seeded global templates and project-scoped custom templates.
- Attachment upload/download via presigned S3 URLs with MinIO-compatible storage.
- GitHub issue linking RPCs (stubbed for upcoming integration).
- Realtime event fan-out: Postgres `event_log` → NATS JetStream → per-board broadcast.
- Authentication middleware: JWT validation, logout watermark checks against Valkey, and Keto dispatch matrix covering all 68 RPCs.
- Health, readiness, and Prometheus metrics endpoints.
- Multi-architecture container image (`linux/amd64`, `linux/arm64`) built with `tonistiigi/xx` and a distroless runtime.
- Integration test harness using testcontainers with automatic container teardown on exit.
- SPDX `AGPL-3.0-or-later` license identifiers across all source and configuration files.
- Product, operations, and development documentation under `docs/`.

### Changed

- Relicensed the project under `AGPL-3.0-or-later`.
- Rewrote module and item docstrings for human developers, removing stage/robot language.
- Removed all frontend code and documentation references; the backend is now headless.
- Pointed `Cargo.toml` repository URL to `https://github.com/sunbeamdotpt/kanban`.

### Fixed

- Testcontainers are now stopped and removed when the test process exits, preventing dangling containers.
- Dockerfile `cargo fetch` invocation uses `--locked` instead of the unsupported `-p` flag.

[1.0.0-rc3]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc3
[1.0.0-rc2]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc2
[1.0.0-rc1]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc1
[1.0.0-rc.0]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc.0
