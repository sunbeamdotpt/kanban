<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
# Changelog

All notable changes to the Kanban backend will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.0.0-rc.0]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0-rc0
