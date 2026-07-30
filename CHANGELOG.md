<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->
# Changelog

All notable changes to the Kanban backend will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project now adheres to [Calendar Versioning](https://calver.org) (CalVer, `YYYY.0M.PATCH`).

## [2026.07.9] - 2026-07-30

### Added

- **KANBAN-027 regression test.** Added `list_cards_by_board_includes_timestamps_and_completed_at`
  to lock in the behavior that `ListCardsByBoard` returns `created_at`,
  `updated_at`, and `completed_at`, and that `MoveCard` sets/clears
  `completed_at` when a card enters or leaves a done-marked column. The
  functional fixes ship in this release train via the KANBAN-023 handler
  changes already on mainline.

## [2026.07.8] - 2026-07-27

### Added

- **`Column.is_done` completion lanes (KANBAN-023).** Columns can be marked as
  completion lanes: `AddColumn` accepts `is_done`, `UpdateColumn` applies it
  when `update_mask` names `is_done` (a bare bool patch cannot distinguish
  false from unset), and the marker is exposed on `Column` and `EventColumn`
  (migration `0034`). Moving a card into a done-marked column sets
  `completed_at` (first transition wins), moving it out clears it, and cards
  created directly into a done column start completed — milestone completion
  stats now advance for board-driven workflows.

### Fixed

- **`UpdateCard` can clear `blocked` (KANBAN-016).** The sparse patch was
  one-way (`blocked=false` meant "no change"), so a blocked card could never
  be unblocked. Naming `blocked` in `update_mask` now applies the patch value
  exactly; without the mask the legacy set-only behavior is unchanged.
- **`GetCardTemplate` 404s on seeded globals (KANBAN-026, KANBAN-022).**
  Migration `0029` seeded four template IDs whose first Crockford character
  exceeds the ULID timestamp range; the `Id` type re-encodes them canonically,
  so `List` returned IDs that `Get` could not resolve. Migration `0033`
  rewrites the stored values to their canonical forms and reseeds the three
  legacy UUID board templates (Kanban/Sprint/Simple) with ULIDs, so every
  seeded global template now exposes a ULID. Legacy UUIDs on project-scoped
  rows remain valid read-side.

## [2026.07.7] - 2026-07-24

### Added

- **MilestoneService (KANBAN-017).** Project-scoped milestone CRUD —
  `CreateMilestone` / `ListMilestones` / `GetMilestone` / `UpdateMilestone` /
  `DeleteMilestone` — finally exposing the `milestones` table referenced by
  `Card.milestone_id` since the beginning. `Milestone` carries computed
  completion stats (`total_cards`, `completed_cards`) per the `MilestoneRef`
  contract. Reads require `view` on the project, writes `manage`
  (handler-enforced, templates pattern). `DeleteMilestone` clears
  `cards.milestone_id` in the same transaction (no FK exists). No realtime
  events are emitted (catalog-style, like templates).
- **Label catalog CRUD (KANBAN-018).** New `LabelService` —
  `CreateLabel` / `ListLabels` / `UpdateLabel` / `DeleteLabel` — over the
  previously write-only label catalog, unblocking CLI label commands.
  Labels are project-scoped or **global** (tenant-wide, migration `0032`:
  nullable `labels.project_id` + partial unique indexes per scope);
  `ListLabels` returns global labels plus the requested project's. Global
  writes require `manage` on at least one project of the tenant (there is no
  tenant-level permission object). Deleting a label cascades `card_labels`.

### Fixed

- **`milestone_id` honored in card writes (KANBAN-017).** `CreateCard` and
  `UpdateCard` now validate that a referenced milestone exists and belongs to
  the card's project (`invalid_argument` otherwise; unparseable IDs were
  previously dropped silently). `UpdateCard` can **clear** a milestone:
  naming `milestone_id` in `update_mask` with an empty patch value sets it to
  NULL — legacy sentinel behavior (non-empty sets, empty = no change) is
  unchanged when the mask does not name it. The `CardUpdated` event patch now
  includes `milestone_id`.

## [2026.07.6] - 2026-07-24

### Added

- **Cross-board and cross-project card dependencies (KANBAN-014).**
  `AddCardDependency`/`RemoveCardDependency` previously required both cards on
  the authorized board ("both cards must belong to the authorized board").
  The source card must still belong to the authorized board (the middleware
  gates `edit` on it), but the dependency target may now live on any board or
  project in the same tenant; cross-tenant targets remain rejected. Both
  cards' revisions bump and both boards receive a `CardUpdated` event, so
  `depends_on_card_ids`/`dependent_card_ids` stay consistent on every
  affected board's stream.

## [2026.07.5] - 2026-07-24

### Added

- **Realtime spine completed.**
  - `SubscribeBoard` now replays a board snapshot before the `Cutover` envelope: synthetic `ColumnAdded` per column and `CardCreated` per card (full `Card` hydration) with `nats_seq = 0`, followed by the live tail. `since_seq` is honored as the resume token — a non-zero value skips the snapshot and cuts over at that JetStream sequence.
  - `SubscribeProject` is implemented: a multi-board merge of one child stream per project board plus project-scoped events (member add/remove/role-change, `ProjectUpdated`) carried on the new `kanban.project.{id}.events` subjects. Child heartbeats are deduplicated into a single stream heartbeat.
  - Outbox dispatcher covers every event variant defined in `events.proto`: column, board, membership, project, aggregated-board, source-board, and GitHub-link payload arms. `CardUpdated` now carries its sparse patch as a `google.protobuf.Struct` (unchanged sentinels filtered, `revision` always set) and `CardCreated` hydrates the full `Card` proto at dispatch time.
  - Every publish sets the `Nats-Msg-Id` header (event_log row id) so JetStream deduplicates the retry-after-ack-failure path server-side.
  - `board_revision` is real: `boards`/`aggregated_boards` gain revision counters (migration `0030`) bumped in the same transaction as every event write, and the envelope carries the value.
  - The outbox wakes on Postgres `LISTEN/NOTIFY` (`pg_notify` issued in the event transaction) with the 250 ms poll as fallback.
  - JetStream bootstrap is drift-correcting: boot compares the live stream config against the desired one and updates it; the stream now also covers `kanban.project.>`.
- **Graceful shutdown:** SIGTERM (and SIGINT) triggers axum's graceful shutdown and flips the outbox dispatcher into a final drain pass, bounded by a 10 s wait before exit.
- **GithubLinkService is fully implemented:** `LinkIssue` (initial title/state sync from GitHub, degraded create when GitHub is unreachable), `UnlinkIssue`, `ListLinksByCard`, `SearchGithubIssues` (live proxy, no persistence), and `ResyncLink`, backed by the `github_links` table (new `kind` column, migration `0028`) and emitting `GitHubLinkAdded`/`GitHubLinkRefreshed` board events. New optional config: `KANBAN_GITHUB_TOKEN` and `KANBAN_GITHUB_API_BASE_URL` (default `https://api.github.com`).
- Org-standard global templates seeded (migration `0029`): board template `standard` (To Do / In Progress / Review / Done — deliberately no Backlog, no Archived) and card templates `feature` (Given/When/Then), `bug` (Expected/Actual/Repro/Impact), and `chore`.

### Fixed

- **KANBAN-006:** `UpdateProject` bound the patch's `prefix` to the `slug` column and never updated `prefix`; the response then echoed a derived fallback. `prefix` now updates (uppercased, sparse), `slug` is immutable, and the response echoes the real column.
- **KANBAN-012:** `ListCardsByBoard` with an unset limit returned exactly one card — `clamp(1, MAX)` rewrote `0` to `1`, making the default-page-size branch dead. Unset now means the default page size, and cursor pagination uses a composite `(column_id, position, id)` keyset that matches the `ORDER BY` (previously `id > cursor` against a different ordering skipped/duplicated rows). The same clamp bug in `ListComments` is fixed.
- The registry pump stamps the JetStream stream sequence onto each envelope: the outbox publishes with `nats_seq = 0` (the real sequence only exists post-ack), so without stamping the cutover tracker dropped every live event as out-of-order.

### Known issues

- `RemoveColumn`/`DeleteBoard` still cascade-delete cards without `CardDeleted` event_log rows (unchanged from 2026.07.3; reconciler territory).
- `AggregatedBoardDeleted` cannot be delivered through the outbox: the event_log row references the aggregate via `ON DELETE CASCADE`, so the row never survives the delete it would describe.

## [2026.07.4] - 2026-07-21

### Added

- New `S3_PUBLIC_ENDPOINT` / `--s3-public-endpoint` config: presigned upload/download URLs are signed for and built with this public host when set, while the service keeps using `S3_ENDPOINT` (possibly cluster-internal) for its own API calls. Deployments with an in-cluster filer should point this at the public S3 hostname.

### Fixed

- Card refs kept minting without the project prefix: the `project_ref_counter` upsert only bumped the sequence on conflict, so counter rows created before the v2026.07.3 prefix fix stayed empty forever. Allocation now always takes the project's current prefix, healing stale counter rows on next use.

## [2026.07.3] - 2026-07-21

### Fixed

- **Card permissions:** `CreateCard` now writes the `KanbanCard:{id}#parent@KanbanBoard:{board_id}` tuple the authorization model expects, so card-scoped RPCs no longer return 403 for everyone. `DeleteCard` removes the card's tuples, and cross-board `MoveCard` re-homes the parent tuple (and the card's `board_id`, which previously stayed behind). A `backfill_card_parent_tuples` system migration repairs existing cards at boot.
- **Card refs:** `CreateProject` now persists the `prefix` column it previously only echoed back from the slug, so new cards mint `TRI-001`-style refs instead of `-001`. Migration `0027_projects_prefix_backfill` repairs existing projects.
- **Search indexing:** card events flowing through the outbox dispatcher are now mirrored into OpenSearch (create/update/move index the document, delete removes it), and a `backfill_opensearch_cards` system migration indexes all existing cards. Previously nothing fed the index, so `SearchCards` always returned zero hits.
- `AddMember` rejects free-form subject strings; subjects must be typed ids (`user:<id>` or `agent:<id>`).

### Known issues

- `RemoveColumn`/`DeleteBoard` cascade-delete cards via FK without emitting `CardDeleted` events; those cards' permission tuples and search documents linger until a reconciler lands (see `.maintainer/known-issues.md`).

## [2026.07.2] - 2026-07-20

### Changed

- The service now serves Connect-RPC natively (plus gRPC and gRPC-Web) through the sunbeam-g2v serving stack. tonic/prost are gone: server traits and message types are generated by `connectrpc-build` (buffa), and all ten services were migrated end-to-end. Wire protocol compatibility is preserved; gRPC-Web now works out of the box.
- **Breaking (clients forwarding tokens):** permission checks no longer forward the caller's bearer token. The dispatcher uses the service's singleton client (client-credentials, `permission:admin`) scoped per tenant via `x-tenant-id`, so end-user OAuth2 clients no longer need any permission scopes.

### Fixed

- `ListTemplates` and `ListCardTemplates` no longer return the global templates twice when a `project_id` is given.
- `AddMember` no longer leaves stale role tuples behind on role change, and `RemoveMember` now revokes exactly the member's tuples on that project — previously it revoked the subject's same-role tuples across every project in the namespace.
- Test harness: S3/OpenSearch config unit tests snapshot and restore the environment they clear; search/attachments integration helpers wait for the container stack and read endpoints under the shared env lock.
- Aggregated-board subscriptions gained regression coverage for live-tail forwarding and mid-stream permission rechecks (no defect found; the reported premature close could not be reproduced).

## [2026.07.1] - 2026-07-20

### Changed

- **Breaking:** Renamed the sso-gateway configuration surface. `HYDRA_INTROSPECTION_URL`, `HYDRA_CLIENT_ID`, and `HYDRA_CLIENT_SECRET` are now `SSO_GATEWAY_INTROSPECTION_URL`, `SSO_GATEWAY_CLIENT_ID`, and `SSO_GATEWAY_CLIENT_SECRET` (flags `--sso-gateway-introspection-url`, `--sso-gateway-client-id`, `--sso-gateway-client-secret`). The old variables are no longer read; deployments must rename them before upgrading.
- Token introspection no longer uses HTTP Basic client authentication. The service exchanges its client credentials for an access token scoped `tenant:admin` (cached by `expires_in`, refreshed on demand, one invalidate-and-retry on 401) and introspects with `Authorization: Bearer`. The OAuth2 application must hold the `tenant:admin` scope in addition to `permission:admin`.

### Fixed

- Fixed full-suite test runs: the S3/OpenSearch config unit tests now snapshot and restore the environment variables they clear (previously they leaked cleared/overridden values into concurrently scheduled integration tests), and the search/attachments integration helpers ensure the container harness is up and read service endpoints under the shared env lock.

## [2026.07.0] - 2026-07-20

### Changed

- **Breaking:** All proto messages are now buf STANDARD compliant, and RPC request/response messages use per-RPC wrapper types. Clients must be updated to the new proto definitions.
- Migrated authorization from Keto to the sso-gateway's `PermissionService` (OpenFGA-backed, one store per tenant). The Kanban permission namespace and OpenFGA model are provisioned at server boot from `.integration/openfga-model.json`.
- Every mutating RPC now requires the `x-sunbeam-object-id` header; handlers read the checked object ID from request extensions instead of the protobuf body.
- Tenant is resolved from the introspected token; trusted service-to-service calls may carry `x-tenant-id`.
- Server boot readiness now proxies the sso-gateway's `/health/ready` and fails fast when the permission namespace cannot be provisioned.

### Added

- Multitenancy across all services: `tenant_id` columns throughout the database schema and dev seeds, with every query and OpenSearch/S3 integration scoped per tenant.
- `sunbeam-g2v` integration via generated sso-gateway/IAM proto clients for permission checks.
- buf configuration and a buf CI workflow for proto linting.

### Removed

- Keto dependency, its namespaces configuration, and the `uuid_to_ulids` system migration.
- Unused JetStream helper functions.
- The `test.sh` wrapper; the integration test harness is now self-contained under `cargo test` and boots the sso-gateway via testcontainers.

## [1.0.0-rc6] - 2026-06-27

### Added

- Added a startup `system_migrations` framework that runs after SQLx schema migrations and records progress in a `system_migrations` ledger table.
- Added `Id::from_uuid_with_timestamp` for deterministic UUID-to-ULID encoding, preserving the original creation timestamp in the ULID.
- Added the `uuid_to_ulids` system migration, which rewrites legacy UUID identifiers to ULIDs across Postgres, Keto, and OpenSearch in a single deterministic pass. The migration persists its UUID→ULID mapping to a backup table so it can resume safely if interrupted after the Postgres rewrite.

### Changed

- Server boot now runs system migrations immediately after SQLx migrations and before NATS JetStream bootstrap.

### Fixed

- OpenSearch migration now processes the first page returned by the initial scroll request and skips gracefully when the target index does not exist.

## [1.0.0-rc5] - 2026-06-26

### Fixed

- Fixed Keto dispatch middleware state extraction: changed `Extension(state)` to `State(state)` so `from_fn_with_state` correctly passes `Arc<DispatchState>`. This resolves the `500 Internal Server Error` caused by a missing request extension on every authenticated RPC.

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

[2026.07.4]: https://github.com/sunbeamdotpt/kanban/releases/tag/v2026.07.4
[2026.07.3]: https://github.com/sunbeamdotpt/kanban/releases/tag/v2026.07.3
[2026.07.2]: https://github.com/sunbeamdotpt/kanban/releases/tag/v2026.07.2
[2026.07.1]: https://github.com/sunbeamdotpt/kanban/releases/tag/v2026.07.1
[2026.07.0]: https://github.com/sunbeamdotpt/kanban/releases/tag/v2026.07.0
[1.0.0-rc3]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc3
[1.0.0-rc2]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc2
[1.0.0-rc1]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc1
[1.0.0-rc.0]: https://github.com/sunbeamdotpt/kanban/releases/tag/v1.0.0-rc.0
