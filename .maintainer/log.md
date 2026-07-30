# Decision log

Append-only. Newest at the bottom. Every entry: what was decided, and *why* —
future sessions need the reasoning, not just the outcome. Never rewrite history.

## 2026-07-20 — Maintainer bundle created

Enrolled kanban as the second repo in the local agent-mail system, same OKF
shape as sbbb. Two charter decisions worth recording: (1) migration edits are
a hard rule, not a judgment call — the v2026.07.0 in-place rewrite of
migrations 0001–0018 (commit 45f88920c6) forced a `kanban_db` reset, and a
maintainer agent must never treat that as precedent; (2) proto breaking
changes escalate because beam-ui is a live consumer and the API is published
to buf.build. Note for accuracy: the migration rewrite shipped in v2026.07.0,
not v2026.07.1 as sbbb's knowledge base initially recorded — corrected there
the same day.

## 2026-07-20 — Location hygiene pass

Scrubbed machine-specific absolute paths and internal hostnames from the
knowledge files and `AGENTS.md` (dead `sunbeam-split` workspace pointer
removed; internal Gitea URL replaced by the sibling-repo name). Rule going
forward: knowledge files name repos and repo-relative paths, never disk
locations or internal hosts — the files outlive any one machine and may be
read outside it.

## 2026-07-20 — Connect-RPC serving migration (v2026.07.2)

Migrated serving from tonic/prost to Connect-RPC via the sunbeam-g2v serving
stack (connectrpc 0.7, buffa messages). Why: g2v is the platform's canonical
serving stack, Connect-Web clients get first-class support, and gRPC-Web now
works out of the box (tonic never had it). All ten services, realtime
(registry/outbox), and the test harness moved to `crate::cpb` types generated
by `connectrpc-build`; prost/tonic deps deleted. Serial conversion (one file at
a time) was chosen over parallel agents because Rust compilation makes
concurrent edits to one crate counterproductive.

Permission checks now use the service's singleton client (client-credentials,
`permission:admin`) scoped per tenant via `x-tenant-id`, instead of forwarding
the caller's bearer token. Why: end-user OAuth2 clients should not need
permission scopes; authorization is the service's job after introspection
establishes identity.

Two member-management bugs fixed with the sweep: `RemoveMember` previously
deleted tuples namespace-wide (revoking the subject's roles on *every*
project) and missed stale tuples from role changes; deletes are now
object-scoped and `AddMember` clears the prior role first. `ListTemplates`/
`ListCardTemplates` double-counted globals when a project id was given.

`SubscribeAggregatedBoard` "closes after cutover" report could not be
reproduced — three regression tests (builder live-tail, handler live-tail,
private-board permission rechecks) all pass; the human will supply repro
details if it recurs.

## 2026-07-21 — Card tuples, refs, and search indexing (v2026.07.3)

Post-deploy sweep by sbbb (message 5) found every card-scoped RPC returning
403: the OpenFGA model always computed card relations via
`KanbanCard#parent@KanbanBoard`, but nothing wrote that tuple — an untested
gap because integration tests inject `AuthContext` directly and bypass the
dispatch middleware. Fix writes the tuple on `CreateCard`, deletes tuples on
`DeleteCard`, and re-homes them on cross-board `MoveCard` (which also never
updated `cards.board_id` — a latent row-corruption bug). Regression tests now
assert permission behavior through the real backend, and a
`backfill_card_parent_tuples` system migration repairs existing rows.

Card refs came out as `-001` because `create_project` never persisted
`projects.prefix` (the response masked this by echoing the slug). Fixed in
the INSERT plus migration 0027 for existing rows. Search returned zero hits
because no write path to OpenSearch ever existed; the outbox dispatcher now
mirrors card events into the index and `backfill_opensearch_cards` covers
existing data. RemoveColumn/DeleteBoard FK-cascades skip `event_log`, leaving
tuple and index drift — documented as a known issue pending the reconciler
rather than silently "fixed" by hand.

## 2026-07-21 — Ref-counter healing and S3_PUBLIC_ENDPOINT (v2026.07.4)

sbbb's re-sweep confirmed the v2026.07.3 fixes but caught two leftovers.
(1) Refs still minted without a prefix: the `project_ref_counter` upsert only
bumped `next_seq` on conflict, so counter rows created while
`projects.prefix` was empty kept `''` forever — the v2026.07.3 backfill fixed
the projects table but not the counter. Allocation now takes
`EXCLUDED.prefix` on conflict, healing stale rows on next use. (2) Presigned
URLs pointed at the cluster-internal filer. New `S3_PUBLIC_ENDPOINT` config
signs and builds presigned URLs for a public host while API calls keep using
`S3_ENDPOINT`; deployment wiring is sbbb's (they own prod env vars), notified
by mail.

## 2026-07-24 — Realtime spine completed + rollout findings (v2026.07.5)

Big sweep: every TODO/unimplemented item in the tree, plus the rollout
findings from sunbeam's task mail (message 73) and a board check via the
sunbeam CLI. Decisions and their reasoning:

- **Event delivery architecture:** one shared `src/event_log.rs` helper
  (`insert_board_event` / `insert_aggregated_event` / `insert_project_event`)
  replaced per-service copies — dedupe was an explicit goal. Board revision
  counters (migration 0030) are bumped in the same tx as the event write and
  ride in the payload JSON, so the outbox stays stateless. Project-scoped
  events needed a home: `event_log.project_id` (migration 0031) + new
  `kanban.project.{id}.events` subjects rather than shoehorning project
  events onto a board subject.
- **BoardCreated/BoardDeleted are written as project events**, because a
  board-scoped row can never survive `DeleteBoard` (`event_log.board_id` is
  `ON DELETE CASCADE`). Same reason `AggregatedBoardDeleted` remains
  undeliverable without schema surgery — documented as a known issue instead
  of hacking around the FK.
- **Registry pump stamps `envelope.nats_seq` from JetStream message info.**
  The outbox publishes with `nats_seq = 0` (the real sequence only exists
  after the publish ack, which is written back to the DB, not the message);
  without stamping, the cutover tracker dropped every live event as
  out-of-order. Latent bug exposed by the snapshot work — before snapshot
  replay, cutover happened at 0 so nothing was ever dropped.
- **Snapshot ordering:** subscribe the live channel first, read
  `MAX(nats_seq)`, then snapshot — an event can be duplicated (client merges
  by card_id) but never lost.
- **JetStream bootstrap no longer uses `get_or_create_stream`:** it errors
  10058 on drift instead of returning the stream, making config upgrades
  impossible. Now get → drift-compare → update; create when missing. This
  also closed both `TODO(g2v)` items without touching the g2v repo.
- **GitHub link service implemented** (was "blocked on product direction";
  the human overrode by asking for all unimplemented features). Reasonable
  defaults: unauthenticated `api.github.com` by default, optional
  `KANBAN_GITHUB_TOKEN` / `KANBAN_GITHUB_API_BASE_URL`; `LinkIssue` creates
  degraded links when GitHub is unreachable (link creation is never held
  hostage by GitHub), `ResyncLink` fails hard on fetch errors (its whole
  point is the fetch). Object id is `card_id` for all five RPCs per the
  permission matrix — link ids are not permission objects.
- **KANBAN-007:** org-standard templates ship as seed migration 0029 (with
  explicit ULID ids — since 0026 id columns are TEXT with no default, which
  is why the first draft of 0029 blew up the harness). API creation of
  globals stays rejected; a seed is idempotent, reviewable in git, and needs
  no new admin permission surface.
- **KANBAN-012** (found via the board check the human asked for):
  `req.limit.clamp(1, MAX)` rewrote unset limits to 1 — the dead
  `if limit == 0` branch below it could never fire. Cursor pagination was
  also inconsistent (`id > cursor` vs `ORDER BY column_id, position`); now a
  composite keyset matching the ORDER BY. Same clamp bug fixed in
  `ListComments`.
- **Graceful shutdown:** SIGTERM + SIGINT → axum graceful shutdown, watch
  channel flips the outbox into a final drain, 10 s bounded wait. No new
  dependency (tokio `watch` instead of tokio-util's CancellationToken).
- Coverage: held the release to the human's >90% gate (llvm-cov).

## 2026-07-24 — Cross-board/project card dependencies (v2026.07.6)

KANBAN-014, filed from a real need (KANBAN-009 on kanban/dev depends on
CLI-004 on cli/dev — the backend rejected the edge with "both cards must
belong to the authorized board"). Decision: keep the middleware gate exactly
as-is (`edit` on the source card's board) and relax only the handler's
target check from same-board to same-tenant. Why: the edge is a mutation of
the *source* card, so edit on its board is the right authorization; the
target card is only referenced, and cross-board visibility inside a tenant
is already the norm (search, aggregated boards). Both cards' revisions bump
and both boards get a `CardUpdated` event so no stream serves stale
depends_on/dependent lists. Cross-tenant stays rejected — permission and
visibility semantics across tenants are undefined (filed as low-priority
KANBAN-015 until a real use case shows up).

## 2026-07-24 — agent-mail → kanban ticketing migration

Cross-repo coordination moved off agent-mail (deprecated) onto kanban cards
via `sunbeam kanban` — the same migration sbbb did earlier. The AGENTS.md
ritual, charter, and state.md now describe the kanban flow; mail references
in older entries are historical. *Why:* the human standardized cross-repo
tracking on kanban (this repo's product) so tickets are visible to
everyone, not just the two mail endpoints.

## 2026-07-24 — MilestoneService + label catalog CRUD (KANBAN-017/018)

Both filed same-day with CLI consumers blocked (cli wants release grouping
under a "3.2" milestone; CLI-007/CLI-009 blocked on label CRUD). Shipped as
additive proto (`milestones.proto`, `labels.proto`), pushed to
buf.build/sunbeamdotpt/kanban after `buf breaking` confirmed additive.

Decisions and the why:
- **None-source dispatch + handler-side KanbanProject checks** (view reads /
  manage writes), copied from templates.rs: milestones/labels have no OpenFGA
  object of their own and their ids are not permission objects. Matrix 68→77.
- **Global labels via nullable `labels.project_id`** (migration 0032; NULL =
  tenant-wide) instead of templates' `is_global` flag: the old
  UNIQUE(tenant_id, project_id, name) can't express NULL-scope uniqueness, so
  it was swapped for two partial unique indexes. *Gotcha:* any
  `ON CONFLICT (tenant_id, project_id, name)` now needs the
  `WHERE project_id IS NOT NULL` predicate — only cards.rs's test seed_label
  used one (fixed); seeds use bare `ON CONFLICT DO NOTHING` and are fine.
- **Global label writes = manage on at least one tenant project.** There is
  no tenant-level OpenFGA object; this is the least-wrong check. Worth
  revisiting if a tenant/admin object ever lands.
- **No realtime events** for milestone/label CRUD — templates set the
  catalog-CRUD-without-events precedent. Card-level milestone changes still
  flow through `CardUpdated`.
- **UpdateCard milestone clear:** the handler is sentinel-based and ignores
  update_mask for every other field, so gating milestone *only* on the mask
  would silently break existing clients that set it without a mask. Hybrid:
  mask naming "milestone_id" applies the patch value exactly (empty clears);
  otherwise legacy behavior (non-empty sets, empty = no change). Create/Update
  now validate the milestone exists in the card's project (was parse-only,
  silently dropping garbage ids).
- **DeleteMilestone clears cards.milestone_id in the same tx** — no FK on
  that column, so it had to be manual.
- Milestone stats (total/completed cards) are computed per request with one
  grouped query; no denormalized counters.
- Release cut (2026.07.7 bump + tag) left to the human per charter;
  CHANGELOG staged under [Unreleased].

## 2026-07-24 — Scoped GitHub issue/PR auto-sync, filed KANBAN-019/020/021

The human asked for scoping + ticketing only (no implementation). Current
state: link sync is manual-only (`ResyncLink`); `github.proto` itself defers
automatic sync and auto-status to "v2". Filed on kanban/dev:
- KANBAN-019 (high) webhook receiver: unauthenticated axum route
  `/webhooks/github` + HMAC-SHA256 (X-Hub-Signature-256), issues/pull_request
  events applied payload-direct (no API call), cross-tenant link lookup via
  idx_github_links_repo_issue, GitHubLinkRefreshed per affected board,
  delivery-id dedupe, fast-2xx + async processing (GitHub's 10 s limit).
- KANBAN-020 (medium) reconciliation worker: periodic stale-link sweep as the
  missed-webhook fallback; rate-limit aware, changed-only events.
- KANBAN-021 (medium) auto-status propagation onto cards — depends_on 019 AND
  020 (needs the state-change feed); carries open product questions (what
  maps where, per-board opt-in, user-override wins) that must be escalated
  in-session before implementation.
Deployment wiring (KANBAN_GITHUB_WEBHOOK_SECRET, ingress must pass the
X-Hub-Signature-256/X-GitHub-* headers intact) filed on sbbb/dev per charter
rule 4.

## 2026-07-27 — Validated and closed KANBAN-009 (card-template field plumbing)

The card was implemented in the cli repo (`cli/src/kanban/card_templates.rs`)
but never moved off todo. Validated live against kanban.sunbeam.pt with the
release CLI: `card-template create` persisted --title/--default-description/
--label/--checklist; `card-template update` replaced labels and checklist
wholesale and applied the update-mask paths; round-trip `get` by ID matched.
Noted (not a blocker): name-based `get` only resolves global templates —
project-scoped templates need the ULID. Moved KANBAN-009 to done.

## 2026-07-27 — Fixed KANBAN-016/022/023/026; triaged + assigned all open cards

Asked to address every open ticket and assign them all to sienna. Outcome:

- **KANBAN-026 root cause was data, not the read path.** The 0029 seed used
  four IDs whose first Crockford char exceeds the ULID timestamp range
  (A/Z/D/K > 7). `Id` decodes by masking the overflow and re-encodes
  canonically, so List returned '7FV6…' while the row stored 'ZFV6…' → Get
  404'd. Fixed with migration 0033 (plain UPDATEs; nothing FK-references
  template ids), which also reseeds the three legacy UUID board templates
  from 0017 with ULIDs (KANBAN-022). Project-scoped legacy UUID rows stay —
  `IdKind::Uuid` round-trips them read-side per the card's acceptance.
- **KANBAN-023 needed a schema answer, not a query change:** there was no
  "done" marker anywhere (no `columns.is_done`, `completed_at` never written
  outside fixtures). Added `Column.is_done` (migration 0034, additive proto
  `Column.is_done=9` / `AddColumnRequest.is_done=7` / `EventColumn.is_done=7`).
  Bool patch fields use FieldMask semantics (mask names `is_done`), same
  pattern as `milestone_id`. MoveCard sets completed_at entering a done
  column (COALESCE — first transition wins) and clears it leaving; CreateCard
  into a done column starts completed. OpenSearch stays fresh because the
  outbox already reindexes on CardMoved.
- **KANBAN-016:** `blocked` got the same FieldMask treatment in UpdateCard;
  legacy set-only behavior kept when the mask is absent (back-compat).
- **KANBAN-024 deferred with a fix sketch on the card** — the real work is
  wiring an IdentityService client (none exists in kanban today) plus a
  gateway scope for the service app (sbbb/sso-gateway config), and a design
  pick between hydrate-at-assign vs read-time resolver.
- Filed **CLI-014** on cli/dev: unblock flag + column is_done CLI support
  (depends on this server changeset being released).
- Verification: cards 44/44, boards 27/27, templates 10/10, realtime 59/59,
  clippy -D warnings clean, fmt clean, permission-coverage OK (77 RPCs).
  Harness flakiness (sso-gateway bootstrap port race + Docker port
  exhaustion) needed the known cleanup loop: rm testcontainers-labelled
  containers + `docker network prune -f` between suite runs.
- All 18 open cards assigned to sienna; every open card carries a triage
  comment with the decision (done / deferred-why / owner).

## 2026-07-27 — Released v2026.07.8

Sienna approved the release train in-session. Version bump 2026.7.7 →
2026.7.8 (Cargo.toml + lock), CHANGELOG [Unreleased] → [2026.07.8],
`chore(release): v2026.07.8` (b316af1bc4), annotated tag pushed;
release workflow #30312520954 kicked off (~20 min build, matches the
v2026.07.7/6 timings). KANBAN-016/022/023/026 moved review → done on
tag push. GHCR image verification + cli/dev heads-up for CLI-014
scheduled as a one-shot reminder. Note: the release series was split
into conventional commits per sienna's request (fix/feat/docs/release)
rather than folding the bump into a feature commit as v2026.07.7 did.

## 2026-07-30 — KANBAN-027 verified, regression-tested, and released v2026.07.9

Card reported two symptoms: (1) `completed_at` never populated when a
card enters a done column, and (2) `ListCardsByBoard` responses omit
`created_at`/`updated_at`/`completed_at`. Code review + targeted tests
show the production observation was already addressed on mainline:
`MoveCard` sets/clears `completed_at` based on the target/source column
`is_done` marker (KANBAN-023, migration 0034), and `ListCardsByBoard`
has selected and returned all three timestamp columns since the initial
CardService implementation. Added a regression test
(`list_cards_by_board_includes_timestamps_and_completed_at`) that
explicitly asserts list responses carry `created_at`, `updated_at`, and
correct `completed_at` state across done-column transitions. All 45
card service tests pass; clippy `-D warnings` and `cargo fmt` clean.
KANBAN-027 moved to done.

Cut v2026.07.9 so the fix ships: conventional commits split as
`test(cards): KANBAN-027 regression test...`,
`docs: changelog + maintainer state for KANBAN-027`, and
`chore(release): v2026.07.9` (84ab8a43af), annotated tag `v2026.07.9`
pushed. Origin had diverged with a README/docs-server commit
(ad026a2669) between local work and push, so mainline was rebased and
the tag force-updated; an accidental local `v1.0.0-rc6` tag pushed in
the first attempt was deleted from remote.

Filed **SBBB-013** on sbbb/dev for production deployment of the
v2026.07.9 image with verification steps. Linked back to KANBAN-027 in
the card description and a comment.
