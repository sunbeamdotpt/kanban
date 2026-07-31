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
the card description and a comment. Assigned to **tony**.

**KANBAN-028 closed as wontfix.** The card asked for direct
`Authorization: Bearer` scripting against `kanban.sunbeam.pt` ConnectRPC
endpoints. That is not a supported use case: kanban is an internal RPC
backend consumed through the sunbeam CLI, and the opaque access token
from `sunbeam auth token` is scoped for the CLI/gateway flow rather than
direct ingress. Card moved to done with a comment explaining the design
and pointing missing-CLI operations (e.g. `--is-done`) to CLI-015.

2026-07-30 — Created **KANBAN-029** on kanban/dev (todo, high priority).
User assignments currently accept identifiers without validation. The card
captures the requirement to accept either a ULID or an email address as input,
resolve/validate it against the authoritative user directory, and persist only
the canonical ULID. This keeps storage stable while making the API more
forgiving for callers. No implementation started; card links back to the
requirement and includes acceptance criteria plus test coverage expectations.

2026-07-30 — **KANBAN-029 implemented: server-side assignee validation.**
AssignCard/UnassignCard now resolve the subject input (identity ULID,
`user:<ulid>`, or email) against the sso-gateway user directory and store only
the canonical `user:<ulid>`; malformed input gets `invalid_argument`, unknown
users `not_found`. Decisions and the why:

- **sdk `AuthClient`, not a local client.** sienna flagged that the SDK
  (`sdk` repo, `auth` feature, generated from `buf.build/sunbeamdotpt/sso-gateway`)
  already provides the identity surface and is what liminal and other apps use.
  Added `sdk = { git, tag = "v3.3.0", features = ["auth"] }` as a real
  dependency; `src/auth/identity_client.rs` is a thin wrapper (scope
  `identity:read`, `x-tenant-id` routing, g2v token cache shared per process).
- **proto/iam re-synced via `buf export`, not hand copy.** The vendored files
  had drifted (mixed `sunbeam-pt`/`sunbeamdotpt` go_package options, missing
  `client_credential.proto`, stale application/oauth2_device). Charter now
  names the sync command: `buf export buf.build/sunbeamdotpt/sso-gateway -o
  proto/iam`. `skip_consent` added to the harness CreateApplicationRequest as
  a result.
- **No legacy passthrough.** Per sienna: the legacy path (accepting arbitrary
  subjects like Kratos UUIDs or unvalidated strings) is broken behavior and is
  rejected, not grandfathered. Consequence: assignee rows written before this
  change with garbage subjects cannot be removed via UnassignCard — cleanup
  needs a data migration if it ever matters.
- **Email resolution is a directory scan.** The gateway has no email-lookup
  RPC (`ListIdentities` ignores filters; SCIM filter is declared unsupported),
  so `find_by_email` pages the tenant directory and matches `traits.email`
  case-insensitively — same approach the CLI already uses. Fine at current
  directory sizes; worth a gateway-side filter RPC eventually.
- **UnassignCard validates too** so both verbs accept identical references.
- **Production needs `identity:read`** on the kanban service app — filed
  SBBB-016 (hard dependency; without it every assign fails with
  permission_denied).
- Harness: test app gained `identity:read`; default identity schema is
  registered once at bootstrap (parallel-test `CreateIdentitySchema` race
  surfaces as `internal` duplicate-key, not `AlreadyExists` — both tolerated).
- Related: CLI-018 filed — the CLI passes non-email assign input through
  unchecked (`card assign <ref> tony` stores "tony").

2026-07-30 — **v2026.07.10 released; release train run for KANBAN-029.**
Feature commit bad67a6776, release commit d2165219d4, tag `v2026.07.10`
pushed; workflow run 30588840172 building the multi-arch GHCR image.
KANBAN-029 moved to done at tag time (house pattern: done on release,
deployment tracked separately). Deployment ticket **SBBB-017** filed on
sbbb/dev and assigned to tony@sunbeam.pt (resolved to
`user:01KWF0KYZ0W8Y42YE36J5SZ0CF`), with an explicit `depends_on` link to
**SBBB-016** (identity:read scope) — the scope must be applied with or
before the deploy or every assign/unassign 403s. The deploy card carries
post-deploy verification steps (assign by email stores `user:<ulid>`;
garbage input rejected).

2026-07-31 — **v2026.07.10 image build failed; re-released as v2026.07.11.**
The scheduled post-release check caught run 30588840172 red: the sdk's
`build.rs` shells out to `buf export buf.build/sunbeamdotpt/sso-gateway` at
compile time, and kanban's Dockerfile builder image has no `buf` — the
multi-arch build died ~9 minutes in, before any image was published.
Root cause notes: (1) this only bites Docker/CI builds — local dev machines
have buf installed, so `cargo build` never exposed it; (2) any repo adding
an sdk dependency needs buf in its Dockerfile from now on. Fix: builder
stage copies the pinned `bufbuild/buf:1.71.0` binary; verified locally with
a full `docker buildx build --platform linux/amd64` before re-tagging
(8189150ade). v2026.07.11 contains the same application code as
v2026.07.10; SBBB-017 updated to the new tag with a "do not deploy
v2026.07.10" warning. Lesson recorded: verify the release workflow, not
just the tag — the 00:17 cron did its job.

2026-07-31 — **v2026.07.11 image live.** The 00:46 verification confirmed
run 30590298809 succeeded and `ghcr.io/sunbeamdotpt/kanban:v2026.07.11`
exists as a multi-arch index (linux/amd64 + linux/arm64; floating
v2026.07 / v2026 / latest updated). SBBB-017 commented image-live; the
deploy itself (tony) and SBBB-016 (identity:read) are the remaining
production steps.

2026-07-31 — **KANBAN-030 verified done (executed by tony overnight).** The
completed_at backfill ran as two documented one-off psql runs against
kanban_db rather than a migration: (1) 41 done-titled columns flagged
is_done=true — the real gap, since both the KANBAN-027 live stamping and any
backfill key on that flag; (2) 67 historical done cards stamped, 66 with
exact move-into-done times recovered from event_log CardMoved payloads, 1
via updated_at fallback (card created directly in done). Verified: 0 done
cards unstamped, 0 non-done stamped; burndown regenerated (29 → 70
completions). Why one-off over migration: the defect only existed in this
prod instance and the code path was already fixed going forward.

2026-07-31 — **New-card triage: 031/033/035 implemented, 032/002 deduped.**
Five new cards landed since the last session; all assigned to sienna.

- **KANBAN-031 split across repos.** Root cause of the empty
  `unauthenticated:` detail and silent rejections is g2v's auth_middleware
  (`authenticate_session` map_err(|_| …) drops the AuthError; the 401 body
  is not Connect-shaped, so RPC clients show an empty detail). Kanban-side
  shipped (4394353ab9): SsoGatewaySessionClient WARN-logs every rejection
  with failure class + latency, and KANBAN_SSO_INTROSPECTION_TIMEOUT_SECS
  (default 10s = old hardcoded value) makes the fail-closed policy explicit.
  The middleware-layer remainder (Connect-shaped detail, method/path
  logging, IntrospectionConfig knobs) filed as **G2V-001** (high) on
  g2v/dev; KANBAN-031 depends_on it and sits in review. *Why not fix g2v
  directly:* charter rule 4 — cross-repo changes flow through cards on the
  owning team's board.
- **KANBAN-033** (046ad2c590): additive `TemplateColumn.is_done` (tag 4) +
  JSON round-trip + migration 0035 flagging Done in the four seeded global
  templates. Project-scoped custom templates deliberately untouched — the
  proto field now lets owners opt in explicitly. Test asserts every seeded
  global template's Done column is flagged.
- **KANBAN-035** (a21571d0b2): additive `Assignee.email` (tag 4) hydrated
  read-time from the directory. Chose read-time hydration over
  hydrate-at-assign (the KANBAN-024 design fork) because the card's clients
  (liminal, sdk) need existing rows covered without a backfill, and the
  identity client already existed from KANBAN-029. A 5-minute TTL
  subject→email cache keeps list endpoints from hammering the gateway;
  legacy/unknown subjects and backend errors resolve to empty email and
  never fail a card read. Wired the identity client into Board/Project/
  AggregatedBoard services and the outbox dispatcher (CardCreated hydration)
  so snapshots and live events carry emails too — snapshots are liminal's
  primary render path, so hydrating only the RPC reads would have left the
  bug half-fixed. Proto pushed to BSR (7933c4d8da80, buf breaking clean),
  unblocking sdk SDK-012 proto-side.
- **KANBAN-002 + KANBAN-032 closed as duplicates of KANBAN-034**, which is
  now the canonical cross-project TransferCard ticket carrying the design
  questions (relocate-in-place vs recreate-and-close, moved disposition,
  soft-delete semantics). No implementation: it needs a design decision
  first, and a new RPC is additive-but-public API.

Verification: full `cargo test` 334/336 (the 2 failures — permission-client
and aggregated-boards flakes — pass in isolation; known gateway/OpenFGA
parallel-load flakiness), clippy `-D warnings` clean, fmt clean,
permission-coverage OK (77 RPCs). Release NOT cut — releases escalate per
charter; CHANGELOG staged under [Unreleased] and cards left in review per
sienna's instruction.

2026-07-31 — **Test-flake root causes ticketed (KANBAN-036/037/038), not
fixed.** Sienna called time on harness work for the day; the three distinct
causes are filed with root-cause analysis instead of being fixed ad hoc:
(036) the leak is structural — statics never drop, so the OnceCell-held
containers are never removed, and the "testcontainers removes them at exit"
comment is wrong; fix is a startup reaper with an age filter so concurrent
suites don't reap each other. (037) transient gateway/OpenFGA failures under
parallel load need harness-side retries (production already has
grant_with_retry). (038) env-reading test helpers depend on another test
having triggered containers::setup() first — subset runs fail spuriously;
fix is self-bootstrapping helpers. All three assigned to sienna, todo,
medium.

2026-07-31 — **Full open-card triage pass (priorities, deps, ownership).**
Triggered by sienna: "properly triage these cards, fix priorities and
dependencies". Outcomes:

- **KANBAN-039 root-caused to the CLI, not the server.** Label add/set 403s
  for every caller because the CLI sends the card id as x-sunbeam-object-id
  while BulkUpdateCardLabels is gated on KanbanBoard+edit (board id in
  header; every other card RPC is KanbanCard-scoped, which is why only this
  one breaks). Server contract is correct; fix is one line CLI-side
  (object_id_options(&card.board_id) — the CLI already fetched the card).
  Filed CLI-023 (high) with the exact fix; KANBAN-039 depends_on it.
  *Why no server-side acceptance of card ids:* changing the matrix entry
  would break any client already sending board ids correctly, and the
  header contract is deliberately uniform.
- **KANBAN-003 closed as implemented** — verified in code: MoveCard
  re-homes board_id + parent tuple, test-covered since v2026.07.3. (The
  2026-07-27 session had proposed closing pending human confirmation;
  sienna's triage request was taken as that confirmation.)
- **KANBAN-008/010 stay as canonical tickets** despite being CLI-owned
  work: no equivalent cards exist on cli/dev, and closing them would lose
  the tracking. Comments reconfirm scope + server-side context.
- **KANBAN-021 marked blocked** — deps on 019/020 were already linked; the
  real gate is the pending product decision (sienna, in-session).
- **KANBAN-024 narrowed by comment** — the email tier shipped with
  KANBAN-035; remaining scope is display_name/avatar hydration (design pick
  + SBBB-016). Priority kept medium since email removes the raw-ULID worst
  case.
- **Priorities reviewed, mostly left as-is:** KANBAN-019 kept high (stale
  link state is a data-visibility defect with only a manual workaround);
  KANBAN-015 stays low (cross-tenant rejected by design until a use case);
  the rest were already correctly medium.

2026-07-31 — **CLI-owned cards transferred; KANBAN-034 design decided.**
KANBAN-008 and KANBAN-010 were CLI work tracked on kanban's board with no
cli/dev equivalents; filed CLI-024 (template columns / board create
--template/--columns) and CLI-025 (member add raw-subject passthrough +
help text) with full server-side context, assigned to sienna, and closed
the kanban copies as transferred. KANBAN-039 closed as transferred to
CLI-023 (the one-line object-id header fix) with post-fix verification
steps. *Why transfer instead of keeping dual tickets:* one canonical ticket
per piece of work, on the board of the repo that implements it (charter
rule 4); kanban's board only tracks kanban work.

**KANBAN-034 design decision (sienna, in-session): relocate-in-place** for
cross-project transfer — the card keeps comments/attachments/checklist/
links/assignees/history; no recreate-and-close, no source stub. Recorded
on the card with the implementation implications (ref re-mint under the
target project with no alias, milestone/project-label scrub, dual-board
authorization, no completed_at stamp, events on both boards). Priority
bumped to high; unblocked and ready for implementation.

2026-07-31 — **KANBAN-034 implemented + two latent bugs found in passing;
harness tickets 036/037/038 implemented via parallel agents.**

Parallel session shape: two coder agents (confined to src/test_support.rs
and to non-cards test modules respectively, no cargo/git allowed) while the
maintainer implemented TransferCard. Integration was synchronous afterward:
clippy --all-targets clean, full suite, then hunk-split conventional
commits. Worked well; the file partitioning is what made it conflict-free.

- **TransferCard (KANBAN-034, relocate-in-place per sienna's decision).**
  Additive proto (TransferCard RPC, CardTransferred event tag 50), matrix
  entry KanbanCard+edit on the source card plus a handler-side edit check on
  the target board (dual authorization — one header object can't span two
  projects). One tx: re-home project/board/column, re-mint ref under the
  target project (previous_ref returned; no alias), milestone cleared,
  project-scoped labels scrubbed (globals survive), completed_at follows
  only the is_done rule, CardTransferred on BOTH boards with the full card
  hydrated at dispatch, search index reindexed, permission parent tuple
  re-homed. Idempotency replay recovers previous_ref from the event payload.
- **Latent bug 1: fetch_labels panicked on global labels.** project_id is
  NULL for globals (migration 0032) but decoded as non-optional Id — any
  card carrying a global label crashed every full-card read. Found by the
  TransferCard test attaching a global label. Fixed (Option decode).
- **Latent bug 2: live CardMoved events carried empty columns.** The
  MoveCard handler writes from_column/to_column/to_position in the event
  payload; the outbox mapped from_column_id/column_id/position. Stored
  payloads were correct (which is why KANBAN-030's event-log backfill
  worked) — only the live stream mapping was wrong, and the outbox unit
  test mirrored the wrong keys. Fixed both; test now documents that keys
  must match the handler.
- **KANBAN-036/037 (agent):** startup reaper for stale testcontainers
  (label + sso<hex>-name based, age-filtered, never blanket-prunes
  networks) + with_retry around gateway bootstrap calls and a readiness
  settle. Reap window tightened 30m → 10m at integration: a full suite is
  ~2–4 min, so 10m still protects concurrent runs but stops iteration-loop
  accumulation (observed today: 6 concurrent stacks, 66 containers, gateway
  flaking from host contention).
- **KANBAN-038 (agent):** env-reading test helpers in boards.rs/projects.rs
  now self-bootstrap via containers::setup(); verified `cargo test
  services::boards::tests::subscribe` standalone 7/7 (was 2/7).

2026-07-31 — **Smart commit integration RFC drafted
(docs/development/smart-commits.md, draft-sunbeam-kanban-smart-commits-00).**
Designed interactively with sienna using the sunbeam-rfc-design skill
(IETF structure, RFC 2119 discipline). Locked decisions: Jira-style
command grammar (#fixes/#comment/#time/#label/#assign) but ZERO process
actions — commits never move cards between columns/projects (explicit
anti-goal, "breaks processes higher than commits"); any branch (no
process actions => no default-branch gating); authority =
GitHub-authenticated PUSHER mapped via github_identities (verified login
trait preferred; never commit author email — spoofing defeated by
construction); project→repo mapping capped at 50 as a project-management
hygiene gate (also acceptance + bare-ref resolution); per-command audit
rows, pushes never fail, summary comments list successes only; placement =
extend the KANBAN-019 receiver with push events (no CLI hooks — sienna
dislikes them). The #time command forces a new TimeTrackingService
(specified in RFC section 7). Filed KANBAN-041 (phase 1, high) and
KANBAN-042 (TimeTrackingService/phase 3, medium, depends_on 041).

2026-07-31 — **RFC out for review: PR #3** (docs/smart-commits-rfc branched
off origin/mainline so the doc reviews standalone, independent of the
unreleased mainline commits). Assigned to tony (GitHub: mckenzietony,
resolved via org member list). Review focus posted in the PR body:
grammar semantics, pusher-identity authorization, time-tracking totals,
rollout phasing + the sso-gateway verified-login dependency.

2026-07-31 — **KANBAN-024 implemented after a schema reality-check; lesson
recorded.** I had marked 024 blocked upstream after reading the
sso-gateway repo's deploy/kratos-identity.schema.json (email-only).
Sienna: check the DEPLOYED schemas via the CLI. Live directory truth: the
'employee' schema carries given_name/family_name/tenant_id/email, all
populated. display_name hydration shipped from those traits
(resolve_profile generalizes the 035 resolver; harness schema now mirrors
employee). SSO-028 narrowed to the only real gap (avatar_url, low) and
024's dependency on it removed. Lesson now documented in
.maintainer/interfaces.md and identity_client.rs: never reason about
identity traits from the vendored schema file — query the live directory
(sunbeam user get/list). CLI upgraded 3.2.1 → 3.3.0 locally to gain the
--unblocked flag. Data note flagged to sienna: directory has her
family_name as 'Satterthwaite', git config says 'Satterwhite' — hydration
shows whatever the directory says.
