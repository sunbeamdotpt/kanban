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
