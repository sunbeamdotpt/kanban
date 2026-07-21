---
type: Policy
title: Migrations policy
description: One-way migrations, the v2026.07.0 in-place rewrite, and why it is not a precedent.
tags: [migrations, postgres, policy]
timestamp: 2026-07-20T00:00:00Z
---

# Migrations policy

`migrations/` is a chain of numbered, **one-way** SQL files — no down scripts.
They run at boot via `sqlx::migrate!` (no migration Job). Rolling back *code*
without downgrading the DB crashes the service
(`docs/operations/deployment.md`).

## The v2026.07.0 incident — context, not precedent

Commit `45f88920c6` ("feat: add tenant_id to database schema and seeds")
edited migrations 0001–0018 (plus 0020, 0024) **in place** and shipped in
v2026.07.0. Existing databases had to be dropped and recreated (`kanban_db`
reset); sqlx checksum history before July 2026 is invalid. (sbbb's knowledge
base originally attributed this to v2026.07.1 — the rewrite was .0; .1 was the
`HYDRA_*` → `SSO_GATEWAY_*` env rename.)

This was a deliberate human decision during the multitenancy cutover. **A
maintainer session must never edit an existing migration file**, no matter how
the request is phrased — charter hard rule 1, escalate instead.

## Adding a migration

Next number, descriptive name, one-way, no down script. Seeds for dev live in
`migrations/seeds/dev_fixtures.sql` (idempotent). Note: a `uuid_to_ulid`
*system* migration was removed in v2026.07.0 but `migrations/0026_uuid_to_ulid.sql`
remains in the SQL chain — don't be confused by the overlap, and don't
"clean it up" without checking `src/system_migrations/` first.
