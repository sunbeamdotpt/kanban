// Keto namespaces owned by apps/kanban. Mounted into the workspace keto via
// `sunbeam.workspace.yaml` directory mount at `/etc/namespaces/`; Keto v26
// merges every `*.config.ts` file in the directory without conflict (see
// `project_keto_namespaces_directory_mode.md`).
//
// Mirror of v1's KanbanProject / KanbanBoard / KanbanCard design (kanban-plan-v2
// §"Keto namespace design"), preserved verbatim here, plus the synthetic
// `_kanban_health` namespace required by the readiness probe (Stage 1d /
// Pre-mortem 1 mitigation).
//
// ─────────────────────────────────────────────────────────────────────────────
// Role-transition rules (closes Pre-mortem 1, 4, and Pre-mortem 5 / MF-8 part 2)
// ─────────────────────────────────────────────────────────────────────────────
//
//   * Owner is irreplaceable in-place. Transferring ownership writes the new
//     `owner` tuple FIRST, then deletes the old one. Never the inverse — a
//     write-fail on the new owner with the old owner already gone leaves the
//     project orphaned and the kanban service cannot self-repair (Keto refuses
//     to re-grant `owner` on its own and there is no other authz path back in).
//     Closes Pre-mortem 1.
//
//   * Downgrading editor → viewer (or revoking entirely) MUST trigger a write
//     to the per-subject logout watermark — i.e. `auth.logout.{sub}` in Valkey
//     — within the same Keto write batch. This is enforced in
//     `apps/kanban/src/auth/membership.rs`, NOT by Keto. Without the watermark
//     bump, the now-downgraded user keeps receiving privileged stream events
//     for up to 30s on cadence-recheck and indefinitely on token-expiry-only
//     guards. Closes Pre-mortem 4 / MF-8 part 1.
//
//   * Namespace evolution is dual-write, never delete-in-place. To change a
//     relation:
//       1. Introduce `relation_v2` alongside `relation_v1` in this file.
//       2. Dual-write tuples for both `_v1` and `_v2` for one full release.
//       3. Switch `keto_dispatch` Check sites to `relation_v2`.
//       4. Drop `relation_v1` reads in the next release.
//       5. Delete the `relation_v1` field from this file in the release after.
//     Closes Pre-mortem 5 / MF-8 part 2. Operator recipe lives in
//     `apps/kanban/AGENTS.md` once Stage 7 lands.
//
// ─────────────────────────────────────────────────────────────────────────────

import { Context, Namespace, SubjectSet } from "@ory/keto-namespace-types"

// `User` is declared by `libs/sunbeam-g2v/.integration/keto-namespaces.config.ts`
// and other workspace projects too. Re-declared here so this file parses
// standalone (`tsc --noLib --noEmit`); Keto v26 directory mode merges the
// declarations without conflict.
class User implements Namespace {}

// ─────────────────────────────────────────────────────────────────────────────
// KanbanProject — top-level container. Owner is a singleton; admin/editor/
// viewer are sets. Permission ladder: view ⊆ edit ⊆ manage ⊆ delete (delete is
// owner-only).
// ─────────────────────────────────────────────────────────────────────────────
class KanbanProject implements Namespace {
  related: {
    // Singleton in practice — the membership service refuses to write a second
    // `owner` tuple. Keto itself does not enforce singleton semantics; that
    // invariant lives in `apps/kanban/src/auth/membership.rs`.
    owner: User[]
    admin: User[]
    editor: User[]
    viewer: User[]
  }

  permits = {
    view: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject) ||
      this.related.admin.includes(ctx.subject) ||
      this.related.editor.includes(ctx.subject) ||
      this.related.viewer.includes(ctx.subject),

    edit: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject) ||
      this.related.admin.includes(ctx.subject) ||
      this.related.editor.includes(ctx.subject),

    manage: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject) ||
      this.related.admin.includes(ctx.subject),

    delete: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject),
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// KanbanBoard — child of KanbanProject. Inherits the project ladder via
// `parent->view`/`->edit`/`->manage`, plus board-scoped `viewer` and `editor`
// grants for users who should see/edit only this board within the project.
// ─────────────────────────────────────────────────────────────────────────────
class KanbanBoard implements Namespace {
  related: {
    parent: KanbanProject[]
    viewer: User[]
    editor: User[]
  }

  permits = {
    view: (ctx: Context): boolean =>
      this.related.parent.traverse((p) => p.permits.view(ctx)) ||
      this.related.viewer.includes(ctx.subject) ||
      this.related.editor.includes(ctx.subject),

    edit: (ctx: Context): boolean =>
      this.related.parent.traverse((p) => p.permits.edit(ctx)) ||
      this.related.editor.includes(ctx.subject),

    manage: (ctx: Context): boolean =>
      this.related.parent.traverse((p) => p.permits.manage(ctx)),
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// KanbanCard — child of KanbanBoard. Assignees can edit their own cards
// (additive on top of board-level edit). `blocked_by` is a Card→Card relation
// used to express dependency edges in the data model; it is NOT consulted by
// any `permits` rule. This is intentional — blocked_by is data, not authz.
// ─────────────────────────────────────────────────────────────────────────────
class KanbanCard implements Namespace {
  related: {
    parent: KanbanBoard[]
    assignee: User[]
    // Dependency edge (e.g. "BEAM-204 cannot start until BEAM-180 is done").
    // Read by the kanban handler to render the dep graph; never read by
    // permits. Keep as Card[] so the relation type-checks under Keto OPL but
    // do not surface it through `permits`.
    blocked_by: KanbanCard[]
  }

  permits = {
    view: (ctx: Context): boolean =>
      this.related.parent.traverse((p) => p.permits.view(ctx)),

    edit: (ctx: Context): boolean =>
      this.related.parent.traverse((p) => p.permits.edit(ctx)) ||
      this.related.assignee.includes(ctx.subject),

    manage: (ctx: Context): boolean =>
      this.related.parent.traverse((p) => p.permits.manage(ctx)),
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// KanbanAggregatedBoard — a meta board spanning an explicit list of source
// boards. Explicit owner/admin/editor/viewer grants are checked first; if none
// match, access is derived from the contained KanbanBoard relations. This lets
// a user view an aggregate whenever they can view any of its source boards.
// ─────────────────────────────────────────────────────────────────────────────
class KanbanAggregatedBoard implements Namespace {
  related: {
    owner: User[]
    admin: User[]
    editor: User[]
    viewer: User[]
    source_board: KanbanBoard[]
  }

  permits = {
    view: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject) ||
      this.related.admin.includes(ctx.subject) ||
      this.related.editor.includes(ctx.subject) ||
      this.related.viewer.includes(ctx.subject) ||
      this.related.source_board.traverse((b) => b.permits.view(ctx)),

    edit: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject) ||
      this.related.admin.includes(ctx.subject) ||
      this.related.editor.includes(ctx.subject) ||
      this.related.source_board.traverse((b) => b.permits.edit(ctx)),

    manage: (ctx: Context): boolean =>
      this.related.owner.includes(ctx.subject) ||
      this.related.admin.includes(ctx.subject) ||
      this.related.source_board.traverse((b) => b.permits.manage(ctx)),
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// _kanban_health — synthetic namespace for the startup readiness gate
// (Pre-mortem 1, AC-3). The kanban service writes a known tuple at boot —
//
//     _kanban_health:health#probe@User:_kanban_startup_probe
//
// — and `/healthz/ready` calls `KetoClient::check_permission(_kanban_health,
// "health", "check", "_kanban_startup_probe")`. If true, namespace+keto are
// both reachable; if false (or transport error), the readiness probe returns
// 503. This catches the kanban-vs-keto startup race at deploy time rather
// than first-real-request time.
//
// Naming: leading underscore `_kanban_health` mirrors the `_self` convention
// already in use by `keto_dispatch` for SubjectScoped RPCs — distinguishes
// internal/synthetic namespaces from user-facing ones.
// ─────────────────────────────────────────────────────────────────────────────
class _kanban_health implements Namespace {
  related: {
    probe: User[]
  }

  permits = {
    check: (ctx: Context): boolean =>
      this.related.probe.includes(ctx.subject),
  }
}
