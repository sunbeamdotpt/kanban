/**
 * fixtures/teardown.ts
 *
 * After-each teardown helpers: delete the test project (cascades cards,
 * boards, comments, attachments), revoke Keto tuples, and delete the Kratos
 * identity.
 *
 * Usage in specs that do manual setup:
 *   import { teardownProject, teardownIdentity } from "../fixtures/teardown";
 *   test.afterEach(async () => {
 *     await teardownProject(bearerToken, projectId);
 *     await teardownIdentity(identityId);
 *   });
 *
 * The auth.ts fixture wires identity deletion automatically for authedPage /
 * ketoDeniedPage. This file handles additional project-level cleanup for
 * specs that create projects via the API (seed.ts).
 */

import { createKanbanApiClient } from "./kanban-api";

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const KETO_WRITE_URL =
  process.env.KETO_WRITE_URL ?? "http://localhost:4467";

// ── Project teardown ─────────────────────────────────────────────────────────

/**
 * Deletes a project via the API. Cascades: boards, cards, comments,
 * attachments, forgejo_links, event_log rows (DB-level cascade).
 */
export async function teardownProject(
  bearerToken: string,
  projectId: string,
): Promise<void> {
  if (!projectId) return;
  try {
    const api = createKanbanApiClient(bearerToken);
    await api.deleteProject(projectId);
  } catch (err) {
    // Non-fatal — project may already be deleted by the test.
    console.warn(`[teardown] deleteProject(${projectId}) failed:`, err);
  }
}

// ── Identity teardown ────────────────────────────────────────────────────────

/**
 * Deletes a Kratos identity. Also invalidates all its sessions.
 */
export async function teardownIdentity(identityId: string): Promise<void> {
  if (!identityId) return;
  try {
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identityId}`, {
      method: "DELETE",
    });
  } catch (err) {
    console.warn(`[teardown] deleteIdentity(${identityId}) failed:`, err);
  }
}

// ── Keto tuple teardown ──────────────────────────────────────────────────────

export interface KetoTuple {
  namespace: string;
  object: string;
  relation: string;
  subject_id: string;
}

/**
 * Revokes a list of Keto relation tuples. Silently ignores 404s.
 */
export async function teardownKetoTuples(tuples: KetoTuple[]): Promise<void> {
  await Promise.all(
    tuples.map(async (tuple) => {
      const params = new URLSearchParams({
        namespace: tuple.namespace,
        object: tuple.object,
        relation: tuple.relation,
        subject_id: tuple.subject_id,
      });
      try {
        await fetch(
          `${KETO_WRITE_URL}/admin/relation-tuples?${params}`,
          { method: "DELETE" },
        );
      } catch (err) {
        console.warn(`[teardown] revokeKetoTuple failed:`, tuple, err);
      }
    }),
  );
}
