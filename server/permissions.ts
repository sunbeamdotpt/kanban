/**
 * Permission checking for project access.
 */

import sql from "./db.ts";
import { Role, roleFromString } from "../gen/kanban/v1/kanban_pb.ts";

const ROLE_WEIGHT: Record<Role, number> = {
  [Role.UNSPECIFIED]: 0,
  [Role.VIEWER]: 1,
  [Role.EDITOR]: 2,
  [Role.ADMIN]: 3,
  [Role.OWNER]: 4,
};

/**
 * Check if a user has at least the given role on a project.
 * Returns the user's actual role, or null if no access.
 */
export async function getUserRole(
  projectId: string,
  userId: string,
): Promise<Role | null> {
  // Check project visibility first
  const [project] = await sql`
    SELECT visibility, owner_id FROM projects WHERE id = ${projectId}
  `;
  if (!project) return null;

  // Owner always has full access
  if (project.owner_id === userId) return Role.OWNER;

  // Check explicit membership
  const [member] = await sql`
    SELECT role FROM project_members
    WHERE project_id = ${projectId} AND user_id = ${userId}
  `;
  if (member) return roleFromString(member.role);

  // Check visibility-based access
  if (project.visibility === "public") return Role.VIEWER;
  if (project.visibility === "members") return Role.VIEWER;

  return null;
}

/**
 * Check if a user has at least the given minimum role.
 */
export async function checkAccess(
  projectId: string,
  userId: string,
  minRole: Role,
): Promise<boolean> {
  const role = await getUserRole(projectId, userId);
  if (role === null) return false;
  return ROLE_WEIGHT[role] >= ROLE_WEIGHT[minRole];
}

/**
 * Get project ID from a board ID.
 */
export async function getProjectIdForBoard(boardId: string): Promise<string | null> {
  const [row] = await sql`SELECT project_id FROM boards WHERE id = ${boardId}`;
  return row?.project_id ?? null;
}

/**
 * Get project ID from a column ID.
 */
export async function getProjectIdForColumn(columnId: string): Promise<string | null> {
  const [row] = await sql`
    SELECT b.project_id FROM columns c
    JOIN boards b ON b.id = c.board_id
    WHERE c.id = ${columnId}
  `;
  return row?.project_id ?? null;
}

/**
 * Get project ID from a card ID.
 */
export async function getProjectIdForCard(cardId: string): Promise<string | null> {
  const [row] = await sql`
    SELECT b.project_id FROM cards c
    JOIN boards b ON b.id = c.board_id
    WHERE c.id = ${cardId}
  `;
  return row?.project_id ?? null;
}

/**
 * Get project ID from an attachment ID.
 */
export async function getProjectIdForAttachment(attachmentId: string): Promise<string | null> {
  const [row] = await sql`
    SELECT b.project_id FROM card_attachments ca
    JOIN cards c ON c.id = ca.card_id
    JOIN boards b ON b.id = c.board_id
    WHERE ca.id = ${attachmentId}
  `;
  return row?.project_id ?? null;
}
