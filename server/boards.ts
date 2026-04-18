/**
 * BoardService RPC handlers.
 */

import sql from "./db.ts";
import { RpcError, type RpcContext } from "./rpc.ts";
import {
  type Board,
  type BoardDetail,
  type Column,
  type Card,
  Role,
  priorityFromString,
} from "../gen/kanban/v1/kanban_pb.ts";
import { checkAccess, getProjectIdForBoard, getProjectIdForColumn } from "./permissions.ts";

// ─── Helpers ───────────────────────────────────────────────────────────────

function toBoard(row: Record<string, unknown>): Board {
  return {
    id: row.id as string,
    projectId: row.project_id as string,
    name: row.name as string,
    slug: row.slug as string,
    description: (row.description as string) ?? "",
    createdAt: (row.created_at as Date).toISOString(),
    updatedAt: (row.updated_at as Date).toISOString(),
  };
}

function parseJsonb<T>(val: unknown): T[] {
  if (Array.isArray(val)) return val;
  if (typeof val === "string") { try { const p = JSON.parse(val); return Array.isArray(p) ? p : []; } catch { return []; } }
  return [];
}

function toCard(row: Record<string, unknown>): Card {
  return {
    id: row.id as string,
    columnId: row.column_id as string,
    boardId: row.board_id as string,
    title: row.title as string,
    description: (row.description as string) ?? "",
    position: row.position as number,
    priority: priorityFromString(row.priority as string),
    dueDate: row.due_date ? (row.due_date as Date).toISOString() : "",
    labels: parseJsonb(row.labels),
    assignees: parseJsonb(row.assignees),
    forgejoLinks: parseJsonb(row.forgejo_links),
    createdBy: row.created_by as string,
    createdAt: (row.created_at as Date).toISOString(),
    updatedAt: (row.updated_at as Date).toISOString(),
    attachments: [],
  };
}

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

// ─── ListBoards ────────────────────────────────────────────────────────────

export async function listBoards(
  req: { projectSlug: string },
  ctx: RpcContext,
): Promise<{ boards: Board[] }> {
  const [project] = await sql`SELECT id FROM projects WHERE slug = ${req.projectSlug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.VIEWER))) {
    throw new RpcError("permission_denied", "No access to this project");
  }

  const rows = await sql`
    SELECT * FROM boards WHERE project_id = ${project.id} ORDER BY created_at
  `;
  return { boards: rows.map(toBoard) };
}

// ─── GetBoard ──────────────────────────────────────────────────────────────

export async function getBoard(
  req: { boardId: string },
  ctx: RpcContext,
): Promise<{ board: BoardDetail }> {
  const [boardRow] = await sql`SELECT * FROM boards WHERE id = ${req.boardId}`;
  if (!boardRow) throw new RpcError("not_found", "Board not found");

  if (!(await checkAccess(boardRow.project_id, ctx.identity.id, Role.VIEWER))) {
    throw new RpcError("permission_denied", "No access");
  }

  const columnRows = await sql`
    SELECT * FROM columns WHERE board_id = ${req.boardId} ORDER BY position
  `;
  const cardRows = await sql`
    SELECT * FROM cards WHERE board_id = ${req.boardId} ORDER BY position
  `;

  const cardsByColumn = new Map<string, Card[]>();
  for (const card of cardRows) {
    const colId = card.column_id as string;
    if (!cardsByColumn.has(colId)) cardsByColumn.set(colId, []);
    cardsByColumn.get(colId)!.push(toCard(card));
  }

  const columns: Column[] = columnRows.map((col) => ({
    id: col.id as string,
    boardId: col.board_id as string,
    title: col.title as string,
    position: col.position as number,
    color: (col.color as string) ?? "",
    wipLimit: (col.wip_limit as number) ?? 0,
    cards: cardsByColumn.get(col.id as string) ?? [],
  }));

  return {
    board: {
      board: toBoard(boardRow),
      columns,
    },
  };
}

// ─── CreateBoard ───────────────────────────────────────────────────────────

export async function createBoard(
  req: { projectSlug: string; name: string; slug?: string; description?: string; templateId?: string },
  ctx: RpcContext,
): Promise<{ board: Board }> {
  const [project] = await sql`SELECT id FROM projects WHERE slug = ${req.projectSlug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  if (!req.name) throw new RpcError("invalid_argument", "Name is required");

  const slug = req.slug || slugify(req.name);

  const [existing] = await sql`
    SELECT id FROM boards WHERE project_id = ${project.id} AND slug = ${slug}
  `;
  if (existing) throw new RpcError("already_exists", "Board slug already exists in this project");

  const [boardRow] = await sql`
    INSERT INTO boards (project_id, name, slug, description)
    VALUES (${project.id}, ${req.name}, ${slug}, ${req.description ?? ""})
    RETURNING *
  `;

  // If a template was specified, create columns from it
  if (req.templateId) {
    const [template] = await sql`SELECT columns FROM board_templates WHERE id = ${req.templateId}`;
    if (template) {
      const cols = template.columns as { title: string; position: number; color?: string; wip_limit?: number }[];
      for (const col of cols) {
        await sql`
          INSERT INTO columns (board_id, title, position, color, wip_limit)
          VALUES (${boardRow.id}, ${col.title}, ${col.position}, ${col.color ?? null}, ${col.wip_limit ?? null})
        `;
      }
    }
  }

  return { board: toBoard(boardRow) };
}

// ─── UpdateBoard ───────────────────────────────────────────────────────────

export async function updateBoard(
  req: { boardId: string; name?: string; description?: string },
  ctx: RpcContext,
): Promise<{ board: Board }> {
  const projectId = await getProjectIdForBoard(req.boardId);
  if (!projectId) throw new RpcError("not_found", "Board not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  const [current] = await sql`SELECT * FROM boards WHERE id = ${req.boardId}`;
  const name = req.name || current.name;
  const description = req.description ?? current.description;

  const [row] = await sql`
    UPDATE boards SET name = ${name}, description = ${description}, updated_at = now()
    WHERE id = ${req.boardId}
    RETURNING *
  `;

  return { board: toBoard(row) };
}

// ─── DeleteBoard ───────────────────────────────────────────────────────────

export async function deleteBoard(
  req: { boardId: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const projectId = await getProjectIdForBoard(req.boardId);
  if (!projectId) throw new RpcError("not_found", "Board not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.ADMIN))) {
    throw new RpcError("permission_denied", "Admin access required");
  }

  await sql`DELETE FROM boards WHERE id = ${req.boardId}`;
  return {};
}

// ─── CreateColumn ──────────────────────────────────────────────────────────

export async function createColumn(
  req: { boardId: string; title: string; color?: string; wipLimit?: number },
  ctx: RpcContext,
): Promise<{ column: Column }> {
  const projectId = await getProjectIdForBoard(req.boardId);
  if (!projectId) throw new RpcError("not_found", "Board not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  if (!req.title) throw new RpcError("invalid_argument", "Title is required");

  // Get next position
  const [maxPos] = await sql`
    SELECT COALESCE(MAX(position), -1) as max_pos FROM columns WHERE board_id = ${req.boardId}
  `;
  const position = (maxPos.max_pos as number) + 1;

  const [row] = await sql`
    INSERT INTO columns (board_id, title, position, color, wip_limit)
    VALUES (${req.boardId}, ${req.title}, ${position}, ${req.color ?? null}, ${req.wipLimit ?? null})
    RETURNING *
  `;

  return {
    column: {
      id: row.id,
      boardId: row.board_id,
      title: row.title,
      position: row.position,
      color: row.color ?? "",
      wipLimit: row.wip_limit ?? 0,
      cards: [],
    },
  };
}

// ─── UpdateColumn ──────────────────────────────────────────────────────────

export async function updateColumn(
  req: { columnId: string; title?: string; position?: number; color?: string; wipLimit?: number },
  ctx: RpcContext,
): Promise<{ column: Column }> {
  const projectId = await getProjectIdForColumn(req.columnId);
  if (!projectId) throw new RpcError("not_found", "Column not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  const [current] = await sql`SELECT * FROM columns WHERE id = ${req.columnId}`;
  const title = req.title || current.title;
  const position = req.position ?? current.position;
  const color = req.color ?? current.color;
  const wipLimit = req.wipLimit ?? current.wip_limit;

  const [row] = await sql`
    UPDATE columns
    SET title = ${title}, position = ${position}, color = ${color}, wip_limit = ${wipLimit}
    WHERE id = ${req.columnId}
    RETURNING *
  `;

  return {
    column: {
      id: row.id,
      boardId: row.board_id,
      title: row.title,
      position: row.position,
      color: row.color ?? "",
      wipLimit: row.wip_limit ?? 0,
      cards: [],
    },
  };
}

// ─── DeleteColumn ──────────────────────────────────────────────────────────

export async function deleteColumn(
  req: { columnId: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const projectId = await getProjectIdForColumn(req.columnId);
  if (!projectId) throw new RpcError("not_found", "Column not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  await sql`DELETE FROM columns WHERE id = ${req.columnId}`;
  return {};
}

// ─── ReorderColumns ────────────────────────────────────────────────────────

export async function reorderColumns(
  req: { boardId: string; columnIds: string[] },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const projectId = await getProjectIdForBoard(req.boardId);
  if (!projectId) throw new RpcError("not_found", "Board not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  for (let i = 0; i < req.columnIds.length; i++) {
    await sql`UPDATE columns SET position = ${i} WHERE id = ${req.columnIds[i]} AND board_id = ${req.boardId}`;
  }

  return {};
}
