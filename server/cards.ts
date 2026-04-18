/**
 * CardService RPC handlers.
 */

import sql from "./db.ts";
import { RpcError, type RpcContext } from "./rpc.ts";
import {
  type Card,
  type Attachment,
  Role,
  Priority,
  PRIORITY_LABELS,
  priorityFromString,
} from "../gen/kanban/v1/kanban_pb.ts";
import {
  checkAccess,
  getProjectIdForColumn,
  getProjectIdForCard,
  getProjectIdForAttachment,
} from "./permissions.ts";
import { syncCard, removeCard } from "./opensearch.ts";
import { presignPutUrl, presignGetUrl } from "./s3-presign.ts";
import { deleteObject } from "./s3.ts";

// ─── Helpers ───────────────────────────────────────────────────────────────

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

function toAttachment(row: Record<string, unknown>): Attachment {
  return {
    id: row.id as string,
    cardId: row.card_id as string,
    filename: row.filename as string,
    mimetype: row.mimetype as string,
    size: Number(row.size),
    uploadedBy: row.uploaded_by as string,
    createdAt: (row.created_at as Date).toISOString(),
  };
}

async function getCardSyncContext(boardId: string, columnId: string) {
  const [board] = await sql`
    SELECT b.name as board_name, p.id as project_id, p.name as project_name
    FROM boards b JOIN projects p ON p.id = b.project_id
    WHERE b.id = ${boardId}
  `;
  const [col] = await sql`SELECT title FROM columns WHERE id = ${columnId}`;
  return {
    boardName: board?.board_name ?? "",
    projectId: board?.project_id ?? "",
    projectName: board?.project_name ?? "",
    columnTitle: col?.title ?? "",
  };
}

// ─── CreateCard ──────────────────────────────────────────────────────────��─

export async function createCard(
  req: {
    columnId: string;
    title: string;
    description?: string;
    priority?: Priority;
    dueDate?: string;
    labels?: Card["labels"];
    assigneeIds?: string[];
  },
  ctx: RpcContext,
): Promise<{ card: Card }> {
  const projectId = await getProjectIdForColumn(req.columnId);
  if (!projectId) throw new RpcError("not_found", "Column not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  if (!req.title) throw new RpcError("invalid_argument", "Title is required");

  // Get board_id from column
  const [col] = await sql`SELECT board_id FROM columns WHERE id = ${req.columnId}`;
  if (!col) throw new RpcError("not_found", "Column not found");

  // Get next position
  const [maxPos] = await sql`
    SELECT COALESCE(MAX(position), -1) as max_pos FROM cards WHERE column_id = ${req.columnId}
  `;
  const position = (maxPos.max_pos as number) + 1;

  const priority = req.priority ? PRIORITY_LABELS[req.priority] : "medium";
  const labels = JSON.stringify(req.labels ?? []);
  const assignees = JSON.stringify(
    (req.assigneeIds ?? []).map((id) => ({ id, username: "", avatar_url: "" })),
  );

  const [row] = await sql`
    INSERT INTO cards (column_id, board_id, title, description, position, priority, due_date, labels, assignees, created_by)
    VALUES (
      ${req.columnId}, ${col.board_id}, ${req.title}, ${req.description ?? ""},
      ${position}, ${priority}, ${req.dueDate ?? null},
      ${labels}::jsonb, ${assignees}::jsonb, ${ctx.identity.id}
    )
    RETURNING *
  `;

  const card = toCard(row);

  // Sync to OpenSearch
  const syncCtx = await getCardSyncContext(row.board_id, row.column_id);
  syncCard(row, syncCtx);

  return { card };
}

// ─── GetCard ────────────────────────────��─────────────────��────────────────

export async function getCard(
  req: { cardId: string },
  ctx: RpcContext,
): Promise<{ card: Card }> {
  const projectId = await getProjectIdForCard(req.cardId);
  if (!projectId) throw new RpcError("not_found", "Card not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.VIEWER))) {
    throw new RpcError("permission_denied", "No access");
  }

  const [row] = await sql`SELECT * FROM cards WHERE id = ${req.cardId}`;
  if (!row) throw new RpcError("not_found", "Card not found");

  const card = toCard(row);

  // Load attachments
  const attachments = await sql`
    SELECT * FROM card_attachments WHERE card_id = ${req.cardId} ORDER BY created_at
  `;
  card.attachments = attachments.map(toAttachment);

  return { card };
}

// ─── UpdateCard ────────────────────────────────────────────────────────────

export async function updateCard(
  req: {
    cardId: string;
    title?: string;
    description?: string;
    priority?: Priority;
    dueDate?: string;
    labels?: Card["labels"];
    assignees?: Card["assignees"];
    forgejoLinks?: Card["forgejoLinks"];
  },
  ctx: RpcContext,
): Promise<{ card: Card }> {
  const projectId = await getProjectIdForCard(req.cardId);
  if (!projectId) throw new RpcError("not_found", "Card not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  const [current] = await sql`SELECT * FROM cards WHERE id = ${req.cardId}`;
  if (!current) throw new RpcError("not_found", "Card not found");

  const title = req.title || current.title;
  const description = req.description ?? current.description;
  const priority = req.priority ? PRIORITY_LABELS[req.priority] : current.priority;
  const dueDate = req.dueDate ?? current.due_date;
  const labels = req.labels !== undefined ? JSON.stringify(req.labels) : JSON.stringify(current.labels);
  const assignees = req.assignees !== undefined ? JSON.stringify(req.assignees) : JSON.stringify(current.assignees);
  const forgejoLinks = req.forgejoLinks !== undefined ? JSON.stringify(req.forgejoLinks) : JSON.stringify(current.forgejo_links);

  const [row] = await sql`
    UPDATE cards
    SET title = ${title}, description = ${description}, priority = ${priority},
        due_date = ${dueDate}, labels = ${labels}::jsonb, assignees = ${assignees}::jsonb,
        forgejo_links = ${forgejoLinks}::jsonb, updated_at = now()
    WHERE id = ${req.cardId}
    RETURNING *
  `;

  const card = toCard(row);

  const syncCtx = await getCardSyncContext(row.board_id, row.column_id);
  syncCard(row, syncCtx);

  return { card };
}

// ─── DeleteCard ───────────────���────────────────────────��───────────────────

export async function deleteCard(
  req: { cardId: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const projectId = await getProjectIdForCard(req.cardId);
  if (!projectId) throw new RpcError("not_found", "Card not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  // Delete attachments from S3
  const attachments = await sql`
    SELECT s3_key FROM card_attachments WHERE card_id = ${req.cardId}
  `;
  for (const a of attachments) {
    deleteObject(a.s3_key);
  }

  await sql`DELETE FROM cards WHERE id = ${req.cardId}`;
  removeCard(req.cardId);

  return {};
}

// ─── MoveCard ───────────────────��─────────────────────────��────────────────

export async function moveCard(
  req: { cardId: string; targetColumnId: string; position: number },
  ctx: RpcContext,
): Promise<{ card: Card }> {
  const projectId = await getProjectIdForCard(req.cardId);
  if (!projectId) throw new RpcError("not_found", "Card not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  // Shift cards in target column to make room
  await sql`
    UPDATE cards SET position = position + 1
    WHERE column_id = ${req.targetColumnId} AND position >= ${req.position}
  `;

  const [row] = await sql`
    UPDATE cards
    SET column_id = ${req.targetColumnId}, position = ${req.position}, updated_at = now()
    WHERE id = ${req.cardId}
    RETURNING *
  `;
  if (!row) throw new RpcError("not_found", "Card not found");

  const card = toCard(row);

  const syncCtx = await getCardSyncContext(row.board_id, row.column_id);
  syncCard(row, syncCtx);

  return { card };
}

// ─── CreateAttachment ──────────────────────────────────────────────────────

export async function createAttachment(
  req: { cardId: string; filename: string; mimetype: string; size: number },
  ctx: RpcContext,
): Promise<{ attachment: Attachment; uploadUrl: string }> {
  const projectId = await getProjectIdForCard(req.cardId);
  if (!projectId) throw new RpcError("not_found", "Card not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  if (!req.filename) throw new RpcError("invalid_argument", "Filename is required");

  const s3Key = `attachments/${req.cardId}/${crypto.randomUUID()}/${req.filename}`;

  const [row] = await sql`
    INSERT INTO card_attachments (card_id, filename, mimetype, size, s3_key, uploaded_by)
    VALUES (${req.cardId}, ${req.filename}, ${req.mimetype || "application/octet-stream"}, ${req.size || 0}, ${s3Key}, ${ctx.identity.id})
    RETURNING *
  `;

  const uploadUrl = await presignPutUrl(s3Key, req.mimetype || "application/octet-stream");

  return {
    attachment: toAttachment(row),
    uploadUrl,
  };
}

// ─── ListAttachments ───────────────────────────��───────────────────────────

export async function listAttachments(
  req: { cardId: string },
  ctx: RpcContext,
): Promise<{ attachments: Attachment[] }> {
  const projectId = await getProjectIdForCard(req.cardId);
  if (!projectId) throw new RpcError("not_found", "Card not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.VIEWER))) {
    throw new RpcError("permission_denied", "No access");
  }

  const rows = await sql`
    SELECT * FROM card_attachments WHERE card_id = ${req.cardId} ORDER BY created_at
  `;
  return { attachments: rows.map(toAttachment) };
}

// ─── DeleteAttachment ─────────────────────────────────────────────��────────

export async function deleteAttachment(
  req: { attachmentId: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const projectId = await getProjectIdForAttachment(req.attachmentId);
  if (!projectId) throw new RpcError("not_found", "Attachment not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.EDITOR))) {
    throw new RpcError("permission_denied", "Editor access required");
  }

  const [att] = await sql`SELECT s3_key FROM card_attachments WHERE id = ${req.attachmentId}`;
  if (att) {
    deleteObject(att.s3_key);
  }

  await sql`DELETE FROM card_attachments WHERE id = ${req.attachmentId}`;
  return {};
}

// ─── GetAttachmentDownloadUrl ─────────────────���─────────────────���──────────

export async function getAttachmentDownloadUrl(
  req: { attachmentId: string },
  ctx: RpcContext,
): Promise<{ url: string }> {
  const projectId = await getProjectIdForAttachment(req.attachmentId);
  if (!projectId) throw new RpcError("not_found", "Attachment not found");

  if (!(await checkAccess(projectId, ctx.identity.id, Role.VIEWER))) {
    throw new RpcError("permission_denied", "No access");
  }

  const [att] = await sql`SELECT s3_key FROM card_attachments WHERE id = ${req.attachmentId}`;
  if (!att) throw new RpcError("not_found", "Attachment not found");

  const url = await presignGetUrl(att.s3_key);
  return { url };
}
