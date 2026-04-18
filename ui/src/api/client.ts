/**
 * Typed RPC client for kanban.v1 services.
 * Uses the Connect protocol (JSON over HTTP POST).
 */

async function call<Req, Res>(
  service: string,
  method: string,
  body: Req,
): Promise<Res> {
  const res = await fetch(`/kanban.v1.${service}/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Connect-Protocol-Version": "1",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message ?? `RPC failed: ${res.status}`);
  }

  return res.json();
}

// ─── Auth ──────────────────────────────────────────────────────────────────

export const auth = {
  getSession: () =>
    call<{}, { user: User }>("AuthService", "GetSession", {}),
};

// ─── Projects ──────────────────────────────────────────────────────────────

export const projects = {
  list: () =>
    call<{}, { projects: Project[] }>("ProjectService", "ListProjects", {}),
  get: (slug: string) =>
    call<{ slug: string }, { project: Project }>("ProjectService", "GetProject", { slug }),
  create: (req: { name: string; slug?: string; description?: string; visibility?: number }) =>
    call<typeof req, { project: Project }>("ProjectService", "CreateProject", req),
  update: (req: { slug: string; name?: string; description?: string; visibility?: number }) =>
    call<typeof req, { project: Project }>("ProjectService", "UpdateProject", req),
  delete: (slug: string) =>
    call<{ slug: string }, {}>("ProjectService", "DeleteProject", { slug }),
  listMembers: (projectSlug: string) =>
    call<{ projectSlug: string }, { members: ProjectMember[] }>("ProjectService", "ListMembers", { projectSlug }),
  addMember: (req: { projectSlug: string; userId: string; role?: number }) =>
    call<typeof req, { member: ProjectMember }>("ProjectService", "AddMember", req),
  updateMember: (req: { projectSlug: string; userId: string; role: number }) =>
    call<typeof req, { member: ProjectMember }>("ProjectService", "UpdateMember", req),
  removeMember: (req: { projectSlug: string; userId: string }) =>
    call<typeof req, {}>("ProjectService", "RemoveMember", req),
};

// ─── Boards ────────────────────────────────────────────────────────────────

export const boards = {
  list: (projectSlug: string) =>
    call<{ projectSlug: string }, { boards: Board[] }>("BoardService", "ListBoards", { projectSlug }),
  get: (boardId: string) =>
    call<{ boardId: string }, { board: BoardDetail }>("BoardService", "GetBoard", { boardId }),
  create: (req: { projectSlug: string; name: string; slug?: string; description?: string; templateId?: string }) =>
    call<typeof req, { board: Board }>("BoardService", "CreateBoard", req),
  update: (req: { boardId: string; name?: string; description?: string }) =>
    call<typeof req, { board: Board }>("BoardService", "UpdateBoard", req),
  delete: (boardId: string) =>
    call<{ boardId: string }, {}>("BoardService", "DeleteBoard", { boardId }),
  createColumn: (req: { boardId: string; title: string; color?: string; wipLimit?: number }) =>
    call<typeof req, { column: Column }>("BoardService", "CreateColumn", req),
  updateColumn: (req: { columnId: string; title?: string; position?: number; color?: string; wipLimit?: number }) =>
    call<typeof req, { column: Column }>("BoardService", "UpdateColumn", req),
  deleteColumn: (columnId: string) =>
    call<{ columnId: string }, {}>("BoardService", "DeleteColumn", { columnId }),
  reorderColumns: (req: { boardId: string; columnIds: string[] }) =>
    call<typeof req, {}>("BoardService", "ReorderColumns", req),
};

// ─── Cards ─────────────────────────────────────────────────────────────────

export const cards = {
  create: (req: {
    columnId: string;
    title: string;
    description?: string;
    priority?: number;
    dueDate?: string;
    labels?: { name: string; color: string }[];
  }) =>
    call<typeof req, { card: Card }>("CardService", "CreateCard", req),
  get: (cardId: string) =>
    call<{ cardId: string }, { card: Card }>("CardService", "GetCard", { cardId }),
  update: (req: {
    cardId: string;
    title?: string;
    description?: string;
    priority?: number;
    labels?: { name: string; color: string }[];
    assignees?: { id: string; username: string; avatarUrl: string }[];
    forgejoLinks?: { type: string; repo: string; number: number; url: string; title: string; state: string }[];
  }) =>
    call<typeof req, { card: Card }>("CardService", "UpdateCard", req),
  delete: (cardId: string) =>
    call<{ cardId: string }, {}>("CardService", "DeleteCard", { cardId }),
  move: (req: { cardId: string; targetColumnId: string; position: number }) =>
    call<typeof req, { card: Card }>("CardService", "MoveCard", req),
  createAttachment: (req: { cardId: string; filename: string; mimetype: string; size: number }) =>
    call<typeof req, { attachment: Attachment; uploadUrl: string }>("CardService", "CreateAttachment", req),
  listAttachments: (cardId: string) =>
    call<{ cardId: string }, { attachments: Attachment[] }>("CardService", "ListAttachments", { cardId }),
  deleteAttachment: (attachmentId: string) =>
    call<{ attachmentId: string }, {}>("CardService", "DeleteAttachment", { attachmentId }),
  getAttachmentDownloadUrl: (attachmentId: string) =>
    call<{ attachmentId: string }, { url: string }>("CardService", "GetAttachmentDownloadUrl", { attachmentId }),
};

// ─── Templates ─────────────────────────────────────────────────────────────

export const templates = {
  list: (projectId?: string) =>
    call<{ projectId?: string }, { templates: BoardTemplate[] }>("TemplateService", "ListTemplates", { projectId }),
  create: (req: {
    name: string;
    description?: string;
    columns: { title: string; position: number; color: string; wipLimit: number }[];
    projectId?: string;
    isGlobal?: boolean;
  }) =>
    call<typeof req, { template: BoardTemplate }>("TemplateService", "CreateTemplate", req),
  delete: (templateId: string) =>
    call<{ templateId: string }, {}>("TemplateService", "DeleteTemplate", { templateId }),
};

// ─── Forgejo ───────────────────────────────────────────────────────────────

export const forgejo = {
  search: (query: string, repo?: string) =>
    call<{ query: string; repo?: string }, { results: ForgejoSearchResult[] }>("ForgejoService", "Search", { query, repo }),
};

// ─── Types (client-side) ───────────────────────────────────────────────────

export interface User {
  id: string;
  email: string;
  name: string;
  avatarUrl: string;
}

export interface Project {
  id: string;
  name: string;
  slug: string;
  description: string;
  ownerId: string;
  visibility: number;
  createdAt: string;
  updatedAt: string;
  currentUserRole: number;
}

export interface ProjectMember {
  userId: string;
  email: string;
  name: string;
  avatarUrl: string;
  role: number;
  createdAt: string;
}

export interface Board {
  id: string;
  projectId: string;
  name: string;
  slug: string;
  description: string;
  createdAt: string;
  updatedAt: string;
}

export interface BoardDetail {
  board: Board;
  columns: Column[];
}

export interface Column {
  id: string;
  boardId: string;
  title: string;
  position: number;
  color: string;
  wipLimit: number;
  cards: Card[];
}

export interface Card {
  id: string;
  columnId: string;
  boardId: string;
  title: string;
  description: string;
  position: number;
  priority: number;
  dueDate: string;
  labels: { name: string; color: string }[];
  assignees: { id: string; username: string; avatarUrl: string }[];
  forgejoLinks: { type: string; repo: string; number: number; url: string; title: string; state: string }[];
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  attachments: Attachment[];
}

export interface Attachment {
  id: string;
  cardId: string;
  filename: string;
  mimetype: string;
  size: number;
  uploadedBy: string;
  createdAt: string;
}

export interface BoardTemplate {
  id: string;
  name: string;
  description: string;
  columns: { title: string; position: number; color: string; wipLimit: number }[];
  createdBy: string;
  isGlobal: boolean;
  projectId: string;
  createdAt: string;
}

export interface ForgejoSearchResult {
  type: string;
  repo: string;
  number: number;
  url: string;
  title: string;
  state: string;
}
