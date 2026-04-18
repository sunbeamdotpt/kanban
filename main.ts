import { Hono } from "hono";
import { serveStatic } from "hono/deno";
import { tracingMiddleware, metricsMiddleware } from "./server/telemetry.ts";
import { authMiddleware, sessionHandler } from "./server/auth.ts";
import { csrfMiddleware } from "./server/csrf.ts";
import { rpc } from "./server/rpc.ts";
import { ensureIndex } from "./server/opensearch.ts";

// ─── Service handlers ──────────────────────────────────────────────────────
import {
  listProjects, getProject, createProject, updateProject, deleteProject,
  listMembers, addMember, updateMember, removeMember,
} from "./server/projects.ts";
import {
  listBoards, getBoard, createBoard, updateBoard, deleteBoard,
  createColumn, updateColumn, deleteColumn, reorderColumns,
} from "./server/boards.ts";
import {
  createCard, getCard, updateCard, deleteCard, moveCard,
  createAttachment, listAttachments, deleteAttachment, getAttachmentDownloadUrl,
} from "./server/cards.ts";
import { listTemplates, createTemplate, deleteTemplate } from "./server/templates.ts";
import { searchForgejoIssues } from "./server/forgejo.ts";

const app = new Hono();

// ─── Telemetry ─────────────────────────────────────────────────────────────
app.use("/*", tracingMiddleware);
app.use("/*", metricsMiddleware);

// ─── Health ────────────────────────────────────────────────────────────────
app.get("/health", (c) =>
  c.json({ ok: true, time: new Date().toISOString() }));

// ─── Auth ──────────────────────────────────────────────────────────────────
app.use("/*", async (c, next) => {
  if (c.req.path === "/health") return await next();
  return await authMiddleware(c, next);
});
app.use("/*", csrfMiddleware);

// ─── Legacy REST endpoint for session ──────────────────────────────────────
app.get("/api/auth/session", sessionHandler);

// ─── ConnectRPC-style routes ───────────────────────────────────────────────
// All RPCs are POST /<package>.<Service>/<Method> with JSON body/response.

// AuthService
app.post("/kanban.v1.AuthService/GetSession", async (c) => {
  const identity = c.get("identity");
  if (!identity) return c.json({ code: "unauthenticated", message: "Not authenticated" }, 401);
  return c.json({
    user: {
      id: identity.id,
      email: identity.email,
      name: identity.name,
      avatarUrl: identity.picture ?? "",
    },
  });
});

// ProjectService
app.post("/kanban.v1.ProjectService/ListProjects", rpc(listProjects));
app.post("/kanban.v1.ProjectService/GetProject", rpc(getProject));
app.post("/kanban.v1.ProjectService/CreateProject", rpc(createProject));
app.post("/kanban.v1.ProjectService/UpdateProject", rpc(updateProject));
app.post("/kanban.v1.ProjectService/DeleteProject", rpc(deleteProject));
app.post("/kanban.v1.ProjectService/ListMembers", rpc(listMembers));
app.post("/kanban.v1.ProjectService/AddMember", rpc(addMember));
app.post("/kanban.v1.ProjectService/UpdateMember", rpc(updateMember));
app.post("/kanban.v1.ProjectService/RemoveMember", rpc(removeMember));

// BoardService
app.post("/kanban.v1.BoardService/ListBoards", rpc(listBoards));
app.post("/kanban.v1.BoardService/GetBoard", rpc(getBoard));
app.post("/kanban.v1.BoardService/CreateBoard", rpc(createBoard));
app.post("/kanban.v1.BoardService/UpdateBoard", rpc(updateBoard));
app.post("/kanban.v1.BoardService/DeleteBoard", rpc(deleteBoard));
app.post("/kanban.v1.BoardService/CreateColumn", rpc(createColumn));
app.post("/kanban.v1.BoardService/UpdateColumn", rpc(updateColumn));
app.post("/kanban.v1.BoardService/DeleteColumn", rpc(deleteColumn));
app.post("/kanban.v1.BoardService/ReorderColumns", rpc(reorderColumns));

// CardService
app.post("/kanban.v1.CardService/CreateCard", rpc(createCard));
app.post("/kanban.v1.CardService/GetCard", rpc(getCard));
app.post("/kanban.v1.CardService/UpdateCard", rpc(updateCard));
app.post("/kanban.v1.CardService/DeleteCard", rpc(deleteCard));
app.post("/kanban.v1.CardService/MoveCard", rpc(moveCard));
app.post("/kanban.v1.CardService/CreateAttachment", rpc(createAttachment));
app.post("/kanban.v1.CardService/ListAttachments", rpc(listAttachments));
app.post("/kanban.v1.CardService/DeleteAttachment", rpc(deleteAttachment));
app.post("/kanban.v1.CardService/GetAttachmentDownloadUrl", rpc(getAttachmentDownloadUrl));

// TemplateService
app.post("/kanban.v1.TemplateService/ListTemplates", rpc(listTemplates));
app.post("/kanban.v1.TemplateService/CreateTemplate", rpc(createTemplate));
app.post("/kanban.v1.TemplateService/DeleteTemplate", rpc(deleteTemplate));

// ForgejoService
app.post("/kanban.v1.ForgejoService/Search", rpc(async (req: { query: string; repo?: string }) => {
  const results = await searchForgejoIssues(req.query, req.repo);
  return { results };
}));

// ─── Static files from ui/dist ─────────────────────────────────────────────
app.use(
  "/*",
  serveStatic({ root: "./ui/dist" }),
);

// SPA fallback
app.use(
  "/*",
  serveStatic({ root: "./ui/dist", path: "index.html" }),
);

// ─── Start ─────────────────────────────────────────────────────────────────
const port = parseInt(Deno.env.get("PORT") ?? "3000", 10);

// Ensure OpenSearch index exists
ensureIndex();

console.log(`Kanban listening on :${port}`);
Deno.serve({ port }, app.fetch);
