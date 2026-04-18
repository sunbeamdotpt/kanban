/**
 * Integration tests for the full RPC API.
 * Requires PostgreSQL running (use docker-compose.test.yml).
 *
 * Run with: deno task test
 * Or with docker:
 *   docker compose -f docker-compose.test.yml up -d
 *   deno task test
 *   docker compose -f docker-compose.test.yml down
 */

import { assertEquals, assertExists, assertNotEquals } from "https://deno.land/std@0.220.0/assert/mod.ts";
import { Hono } from "hono";
import { rpc } from "../../server/rpc.ts";
import { authMiddleware } from "../../server/auth.ts";
import { csrfMiddleware } from "../../server/csrf.ts";
import {
  listProjects, getProject, createProject, updateProject, deleteProject,
  listMembers, addMember, updateMember, removeMember,
} from "../../server/projects.ts";
import {
  listBoards, getBoard, createBoard, updateBoard, deleteBoard,
  createColumn, updateColumn, deleteColumn, reorderColumns,
} from "../../server/boards.ts";
import {
  createCard, getCard, updateCard, deleteCard, moveCard,
  listAttachments,
} from "../../server/cards.ts";
import { listTemplates, createTemplate, deleteTemplate } from "../../server/templates.ts";

// Helper to build a test Hono app with auth middleware (test mode injects identity)
function buildApp(): Hono {
  const app = new Hono();
  app.use("/*", authMiddleware);
  app.use("/*", csrfMiddleware);

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
  app.post("/kanban.v1.CardService/ListAttachments", rpc(listAttachments));

  // TemplateService
  app.post("/kanban.v1.TemplateService/ListTemplates", rpc(listTemplates));
  app.post("/kanban.v1.TemplateService/CreateTemplate", rpc(createTemplate));
  app.post("/kanban.v1.TemplateService/DeleteTemplate", rpc(deleteTemplate));

  return app;
}

async function rpcCall(app: Hono, service: string, method: string, body: unknown = {}) {
  const res = await app.request(`/kanban.v1.${service}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  return { status: res.status, body: json };
}

// Check if we can connect to the test DB
let dbAvailable = false;
try {
  const sql = (await import("../../server/db.ts")).default;
  await sql`SELECT 1`;
  dbAvailable = true;
  // Run migrations
  const { migrate } = await import("../../server/migrate.ts");
  await migrate();
} catch {
  console.warn("⚠️  PostgreSQL not available — skipping integration tests. Run docker-compose.test.yml to enable.");
}

// Conditional test helper
function dbTest(name: string, fn: () => Promise<void>) {
  if (dbAvailable) {
    Deno.test(name, fn);
  } else {
    Deno.test({ name, ignore: true, fn: async () => {} });
  }
}

// ─── Project CRUD ──────────────────────────────────────────────────────────

dbTest("integration — create and list projects", async () => {
  const app = buildApp();
  const slug = `test-project-${Date.now()}`;

  // Create
  const { status, body } = await rpcCall(app, "ProjectService", "CreateProject", {
    name: "Test Project",
    slug,
    description: "A test project",
    visibility: 1, // PRIVATE
  });
  assertEquals(status, 200);
  assertExists(body.project);
  assertEquals(body.project.name, "Test Project");
  assertEquals(body.project.slug, slug);

  // Get
  const { body: getBody } = await rpcCall(app, "ProjectService", "GetProject", { slug });
  assertEquals(getBody.project.name, "Test Project");

  // List
  const { body: listBody } = await rpcCall(app, "ProjectService", "ListProjects");
  const found = listBody.projects.find((p: { slug: string }) => p.slug === slug);
  assertExists(found);

  // Update
  const { body: updateBody } = await rpcCall(app, "ProjectService", "UpdateProject", {
    slug,
    name: "Updated Project",
    description: "Updated description",
  });
  assertEquals(updateBody.project.name, "Updated Project");

  // Cleanup
  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});

dbTest("integration — create project with duplicate slug fails", async () => {
  const app = buildApp();
  const slug = `dup-test-${Date.now()}`;

  await rpcCall(app, "ProjectService", "CreateProject", { name: "P1", slug });
  const { status } = await rpcCall(app, "ProjectService", "CreateProject", { name: "P2", slug });
  assertEquals(status, 409); // already_exists

  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});

// ─── Members ───────────────────────────────────────────────────────────────

dbTest("integration — project members CRUD", async () => {
  const app = buildApp();
  const slug = `members-test-${Date.now()}`;

  await rpcCall(app, "ProjectService", "CreateProject", { name: "Members Test", slug });

  // Add member
  const { body: addBody } = await rpcCall(app, "ProjectService", "AddMember", {
    projectSlug: slug,
    userId: "test-user-2",
    role: 2, // EDITOR
  });
  assertEquals(addBody.member.userId, "test-user-2");

  // List members
  const { body: listBody } = await rpcCall(app, "ProjectService", "ListMembers", {
    projectSlug: slug,
  });
  assertEquals(listBody.members.length >= 2, true); // owner + added member

  // Update member role
  await rpcCall(app, "ProjectService", "UpdateMember", {
    projectSlug: slug,
    userId: "test-user-2",
    role: 3, // ADMIN
  });

  // Remove member
  await rpcCall(app, "ProjectService", "RemoveMember", {
    projectSlug: slug,
    userId: "test-user-2",
  });

  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});

// ─── Board CRUD ────────────────────────────────────────────────────────────

dbTest("integration — board CRUD with template", async () => {
  const app = buildApp();
  const slug = `board-test-${Date.now()}`;

  await rpcCall(app, "ProjectService", "CreateProject", { name: "Board Test", slug });

  // List templates (should have global ones from seed)
  const { body: tplBody } = await rpcCall(app, "TemplateService", "ListTemplates", {});
  assertEquals(tplBody.templates.length >= 3, true);

  const kanbanTemplate = tplBody.templates.find((t: { name: string }) => t.name === "Kanban");
  assertExists(kanbanTemplate);

  // Create board from template
  const { body: boardBody } = await rpcCall(app, "BoardService", "CreateBoard", {
    projectSlug: slug,
    name: "Sprint Board",
    templateId: kanbanTemplate.id,
  });
  assertExists(boardBody.board);
  const boardId = boardBody.board.id;

  // Get board with columns
  const { body: detailBody } = await rpcCall(app, "BoardService", "GetBoard", { boardId });
  assertExists(detailBody.board);
  assertEquals(detailBody.board.columns.length, 5); // Kanban has 5 columns

  // Update board
  const { body: updateBody } = await rpcCall(app, "BoardService", "UpdateBoard", {
    boardId,
    name: "Updated Sprint Board",
  });
  assertEquals(updateBody.board.name, "Updated Sprint Board");

  // List boards
  const { body: listBody } = await rpcCall(app, "BoardService", "ListBoards", { projectSlug: slug });
  assertEquals(listBody.boards.length, 1);

  // Add a column
  const { body: colBody } = await rpcCall(app, "BoardService", "CreateColumn", {
    boardId,
    title: "Blocked",
    color: "#ef4444",
  });
  assertExists(colBody.column);

  // Delete board
  await rpcCall(app, "BoardService", "DeleteBoard", { boardId });

  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});

// ─── Card CRUD ─────────────────────────────────────────────────────────────

dbTest("integration — card CRUD and move", async () => {
  const app = buildApp();
  const slug = `card-test-${Date.now()}`;

  await rpcCall(app, "ProjectService", "CreateProject", { name: "Card Test", slug });

  // Create board with Simple template
  const { body: tplBody } = await rpcCall(app, "TemplateService", "ListTemplates", {});
  const simpleTemplate = tplBody.templates.find((t: { name: string }) => t.name === "Simple");

  const { body: boardBody } = await rpcCall(app, "BoardService", "CreateBoard", {
    projectSlug: slug,
    name: "Card Board",
    templateId: simpleTemplate?.id,
  });
  const boardId = boardBody.board.id;

  const { body: detailBody } = await rpcCall(app, "BoardService", "GetBoard", { boardId });
  const columns = detailBody.board.columns;
  assertEquals(columns.length, 3); // To Do, Doing, Done

  const todoColId = columns[0].id;
  const doingColId = columns[1].id;

  // Create card
  const { body: cardBody } = await rpcCall(app, "CardService", "CreateCard", {
    columnId: todoColId,
    title: "Test Task",
    description: "A task to do",
    priority: 2, // MEDIUM
    labels: [{ name: "bug", color: "#ef4444" }],
  });
  assertExists(cardBody.card);
  const cardId = cardBody.card.id;
  assertEquals(cardBody.card.title, "Test Task");

  // Get card
  const { body: getBody } = await rpcCall(app, "CardService", "GetCard", { cardId });
  assertEquals(getBody.card.title, "Test Task");
  assertEquals(getBody.card.description, "A task to do");

  // Update card
  const { body: updateBody } = await rpcCall(app, "CardService", "UpdateCard", {
    cardId,
    title: "Updated Task",
    forgejoLinks: [{ type: "issue", repo: "org/repo", number: 42, url: "https://example.com", title: "Fix bug", state: "open" }],
  });
  assertEquals(updateBody.card.title, "Updated Task");

  // Move card to Doing column
  const { body: moveBody } = await rpcCall(app, "CardService", "MoveCard", {
    cardId,
    targetColumnId: doingColId,
    position: 0,
  });
  assertEquals(moveBody.card.columnId, doingColId);

  // Verify board shows card in new column
  const { body: boardAfterMove } = await rpcCall(app, "BoardService", "GetBoard", { boardId });
  const doingCol = boardAfterMove.board.columns.find((c: { id: string }) => c.id === doingColId);
  const cardInDoing = doingCol?.cards.find((c: { id: string }) => c.id === cardId);
  assertExists(cardInDoing);

  // List attachments (empty)
  const { body: attBody } = await rpcCall(app, "CardService", "ListAttachments", { cardId });
  assertEquals(attBody.attachments.length, 0);

  // Delete card
  await rpcCall(app, "CardService", "DeleteCard", { cardId });

  // Verify card is gone
  const { status } = await rpcCall(app, "CardService", "GetCard", { cardId });
  assertEquals(status, 404);

  await rpcCall(app, "BoardService", "DeleteBoard", { boardId });
  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});

// ─── Template CRUD ─────────────────────────────────────────────────────────

dbTest("integration — custom template CRUD", async () => {
  const app = buildApp();

  // Create custom template
  const { body: createBody } = await rpcCall(app, "TemplateService", "CreateTemplate", {
    name: "Custom Flow",
    description: "Test template",
    columns: [
      { title: "Inbox", position: 0, color: "#60a5fa", wipLimit: 5 },
      { title: "Active", position: 1, color: "#fbbf24", wipLimit: 3 },
      { title: "Archive", position: 2, color: "#94a3b8", wipLimit: 0 },
    ],
  });
  assertExists(createBody.template);
  assertEquals(createBody.template.name, "Custom Flow");
  assertEquals(createBody.template.columns.length, 3);

  const templateId = createBody.template.id;

  // List templates (should include custom one)
  const { body: listBody } = await rpcCall(app, "TemplateService", "ListTemplates", {});
  const found = listBody.templates.find((t: { id: string }) => t.id === templateId);
  assertExists(found);

  // Delete
  await rpcCall(app, "TemplateService", "DeleteTemplate", { templateId });
});

// ─── Column operations ─────────────────────────────────────────────────────

dbTest("integration — column update and reorder", async () => {
  const app = buildApp();
  const slug = `col-test-${Date.now()}`;

  await rpcCall(app, "ProjectService", "CreateProject", { name: "Col Test", slug });

  const { body: tplBody } = await rpcCall(app, "TemplateService", "ListTemplates", {});
  const simpleTemplate = tplBody.templates.find((t: { name: string }) => t.name === "Simple");

  const { body: boardBody } = await rpcCall(app, "BoardService", "CreateBoard", {
    projectSlug: slug,
    name: "Col Board",
    templateId: simpleTemplate?.id,
  });
  const boardId = boardBody.board.id;

  const { body: detailBody } = await rpcCall(app, "BoardService", "GetBoard", { boardId });
  const columns = detailBody.board.columns;

  // Update column
  const { body: updateBody } = await rpcCall(app, "BoardService", "UpdateColumn", {
    columnId: columns[0].id,
    title: "Renamed Column",
    wipLimit: 10,
    color: "#ff0000",
  });
  assertEquals(updateBody.column.title, "Renamed Column");

  // Reorder columns (reverse)
  const reversed = columns.map((c: { id: string }) => c.id).reverse();
  await rpcCall(app, "BoardService", "ReorderColumns", {
    boardId,
    columnIds: reversed,
  });

  // Verify order
  const { body: afterReorder } = await rpcCall(app, "BoardService", "GetBoard", { boardId });
  assertEquals(afterReorder.board.columns[0].id, reversed[0]);

  // Delete column
  await rpcCall(app, "BoardService", "DeleteColumn", { columnId: columns[2].id });

  const { body: afterDelete } = await rpcCall(app, "BoardService", "GetBoard", { boardId });
  assertEquals(afterDelete.board.columns.length, 2);

  await rpcCall(app, "BoardService", "DeleteBoard", { boardId });
  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});

// ─── Permission errors ─────────────────────────────────────────────────────

dbTest("integration — get nonexistent project returns 404", async () => {
  const app = buildApp();
  const { status } = await rpcCall(app, "ProjectService", "GetProject", { slug: "nonexistent-project-999" });
  assertEquals(status, 404);
});

dbTest("integration — create project without name returns 400", async () => {
  const app = buildApp();
  const { status } = await rpcCall(app, "ProjectService", "CreateProject", { name: "" });
  assertEquals(status, 400);
});

dbTest("integration — create card without title returns 400", async () => {
  const app = buildApp();
  const slug = `err-test-${Date.now()}`;
  await rpcCall(app, "ProjectService", "CreateProject", { name: "Err Test", slug });

  const { body: boardBody } = await rpcCall(app, "BoardService", "CreateBoard", {
    projectSlug: slug,
    name: "Err Board",
  });

  const { body: colBody } = await rpcCall(app, "BoardService", "CreateColumn", {
    boardId: boardBody.board.id,
    title: "Col",
  });

  const { status } = await rpcCall(app, "CardService", "CreateCard", {
    columnId: colBody.column.id,
    title: "",
  });
  assertEquals(status, 400);

  await rpcCall(app, "ProjectService", "DeleteProject", { slug });
});
