import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock fetch globally
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

// Import after stubbing
const { auth, projects, boards, cards, templates, forgejo } = await import("../client");

beforeEach(() => {
  mockFetch.mockReset();
});

function mockOk(body: unknown) {
  mockFetch.mockResolvedValueOnce({
    ok: true,
    status: 200,
    json: () => Promise.resolve(body),
  });
}

function mockError(status: number, message: string) {
  mockFetch.mockResolvedValueOnce({
    ok: false,
    status,
    statusText: "Error",
    json: () => Promise.resolve({ message }),
  });
}

describe("auth", () => {
  it("getSession calls correct endpoint", async () => {
    mockOk({ user: { id: "1", email: "test@test.com", name: "Test", avatarUrl: "" } });
    const result = await auth.getSession();
    expect(result.user.id).toBe("1");
    expect(mockFetch).toHaveBeenCalledWith(
      "/kanban.v1.AuthService/GetSession",
      expect.objectContaining({ method: "POST" }),
    );
  });
});

describe("projects", () => {
  it("list calls ListProjects", async () => {
    mockOk({ projects: [{ id: "1", name: "Test" }] });
    const result = await projects.list();
    expect(result.projects).toHaveLength(1);
    expect(mockFetch).toHaveBeenCalledWith(
      "/kanban.v1.ProjectService/ListProjects",
      expect.anything(),
    );
  });

  it("get calls GetProject with slug", async () => {
    mockOk({ project: { id: "1", slug: "test" } });
    await projects.get("test");
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ slug: "test" });
  });

  it("create sends name and visibility", async () => {
    mockOk({ project: { id: "1" } });
    await projects.create({ name: "New", visibility: 1 });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ name: "New", visibility: 1 });
  });

  it("delete sends slug", async () => {
    mockOk({});
    await projects.delete("test");
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ slug: "test" });
  });

  it("listMembers sends projectSlug", async () => {
    mockOk({ members: [] });
    await projects.listMembers("test");
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ projectSlug: "test" });
  });

  it("addMember sends correct payload", async () => {
    mockOk({ member: { userId: "u1" } });
    await projects.addMember({ projectSlug: "test", userId: "u1", role: 2 });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ projectSlug: "test", userId: "u1", role: 2 });
  });
});

describe("boards", () => {
  it("list calls ListBoards", async () => {
    mockOk({ boards: [] });
    await boards.list("test");
    expect(mockFetch).toHaveBeenCalledWith(
      "/kanban.v1.BoardService/ListBoards",
      expect.anything(),
    );
  });

  it("get calls GetBoard", async () => {
    mockOk({ board: { board: {}, columns: [] } });
    await boards.get("board-1");
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ boardId: "board-1" });
  });

  it("create sends templateId when provided", async () => {
    mockOk({ board: {} });
    await boards.create({ projectSlug: "p", name: "B", templateId: "t1" });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body).templateId).toBe("t1");
  });

  it("createColumn sends board and title", async () => {
    mockOk({ column: {} });
    await boards.createColumn({ boardId: "b1", title: "Col" });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ boardId: "b1", title: "Col" });
  });

  it("reorderColumns sends column IDs", async () => {
    mockOk({});
    await boards.reorderColumns({ boardId: "b1", columnIds: ["c1", "c2"] });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body).columnIds).toEqual(["c1", "c2"]);
  });
});

describe("cards", () => {
  it("create sends columnId and title", async () => {
    mockOk({ card: {} });
    await cards.create({ columnId: "c1", title: "Task" });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body)).toEqual({ columnId: "c1", title: "Task" });
  });

  it("move sends target column and position", async () => {
    mockOk({ card: {} });
    await cards.move({ cardId: "card1", targetColumnId: "c2", position: 3 });
    const [, opts] = mockFetch.mock.calls[0];
    const body = JSON.parse(opts.body);
    expect(body.targetColumnId).toBe("c2");
    expect(body.position).toBe(3);
  });

  it("createAttachment sends file metadata", async () => {
    mockOk({ attachment: {}, uploadUrl: "https://s3/upload" });
    const result = await cards.createAttachment({
      cardId: "card1",
      filename: "file.pdf",
      mimetype: "application/pdf",
      size: 1024,
    });
    expect(result.uploadUrl).toBe("https://s3/upload");
  });

  it("getAttachmentDownloadUrl returns URL", async () => {
    mockOk({ url: "https://s3/download" });
    const result = await cards.getAttachmentDownloadUrl("att1");
    expect(result.url).toBe("https://s3/download");
  });
});

describe("templates", () => {
  it("list calls ListTemplates", async () => {
    mockOk({ templates: [] });
    await templates.list();
    expect(mockFetch).toHaveBeenCalledWith(
      "/kanban.v1.TemplateService/ListTemplates",
      expect.anything(),
    );
  });

  it("create sends columns", async () => {
    mockOk({ template: {} });
    await templates.create({
      name: "Custom",
      columns: [{ title: "A", position: 0, color: "#fff", wipLimit: 0 }],
    });
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body).columns).toHaveLength(1);
  });
});

describe("forgejo", () => {
  it("search sends query", async () => {
    mockOk({ results: [] });
    await forgejo.search("bug fix");
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body).query).toBe("bug fix");
  });

  it("search with repo filter", async () => {
    mockOk({ results: [] });
    await forgejo.search("fix", "org/repo");
    const [, opts] = mockFetch.mock.calls[0];
    expect(JSON.parse(opts.body).repo).toBe("org/repo");
  });
});

describe("error handling", () => {
  it("throws on non-ok response", async () => {
    mockError(404, "Not found");
    await expect(projects.get("missing")).rejects.toThrow("Not found");
  });

  it("throws on 500", async () => {
    mockError(500, "Internal error");
    await expect(projects.list()).rejects.toThrow("Internal error");
  });
});
