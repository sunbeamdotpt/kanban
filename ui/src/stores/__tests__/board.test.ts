import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the API client
vi.mock("../../api/client", () => ({
  boards: {
    get: vi.fn(),
    createColumn: vi.fn(),
    deleteColumn: vi.fn(),
  },
  cards: {
    create: vi.fn(),
    delete: vi.fn(),
    move: vi.fn(),
  },
}));

const { boards: boardsApi, cards: cardsApi } = await import("../../api/client");
const { useBoardStore } = await import("../board");

const mockBoard = {
  board: { id: "b1", projectId: "p1", name: "Test", slug: "test", description: "", createdAt: "", updatedAt: "" },
  columns: [
    {
      id: "c1",
      boardId: "b1",
      title: "To Do",
      position: 0,
      color: "",
      wipLimit: 0,
      cards: [
        { id: "card1", columnId: "c1", boardId: "b1", title: "Task 1", description: "", position: 0, priority: 2, dueDate: "", labels: [], assignees: [], forgejoLinks: [], createdBy: "", createdAt: "", updatedAt: "", attachments: [] },
        { id: "card2", columnId: "c1", boardId: "b1", title: "Task 2", description: "", position: 1, priority: 1, dueDate: "", labels: [], assignees: [], forgejoLinks: [], createdBy: "", createdAt: "", updatedAt: "", attachments: [] },
      ],
    },
    {
      id: "c2",
      boardId: "b1",
      title: "Done",
      position: 1,
      color: "",
      wipLimit: 0,
      cards: [],
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  useBoardStore.setState({
    board: null,
    loading: false,
    error: null,
    selectedCardId: null,
  });
});

describe("useBoardStore", () => {
  it("initial state is correct", () => {
    const state = useBoardStore.getState();
    expect(state.board).toBeNull();
    expect(state.loading).toBe(false);
    expect(state.error).toBeNull();
    expect(state.selectedCardId).toBeNull();
  });

  it("loadBoard sets board from API", async () => {
    vi.mocked(boardsApi.get).mockResolvedValueOnce({ board: mockBoard });

    await useBoardStore.getState().loadBoard("b1");

    const state = useBoardStore.getState();
    expect(state.board).toBeDefined();
    expect(state.board!.board.id).toBe("b1");
    expect(state.loading).toBe(false);
  });

  it("loadBoard sets error on failure", async () => {
    vi.mocked(boardsApi.get).mockRejectedValueOnce(new Error("Network error"));

    await useBoardStore.getState().loadBoard("b1");

    const state = useBoardStore.getState();
    expect(state.error).toBe("Network error");
    expect(state.loading).toBe(false);
  });

  it("selectCard updates selectedCardId", () => {
    useBoardStore.getState().selectCard("card1");
    expect(useBoardStore.getState().selectedCardId).toBe("card1");

    useBoardStore.getState().selectCard(null);
    expect(useBoardStore.getState().selectedCardId).toBeNull();
  });

  it("addCard adds to correct column", async () => {
    useBoardStore.setState({ board: structuredClone(mockBoard) });

    const newCard = { ...mockBoard.columns[0].cards[0], id: "card3", title: "New" };
    vi.mocked(cardsApi.create).mockResolvedValueOnce({ card: newCard });

    const result = await useBoardStore.getState().addCard("c1", "New");

    expect(result).toBeDefined();
    expect(result!.title).toBe("New");
    expect(useBoardStore.getState().board!.columns[0].cards).toHaveLength(3);
  });

  it("removeCard removes from column optimistically", async () => {
    useBoardStore.setState({ board: structuredClone(mockBoard) });
    vi.mocked(cardsApi.delete).mockResolvedValueOnce({});

    await useBoardStore.getState().removeCard("card1");

    expect(useBoardStore.getState().board!.columns[0].cards).toHaveLength(1);
    expect(useBoardStore.getState().board!.columns[0].cards[0].id).toBe("card2");
  });

  it("addColumn appends to board", async () => {
    useBoardStore.setState({ board: structuredClone(mockBoard) });

    const newCol = { id: "c3", boardId: "b1", title: "Review", position: 2, color: "", wipLimit: 0, cards: [] };
    vi.mocked(boardsApi.createColumn).mockResolvedValueOnce({ column: newCol });

    await useBoardStore.getState().addColumn("b1", "Review");

    expect(useBoardStore.getState().board!.columns).toHaveLength(3);
  });

  it("removeColumn removes from board optimistically", async () => {
    useBoardStore.setState({ board: structuredClone(mockBoard) });
    vi.mocked(boardsApi.deleteColumn).mockResolvedValueOnce({});

    await useBoardStore.getState().removeColumn("c2");

    expect(useBoardStore.getState().board!.columns).toHaveLength(1);
  });

  it("moveCard — optimistic update moves card between columns", async () => {
    useBoardStore.setState({ board: structuredClone(mockBoard) });
    vi.mocked(cardsApi.move).mockResolvedValueOnce({
      card: { ...mockBoard.columns[0].cards[0], columnId: "c2" },
    });

    await useBoardStore.getState().moveCard("card1", "c2", 0);

    const state = useBoardStore.getState();
    // card1 should no longer be in c1
    expect(state.board!.columns[0].cards.find((c) => c.id === "card1")).toBeUndefined();
    // card1 should be in c2
    expect(state.board!.columns[1].cards.find((c) => c.id === "card1")).toBeDefined();
  });

  it("updateColumnsOptimistic replaces columns", () => {
    useBoardStore.setState({ board: structuredClone(mockBoard) });

    const newColumns = [mockBoard.columns[1], mockBoard.columns[0]]; // swapped
    useBoardStore.getState().updateColumnsOptimistic(newColumns);

    expect(useBoardStore.getState().board!.columns[0].id).toBe("c2");
    expect(useBoardStore.getState().board!.columns[1].id).toBe("c1");
  });
});
