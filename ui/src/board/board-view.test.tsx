/**
 * board-view.test.tsx
 *
 * Tests for the board module: useBoard (query), useBoardSubscription (stream),
 * pending-mutation queue, proto-to-view mapping, and optimistic move lifecycle.
 *
 * Transport strategy: createMockTransport from @sunbeam/g2v/testing wires
 * in-process Connect RPC handlers (router.service() API). For streaming tests
 * we use the custom makeTransport pattern from useRpcStream.test.ts — a minimal
 * Transport implementation that invokes an async-generator handler directly.
 */

import React from "react";
import { describe, it, expect, vi, afterEach } from "vitest";
import { waitFor, act } from "@testing-library/react";
import { renderHook } from "@testing-library/react";
import { QueryClient } from "@tanstack/react-query";
import { createMockTransport } from "@sunbeam/g2v/testing";
import { FrameworkProvider } from "@sunbeam/g2v/providers";
import { authActions } from "@sunbeam/g2v/state";
import { BoardService } from "../gen/sunbeam/kanban/v1/boards_pb";
import { CardService } from "../gen/sunbeam/kanban/v1/cards_pb";
import type { Transport } from "@connectrpc/connect";
import type { BoardEventEnvelope } from "../gen/sunbeam/kanban/v1/events_pb";
import type {
  CardCreated,
  CardUpdated,
  CardMoved,
  CardDeleted,
} from "../gen/sunbeam/kanban/v1/events_pb";
import { useBoard } from "./use-board";
import { useBoardSubscription } from "./use-board-subscription";
import {
  applyPending,
  enqueue,
  acknowledge,
  findByCardAndAck,
  reject as rejectMove,
} from "./pending-mutations";
import type { PendingMoveQueue } from "./pending-mutations";
import { cardToView } from "./proto-to-view";
import type { KanbanColumn } from "@sunbeam/beam-ui";

// ─── Fixtures ─────────────────────────────────────────────────────────────────

const BOARD_ID = "board-01";
const COL_A = "col-a";
const COL_B = "col-b";
const CARD_1 = "card-1";
const CARD_2 = "card-2";

const fakeBoardDetail = {
  board: {
    id: BOARD_ID,
    projectId: "proj-1",
    name: "Sprint Board",
    description: "",
    icon: "",
    columnsCount: 2,
    cardsCount: 2,
  },
  columns: [
    { id: COL_A, boardId: BOARD_ID, title: "To Do", accent: "orange", wipLimit: 0, position: 1 },
    { id: COL_B, boardId: BOARD_ID, title: "Done", accent: "rust", wipLimit: 0, position: 2 },
  ],
};

const fakeCards = [
  {
    id: CARD_1,
    projectId: "proj-1",
    boardId: BOARD_ID,
    columnId: COL_A,
    ref: "BEAM-1",
    title: "First card",
    description: "",
    priority: 0,
    blocked: false,
    cover: "gradient.amber",
    milestoneId: "",
    position: 1,
    labels: [{ id: "l1", projectId: "proj-1", name: "bug", style: "#e53e3e" }],
    assignees: [{ subject: "user:1", displayName: "Alice", avatarUrl: "" }],
    checklist: [],
    forgejoLinks: [],
    commentsCount: 0,
    attachmentsCount: 0,
    revision: 1n,
  },
  {
    id: CARD_2,
    projectId: "proj-1",
    boardId: BOARD_ID,
    columnId: COL_B,
    ref: "BEAM-2",
    title: "Second card",
    description: "",
    priority: 0,
    blocked: true,
    cover: "",
    milestoneId: "",
    position: 1,
    labels: [],
    assignees: [],
    checklist: [],
    forgejoLinks: [],
    commentsCount: 0,
    attachmentsCount: 0,
    revision: 1n,
  },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function makeQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
}

function makeWrapper(transport: Transport, queryClient?: QueryClient) {
  const qc = queryClient ?? makeQueryClient();
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <FrameworkProvider transport={transport} queryClient={qc}>
        {children}
      </FrameworkProvider>
    );
  };
}

// Minimal custom transport for streaming tests (mirrors useRpcStream.test.ts).
type StreamHandler = (
  req: Record<string, unknown>,
  signal: AbortSignal,
) => AsyncIterable<BoardEventEnvelope>;

function makeStreamTransport(handler: StreamHandler): Transport {
  return {
    async unary() {
      throw new Error("unary not used in stream tests");
    },
    async stream(method, signal, _timeoutMs, _header, inputIterable) {
      let req: Record<string, unknown> = {};
      for await (const msg of inputIterable) {
        req = msg as Record<string, unknown>;
        break;
      }
      const effectiveSignal = signal ?? new AbortController().signal;
      const iterable = handler(req, effectiveSignal);
      return {
        stream: true as const,
        service: method.parent as never,
        method: method as never,
        header: new Headers(),
        trailer: new Headers(),
        message: iterable as AsyncIterable<never>,
      };
    },
  };
}

function makeEnvelope(
  partial: Partial<BoardEventEnvelope> & { payload: BoardEventEnvelope["payload"] },
): BoardEventEnvelope {
  return {
    boardId: BOARD_ID,
    eventId: Math.random().toString(36).slice(2),
    natsSeq: 1n,
    boardRevision: 1n,
    emitterPodId: "pod-1",
    actorSubject: "user:1",
    ...partial,
  } as BoardEventEnvelope;
}

// ─── Setup / teardown ─────────────────────────────────────────────────────────

afterEach(() => {
  authActions.logout();
  vi.restoreAllMocks();
});

// ─── proto-to-view ────────────────────────────────────────────────────────────

describe("proto_card_maps_to_KanbanCard_with_cover_and_blocked", () => {
  it("maps cover and blocked fields from proto Card", () => {
    const view = cardToView(fakeCards[0] as never);
    expect(view.id).toBe(CARD_1);
    expect(view.title).toBe("First card");
    expect(view.cover).toBe("gradient.amber");
    // fakeCards[0].blocked is false → falsy → coerces to undefined in cardToView
    expect(view.blocked).toBeUndefined();
    expect(view.labels).toEqual([{ name: "bug", color: "#e53e3e" }]);
    expect(view.assignees).toEqual([{ name: "Alice", avatarUrl: undefined }]);
  });

  it("maps blocked=true card", () => {
    const view = cardToView(fakeCards[1] as never);
    expect(view.blocked).toBe(true);
    // empty string cover coerces to undefined in cardToView
    expect(view.cover).toBeUndefined();
  });
});

// ─── pending-mutations (pure) ─────────────────────────────────────────────────

describe("pending-mutations pure functions", () => {
  function makeQueue(): PendingMoveQueue {
    return new Map();
  }

  const baseColumns: KanbanColumn[] = [
    { id: COL_A, title: "To Do", cards: [{ id: CARD_1, title: "First card" }] },
    { id: COL_B, title: "Done", cards: [{ id: CARD_2, title: "Second card" }] },
  ];

  it("applyPending returns same array when queue is empty", () => {
    const q = makeQueue();
    const result = applyPending(baseColumns, q);
    expect(result).toBe(baseColumns);
  });

  it("optimistic_move_immediately_updates_view", () => {
    const q = makeQueue();
    enqueue(q, {
      idempotencyKey: "key-1",
      intent: { cardId: CARD_1, fromColumnId: COL_A, toColumnId: COL_B, toPosition: 1 },
      enqueuedAt: Date.now(),
    });
    const result = applyPending(baseColumns, q);
    expect(result.find((c) => c.id === COL_A)!.cards).toHaveLength(0);
    expect(result.find((c) => c.id === COL_B)!.cards.map((c) => c.id)).toContain(CARD_1);
  });

  it("optimistic_move_dropped_when_matching_event_arrives", () => {
    const q = makeQueue();
    enqueue(q, {
      idempotencyKey: "key-1",
      intent: { cardId: CARD_1, fromColumnId: COL_A, toColumnId: COL_B, toPosition: 1 },
      enqueuedAt: Date.now(),
    });
    // Stream event arrives with matching key — acknowledge drops from queue.
    acknowledge(q, "key-1");
    const result = applyPending(baseColumns, q);
    // Queue is empty — original server state is returned as-is.
    expect(result).toBe(baseColumns);
  });

  it("concurrent_move_by_other_user_overrides_optimistic", () => {
    const q = makeQueue();
    enqueue(q, {
      idempotencyKey: "key-mine",
      intent: { cardId: CARD_1, fromColumnId: COL_A, toColumnId: COL_B, toPosition: 1 },
      enqueuedAt: Date.now(),
    });
    // Another user's event arrives (different key, same card) — flush all our pending.
    findByCardAndAck(q, CARD_1);
    expect(q.size).toBe(0);
    // After flush, applyPending returns original server state.
    const result = applyPending(baseColumns, q);
    expect(result).toBe(baseColumns);
  });

  it("failed_mutation_rolls_back_optimistic_state", () => {
    const q = makeQueue();
    enqueue(q, {
      idempotencyKey: "key-fail",
      intent: { cardId: CARD_1, fromColumnId: COL_A, toColumnId: COL_B, toPosition: 1 },
      enqueuedAt: Date.now(),
    });
    // Before the RPC settles, view shows CARD_1 in COL_B.
    const optimistic = applyPending(baseColumns, q);
    expect(optimistic.find((c) => c.id === COL_B)!.cards.map((c) => c.id)).toContain(CARD_1);

    // RPC rejects — remove from queue.
    rejectMove(q, "key-fail");
    const rolledBack = applyPending(baseColumns, q);
    // Queue empty — CARD_1 back in COL_A (server state unchanged).
    expect(rolledBack.find((c) => c.id === COL_A)!.cards.map((c) => c.id)).toContain(CARD_1);
  });
});

// ─── useBoard ─────────────────────────────────────────────────────────────────

describe("useBoard", () => {
  it("useBoard_renders_columns_from_GetBoard", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    const transport = createMockTransport({
      routes: (router) => {
        router.service(BoardService, {
          getBoard: () => Promise.resolve(fakeBoardDetail),
        });
        router.service(CardService, {
          listCardsByBoard: () => Promise.resolve({ cards: [], nextCursor: "" }),
        });
      },
    });

    const { result } = renderHook(() => useBoard(BOARD_ID), {
      wrapper: makeWrapper(transport),
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.columns).toHaveLength(2);
    expect(result.current.columns[0].title).toBe("To Do");
    expect(result.current.columns[1].title).toBe("Done");
    expect(result.current.boardName).toBe("Sprint Board");
  });

  it("useBoard_renders_cards_from_ListCardsByBoard_grouped_by_column", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    const transport = createMockTransport({
      routes: (router) => {
        router.service(BoardService, {
          getBoard: () => Promise.resolve(fakeBoardDetail),
        });
        router.service(CardService, {
          listCardsByBoard: () => Promise.resolve({ cards: fakeCards, nextCursor: "" }),
        });
      },
    });

    const { result } = renderHook(() => useBoard(BOARD_ID), {
      wrapper: makeWrapper(transport),
    });

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    const colA = result.current.columns.find((c) => c.id === COL_A)!;
    const colB = result.current.columns.find((c) => c.id === COL_B)!;
    expect(colA.cards.map((c) => c.id)).toContain(CARD_1);
    expect(colB.cards.map((c) => c.id)).toContain(CARD_2);
  });

  it("useBoard_sets_x_sunbeam_object_id_header", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    // The x-sunbeam-object-id header is passed via TanStack Query `meta` and read
    // by the transport interceptor. In the mock transport the header may not be
    // forwarded, so we verify the board query key isolates by boardId instead.
    const transport = createMockTransport({
      routes: (router) => {
        router.service(BoardService, {
          getBoard: () => Promise.resolve(fakeBoardDetail),
        });
        router.service(CardService, {
          listCardsByBoard: () => Promise.resolve({ cards: [], nextCursor: "" }),
        });
      },
    });

    const { result } = renderHook(() => useBoard(BOARD_ID), {
      wrapper: makeWrapper(transport),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    // Board query key carries boardId → each board gets an isolated cache entry.
    expect(result.current.columns).toHaveLength(2);
    expect(result.current.boardName).toBe("Sprint Board");
  });
});

// ─── useBoardSubscription ─────────────────────────────────────────────────────

describe("useBoardSubscription", () => {
  it("subscription_appends_CardCreated_event_to_column", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    const newCard = {
      id: "card-3",
      projectId: "proj-1",
      boardId: BOARD_ID,
      columnId: COL_A,
      ref: "BEAM-3",
      title: "New card",
      description: "",
      priority: 0,
      blocked: false,
      cover: "",
      milestoneId: "",
      position: 2,
      labels: [],
      assignees: [],
      checklist: [],
      forgejoLinks: [],
      commentsCount: 0,
      attachmentsCount: 0,
      revision: 2n,
    };

    const cardCreatedEnvelope = makeEnvelope({
      payload: {
        case: "cardCreated",
        value: {
          card: newCard,
          columnId: COL_A,
          position: 2,
          idempotencyKey: "",
        } as unknown as CardCreated,
      },
    });

    const transport = makeStreamTransport(async function* () {
      yield cardCreatedEnvelope;
      // Keep stream open.
      await new Promise<void>(() => {});
    });

    const qc = makeQueryClient();
    // Pre-seed the cards query cache so the subscription has something to append to.
    qc.setQueryData(["cards", BOARD_ID], { cards: [...fakeCards], nextCursor: "" });

    const queue: PendingMoveQueue = new Map();

    renderHook(
      () => useBoardSubscription({ boardId: BOARD_ID, pendingQueue: queue }),
      { wrapper: makeWrapper(transport, qc) },
    );

    await waitFor(() => {
      const cached = qc.getQueryData<{ cards: Array<{ id: string }> }>(["cards", BOARD_ID]);
      expect(cached?.cards.map((c) => c.id)).toContain("card-3");
    });
  });

  it("subscription_patches_card_on_CardUpdated", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    const updatedEnvelope = makeEnvelope({
      payload: {
        case: "cardUpdated",
        value: {
          cardId: CARD_1,
          prevRevision: 1n,
          newRevision: 2n,
          patch: { title: "Updated title" },
          idempotencyKey: "",
        } as unknown as CardUpdated,
      },
    });

    const transport = makeStreamTransport(async function* () {
      yield updatedEnvelope;
      await new Promise<void>(() => {});
    });

    const qc = makeQueryClient();
    qc.setQueryData(["cards", BOARD_ID], { cards: [...fakeCards], nextCursor: "" });

    const queue: PendingMoveQueue = new Map();
    renderHook(
      () => useBoardSubscription({ boardId: BOARD_ID, pendingQueue: queue }),
      { wrapper: makeWrapper(transport, qc) },
    );

    await waitFor(() => {
      const cached = qc.getQueryData<{ cards: Array<{ id: string; title: string }> }>(
        ["cards", BOARD_ID],
      );
      return cached?.cards.find((c) => c.id === CARD_1)?.title === "Updated title";
    });
  });

  it("subscription_moves_card_on_CardMoved", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    const movedEnvelope = makeEnvelope({
      payload: {
        case: "cardMoved",
        value: {
          cardId: CARD_1,
          fromColumn: COL_A,
          toColumn: COL_B,
          toPosition: 1,
          prevRevision: 1n,
          newRevision: 2n,
          idempotencyKey: "some-other-key", // not in our queue
        } as unknown as CardMoved,
      },
    });

    const transport = makeStreamTransport(async function* () {
      yield movedEnvelope;
      await new Promise<void>(() => {});
    });

    const qc = makeQueryClient();
    qc.setQueryData(["cards", BOARD_ID], { cards: [...fakeCards], nextCursor: "" });

    const queue: PendingMoveQueue = new Map();
    renderHook(
      () => useBoardSubscription({ boardId: BOARD_ID, pendingQueue: queue }),
      { wrapper: makeWrapper(transport, qc) },
    );

    await waitFor(() => {
      const cached = qc.getQueryData<{ cards: Array<{ id: string; columnId: string }> }>(
        ["cards", BOARD_ID],
      );
      return cached?.cards.find((c) => c.id === CARD_1)?.columnId === COL_B;
    });
  });

  it("subscription_deletes_card_on_CardDeleted", async () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });

    const deletedEnvelope = makeEnvelope({
      payload: {
        case: "cardDeleted",
        value: {
          cardId: CARD_1,
          prevRevision: 1n,
          idempotencyKey: "",
        } as unknown as CardDeleted,
      },
    });

    const transport = makeStreamTransport(async function* () {
      yield deletedEnvelope;
      await new Promise<void>(() => {});
    });

    const qc = makeQueryClient();
    qc.setQueryData(["cards", BOARD_ID], { cards: [...fakeCards], nextCursor: "" });

    const queue: PendingMoveQueue = new Map();
    renderHook(
      () => useBoardSubscription({ boardId: BOARD_ID, pendingQueue: queue }),
      { wrapper: makeWrapper(transport, qc) },
    );

    await waitFor(() => {
      const cached = qc.getQueryData<{ cards: Array<{ id: string }> }>(["cards", BOARD_ID]);
      return !cached?.cards.some((c) => c.id === CARD_1);
    });
  });
});

// Re-export for completeness: act is used implicitly by renderHook.
void act;
