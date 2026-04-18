/**
 * Zustand store for the active board view.
 * Manages optimistic updates for drag-and-drop operations.
 */

import { create } from "zustand";
import type { BoardDetail, Column, Card } from "../api/client";
import { boards as boardsApi, cards as cardsApi } from "../api/client";

interface BoardState {
  board: BoardDetail | null;
  loading: boolean;
  error: string | null;
  selectedCardId: string | null;

  loadBoard: (boardId: string) => Promise<void>;
  moveCard: (cardId: string, targetColumnId: string, position: number) => Promise<void>;
  addCard: (columnId: string, title: string) => Promise<Card | null>;
  removeCard: (cardId: string) => Promise<void>;
  addColumn: (boardId: string, title: string) => Promise<void>;
  removeColumn: (columnId: string) => Promise<void>;
  selectCard: (cardId: string | null) => void;
  updateColumnsOptimistic: (columns: Column[]) => void;
}

export const useBoardStore = create<BoardState>((set, get) => ({
  board: null,
  loading: false,
  error: null,
  selectedCardId: null,

  loadBoard: async (boardId: string) => {
    set({ loading: true, error: null });
    try {
      const { board } = await boardsApi.get(boardId);
      set({ board, loading: false });
    } catch (err) {
      set({ error: (err as Error).message, loading: false });
    }
  },

  moveCard: async (cardId: string, targetColumnId: string, position: number) => {
    const { board } = get();
    if (!board) return;

    // Optimistic update
    const newColumns = board.columns.map((col) => ({
      ...col,
      cards: col.cards.filter((c) => c.id !== cardId),
    }));

    const card = board.columns
      .flatMap((c) => c.cards)
      .find((c) => c.id === cardId);
    if (!card) return;

    const targetCol = newColumns.find((c) => c.id === targetColumnId);
    if (!targetCol) return;

    const movedCard = { ...card, columnId: targetColumnId, position };
    targetCol.cards.splice(position, 0, movedCard);
    targetCol.cards = targetCol.cards.map((c, i) => ({ ...c, position: i }));

    set({ board: { ...board, columns: newColumns } });

    try {
      await cardsApi.move({ cardId, targetColumnId, position });
    } catch {
      // Revert on failure
      get().loadBoard(board.board.id);
    }
  },

  addCard: async (columnId: string, title: string) => {
    try {
      const { card } = await cardsApi.create({ columnId, title });
      const { board } = get();
      if (board) {
        const newColumns = board.columns.map((col) => {
          if (col.id !== columnId) return col;
          return { ...col, cards: [...col.cards, card] };
        });
        set({ board: { ...board, columns: newColumns } });
      }
      return card;
    } catch {
      return null;
    }
  },

  removeCard: async (cardId: string) => {
    const { board } = get();
    if (!board) return;

    const newColumns = board.columns.map((col) => ({
      ...col,
      cards: col.cards.filter((c) => c.id !== cardId),
    }));
    set({ board: { ...board, columns: newColumns } });

    try {
      await cardsApi.delete(cardId);
    } catch {
      get().loadBoard(board.board.id);
    }
  },

  addColumn: async (boardId: string, title: string) => {
    try {
      const { column } = await boardsApi.createColumn({ boardId, title });
      const { board } = get();
      if (board) {
        set({ board: { ...board, columns: [...board.columns, column] } });
      }
    } catch { /* ignore */ }
  },

  removeColumn: async (columnId: string) => {
    const { board } = get();
    if (!board) return;

    set({
      board: {
        ...board,
        columns: board.columns.filter((c) => c.id !== columnId),
      },
    });

    try {
      await boardsApi.deleteColumn(columnId);
    } catch {
      get().loadBoard(board.board.id);
    }
  },

  selectCard: (cardId) => set({ selectedCardId: cardId }),

  updateColumnsOptimistic: (columns) => {
    const { board } = get();
    if (board) {
      set({ board: { ...board, columns } });
    }
  },
}));
