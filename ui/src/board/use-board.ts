/**
 * use-board.ts
 *
 * Fetches board structure (columns) + cards in parallel via useRpcQuery,
 * then merges them into a KanbanColumn[] view for the board canvas.
 *
 * Sets `x-sunbeam-object-id: boardId` on each RPC call so the server-side
 * keto_dispatch middleware can resolve the Keto object without body inspection.
 */

import { useMemo } from "react";
import type { KanbanColumn } from "@sunbeam/beam-ui";
import { useRpcQuery } from "@sunbeam/g2v/hooks";
import { BoardService } from "../gen/sunbeam/kanban/v1/boards_pb";
import { CardService } from "../gen/sunbeam/kanban/v1/cards_pb";
import { cardToView } from "./proto-to-view";

// ─── Query key factories ──────────────────────────────────────────────────────

export function boardQueryKey(boardId: string) {
  return ["board", boardId] as const;
}

export function cardsQueryKey(boardId: string) {
  return ["cards", boardId] as const;
}

// ─── Hook ─────────────────────────────────────────────────────────────────────

export interface UseBoardResult {
  /** Merged column + card view for the board canvas. */
  columns: KanbanColumn[];
  /** True while any of the underlying queries are loading. */
  isLoading: boolean;
  /** First error encountered, or null. */
  error: Error | null;
  /** Raw board name for the page title. */
  boardName: string;
}

export function useBoard(boardId: string): UseBoardResult {
  // Attach x-sunbeam-object-id so keto_dispatch can gate without body inspection.
  const objectIdHeaders = useMemo(
    () => ({ headers: { "x-sunbeam-object-id": boardId } }),
    [boardId],
  );

  const boardQuery = useRpcQuery(
    BoardService,
    "getBoard",
    { boardId },
    {
      queryKey: boardQueryKey(boardId),
      // Pass headers via the options bag — the g2v transport merges them.
      meta: objectIdHeaders,
      enabled: Boolean(boardId),
      staleTime: 30_000,
    },
  );

  const cardsQuery = useRpcQuery(
    CardService,
    "listCardsByBoard",
    { boardId, limit: 200 },
    {
      queryKey: cardsQueryKey(boardId),
      meta: objectIdHeaders,
      enabled: Boolean(boardId),
      staleTime: 30_000,
    },
  );

  const columns = useMemo<KanbanColumn[]>(() => {
    const boardDetail = boardQuery.data;
    const cardsData = cardsQuery.data;
    if (!boardDetail) return [];

    // Group cards by column_id.
    const cardsByColumn = new Map<string, ReturnType<typeof cardToView>[]>();
    for (const col of boardDetail.columns) {
      cardsByColumn.set(col.id, []);
    }
    for (const card of cardsData?.cards ?? []) {
      const bucket = cardsByColumn.get(card.columnId);
      if (bucket) {
        bucket.push(cardToView(card));
      }
    }

    // Sort columns by position, then sort cards within each column by position.
    return [...boardDetail.columns]
      .sort((a, b) => a.position - b.position)
      .map((col) => ({
        id: col.id,
        title: col.title,
        cards: (cardsByColumn.get(col.id) ?? []).sort(
          (a, b) =>
            (cardsData?.cards.find((c: { id: string }) => c.id === a.id)?.position ?? 0) -
            (cardsData?.cards.find((c: { id: string }) => c.id === b.id)?.position ?? 0),
        ),
      }));
  }, [boardQuery.data, cardsQuery.data]);

  return {
    columns,
    isLoading: boardQuery.isLoading || cardsQuery.isLoading,
    error: (boardQuery.error ?? cardsQuery.error) as Error | null,
    boardName: boardQuery.data?.board?.name ?? "",
  };
}
