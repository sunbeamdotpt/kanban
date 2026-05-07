/**
 * board-view.tsx
 *
 * BoardView — the primary board canvas component.
 *
 * Wires together:
 *   - useBoard() for initial data (GetBoard + ListCardsByBoard)
 *   - useBoardSubscription() for live event stream
 *   - Optimistic move via useRpcMutation + pending-mutation queue
 *   - KanbanBoard from beam-ui for rendering + DnD
 */

import { useRef, useCallback } from "react";
import type { KanbanColumn } from "@sunbeam/beam-ui";
import { KanbanBoard } from "@sunbeam/beam-ui";
import { useQueryClient } from "@tanstack/react-query";
import { ulid } from "ulid";
import { useRpcMutation } from "@sunbeam/g2v/hooks";
import { CardService } from "../gen/sunbeam/kanban/v1/cards_pb";
import { useBoard, cardsQueryKey } from "./use-board";
import { useBoardSubscription } from "./use-board-subscription";
import type { PendingMoveQueue } from "./pending-mutations";
import {
  enqueue,
  reject,
  applyPending,
} from "./pending-mutations";
import type { ListCardsByBoardResponse } from "../gen/sunbeam/kanban/v1/cards_pb";

// ─── Props ────────────────────────────────────────────────────────────────────

export interface BoardViewProps {
  boardId: string;
  projectId: string;
}

// ─── Component ────────────────────────────────────────────────────────────────

export function BoardView({ boardId }: BoardViewProps) {
  const queryClient = useQueryClient();

  // Pending-mutation queue scoped to this board mount.
  const pendingQueueRef = useRef<PendingMoveQueue>(new Map());

  // Initial data from server.
  const { columns: serverColumns, isLoading, error, boardName } = useBoard(boardId);

  // Live stream — applies events directly to query cache.
  const { status: streamStatus } = useBoardSubscription({
    boardId,
    pendingQueue: pendingQueueRef.current,
    onConflict: (_cardId) => {
      // A concurrent move from another user overrode our optimistic state.
      // The cache was already restored by useBoardSubscription; nothing more to do here.
      // (A toast could be shown in a real app.)
    },
  });

  // MoveCard mutation — fires the RPC, handles rollback on error.
  const moveCardMutation = useRpcMutation(CardService, "moveCard", {
    onError: (_err: unknown, variables: { idempotencyKey: string }) => {
      // Roll back the failed optimistic move.
      reject(pendingQueueRef.current, variables.idempotencyKey);
      // Restore authoritative card position from server data.
      queryClient.invalidateQueries({ queryKey: cardsQueryKey(boardId) });
    },
  });

  // Derive the view: server authoritative state + pending moves re-projected on top.
  const columns = applyPending(serverColumns, pendingQueueRef.current);

  // KanbanBoard onChange fires for every DnD re-order (within or between columns).
  // We detect card moves by diffing the provided columns against our current view.
  const handleChange = useCallback(
    (updatedColumns: KanbanColumn[]) => {
      // Find which card moved and to which column / position.
      for (const col of updatedColumns) {
        for (let i = 0; i < col.cards.length; i++) {
          const card = col.cards[i];
          // Find where this card was before.
          const prevColEntry = columns.find((c) =>
            c.cards.some((cc) => cc.id === card.id),
          );
          if (!prevColEntry) continue;

          const prevPos = prevColEntry.cards.findIndex((cc) => cc.id === card.id);
          const sameCol = prevColEntry.id === col.id;
          const samePos = sameCol && prevPos === i;
          if (samePos) continue;

          // This card moved.
          const idempotencyKey = ulid();
          const toPosition = i + 1; // 1-based

          // 1. Enqueue the pending move.
          enqueue(pendingQueueRef.current, {
            idempotencyKey,
            intent: {
              cardId: card.id,
              fromColumnId: prevColEntry.id,
              toColumnId: col.id,
              toPosition,
            },
            enqueuedAt: Date.now(),
          });

          // 2. Optimistically patch the query cache position.
          queryClient.setQueryData<ListCardsByBoardResponse>(
            cardsQueryKey(boardId),
            (old) => {
              if (!old) return old;
              return {
                ...old,
                cards: old.cards.map((c) =>
                  c.id === card.id
                    ? { ...c, columnId: col.id, position: toPosition }
                    : c,
                ),
              };
            },
          );

          // 3. Fire the RPC. Stream reconciles on success; onError rolls back.
          moveCardMutation.mutate({
            cardId: card.id,
            toColumnId: col.id,
            toPosition,
            idempotencyKey,
          });

          // Only handle the first detected move per onChange call (DnD emits one
          // card move at a time).
          break;
        }
      }
    },
    [columns, boardId, queryClient, moveCardMutation],
  );

  // ─── Render ─────────────────────────────────────────────────────────────────

  if (isLoading) {
    return (
      <div className="p-6 text-sm text-muted">Loading board…</div>
    );
  }

  if (error) {
    return (
      <div className="p-6 text-sm text-muted">
        Failed to load board: {error.message}
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-3 px-6 py-4 border-b border-default">
        <h1 className="text-sm font-button uppercase tracking-wider text-primary">
          {boardName}
        </h1>
        {streamStatus === "reconnecting" && (
          <span className="text-xs text-muted">Reconnecting…</span>
        )}
        {streamStatus === "closed-by-logout" && (
          <span className="text-xs text-muted">Session ended.</span>
        )}
      </div>

      <div className="flex-1 overflow-auto p-6">
        <KanbanBoard columns={columns} onChange={handleChange} />
      </div>
    </div>
  );
}
