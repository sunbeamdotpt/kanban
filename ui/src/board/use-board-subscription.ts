/**
 * use-board-subscription.ts
 *
 * Opens a BoardService.SubscribeBoard server-streaming connection and applies
 * incoming events to the TanStack Query cache so useBoard() sees live updates.
 *
 * Event handling:
 *   CardCreated      → add card to column in cache.
 *   CardUpdated      → patch card fields in cache.
 *   CardMoved        → relocate card between/within columns in cache.
 *   CardDeleted      → remove card from cache.
 *   ColumnAdded      → add column to board detail cache.
 *   ColumnRenamed    → patch column title in board detail cache.
 *   ColumnRemoved    → remove column from board detail cache.
 *   MembershipChanged→ invalidate getBoard query (membership change may affect visibility).
 *   Heartbeat/Cutover→ no-op (stream control only).
 *   Everything else  → ignored (board/member/project events not rendered in board view).
 */

import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useRpcStream } from "@sunbeam/g2v/hooks";
import type { StreamMethodDescriptor } from "@sunbeam/g2v/hooks";
import type { BoardEventEnvelope } from "../gen/sunbeam/kanban/v1/events_pb";
import { BoardService } from "../gen/sunbeam/kanban/v1/boards_pb";
import type { PendingMoveQueue } from "./pending-mutations";
import { acknowledge, findByCardAndAck } from "./pending-mutations";
import { boardQueryKey, cardsQueryKey } from "./use-board";
import type { BoardDetail } from "../gen/sunbeam/kanban/v1/boards_pb";
import type { ListCardsByBoardResponse } from "../gen/sunbeam/kanban/v1/cards_pb";

// ─── Stream method descriptor ─────────────────────────────────────────────────
// protobuf-es v2 DescMethod has kind="rpc" at runtime, but StreamMethodDescriptor
// requires kind="server_streaming". Cast via unknown to satisfy the interface —
// the runtime dispatch in useRpcStream uses `localName` only, not `kind`.
const subscribeBoardMethod = BoardService.method
  .subscribeBoard as unknown as StreamMethodDescriptor<
  { boardId: string; sinceSeq: bigint; resume_from?: bigint | number },
  BoardEventEnvelope
>;

// ─── Hook ─────────────────────────────────────────────────────────────────────

export interface UseBoardSubscriptionOptions {
  boardId: string;
  pendingQueue: PendingMoveQueue;
  /** Called when a conflict is detected (another user's move overrides ours). */
  onConflict?: (cardId: string) => void;
}

export function useBoardSubscription({
  boardId,
  pendingQueue,
  onConflict,
}: UseBoardSubscriptionOptions) {
  const queryClient = useQueryClient();

  const { events, status, error } = useRpcStream<
    { boardId: string; sinceSeq: bigint; resume_from?: bigint | number },
    BoardEventEnvelope
  >({
    method: subscribeBoardMethod,
    request: { boardId, sinceSeq: 0n },
    getSeq: (e) => e.natsSeq,
    getEventId: (e) => e.eventId,
    enabled: Boolean(boardId),
  });

  // Apply each new event to the query cache.
  useEffect(() => {
    for (const envelope of events) {
      applyEnvelope(envelope, boardId, queryClient, pendingQueue, onConflict);
    }
    // We intentionally depend on the `events` array reference — useRpcStream
    // returns a new array reference each time an event arrives.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events]);

  return { status, error };
}

// ─── Event application ────────────────────────────────────────────────────────

function applyEnvelope(
  envelope: BoardEventEnvelope,
  boardId: string,
  queryClient: ReturnType<typeof useQueryClient>,
  pendingQueue: PendingMoveQueue,
  onConflict?: (cardId: string) => void,
) {
  const { payload } = envelope;
  if (!payload.case) return;

  switch (payload.case) {
    case "cardCreated": {
      const { card } = payload.value;
      if (!card) return;
      queryClient.setQueryData<ListCardsByBoardResponse>(
        cardsQueryKey(boardId),
        (old) => {
          if (!old) return old;
          return { ...old, cards: [...old.cards, card] };
        },
      );
      break;
    }

    case "cardUpdated": {
      const { cardId, patch } = payload.value;
      queryClient.setQueryData<ListCardsByBoardResponse>(
        cardsQueryKey(boardId),
        (old) => {
          if (!old) return old;
          return {
            ...old,
            cards: old.cards.map((c) =>
              c.id === cardId ? { ...c, ...(patch as object) } : c,
            ),
          };
        },
      );
      break;
    }

    case "cardMoved": {
      const { cardId, toColumn, toPosition, idempotencyKey } = payload.value;

      // Reconcile pending-mutation queue.
      if (idempotencyKey && pendingQueue.has(idempotencyKey)) {
        // Our own move confirmed — drop from queue.
        acknowledge(pendingQueue, idempotencyKey);
      } else {
        // Someone else's move for this card — clear our pending moves for it.
        const hadPending = [...pendingQueue.values()].some(
          (m) => m.intent.cardId === cardId,
        );
        if (hadPending) {
          findByCardAndAck(pendingQueue, cardId);
          onConflict?.(cardId);
        }
      }

      queryClient.setQueryData<ListCardsByBoardResponse>(
        cardsQueryKey(boardId),
        (old) => {
          if (!old) return old;
          return {
            ...old,
            cards: old.cards.map((c) =>
              c.id === cardId
                ? { ...c, columnId: toColumn, position: toPosition }
                : c,
            ),
          };
        },
      );
      break;
    }

    case "cardDeleted": {
      const { cardId } = payload.value;
      queryClient.setQueryData<ListCardsByBoardResponse>(
        cardsQueryKey(boardId),
        (old) => {
          if (!old) return old;
          return { ...old, cards: old.cards.filter((c) => c.id !== cardId) };
        },
      );
      break;
    }

    case "columnAdded": {
      const { column } = payload.value;
      if (!column) return;
      queryClient.setQueryData<BoardDetail>(boardQueryKey(boardId), (old) => {
        if (!old) return old;
        // Build a minimal Column with required proto fields via spread.
        // The cache is typed as BoardDetail (proto Message) but TanStack Query
        // allows partial structural updates; we spread the existing record to
        // satisfy the type shape without importing create().
        const newColumn = {
          ...old.columns[0], // inherit $typeName + $unknown from a sibling
          id: column.id,
          boardId: column.boardId,
          title: column.title,
          accent: column.accent,
          wipLimit: column.wipLimit,
          position: column.position,
        };
        return { ...old, columns: [...old.columns, newColumn] };
      });
      break;
    }

    case "columnRenamed": {
      const { columnId, newTitle } = payload.value;
      queryClient.setQueryData<BoardDetail>(boardQueryKey(boardId), (old) => {
        if (!old) return old;
        return {
          ...old,
          columns: old.columns.map((c) =>
            c.id === columnId ? { ...c, title: newTitle } : c,
          ),
        };
      });
      break;
    }

    case "columnRemoved": {
      const { columnId } = payload.value;
      queryClient.setQueryData<BoardDetail>(boardQueryKey(boardId), (old) => {
        if (!old) return old;
        return {
          ...old,
          columns: old.columns.filter((c) => c.id !== columnId),
        };
      });
      break;
    }

    case "membershipChanged":
    case "memberAdded":
    case "memberRemoved":
    case "memberRoleChanged": {
      // Membership changes may revoke access — invalidate the board query so
      // the server can return a fresh response (or a permission error).
      void queryClient.invalidateQueries({ queryKey: boardQueryKey(boardId) });
      break;
    }

    // Stream control — no-op for UI.
    case "heartbeat":
    case "cutover":
      break;

    // All other events (board rename/update, project events, checklist, etc.)
    // are not rendered in the board canvas view — ignore.
    default:
      break;
  }
}
