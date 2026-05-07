/**
 * pending-mutations.ts
 *
 * In-memory pending-mutation queue for optimistic card moves.
 * Scoped per-board via a useRef<PendingMoveQueue> inside BoardView.
 *
 * Lifecycle (Tension 7 / Pre-mortem 3):
 *   1. enqueue(key, intent) — add a pending move after firing MoveCard RPC.
 *   2. applyPending(columns, queue) — re-project optimistic state on top of
 *      authoritative server columns.
 *   3. acknowledge(key) — stream event with matching idempotency_key arrived;
 *      drop the mutation and accept server state.
 *   4. findByCardAndAck(cardId, serverKey) — a CardMoved event arrived for the
 *      same card but with a different idempotency_key (conflict); ack all
 *      pending moves for that card and let server state win.
 *   5. reject(key) — RPC failed; remove from queue so next applyPending
 *      restores authoritative state (no later mutation for this card is
 *      enqueued to absorb the failure).
 */

import type { KanbanColumn } from "@sunbeam/beam-ui";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface PendingMoveIntent {
  cardId: string;
  fromColumnId: string;
  toColumnId: string;
  toPosition: number;
}

export interface PendingMove {
  idempotencyKey: string;
  intent: PendingMoveIntent;
  enqueuedAt: number;
}

export type PendingMoveQueue = Map<string, PendingMove>;

// ─── Queue mutations ──────────────────────────────────────────────────────────

export function enqueue(queue: PendingMoveQueue, move: PendingMove): void {
  queue.set(move.idempotencyKey, move);
}

export function acknowledge(queue: PendingMoveQueue, idempotencyKey: string): boolean {
  return queue.delete(idempotencyKey);
}

/**
 * A CardMoved event arrived for cardId with a server idempotency key that does
 * not match any pending move. Remove ALL pending moves for this card so server
 * state takes over.
 */
export function findByCardAndAck(queue: PendingMoveQueue, cardId: string): void {
  for (const [key, move] of queue.entries()) {
    if (move.intent.cardId === cardId) {
      queue.delete(key);
    }
  }
}

export function reject(queue: PendingMoveQueue, idempotencyKey: string): void {
  queue.delete(idempotencyKey);
}

// ─── Re-projection ────────────────────────────────────────────────────────────

/**
 * Re-apply all pending moves on top of authoritative server columns.
 * Deterministic: moves applied in insertion order (Map preserves insertion order).
 * Returns a new columns array — original is not mutated.
 */
export function applyPending(
  columns: KanbanColumn[],
  queue: PendingMoveQueue,
): KanbanColumn[] {
  if (queue.size === 0) return columns;

  // Deep-clone cards arrays so we can splice without mutating server state.
  let cols: KanbanColumn[] = columns.map((c) => ({
    ...c,
    cards: [...c.cards],
  }));

  for (const move of queue.values()) {
    const { cardId, fromColumnId, toColumnId, toPosition } = move.intent;

    const srcCol = cols.find((c) => c.id === fromColumnId);
    const dstCol = cols.find((c) => c.id === toColumnId);

    if (!srcCol || !dstCol) continue;

    const cardIdx = srcCol.cards.findIndex((c) => c.id === cardId);
    if (cardIdx === -1) {
      // Card might already be in destination from a prior pending move.
      const alreadyInDst = dstCol.cards.findIndex((c) => c.id === cardId);
      if (alreadyInDst === -1) continue;
      // Move within destination to the new position.
      const [card] = dstCol.cards.splice(alreadyInDst, 1);
      const insertAt = Math.max(0, Math.min(toPosition - 1, dstCol.cards.length));
      dstCol.cards.splice(insertAt, 0, card);
      continue;
    }

    const [card] = srcCol.cards.splice(cardIdx, 1);
    const insertAt = Math.max(0, Math.min(toPosition - 1, dstCol.cards.length));
    dstCol.cards.splice(insertAt, 0, card);
  }

  return cols;
}
