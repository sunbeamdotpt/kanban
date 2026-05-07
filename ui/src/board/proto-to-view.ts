/**
 * proto-to-view.ts
 *
 * Maps kanban.v1.Card → beam-ui KanbanCard interface.
 * Pure functions — no React, no side effects.
 */

import type { KanbanCard } from "@sunbeam/beam-ui";
import type { Card } from "../gen/sunbeam/kanban/v1/cards_pb";

/**
 * Map a single proto Card to a beam-ui KanbanCard.
 * Label style tokens are passed through as-is (beam-ui resolves them against
 * the dark token palette per feedback_dark_palette_beam_ui_only).
 */
export function cardToView(card: Card): KanbanCard {
  return {
    id: card.id,
    title: card.title,
    labels: card.labels.map((l) => ({
      name: l.name,
      // style is a beam-ui token; use as the CSS color value.
      // The UI will render the style token via beam-ui's palette.
      color: l.style,
    })),
    assignees: card.assignees.map((a) => ({
      name: a.displayName || a.subject,
      avatarUrl: a.avatarUrl || undefined,
    })),
    milestone: card.milestoneId || undefined,
    cover: card.cover || undefined,
    blocked: card.blocked || undefined,
  };
}
