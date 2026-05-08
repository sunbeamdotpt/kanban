/**
 * list-row.tsx
 *
 * Type-safe row data for ListView table rendering.
 * Composes card + column data into a flattened table row.
 */

import { type ReactNode } from "react";
import type { KanbanCard } from "@sunbeam/beam-ui";

/**
 * ListRow represents a card flattened into a single table row.
 * Contains ref, title, column name, assignee initials, label pills,
 * priority, due date, and status derived from the column.
 */
export interface ListRow {
  id: string;
  ref: string;
  title: string;
  columnId: string;
  columnName: string;
  assignees: string; // Comma-separated initials
  labels: ReactNode; // Rendered label pills
  priority: string; // "Critical", "High", "Medium", "Low", or empty
  due: string; // Formatted date or empty
  status: string; // Column title (Done, In progress, etc.)
}

/**
 * Build ListRow from a card and the board's column list.
 * Flattens card data + column context into a sortable table row.
 */
export function cardToListRow(
  card: KanbanCard,
  columnId: string,
  columnName: string,
  cardRef: string,
  cardPriority: string | undefined,
  cardDue: string | undefined,
): ListRow {
  // Assignees: comma-separated initials (first char of each word).
  const assigneeInitials = (card.assignees || [])
    .map((a) => {
      const parts = a.name.split(" ");
      return parts.map((p) => p[0]).join("");
    })
    .join(", ");

  // Labels: render as pill components.
  const labelPills = (card.labels || []).map((label, idx) => (
    <span
      key={`${label.name}-${idx}`}
      style={{
        display: "inline-block",
        padding: "4px 8px",
        marginRight: "4px",
        backgroundColor: `var(--label-${label.color}, #e0e0e0)`,
        borderRadius: "4px",
        fontSize: "12px",
        fontWeight: 500,
      }}
    >
      {label.name}
    </span>
  ));

  return {
    id: card.id,
    ref: cardRef,
    title: card.title,
    columnId,
    columnName,
    assignees: assigneeInitials || "—",
    labels: labelPills.length > 0 ? <div>{labelPills}</div> : "—",
    priority: cardPriority || "—",
    due: cardDue || "—",
    status: columnName,
  };
}
