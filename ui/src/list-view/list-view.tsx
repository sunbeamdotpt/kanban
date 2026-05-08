/**
 * list-view.tsx
 *
 * ListView — sortable table of all cards on a board.
 * Flattens columns + cards into a single sorted list.
 * Row click sets `?card=<cardId>` URL param.
 */

import { useMemo, useState } from "react";
import { useParams, useSearchParams } from "react-router";
import { Table, Spinner, EmptyState } from "@sunbeam/beam-ui";
import { useBoard } from "../board/use-board";
import { listViewColumns } from "./columns";
import { cardToListRow, type ListRow } from "./list-row";

/**
 * Maps a proto Card to list-view row fields.
 * For now, extracts ref from card.id, priority from a hypothetical field.
 */
function extractCardFields(cardId: string) {
  // Placeholder: derive ref from cardId or fetch from card metadata.
  // Real implementation would read card.ref from the proto.
  return {
    ref: `CARD-${cardId.substring(0, 5).toUpperCase()}`,
    priority: undefined as string | undefined,
    due: undefined as string | undefined,
  };
}

export function ListView() {
  const { boardId } = useParams<{ boardId: string }>();
  const [, setSearchParams] = useSearchParams();

  if (!boardId) {
    return <EmptyState title="No board selected" />;
  }

  const { columns, isLoading, error } = useBoard(boardId);
  const [sortKey, setSortKey] = useState<string>("ref");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  // Flatten columns + cards into a single list, then sort.
  const flattenedAndSorted = useMemo(() => {
    const flattened: ListRow[] = [];

    for (const column of columns) {
      for (const card of column.cards) {
        const { ref, priority, due } = extractCardFields(card.id);
        const row = cardToListRow(
          card,
          column.id,
          column.title,
          ref,
          priority,
          due,
        );
        flattened.push(row);
      }
    }

    // Sort based on sortKey and sortDir.
    flattened.sort((a, b) => {
      let aVal: any = (a as any)[sortKey];
      let bVal: any = (b as any)[sortKey];

      // Handle null/empty values.
      if (aVal === "—") aVal = sortDir === "asc" ? "" : "￿";
      if (bVal === "—") bVal = sortDir === "asc" ? "" : "￿";

      // Type-safe comparison.
      if (aVal < bVal) return sortDir === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDir === "asc" ? 1 : -1;
      return 0;
    });

    return flattened;
  }, [columns, sortKey, sortDir]);

  // Handle sort column click.
  const handleSort = (key: string) => {
    if (key === sortKey) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  };

  // Handle row click → set ?card=<id> param.
  const handleRowClick = (row: ListRow) => {
    setSearchParams({ card: row.id }, { replace: false });
  };

  // ─── Render ─────────────────────────────────────────────────────────────────

  if (isLoading) {
    return (
      <div style={{ padding: "24px", display: "flex", justifyContent: "center" }}>
        <Spinner />
      </div>
    );
  }

  if (error) {
    return (
      <EmptyState
        title="Error loading board"
        description={error.message}
      />
    );
  }

  if (flattenedAndSorted.length === 0) {
    return (
      <EmptyState
        title="No cards"
        description="Start by adding a card to the board."
      />
    );
  }

  // Prepare rows for Table: labels is JSX, but Table expects Record<string, any>.
  // We'll render labels in a custom way per row.
  const tableRows = flattenedAndSorted.map((row) => ({
    id: row.id,
    ref: row.ref,
    title: row.title,
    columnName: row.columnName,
    assignees: row.assignees,
    labels: row.labels, // This is JSX; the table will render it.
    priority: row.priority,
    due: row.due,
    status: row.status,
  }));

  return (
    <div style={{ padding: "24px" }}>
      <Table
        columns={listViewColumns}
        rows={tableRows}
        onSort={handleSort}
        rowKey="id"
        caption="Cards in board"
      />
      {/* Optional: overlay for row click to set card param. */}
      <div
        style={{
          position: "absolute",
          pointerEvents: "none",
        }}
      >
        {flattenedAndSorted.map((row) => (
          <div
            key={row.id}
            style={{
              cursor: "pointer",
              pointerEvents: "auto",
              position: "relative",
              zIndex: 10,
            }}
            onClick={() => handleRowClick(row)}
          />
        ))}
      </div>
    </div>
  );
}
