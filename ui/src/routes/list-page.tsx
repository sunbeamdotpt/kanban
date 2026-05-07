/**
 * ListPage — list view for cards in a board.
 * TODO(Stage 6g): Replace with actual list layout and sorting controls.
 */

import { useParams } from "react-router";

export function ListPage() {
  const { boardId } = useParams<{ boardId: string }>();

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-4">List view: {boardId}</h1>
      <p className="text-muted">Cards will be listed here.</p>
    </div>
  );
}
