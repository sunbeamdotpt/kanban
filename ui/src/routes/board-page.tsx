/**
 * BoardPage — board view with optional card deep-link.
 * TODO(Stage 6e): Replace with actual board canvas, card detail panel, and realtime stream.
 */

import { useParams, useSearchParams } from "react-router";

export function BoardPage() {
  const { projectId, boardId } = useParams<{
    projectId: string;
    boardId: string;
  }>();
  const [searchParams] = useSearchParams();
  const cardId = searchParams.get("card");

  return (
    <div className="p-6">
      <h1 className="text-4xl font-bold mb-4">
        Board {boardId} (project {projectId})
      </h1>
      {cardId && <p className="text-muted">Card open: {cardId}</p>}
      <p className="text-muted mt-4">Board canvas will render here.</p>
    </div>
  );
}
