/**
 * BoardPage — board canvas with live data via BoardView.
 * Stage 6e: wires BoardView with useRpcQuery + useRpcStream + optimistic moves.
 */

import { useParams } from "react-router";
import { CardDrawer } from "../card-drawer";
import { BoardView } from "../board";

export function BoardPage() {
  const { projectId, boardId } = useParams<{
    projectId: string;
    boardId: string;
  }>();

  if (!boardId || !projectId) {
    return <div className="p-6 text-sm">Board not found.</div>;
  }

  return (
    <>
      <BoardView boardId={boardId} projectId={projectId} />
      <CardDrawer />
    </>
  );
}
