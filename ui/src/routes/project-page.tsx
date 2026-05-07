/**
 * ProjectPage — project header + board list.
 * TODO(Stage 6e): Replace with actual board list and new board CTA.
 */

import { useParams } from "react-router";

export function ProjectPage() {
  const { projectId } = useParams<{ projectId: string }>();

  return (
    <div className="p-6">
      <h1 className="text-4xl font-bold mb-4">Project {projectId}</h1>
      <p className="text-muted">Boards will be listed here.</p>
    </div>
  );
}
