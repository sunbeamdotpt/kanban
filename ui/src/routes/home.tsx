/**
 * HomePage — project list landing page.
 * Calls ProjectService.ListProjects and renders a link per project.
 */

import { Link } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { useRpcClient } from "@sunbeam/g2v/hooks";
import { css } from "styled-system/css";
import { ProjectService } from "../gen/sunbeam/kanban/v1/projects_pb";
import { useSubheader } from "../shell/subheader-context";
import { useEffect } from "react";

const page = css({
  padding: "24px 32px",
});

const heading = css({
  fontSize: "32px",
  fontWeight: "bold",
  color: "text.primary",
});

const muted = css({
  color: "text.tertiary",
  fontSize: "14px",
});

const projectLink = css({
  color: "accent",
  textDecoration: "underline",
  fontSize: "14px",
});

const projectList = css({
  display: "flex",
  flexDirection: "column",
  gap: "8px",
  marginTop: "16px",
});

export function HomePage() {
  const projectClient = useRpcClient(ProjectService);
  const { setContent } = useSubheader();

  const projectsQuery = useQuery({
    queryKey: ["projects"],
    staleTime: 0,
    retry: 3,
    retryDelay: 500,
    queryFn: () => projectClient.listProjects({}),
  });

  const projects = projectsQuery.data?.projects ?? [];

  // Inject heading into subheader
  useEffect(() => {
    setContent(
      <div>
        <div className={heading}>Projects</div>
        {projectsQuery.isLoading && <p className={muted}>Loading projects…</p>}
        {!projectsQuery.isLoading && projects.length === 0 && (
          <p className={muted}>
            No projects yet. Use the + button in the sidebar to create one.
          </p>
        )}
      </div>
    );
    return () => setContent(null);
  }, [projectsQuery.isLoading, projects.length, setContent]);

  return (
    <div className={page}>
      <div className={projectList}>
        {projects.map((project) => (
          <Link
            key={project.id}
            to={`/p/${project.id}`}
            className={projectLink}
          >
            {project.name}
          </Link>
        ))}
      </div>
    </div>
  );
}
