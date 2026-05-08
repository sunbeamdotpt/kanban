/**
 * Breadcrumbs: renders project / board / view based on route params.
 *
 * Routes:
 * - /p/:projectId → "Project"
 * - /p/:projectId/b/:boardId → "Project / Board"
 * - /p/:projectId/b/:boardId/list → "Project / Board / List"
 * - /p/:projectId/b/:boardId/settings → "Project / Board / Settings"
 */

import { useNavigate } from "react-router";
import { css } from "styled-system/css";

export interface BreadcrumbsProps {
  projectId?: string;
  boardId?: string;
  pathname: string;
}

export function Breadcrumbs({
  projectId,
  boardId,
  pathname,
}: BreadcrumbsProps) {
  const navigate = useNavigate();

  // TODO (Stage 6e+): fetch actual project/board names via RPC.
  // Until then, derive readable names from the slug (kebab-case → Title Case).
  const projectName = projectId ? slugToName(projectId) : null;
  const boardName = boardId ? slugToName(boardId) : null;

  let viewLabel: string | null = null;
  if (pathname.endsWith("/list")) {
    viewLabel = "List";
  } else if (pathname.endsWith("/settings")) {
    viewLabel = "Settings";
  }

  const crumbs: { label: string; href?: string }[] = [];
  if (projectName) {
    crumbs.push({ label: projectName, href: `/p/${projectId}` });
  }
  // The reference design omits the board crumb on the settings view (project-
  // level settings). Keep it for list/board views.
  if (boardName && viewLabel !== "Settings") {
    crumbs.push({ label: boardName, href: `/p/${projectId}/b/${boardId}` });
  }
  if (viewLabel) {
    crumbs.push({ label: viewLabel });
  }

  if (crumbs.length === 0) {
    return <div className={fallback}>Home</div>;
  }

  return (
    <div className={trail}>
      {crumbs.map((crumb, idx) => (
        <div key={idx} className={trailItem}>
          {idx > 0 && <span className={separator}>/</span>}
          {crumb.href ? (
            <a
              href={crumb.href}
              className={link}
              onClick={(e) => {
                e.preventDefault();
                navigate(crumb.href!);
              }}
            >
              {crumb.label}
            </a>
          ) : (
            <span className={current}>{crumb.label}</span>
          )}
        </div>
      ))}
    </div>
  );
}

function slugToName(slug: string): string {
  return slug
    .split("-")
    .map((part) => (part.length === 0 ? part : part[0].toUpperCase() + part.slice(1)))
    .join(" ");
}

const trail = css({
  display: "flex",
  alignItems: "center",
  gap: "10px",
  fontFamily: "body",
  fontSize: "12px",
  fontWeight: "button",
  letterSpacing: "0.12em",
  textTransform: "uppercase",
});

const trailItem = css({
  display: "flex",
  alignItems: "center",
  gap: "10px",
});

const link = css({
  color: "sunbeam.orange",
  textDecoration: "none",
  cursor: "pointer",
  _hover: { textDecoration: "underline" },
});

const current = css({
  color: "text.primary",
});

const separator = css({
  color: "text.muted",
});

const fallback = css({
  fontFamily: "body",
  fontSize: "12px",
  fontWeight: "button",
  letterSpacing: "0.12em",
  textTransform: "uppercase",
  color: "text.muted",
});
