/**
 * Breadcrumbs: renders project / board / view based on route params.
 *
 * Routes:
 * - /p/:projectId → "ProjectName"
 * - /p/:projectId/b/:boardId → "ProjectName / BoardName"
 * - /p/:projectId/b/:boardId/list → "ProjectName / BoardName / List"
 * - /p/:projectId/b/:boardId/settings → "ProjectName / BoardName / Settings"
 */

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
  // TODO (Stage 6e+): fetch actual project/board names via RPC.
  // For now, use placeholder names derived from IDs.
  const projectName = projectId ? `Project ${projectId.slice(0, 8)}` : null;
  const boardName = boardId ? `Board ${boardId.slice(0, 8)}` : null;

  // Determine view label from pathname.
  let viewLabel: string | null = null;
  if (pathname.endsWith("/list")) {
    viewLabel = "List";
  } else if (pathname.endsWith("/settings")) {
    viewLabel = "Settings";
  }

  // Build crumbs array.
  const crumbs: { label: string; href?: string }[] = [];
  if (projectName) {
    crumbs.push({
      label: projectName,
      href: `/p/${projectId}`,
    });
  }
  if (boardName) {
    crumbs.push({
      label: boardName,
      href: `/p/${projectId}/b/${boardId}`,
    });
  }
  if (viewLabel) {
    crumbs.push({
      label: viewLabel,
    });
  }

  if (crumbs.length === 0) {
    return (
      <div style={{ color: "var(--beam-color-text-tertiary)", fontSize: "14px" }}>
        Home
      </div>
    );
  }

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "8px",
        fontSize: "14px",
        color: "var(--beam-color-text-secondary)",
      }}
    >
      {crumbs.map((crumb, idx) => (
        <div key={idx} style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          {idx > 0 && <span style={{ color: "var(--beam-color-text-tertiary)" }}>/</span>}
          {crumb.href ? (
            <a
              href={crumb.href}
              style={{
                color: "var(--beam-color-link)",
                textDecoration: "none",
                cursor: "pointer",
              }}
              onClick={(e) => {
                e.preventDefault();
                window.location.href = crumb.href!;
              }}
            >
              {crumb.label}
            </a>
          ) : (
            <span style={{ color: "var(--beam-color-text)" }}>{crumb.label}</span>
          )}
        </div>
      ))}
    </div>
  );
}
