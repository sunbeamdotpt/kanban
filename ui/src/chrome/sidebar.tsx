/**
 * Sidebar: workspace > project > boards tree with expand/collapse.
 *
 * Wired to real data via:
 *   - ProjectService.ListProjects  → project rows
 *   - BoardService.ListBoards      → board rows under each project
 *
 * Active project/board highlighted from URL params.
 */

import { useState } from "react";
import { useParams, useNavigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { useRpcClient } from "@sunbeam/g2v/hooks";
import { css, cx } from "styled-system/css";
import { ProjectService } from "../gen/sunbeam/kanban/v1/projects_pb";
import { BoardService } from "../gen/sunbeam/kanban/v1/boards_pb";
import { ProjectWizard } from "../project-wizard";
import { BoardWizard } from "../board-wizard/board-wizard";

// ── Deterministic project icon palette ───────────────────────────────────────

const PROJECT_ICONS = ["palette", "auto_awesome", "sports_score", "language", "hub", "bolt", "rocket_launch", "diamond"];
const PROJECT_COLORS = ["#fa520f", "#fb6424", "#7c3aed", "#1abc9c", "#ffb83e", "#0ea5e9", "#ec4899", "#84cc16"];

function iconForProject(id: string): string {
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) | 0;
  return PROJECT_ICONS[Math.abs(h) % PROJECT_ICONS.length];
}

function colorForProject(id: string, explicitColor?: string): string {
  if (explicitColor && explicitColor.startsWith("#")) return explicitColor;
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) | 0;
  return PROJECT_COLORS[Math.abs(h) % PROJECT_COLORS.length];
}

// ── Styles ───────────────────────────────────────────────────────────────────

const sidebar = css({
  backgroundColor: "bg.page",
  overflowY: "auto",
  padding: "12px 0 24px",
  display: "flex",
  flexDirection: "column",
  height: "100%",
});

const section = css({
  padding: "0 8px",
});

const sectionHead = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  padding: "6px 8px",
  fontFamily: "body",
  fontSize: "10px",
  fontWeight: "button",
  letterSpacing: "0.15em",
  textTransform: "uppercase",
  color: "sunbeam.orange",
});

const sectionHeadBtn = css({
  background: "transparent",
  border: "none",
  cursor: "pointer",
  color: "text.muted",
  padding: "2px",
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  _hover: { color: "sunbeam.orange" },
});

const projectRow = css({
  display: "flex",
  alignItems: "center",
  gap: "8px",
  width: "100%",
  padding: "7px 8px",
  background: "transparent",
  borderLeft: "2px solid transparent",
  marginLeft: "-2px",
  cursor: "pointer",
  fontFamily: "body",
  fontWeight: "button",
  fontSize: "13px",
  color: "text.primary",
  textAlign: "left",
  borderRadius: "0 2px 2px 0",
  transition: "background 0.12s",
  _hover: { backgroundColor: "cream" },
});

const chevron = css({
  color: "text.muted",
  transition: "transform 0.15s",
  flexShrink: 0,
  fontSize: "16px !important",
});

const projectIcon = css({
  width: "22px",
  height: "22px",
  borderRadius: "sm",
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  fontFamily: "heading",
  fontWeight: "button",
  fontSize: "11px",
  color: "ivory",
  letterSpacing: "-0.02em",
  flexShrink: 0,
});

const itemLabel = css({
  flex: "1 1 0%",
  minWidth: 0,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
});

const projectCount = css({
  fontFamily: "mono",
  fontSize: "10px",
  color: "text.muted",
});

const boardsWrap = css({
  display: "flex",
  flexDirection: "column",
  padding: "2px 0 6px",
});

const boardRow = css({
  display: "flex",
  alignItems: "center",
  gap: "8px",
  padding: "5px 8px 5px 38px",
  borderLeft: "2px solid transparent",
  marginLeft: "-2px",
  cursor: "pointer",
  background: "transparent",
  fontFamily: "body",
  fontWeight: "body",
  fontSize: "13px",
  color: "text.secondary",
  textAlign: "left",
  width: "100%",
  borderRadius: "0 2px 2px 0",
  transition: "color 0.12s, background 0.12s",
  position: "relative",
  _hover: {
    color: "sunbeam.orange",
    backgroundColor: "cream",
  },
  "& .board-gear": { display: "none" },
  "& .board-count": { marginLeft: "auto" },
  "&:hover .board-gear, &.is-active .board-gear": { display: "inline-flex", opacity: 1 },
  "&:hover .board-count, &.is-active .board-count": { display: "none" },
});

const boardRowActive = css({
  color: "sunbeam.orange !important",
  borderLeftColor: "sunbeam.orange !important",
  backgroundColor: "rgba(250, 82, 15, 0.06) !important",
  fontWeight: "button !important",
});

const boardIcon = css({
  fontSize: "14px !important",
  color: "text.muted",
});

const boardCount = css({
  fontFamily: "mono",
  fontSize: "10px",
  color: "text.muted",
});

const boardGear = css({
  color: "text.muted",
  marginLeft: "auto",
  opacity: 0,
  padding: "2px",
  borderRadius: "sm",
  transition: "opacity 0.12s, color 0.12s, background 0.12s",
  fontSize: "14px !important",
  _hover: { color: "sunbeam.orange", backgroundColor: "cream.deep" },
});

const addBoardBtn = css({
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "5px 8px 5px 38px",
  background: "transparent",
  border: "none",
  fontFamily: "body",
  fontWeight: "body",
  fontSize: "12px",
  color: "text.muted",
  cursor: "pointer",
  textAlign: "left",
  width: "100%",
  _hover: { color: "sunbeam.orange" },
});

// ── BoardRow (fetches boards for one project) ─────────────────────────────────

function BoardRows({
  projectId,
  activeProjectId,
  activeBoardId,
  onBoardClick,
  onNewBoard,
}: {
  projectId: string;
  activeProjectId?: string;
  activeBoardId?: string;
  onBoardClick: (pid: string, bid: string) => void;
  onNewBoard: (pid: string) => void;
}) {
  const boardClient = useRpcClient(BoardService);
  const boardsQuery = useQuery({
    queryKey: ["boards", projectId],
    enabled: Boolean(projectId),
    staleTime: 30_000,
    queryFn: () => boardClient.listBoards({ projectId }),
  });

  const boards = boardsQuery.data?.boards ?? [];

  return (
    <div className={boardsWrap}>
      {boardsQuery.isLoading && (
        <div style={{ padding: "4px 8px", fontSize: "11px", color: "var(--text-muted)" }}>
          Loading…
        </div>
      )}
      {boards.map((board) => {
        const isActive = activeProjectId === projectId && activeBoardId === board.id;
        return (
          <button
            key={board.id}
            onClick={() => onBoardClick(projectId, board.id)}
            className={cx(boardRow, isActive && boardRowActive, isActive && "is-active")}
          >
            <span className={cx("material-symbols-outlined", boardIcon)} aria-hidden="true">
              {board.icon || "view_kanban"}
            </span>
            <span className={itemLabel}>{board.name}</span>
            <span
              className={cx("material-symbols-outlined", boardGear, "board-gear")}
              aria-hidden="true"
              title="Board settings"
              onClick={(e) => {
                e.stopPropagation();
                onBoardClick(projectId, board.id);
                // Navigate to board settings after a tick so the board route loads first
                setTimeout(() => {
                  window.location.href = `/p/${projectId}/b/${board.id}/settings`;
                }, 0);
              }}
            >
              settings
            </span>
            <span className={cx(boardCount, "board-count")}>{board.cardsCount}</span>
          </button>
        );
      })}

      <button className={addBoardBtn} onClick={() => onNewBoard(projectId)}>
        <span className="material-symbols-outlined" aria-hidden="true" style={{ fontSize: "14px", lineHeight: 1 }}>add</span>
        <span>New board</span>
      </button>
    </div>
  );
}

// ── Sidebar ───────────────────────────────────────────────────────────────────

export function Sidebar() {
  const params = useParams<{ projectId?: string; boardId?: string }>();
  const navigate = useNavigate();
  const projectClient = useRpcClient(ProjectService);

  const [openProjects, setOpenProjects] = useState<Record<string, boolean>>({});
  const [wizardOpen, setWizardOpen] = useState(false);
  const [boardWizardProjectId, setBoardWizardProjectId] = useState<string | null>(null);

  const projectsQuery = useQuery({
    queryKey: ["projects"],
    staleTime: 0,
    retry: 3,
    retryDelay: 500,
    queryFn: () => projectClient.listProjects({}),
  });

  const projects = projectsQuery.data?.projects ?? [];

  const toggleProject = (projectId: string) => {
    setOpenProjects((prev) => ({ ...prev, [projectId]: !prev[projectId] }));
  };

  const handleBoardClick = (pid: string, bid: string) => {
    navigate(`/p/${pid}/b/${bid}`);
  };

  // Auto-expand the active project on first render.
  const activeProjectId = params.projectId;
  if (activeProjectId && !(activeProjectId in openProjects) && projects.some((p) => p.id === activeProjectId)) {
    setOpenProjects((prev) => ({ ...prev, [activeProjectId]: true }));
  }

  return (
    <div className={sidebar}>
      <div className={section}>
        {/* Workspace heading + New project button */}
        <div className={sectionHead}>
          <span>Workspace</span>
          <button
            onClick={() => setWizardOpen(true)}
            title="New project"
            aria-label="New project"
            className={sectionHeadBtn}
          >
            <span className="material-symbols-outlined" style={{ fontSize: "16px", lineHeight: 1 }}>add</span>
          </button>
        </div>

        {/* Loading state */}
        {projectsQuery.isLoading && (
          <div style={{ padding: "8px", textAlign: "center", color: "var(--text-muted)", fontSize: "12px" }}>
            Loading projects…
          </div>
        )}

        {/* Empty state */}
        {!projectsQuery.isLoading && projects.length === 0 && (
          <div style={{ padding: "12px 8px", textAlign: "center", color: "var(--text-muted)", fontSize: "12px" }}>
            No projects yet.
          </div>
        )}

        {/* Project rows */}
        {projects.map((project) => {
          const isOpen = openProjects[project.id] ?? false;
          const isActive = params.projectId === project.id;
          const icon = project.icon || iconForProject(project.id);
          const color = colorForProject(project.id, project.color);

          return (
            <div key={project.id}>
              <button
                onClick={() => toggleProject(project.id)}
                className={projectRow}
                data-open={isOpen}
              >
                <span
                  className={cx("material-symbols-outlined", chevron)}
                  aria-hidden="true"
                  style={{ transform: isOpen ? "rotate(90deg)" : "rotate(0deg)" }}
                >
                  chevron_right
                </span>

                <span className={projectIcon} style={{ background: color }}>
                  <span className="material-symbols-outlined" style={{ fontSize: "14px", color: "#fff" }}>
                    {icon}
                  </span>
                </span>

                <span className={itemLabel}>{project.name}</span>

                <span className={projectCount}>
                  {/* Count is computed from boards query when expanded, or we could show nothing */}
                </span>
              </button>

              {isOpen && (
                <BoardRows
                  projectId={project.id}
                  activeProjectId={params.projectId}
                  activeBoardId={params.boardId}
                  onBoardClick={handleBoardClick}
                  onNewBoard={(pid) => setBoardWizardProjectId(pid)}
                />
              )}
            </div>
          );
        })}
      </div>

      {wizardOpen && <ProjectWizard open onOpenChange={setWizardOpen} />}
      {boardWizardProjectId && (
        <BoardWizard
          open={Boolean(boardWizardProjectId)}
          onOpenChange={(open) => { if (!open) setBoardWizardProjectId(null); }}
          projectId={boardWizardProjectId}
          onCreated={(boardId) => {
            navigate(`/p/${boardWizardProjectId}/b/${boardId}`);
            setBoardWizardProjectId(null);
          }}
        />
      )}
    </div>
  );
}
