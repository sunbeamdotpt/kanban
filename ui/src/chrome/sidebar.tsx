/**
 * Sidebar: workspace > project > boards tree with expand/collapse.
 *
 * Renders:
 * - "Workspace" heading with "+ New project" button
 * - Project list (initially empty; TODO Stage 6e+ wire to useProjects())
 * - Each project row with chevron to expand/show boards
 * - Each board links to /p/:projectId/b/:boardId
 * - "+ New board" button under active project
 *
 * TODO (Stage 6e+): wire sidebar data to ProjectService.ListProjects + BoardService.ListBoards.
 */

import { useState } from "react";
import { useParams, useNavigate } from "react-router";
import { ScrollArea } from "@sunbeam/beam-ui";
import { ProjectWizard } from "../project-wizard";

interface Project {
  id: string;
  name: string;
  icon: string;
  color: string;
  boards: Board[];
}

interface Board {
  id: string;
  name: string;
  icon: string;
}

/**
 * Placeholder data for Stage 6d.
 * TODO (Stage 6e+): replace with useProjects() hook.
 */
const EMPTY_PROJECTS: Project[] = [];

export function Sidebar() {
  const params = useParams<{ projectId?: string; boardId?: string }>();
  const navigate = useNavigate();

  const [projects, setProjects] = useState<Project[]>(EMPTY_PROJECTS);
  const [openProjects, setOpenProjects] = useState<Record<string, boolean>>({});
  const [wizardOpen, setWizardOpen] = useState(false);

  const toggleProject = (projectId: string) => {
    setOpenProjects((prev) => ({
      ...prev,
      [projectId]: !prev[projectId],
    }));
  };

  const handleNewProject = () => {
    setWizardOpen(true);
  };

  const handleNewBoard = (projectId: string) => {
    // TODO (Stage 6e+): open new board modal.
    console.log("new board clicked for project:", projectId);
  };

  const handleBoardClick = (projectId: string, boardId: string) => {
    navigate(`/p/${projectId}/b/${boardId}`);
  };

  return (
    <ScrollArea>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          padding: "12px",
          minHeight: "100%",
        }}
      >
        {/* Workspace heading + New project button */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: "16px",
            paddingBottom: "12px",
            borderBottom: "1px solid var(--beam-color-border)",
          }}
        >
          <h3
            style={{
              fontSize: "13px",
              fontWeight: "600",
              color: "var(--beam-color-text-secondary)",
              margin: 0,
              textTransform: "uppercase",
              letterSpacing: "0.5px",
            }}
          >
            Workspace
          </h3>
          <button
            onClick={handleNewProject}
            title="New project"
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              width: "28px",
              height: "28px",
              borderRadius: "4px",
              border: "none",
              backgroundColor: "transparent",
              cursor: "pointer",
              color: "var(--beam-color-text-secondary)",
              fontSize: "16px",
              transition: "background-color 0.2s",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor =
                "var(--beam-color-bg-secondary)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = "transparent";
            }}
          >
            +
          </button>
        </div>

        {/* Projects list */}
        {projects.length === 0 ? (
          <div
            style={{
              padding: "16px 8px",
              textAlign: "center",
              color: "var(--beam-color-text-tertiary)",
              fontSize: "12px",
            }}
          >
            No projects yet. Click the + button to create one.
          </div>
        ) : (
          projects.map((project) => {
            const isOpen = openProjects[project.id] ?? true;
            const isActive = params.projectId === project.id;

            return (
              <div key={project.id} style={{ marginBottom: "8px" }}>
                {/* Project row */}
                <button
                  onClick={() => toggleProject(project.id)}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    width: "100%",
                    padding: "8px",
                    backgroundColor: isActive
                      ? "var(--beam-color-bg-secondary)"
                      : "transparent",
                    border: "none",
                    borderRadius: "4px",
                    cursor: "pointer",
                    color: "var(--beam-color-text)",
                    fontSize: "13px",
                    transition: "background-color 0.2s",
                  }}
                  onMouseEnter={(e) => {
                    if (!isActive) {
                      e.currentTarget.style.backgroundColor =
                        "var(--beam-color-bg-secondary)";
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (!isActive) {
                      e.currentTarget.style.backgroundColor = "transparent";
                    }
                  }}
                >
                  {/* Chevron */}
                  <span
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      width: "20px",
                      transform: isOpen ? "rotate(90deg)" : "rotate(0deg)",
                      transition: "transform 0.2s",
                      color: "var(--beam-color-text-tertiary)",
                      fontSize: "14px",
                    }}
                  >
                    &gt;
                  </span>

                  {/* Project icon + name + board count */}
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      width: "24px",
                      height: "24px",
                      borderRadius: "4px",
                      backgroundColor: project.color,
                      color: "white",
                      fontSize: "12px",
                      fontWeight: "600",
                    }}
                  >
                    {project.icon[0].toUpperCase()}
                  </div>

                  <span style={{ flex: 1, textAlign: "left" }}>
                    {project.name}
                  </span>

                  <span
                    style={{
                      backgroundColor: "var(--beam-color-bg-tertiary)",
                      padding: "2px 6px",
                      borderRadius: "3px",
                      fontSize: "11px",
                      color: "var(--beam-color-text-tertiary)",
                    }}
                  >
                    {project.boards.length}
                  </span>
                </button>

                {/* Boards list (when expanded) */}
                {isOpen && (
                  <div style={{ paddingLeft: "8px", marginTop: "4px" }}>
                    {project.boards.map((board) => {
                      const isBoardActive =
                        params.projectId === project.id &&
                        params.boardId === board.id;

                      return (
                        <button
                          key={board.id}
                          onClick={() =>
                            handleBoardClick(project.id, board.id)
                          }
                          style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "8px",
                            width: "100%",
                            padding: "6px 8px",
                            backgroundColor: isBoardActive
                              ? "var(--beam-color-accent)"
                              : "transparent",
                            border: "none",
                            borderRadius: "4px",
                            cursor: "pointer",
                            color: isBoardActive
                              ? "white"
                              : "var(--beam-color-text)",
                            fontSize: "12px",
                            transition: "background-color 0.2s",
                            marginBottom: "2px",
                          }}
                          onMouseEnter={(e) => {
                            if (!isBoardActive) {
                              e.currentTarget.style.backgroundColor =
                                "var(--beam-color-bg-secondary)";
                            }
                          }}
                          onMouseLeave={(e) => {
                            if (!isBoardActive) {
                              e.currentTarget.style.backgroundColor =
                                "transparent";
                            }
                          }}
                        >
                          <span style={{ fontSize: "12px" }}>
                            {board.icon === "kanban" ? "📊" : "📋"}
                          </span>
                          <span style={{ flex: 1, textAlign: "left" }}>
                            {board.name}
                          </span>
                        </button>
                      );
                    })}

                    {/* New board button */}
                    {isActive && (
                      <button
                        onClick={() => handleNewBoard(project.id)}
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: "6px",
                          width: "100%",
                          padding: "6px 8px",
                          backgroundColor: "transparent",
                          border: "1px dashed var(--beam-color-border)",
                          borderRadius: "4px",
                          cursor: "pointer",
                          color: "var(--beam-color-text-tertiary)",
                          fontSize: "12px",
                          transition: "all 0.2s",
                          marginTop: "4px",
                        }}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.backgroundColor =
                            "var(--beam-color-bg-secondary)";
                          e.currentTarget.style.borderColor =
                            "var(--beam-color-text-tertiary)";
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.backgroundColor = "transparent";
                          e.currentTarget.style.borderColor =
                            "var(--beam-color-border)";
                        }}
                      >
                        <span>+</span>
                        <span>New board</span>
                      </button>
                    )}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
      {wizardOpen && <ProjectWizard open onOpenChange={setWizardOpen} />}
    </ScrollArea>
  );
}
