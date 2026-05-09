/**
 * Centralized route map for the Kanban app.
 * Protected pages are wrapped in <RequireAuth> and beam-ui <Shell>.
 */

import { Navigate, Outlet } from "react-router";
import { authRoutes } from "../auth/routes";
import { RequireAuth } from "../auth/require-auth";
import { KanbanShell } from "../shell/kanban-shell";
import { HomePage } from "./home";
import { ProjectPage } from "./project-page";
import { BoardPage } from "./board-page";
import { ListPage } from "./list-page";
import { SettingsPage } from "./settings-page";
import { NotFound } from "./not-found";
import { PreviewPage } from "./preview";
import { ShellTestPage } from "./shell-test";

const devOnlyRoutes = import.meta.env.DEV
  ? [
      { path: "/__preview", element: <PreviewPage /> },
      { path: "/__shell", element: <ShellTestPage /> },
    ]
  : [];

export const appRoutes = [
  // Public auth routes
  ...authRoutes,
  // Dev-only design preview (excluded from prod build)
  ...devOnlyRoutes,

  // Protected application routes (all wrapped in Shell via layout)
  {
    element: (
      <RequireAuth>
        <KanbanShell>
          <Outlet />
        </KanbanShell>
      </RequireAuth>
    ),
    children: [
      { path: "/", element: <HomePage /> },
      { path: "/p/:projectId", element: <ProjectPage /> },
      { path: "/p/:projectId/b/:boardId", element: <BoardPage /> },
      { path: "/p/:projectId/b/:boardId/list", element: <ListPage /> },
      { path: "/p/:projectId/b/:boardId/settings", element: <SettingsPage /> },
    ],
  },

  // 404 catch-all
  {
    path: "*",
    element: <NotFound />,
  },
];
