/**
 * Centralized route map for the Kanban app.
 * Every protected page is wrapped in <RequireAuth> and <AppChrome>.
 */

import { Navigate } from "react-router";
import { authRoutes } from "../auth/routes";
import { RequireAuth } from "../auth/require-auth";
import { AppChrome } from "../chrome";
import { HomePage } from "./home";
import { ProjectPage } from "./project-page";
import { BoardPage } from "./board-page";
import { ListPage } from "./list-page";
import { SettingsPage } from "./settings-page";
import { NotFound } from "./not-found";
import { PreviewPage } from "./preview";

const devOnlyRoutes = import.meta.env.DEV
  ? [{ path: "/__preview", element: <PreviewPage /> }]
  : [];

export const appRoutes = [
  // Public auth routes
  ...authRoutes,
  // Dev-only design preview (excluded from prod build)
  ...devOnlyRoutes,

  // Protected application routes (all wrapped in AppChrome)
  {
    path: "/",
    element: (
      <RequireAuth>
        <AppChrome>
          <HomePage />
        </AppChrome>
      </RequireAuth>
    ),
  },
  {
    path: "/p/:projectId",
    element: (
      <RequireAuth>
        <AppChrome>
          <ProjectPage />
        </AppChrome>
      </RequireAuth>
    ),
  },
  {
    path: "/p/:projectId/b/:boardId",
    element: (
      <RequireAuth>
        <AppChrome>
          <BoardPage />
        </AppChrome>
      </RequireAuth>
    ),
  },
  {
    path: "/p/:projectId/b/:boardId/list",
    element: (
      <RequireAuth>
        <AppChrome>
          <ListPage />
        </AppChrome>
      </RequireAuth>
    ),
  },
  {
    path: "/p/:projectId/b/:boardId/settings",
    element: (
      <RequireAuth>
        <AppChrome>
          <SettingsPage />
        </AppChrome>
      </RequireAuth>
    ),
  },

  // 404 catch-all
  {
    path: "*",
    element: <NotFound />,
  },
];
