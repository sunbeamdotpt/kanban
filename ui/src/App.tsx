import { Route, Routes } from "react-router";
import { authRoutes } from "./auth/routes";
import { RequireAuth } from "./auth/require-auth";

/** Placeholder for the board view — replaced in Stage 6b. */
function KanbanHome() {
  return (
    <div className="flex items-center justify-center min-h-screen">
      <h1 className="text-4xl font-bold text-accent">Sunbeam Kanban — booting…</h1>
    </div>
  );
}

/**
 * App route tree. The router (BrowserRouter) and FrameworkProvider live in main.tsx
 * so tests can substitute MemoryRouter without a double-router conflict.
 */
export function App() {
  return (
    <Routes>
      {authRoutes.map((r) => (
        <Route key={r.path} path={r.path} element={r.element} />
      ))}
      <Route
        path="/*"
        element={
          <RequireAuth>
            <KanbanHome />
          </RequireAuth>
        }
      />
    </Routes>
  );
}
