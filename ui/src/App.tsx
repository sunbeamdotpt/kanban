import { useRoutes } from "react-router";
import { appRoutes } from "./routes";

/**
 * App route tree. The router (BrowserRouter) and FrameworkProvider live in main.tsx
 * so tests can substitute MemoryRouter without a double-router conflict.
 */
export function App() {
  const routes = useRoutes(appRoutes);
  return <>{routes}</>;
}
