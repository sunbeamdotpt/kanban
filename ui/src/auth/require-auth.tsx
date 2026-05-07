/**
 * <RequireAuth> — gate a subtree behind authentication.
 * Redirects to /auth/login when the user is anonymous or expired.
 * Shows a loading spinner while authenticating.
 */

import { type ReactNode } from "react";
import { Navigate } from "react-router";
import { useAuthStatus } from "./state-bridge";

/** Minimal splash shown while an OIDC round-trip is in flight. */
function SplashLoading(): ReactNode {
  return (
    <div className="flex items-center justify-center min-h-screen">
      <p className="text-lg text-accent">Signing in…</p>
    </div>
  );
}

export interface RequireAuthProps {
  children: ReactNode;
}

/**
 * Wrap protected pages with <RequireAuth>.
 * - anonymous | expired → redirect to /auth/login
 * - authenticating → show splash
 * - authenticated → render children
 */
export function RequireAuth({ children }: RequireAuthProps): ReactNode {
  const status = useAuthStatus();
  if (status === "anonymous" || status === "expired") {
    return <Navigate to="/auth/login" replace />;
  }
  if (status === "authenticating") {
    return <SplashLoading />;
  }
  return <>{children}</>;
}
