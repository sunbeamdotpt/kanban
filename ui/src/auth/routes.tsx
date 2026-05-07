/**
 * Auth route definitions for /auth/login and /auth/callback.
 * The Stage 6b agent picks these up and merges them into the full route map.
 */

import { useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { handleCallback, loadHydraConfig, beginLogin } from "./oidc";

/** /auth/login — triggers the PKCE redirect on mount. */
function LoginPage() {
  useEffect(() => {
    const cfg = loadHydraConfig();
    void beginLogin(cfg);
  }, []);

  return (
    <div className="flex items-center justify-center min-h-screen">
      <p className="text-lg text-accent">Redirecting to sign-in…</p>
    </div>
  );
}

/** /auth/callback — handles the code exchange and navigates to /. */
function CallbackPage() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  useEffect(() => {
    const cfg = loadHydraConfig();
    handleCallback(cfg, searchParams)
      .then(() => navigate("/", { replace: true }))
      .catch((err: unknown) => {
        console.error("oidc: callback failed", err);
        navigate("/auth/login", { replace: true });
      });
  }, [searchParams, navigate]);

  return (
    <div className="flex items-center justify-center min-h-screen">
      <p className="text-lg text-accent">Completing sign-in…</p>
    </div>
  );
}

/** Route descriptors consumed by the app router. */
export const authRoutes = [
  { path: "/auth/login", element: <LoginPage /> },
  { path: "/auth/callback", element: <CallbackPage /> },
] as const;
