/**
 * OIDC PKCE flow against Hydra.
 * beginLogin → redirect → handleCallback → exchange → authStore.
 */

import { createClient } from "@connectrpc/connect";
import { authActions, type AuthClaims } from "@sunbeam/g2v/state";
import { AuthService } from "../gen/sunbeam/kanban/v1/auth_pb";
import { generateCodeChallenge, generateCodeVerifier, randomState } from "./pkce";
import { createKanbanTransport } from "./transport";

// ─── sessionStorage keys ──────────────────────────────────────────────────────
const SK_VERIFIER = "oidc.code_verifier";
const SK_STATE = "oidc.state";

// ─── Config ───────────────────────────────────────────────────────────────────

export interface HydraConfig {
  /** e.g. "https://hydra.sunbeam.pt" */
  issuer: string;
  /** "kanban-ui" */
  clientId: string;
  /** "https://kanban.sunbeam.pt/auth/callback" (prod) or "http://localhost:47823/auth/callback" (dev) */
  redirectUri: string;
  /** ["openid", "profile", "email", "kanban.read", "kanban.write"] */
  scopes: string[];
}

/**
 * Read Hydra config from Vite env vars.
 * Falls back to sensible defaults for local dev.
 */
export function loadHydraConfig(): HydraConfig {
  const origin =
    typeof location !== "undefined" ? location.origin : "http://localhost:47823";

  return {
    issuer:
      (import.meta.env.VITE_HYDRA_ISSUER as string | undefined) ??
      "https://hydra.sunbeam.pt",
    clientId:
      (import.meta.env.VITE_HYDRA_CLIENT_ID as string | undefined) ??
      "kanban-ui",
    redirectUri:
      (import.meta.env.VITE_HYDRA_REDIRECT_URI as string | undefined) ??
      `${origin}/auth/callback`,
    scopes: ["openid", "profile", "email", "kanban.read", "kanban.write"],
  };
}

// ─── JWT claim parsing (no signature verification — server's job) ─────────────

function parseJwtClaims(token: string): AuthClaims {
  const parts = token.split(".");
  if (parts.length < 2) throw new Error("oidc: malformed JWT");
  // base64url → base64 → JSON
  const payload = parts[1].replace(/-/g, "+").replace(/_/g, "/");
  const json = atob(payload);
  const raw = JSON.parse(json) as Record<string, unknown>;

  return {
    sub: typeof raw["sub"] === "string" ? raw["sub"] : "",
    email: typeof raw["email"] === "string" ? raw["email"] : undefined,
    name: typeof raw["name"] === "string" ? raw["name"] : undefined,
    roles: Array.isArray(raw["roles"])
      ? (raw["roles"] as unknown[]).filter((r): r is string => typeof r === "string")
      : undefined,
    ...raw,
  };
}

// ─── Token response shape ─────────────────────────────────────────────────────

interface TokenResponse {
  access_token: string;
  refresh_token?: string;
  expires_in?: number;
  id_token?: string;
}

// ─── Flow steps ───────────────────────────────────────────────────────────────

/**
 * Step 1 of 4 — Begin login.
 * Stash code_verifier + state in sessionStorage, then redirect to Hydra.
 */
export async function beginLogin(cfg: HydraConfig): Promise<void> {
  const verifier = generateCodeVerifier();
  const challenge = await generateCodeChallenge(verifier);
  const state = randomState();

  sessionStorage.setItem(SK_VERIFIER, verifier);
  sessionStorage.setItem(SK_STATE, state);

  const url = new URL(`${cfg.issuer}/oauth2/auth`);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", cfg.clientId);
  url.searchParams.set("redirect_uri", cfg.redirectUri);
  url.searchParams.set("scope", cfg.scopes.join(" "));
  url.searchParams.set("state", state);
  url.searchParams.set("code_challenge", challenge);
  url.searchParams.set("code_challenge_method", "S256");

  location.assign(url.toString());
}

/**
 * Step 3 of 4 — Handle the /auth/callback redirect from Hydra.
 * Verifies state, exchanges code for tokens, populates authStore.
 * Deletes sessionStorage keys immediately after exchange.
 */
export async function handleCallback(
  cfg: HydraConfig,
  params: URLSearchParams,
): Promise<void> {
  const code = params.get("code");
  const returnedState = params.get("state");
  const error = params.get("error");

  if (error) {
    const desc = params.get("error_description") ?? error;
    authActions.loginFailure(desc);
    throw new Error(`oidc: authorization error: ${desc}`);
  }

  const expectedState = sessionStorage.getItem(SK_STATE);
  const verifier = sessionStorage.getItem(SK_VERIFIER);

  if (!expectedState || returnedState !== expectedState) {
    sessionStorage.removeItem(SK_STATE);
    sessionStorage.removeItem(SK_VERIFIER);
    authActions.loginFailure("state mismatch");
    throw new Error("oidc: state mismatch — possible CSRF");
  }

  if (!code || !verifier) {
    sessionStorage.removeItem(SK_STATE);
    sessionStorage.removeItem(SK_VERIFIER);
    authActions.loginFailure("missing code or verifier");
    throw new Error("oidc: missing code or verifier");
  }

  authActions.beginLogin();

  // Step 4 of 4 — Exchange code for tokens.
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    redirect_uri: cfg.redirectUri,
    client_id: cfg.clientId,
    code_verifier: verifier,
  });

  // Clean up sessionStorage before the fetch (ensure cleanup even on throw).
  sessionStorage.removeItem(SK_VERIFIER);
  sessionStorage.removeItem(SK_STATE);

  const resp = await fetch(`${cfg.issuer}/oauth2/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!resp.ok) {
    const text = await resp.text();
    authActions.loginFailure(`token exchange failed: ${resp.status}`);
    throw new Error(`oidc: token exchange failed ${resp.status}: ${text}`);
  }

  const tokens = (await resp.json()) as TokenResponse;
  const claims = parseJwtClaims(tokens.access_token);
  const expiresAt =
    typeof tokens.expires_in === "number"
      ? Math.floor(Date.now() / 1000) + tokens.expires_in
      : undefined;

  authActions.loginSuccess({
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token,
    expiresAt,
    claims,
  });
}

/**
 * Logout sequence (4 steps):
 * 1. Build a one-shot Connect client with the current bearer token.
 * 2. Call AuthService.SignalLogout to write the Valkey watermark (best-effort).
 * 3. authStore.logout() clears local state.
 * 4. Redirect to Hydra logout endpoint for clean session termination.
 */
export async function logout(cfg: HydraConfig): Promise<void> {
  // Step 1: one-shot transport carrying the current bearer.
  const transport = createKanbanTransport();
  const client = createClient(AuthService, transport);

  // Step 2: best-effort watermark write.
  try {
    await client.signalLogout({});
  } catch (err) {
    console.warn("oidc: SignalLogout RPC failed — proceeding with local logout", err);
  }

  // Step 3: clear local auth state.
  authActions.logout();

  // Step 4: redirect to Hydra logout.
  location.assign(`${cfg.issuer}/oauth2/sessions/logout`);
}
