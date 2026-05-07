/**
 * Tests for the OIDC PKCE auth flow.
 *
 * fetch mocking: vi.spyOn(globalThis, "fetch") — HTTP boundary instrumentation,
 * not infrastructure mocking. Acceptable per CLAUDE.md.
 */

import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type MockInstance,
} from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { authActions, authStore } from "@sunbeam/g2v/state";
import { generateCodeChallenge, generateCodeVerifier, randomState } from "./pkce";
import { beginLogin, handleCallback } from "./oidc";
import { createKanbanTransport } from "./transport";
import { RequireAuth } from "./require-auth";
import { createClient } from "@connectrpc/connect";
import { AuthService } from "../gen/sunbeam/kanban/v1/auth_pb";
import { createMockTransport } from "@sunbeam/g2v/testing";
import { create } from "@bufbuild/protobuf";
import { SignalLogoutResponseSchema } from "../gen/sunbeam/kanban/v1/auth_pb";
import { logout } from "./oidc";

// ─── Helpers ──────────────────────────────────────────────────────────────────

const TEST_CFG = {
  issuer: "https://hydra.test",
  clientId: "kanban-ui",
  redirectUri: "http://localhost:47823/auth/callback",
  scopes: ["openid", "profile"],
};

/** Minimal valid JWT with known payload (no real signature — just structure). */
function makeJwt(payload: Record<string, unknown>): string {
  const header = btoa(JSON.stringify({ alg: "RS256", typ: "JWT" }))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
  const body = btoa(JSON.stringify(payload))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
  return `${header}.${body}.fakesig`;
}

function resetAuthStore() {
  authActions.logout();
}

// ─── PKCE ─────────────────────────────────────────────────────────────────────

describe("pkce", () => {
  it("generateCodeVerifier_yields_unique_64char_strings", () => {
    const a = generateCodeVerifier();
    const b = generateCodeVerifier();
    // base64url of 64 bytes = 86 chars
    expect(a.length).toBe(86);
    expect(b.length).toBe(86);
    expect(a).not.toBe(b);
    // only base64url characters
    expect(a).toMatch(/^[A-Za-z0-9\-_]+$/);
  });

  it("generateCodeChallenge_matches_rfc7636_for_known_input", async () => {
    // RFC 7636 Appendix B test vector.
    const verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    const challenge = await generateCodeChallenge(verifier);
    // Expected: SHA-256 of the ASCII verifier, base64url-encoded (no padding).
    expect(challenge).toBe("E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
  });
});

// ─── OIDC — beginLogin ────────────────────────────────────────────────────────

/**
 * jsdom marks location.assign as non-configurable on the location object,
 * but we can replace window.location entirely with a plain object.
 * Returns a cleanup function that restores the original.
 */
function mockWindowLocation(): { calls: string[]; restore: () => void } {
  const calls: string[] = [];
  const original = window.location;
  Object.defineProperty(window, "location", {
    configurable: true,
    writable: true,
    value: { ...original, assign: (url: string) => calls.push(url) },
  });
  return {
    calls,
    restore: () => {
      Object.defineProperty(window, "location", {
        configurable: true,
        writable: true,
        value: original,
      });
    },
  };
}

describe("oidc.beginLogin", () => {
  let assignCalls: string[];
  let restoreLocation: () => void;

  beforeEach(() => {
    sessionStorage.clear();
    const mock = mockWindowLocation();
    assignCalls = mock.calls;
    restoreLocation = mock.restore;
  });

  afterEach(() => {
    restoreLocation();
    vi.restoreAllMocks();
    sessionStorage.clear();
  });

  it("beginLogin_redirects_with_correct_query_params", async () => {
    await beginLogin(TEST_CFG);

    expect(assignCalls).toHaveLength(1);
    const url = new URL(assignCalls[0]);
    expect(url.origin + url.pathname).toBe("https://hydra.test/oauth2/auth");
    expect(url.searchParams.get("response_type")).toBe("code");
    expect(url.searchParams.get("client_id")).toBe("kanban-ui");
    expect(url.searchParams.get("redirect_uri")).toBe(
      "http://localhost:47823/auth/callback",
    );
    expect(url.searchParams.get("code_challenge_method")).toBe("S256");
    expect(url.searchParams.get("code_challenge")).toBeTruthy();
    expect(url.searchParams.get("state")).toBeTruthy();
    expect(url.searchParams.get("scope")).toContain("openid");
  });

  it("beginLogin_persists_code_verifier_to_sessionStorage", async () => {
    await beginLogin(TEST_CFG);
    expect(sessionStorage.getItem("oidc.code_verifier")).toBeTruthy();
  });
});

// ─── OIDC — handleCallback ────────────────────────────────────────────────────

describe("oidc.handleCallback", () => {
  let fetchSpy: MockInstance;

  beforeEach(() => {
    sessionStorage.clear();
    resetAuthStore();
    fetchSpy = vi.spyOn(globalThis, "fetch");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    sessionStorage.clear();
    resetAuthStore();
  });

  it("handleCallback_rejects_state_mismatch", async () => {
    sessionStorage.setItem("oidc.state", "correct-state");
    sessionStorage.setItem("oidc.code_verifier", "some-verifier");

    const params = new URLSearchParams({ code: "abc", state: "wrong-state" });
    await expect(handleCallback(TEST_CFG, params)).rejects.toThrow(
      /state mismatch/,
    );
  });

  it("handleCallback_clears_sessionStorage_on_success", async () => {
    sessionStorage.setItem("oidc.state", "mystate");
    sessionStorage.setItem("oidc.code_verifier", "myverifier");

    const token = makeJwt({ sub: "user:1", email: "a@b.com", exp: 9999999999 });
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ access_token: token, expires_in: 3600 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    const params = new URLSearchParams({ code: "mycode", state: "mystate" });
    await handleCallback(TEST_CFG, params);

    expect(sessionStorage.getItem("oidc.code_verifier")).toBeNull();
    expect(sessionStorage.getItem("oidc.state")).toBeNull();
  });

  it("handleCallback_populates_authStore_on_success", async () => {
    sessionStorage.setItem("oidc.state", "mystate");
    sessionStorage.setItem("oidc.code_verifier", "myverifier");

    const token = makeJwt({
      sub: "user:42",
      email: "test@sunbeam.pt",
      name: "Test User",
      exp: 9999999999,
    });
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ access_token: token, expires_in: 3600 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    const params = new URLSearchParams({ code: "mycode", state: "mystate" });
    await handleCallback(TEST_CFG, params);

    expect(authStore.status.get()).toBe("authenticated");
    const session = authStore.session.get();
    expect(session?.accessToken).toBe(token);
    expect(session?.claims?.sub).toBe("user:42");
    expect(session?.claims?.email).toBe("test@sunbeam.pt");
  });
});

// ─── Transport ────────────────────────────────────────────────────────────────

describe("transport", () => {
  let fetchSpy: MockInstance;

  beforeEach(() => {
    resetAuthStore();
    fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({}), { status: 200 }),
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
    resetAuthStore();
  });

  it("createKanbanTransport_attaches_bearer_when_authStore_has_session", async () => {
    authActions.loginSuccess({
      accessToken: "test-bearer-token",
      claims: { sub: "user:1" },
    });

    const transport = createKanbanTransport();
    const client = createClient(AuthService, transport);

    // The request will fail (no real server) but fetch should have been called
    // with the Authorization header.
    try {
      await client.whoAmI({});
    } catch {
      // expected — no real server
    }

    expect(fetchSpy).toHaveBeenCalled();
    const [, init] = fetchSpy.mock.calls[0] as [unknown, RequestInit];
    const headers = new Headers(init?.headers as HeadersInit);
    expect(headers.get("authorization")).toBe("Bearer test-bearer-token");
  });

  it("no_authorization_header_when_authStore_anonymous", async () => {
    // authStore is already anonymous from resetAuthStore()
    const transport = createKanbanTransport();
    const client = createClient(AuthService, transport);

    try {
      await client.whoAmI({});
    } catch {
      // expected
    }

    expect(fetchSpy).toHaveBeenCalled();
    const [, init] = fetchSpy.mock.calls[0] as [unknown, RequestInit];
    const headers = new Headers(init?.headers as HeadersInit);
    expect(headers.get("authorization")).toBeNull();
  });
});

// ─── RequireAuth ──────────────────────────────────────────────────────────────

describe("require-auth", () => {
  afterEach(() => {
    resetAuthStore();
  });

  it("redirects_to_login_when_anonymous", () => {
    // authStore is anonymous by default
    render(
      <MemoryRouter initialEntries={["/secret"]}>
        <RequireAuth>
          <div>secret content</div>
        </RequireAuth>
      </MemoryRouter>,
    );
    expect(screen.queryByText("secret content")).not.toBeInTheDocument();
  });

  it("renders_children_when_authenticated", () => {
    authActions.loginSuccess({
      accessToken: "tok",
      claims: { sub: "user:1" },
    });

    render(
      <MemoryRouter>
        <RequireAuth>
          <div>protected content</div>
        </RequireAuth>
      </MemoryRouter>,
    );
    expect(screen.getByText("protected content")).toBeInTheDocument();
  });
});

// ─── Logout ───────────────────────────────────────────────────────────────────

describe("logout", () => {
  let assignCalls: string[];
  let restoreLocation: () => void;
  const signalLogoutCalls: unknown[] = [];

  beforeEach(() => {
    signalLogoutCalls.length = 0;
    const mock = mockWindowLocation();
    assignCalls = mock.calls;
    restoreLocation = mock.restore;
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });
  });

  afterEach(() => {
    restoreLocation();
    vi.restoreAllMocks();
    resetAuthStore();
  });

  it("calls_signalLogout_then_clears_authStore", async () => {
    // Wire a mock transport that records the signalLogout call.
    const mockTransport = createMockTransport({
      routes(router) {
        router.service(AuthService, {
          signalLogout: async () => {
            signalLogoutCalls.push("signalLogout");
            return create(SignalLogoutResponseSchema, {
              subject: "user:1",
              watermarkMs: BigInt(Date.now()),
            });
          },
        });
      },
    });

    // Patch createKanbanTransport to return the mock transport for this test.
    const transportModule = await import("./transport");
    const createSpy = vi
      .spyOn(transportModule, "createKanbanTransport")
      .mockReturnValue(mockTransport);

    await logout(TEST_CFG);

    // Step 2 happened (SignalLogout was called).
    expect(signalLogoutCalls).toHaveLength(1);
    expect(signalLogoutCalls[0]).toBe("signalLogout");

    // Step 3: authStore cleared.
    await waitFor(() => {
      expect(authStore.status.get()).toBe("anonymous");
    });

    // Step 4: redirect to Hydra logout.
    expect(assignCalls).toContain("https://hydra.test/oauth2/sessions/logout");

    createSpy.mockRestore();
  });
});
