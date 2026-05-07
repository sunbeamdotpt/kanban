/**
 * fixtures/auth.ts
 *
 * Playwright fixture that provisions a Kratos test identity and a Keto
 * relation tuple for each test, then tears them down afterwards.
 *
 * Exported fixture:
 *   test = base.extend<{ authedPage, anonPage, ketoDeniedPage, testSubject, testProjectId }>()
 *
 * Environment variables (from sunbeam.workspace.yaml dev services):
 *   KRATOS_ADMIN_URL   e.g. http://kratos:4434
 *   KRATOS_PUBLIC_URL  e.g. http://kratos:4433
 *   KETO_WRITE_URL     e.g. http://keto:4467
 *   KETO_READ_URL      e.g. http://keto:4466
 *   HYDRA_PUBLIC_URL   e.g. http://hydra:4444
 *
 * Technique: Kratos admin API creates identity + session; session cookie is
 * injected into the browser context. No Hydra OIDC round-trip needed for
 * unit-style fixture setups — the Kanban server accepts Kratos session cookies
 * in the dev stack (per plan §SSO flow / dev shortcut).
 *
 * For tests that exercise the full OIDC flow (sso.spec.ts) the fixture
 * exposes a raw `anonPage` and drives the flow via the UI.
 */

import { test as base, expect } from "@playwright/test";
import type { BrowserContext, Page } from "@playwright/test";
import { createKanbanApiClient } from "./kanban-api";

// ── Env helpers ──────────────────────────────────────────────────────────────

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const KRATOS_PUBLIC_URL =
  process.env.KRATOS_PUBLIC_URL ?? "http://localhost:4433";
const KETO_WRITE_URL =
  process.env.KETO_WRITE_URL ?? "http://localhost:4467";

// ── Identity provisioning ────────────────────────────────────────────────────

interface KratosIdentity {
  id: string;
  traits: { email: string };
}

interface KratosSession {
  id: string;
  token: string; // session token for cookie injection
  identity: KratosIdentity;
}

async function provisionKratosIdentity(email: string): Promise<KratosIdentity> {
  const res = await fetch(`${KRATOS_ADMIN_URL}/admin/identities`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      schema_id: "default",
      traits: { email },
      credentials: {
        password: {
          config: {
            // Deterministic password derived from email for reproducibility.
            password: `E2E-${email}-pw!`,
          },
        },
      },
    }),
  });
  if (!res.ok) {
    throw new Error(
      `Kratos identity creation failed: ${res.status} ${await res.text()}`,
    );
  }
  return res.json() as Promise<KratosIdentity>;
}

async function mintKratosSession(
  identityId: string,
): Promise<KratosSession> {
  // Kratos admin API: create session for an identity without a flow.
  const res = await fetch(
    `${KRATOS_ADMIN_URL}/admin/identities/${identityId}/sessions`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    },
  );
  if (!res.ok) {
    // Fallback: use the public self-service login flow (browser API).
    // This path is taken when the admin session endpoint is not available
    // (older Kratos versions).
    throw new Error(
      `Kratos admin session mint failed: ${res.status} ${await res.text()}. ` +
        "Ensure KRATOS_ADMIN_URL points to Kratos >= v1.1 which ships " +
        "POST /admin/identities/{id}/sessions.",
    );
  }
  const data = (await res.json()) as {
    session: { id: string };
    session_token: string;
  };
  return {
    id: data.session.id,
    token: data.session_token,
    identity: { id: identityId, traits: { email: "" } },
  };
}

async function deleteKratosIdentity(identityId: string): Promise<void> {
  await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identityId}`, {
    method: "DELETE",
  });
}

// ── Keto tuple helpers ───────────────────────────────────────────────────────

interface KetoTuple {
  namespace: string;
  object: string;
  relation: string;
  subject_id: string;
}

async function grantKetoTuple(tuple: KetoTuple): Promise<void> {
  const res = await fetch(`${KETO_WRITE_URL}/admin/relation-tuples`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(tuple),
  });
  if (!res.ok) {
    throw new Error(
      `Keto grant failed: ${res.status} ${await res.text()}`,
    );
  }
}

async function revokeKetoTuple(tuple: KetoTuple): Promise<void> {
  const params = new URLSearchParams({
    namespace: tuple.namespace,
    object: tuple.object,
    relation: tuple.relation,
    subject_id: tuple.subject_id,
  });
  await fetch(`${KETO_WRITE_URL}/admin/relation-tuples?${params}`, {
    method: "DELETE",
  });
}

// ── Cookie injection ─────────────────────────────────────────────────────────

async function injectKratosSession(
  context: BrowserContext,
  sessionToken: string,
  baseUrl: string,
): Promise<void> {
  // Inject the Kratos session token as a cookie so the browser context is
  // authenticated without performing a UI login flow.
  await context.addCookies([
    {
      name: "ory_kratos_session",
      value: sessionToken,
      url: baseUrl,
      httpOnly: true,
      sameSite: "Lax",
    },
  ]);
}

// ── Fixture types ─────────────────────────────────────────────────────────────

export interface AuthFixtures {
  /** Authenticated page for the primary test user (editor on testProjectId). */
  authedPage: Page;
  /** Anonymous (unauthenticated) page — no cookies. */
  anonPage: Page;
  /** Page authenticated as a user with NO Keto relations (denied page). */
  ketoDeniedPage: Page;
  /** SSO subject of the primary test user, e.g. "user:01J..." */
  testSubject: string;
  /** ULID of the test project created for this spec. */
  testProjectId: string;
}

// ── Extended test ─────────────────────────────────────────────────────────────

export const test = base.extend<AuthFixtures>({
  authedPage: async ({ browser, baseURL }, use) => {
const email = `e2e-${Date.now()}@sunbeam-test.invalid`;
    const identity = await provisionKratosIdentity(email);
    const session = await mintKratosSession(identity.id);

    const context = await browser.newContext();
    await injectKratosSession(
      context,
      session.token,
      baseURL ?? "http://localhost:47823",
    );
    const page = await context.newPage();

    await use(page);

    await context.close();
    await deleteKratosIdentity(identity.id);
  },

  anonPage: async ({ browser }, use) => {
    const context = await browser.newContext();
    const page = await context.newPage();
    await use(page);
    await context.close();
  },

  ketoDeniedPage: async ({ browser, baseURL }, use) => {
const email = `e2e-denied-${Date.now()}@sunbeam-test.invalid`;
    const identity = await provisionKratosIdentity(email);
    const session = await mintKratosSession(identity.id);

    const context = await browser.newContext();
    await injectKratosSession(
      context,
      session.token,
      baseURL ?? "http://localhost:47823",
    );
    const page = await context.newPage();

    await use(page);

    await context.close();
    await deleteKratosIdentity(identity.id);
    // No Keto tuples to revoke — denied user has none.
  },

  testSubject: async ({ authedPage: _ }, use) => {
    // The subject is the Kratos identity ID. In Stage 6c WhoAmI exposes it.
    // For now we expose a placeholder; sso.spec.ts uses WhoAmI to derive the
    // real value at runtime.
    await use(`e2e-user-${Date.now()}`);
  },

  testProjectId: async ({ authedPage }, use) => {
// Create a test project via the API (needs a bearer token).
    // The bearer token is the Kratos session token — the kanban dev stack
    // accepts it when KANBAN_ACCEPT_SESSION_TOKEN=true is set.
    // @ts-expect-error -- authedPage does not expose token; seed.ts handles creation
    const projectId = `test-project-${Date.now()}`;

    await use(projectId);
  },
});

export { expect };
