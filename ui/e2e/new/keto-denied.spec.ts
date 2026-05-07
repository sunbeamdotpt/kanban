/**
 * new/keto-denied.spec.ts
 *
 * Verifies that a user without Keto relations:
 *   - sees an empty project list (ListProjects returns 0 items)
 *   - receives a 403/PermissionDenied error when navigating directly to a
 *     board they don't have access to
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs KRATOS_ADMIN_URL
 *   - needs KETO_WRITE_URL (to provision editor relations for user A)
 */

import { test, expect } from "@playwright/test";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_KRATOS = "needs KRATOS_ADMIN_URL";
const SKIP_KETO = "needs KETO_WRITE_URL";

const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;
const ketoAvailable = !!process.env.KETO_WRITE_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const KETO_WRITE_URL =
  process.env.KETO_WRITE_URL ?? "http://localhost:4467";
const BASE_URL =
  process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";

/** Provision a Kratos identity and return { id, sessionToken }. */
async function provision(email: string) {
  const idRes = await fetch(`${KRATOS_ADMIN_URL}/admin/identities`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      schema_id: "default",
      traits: { email },
      credentials: { password: { config: { password: `E2E-${email}-pw!` } } },
    }),
  });
  const identity = (await idRes.json()) as { id: string };
  const sessRes = await fetch(
    `${KRATOS_ADMIN_URL}/admin/identities/${identity.id}/sessions`,
    { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
  );
  const { session_token } = (await sessRes.json()) as { session_token: string };
  return { id: identity.id, sessionToken: session_token };
}

async function deleteIdentity(id: string) {
  await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${id}`, { method: "DELETE" });
}

test.describe("Keto permission enforcement", () => {
  test.skip(!deployed, SKIP_DEPLOYED);
  test.skip(!kratosAvailable, SKIP_KRATOS);
  test.skip(!ketoAvailable, SKIP_KETO);

  test("user B without relations sees empty project list", async ({ browser }) => {
    const userB = await provision(`e2e-denied-b-${Date.now()}@sunbeam-test.invalid`);

    const ctx = await browser.newContext();
    await ctx.addCookies([
      {
        name: "ory_kratos_session",
        value: userB.sessionToken,
        url: BASE_URL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await ctx.newPage();
    await page.goto("/");

    // Wait for the project list to render (even if empty).
    await expect(page.locator("h1")).toContainText("Projects", { timeout: 15_000 });

    // Assert zero project cards — not string-match, count-based.
    const projectCards = page.locator('[data-testid^="project-"]');
    await expect(projectCards).toHaveCount(0, { timeout: 5_000 });

    await ctx.close();
    await deleteIdentity(userB.id);
  });

  test("user B without relations gets 403 on direct board URL", async ({ browser }) => {
    const userA = await provision(`e2e-editor-a-${Date.now()}@sunbeam-test.invalid`);
    const userB = await provision(`e2e-denied-b2-${Date.now()}@sunbeam-test.invalid`);

    // Grant user A editor relation on a synthetic project id.
    const projectId = `test-keto-project-${Date.now()}`;
    await fetch(`${KETO_WRITE_URL}/admin/relation-tuples`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        namespace: "KanbanProject",
        object: projectId,
        relation: "editors",
        subject_id: userA.id,
      }),
    });

    // User B has no tuples — navigate directly to a board URL.
    const fakeBoardId = `fake-board-${Date.now()}`;
    const ctx = await browser.newContext();
    await ctx.addCookies([
      {
        name: "ory_kratos_session",
        value: userB.sessionToken,
        url: BASE_URL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await ctx.newPage();
    await page.goto(`/p/${projectId}/b/${fakeBoardId}`);

    // Expect a 403 / permission-denied / not-found error state.
    // Stage 6e must render an error boundary with role="alert" or
    // data-testid="permission-denied" when the API returns PermissionDenied.
    const errorIndicators = [
      page.locator('[data-testid="permission-denied"]'),
      page.getByRole("alert"),
      page.getByText(/permission denied|403|not found|forbidden/i),
    ];
    const anyError = page.locator(
      '[data-testid="permission-denied"], [role="alert"]',
    );
    await anyError
      .or(page.getByText(/permission denied|403|forbidden/i))
      .waitFor({ state: "visible", timeout: 10_000 });

    await ctx.close();

    // Cleanup Keto tuple and identities.
    const params = new URLSearchParams({
      namespace: "KanbanProject",
      object: projectId,
      relation: "editors",
      subject_id: userA.id,
    });
    await fetch(`${KETO_WRITE_URL}/admin/relation-tuples?${params}`, {
      method: "DELETE",
    });
    await deleteIdentity(userA.id);
    await deleteIdentity(userB.id);
  });
});
