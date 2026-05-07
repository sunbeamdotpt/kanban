/**
 * new/sso.spec.ts
 *
 * SSO flow: Kratos → Hydra OIDC redirect → post-login lands on /p/...
 * with user avatar visible; logout clears session.
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs Stage 6c FE: window.__sunbeam_auth_status exposed for tests
 *   - needs KRATOS_ADMIN_URL (Kratos admin API)
 */

import { test, expect } from "@playwright/test";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_AUTH_STATUS =
  "needs Stage 6c: window.__sunbeam_auth_status exposed on window";
const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const KRATOS_PUBLIC_URL =
  process.env.KRATOS_PUBLIC_URL ?? "http://localhost:4433";

test.describe("SSO authentication flow", () => {

  test("anonymous visit to / redirects to Kratos login", async ({ page }) => {
    await page.goto("/");
    // The app must redirect unauthenticated visitors to Kratos.
    await page.waitForURL(
      (url) =>
        url.hostname.includes("kratos") ||
        url.pathname.includes("/sessions/login") ||
        url.pathname.includes("/self-service/login"),
      { timeout: 10_000 },
    );
    // Assert login form is present — role-based, not string-match.
    await expect(
      page.getByRole("button", { name: /sign.?in/i }),
    ).toBeVisible({ timeout: 10_000 });
  });

  test("authenticated user lands on project list after login", async ({
    browser,
  }) => {

    const email = `e2e-sso-${Date.now()}@sunbeam-test.invalid`;

    // Provision Kratos identity.
    const idRes = await fetch(`${KRATOS_ADMIN_URL}/admin/identities`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schema_id: "default",
        traits: { email },
        credentials: {
          password: { config: { password: `E2E-${email}-pw!` } },
        },
      }),
    });
    expect(idRes.status).toBe(201);
    const identity = (await idRes.json()) as { id: string };

    // Mint session and inject cookie.
    const sessRes = await fetch(
      `${KRATOS_ADMIN_URL}/admin/identities/${identity.id}/sessions`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
    );
    expect(sessRes.status).toBe(201);
    const { session_token: sessionToken } = (await sessRes.json()) as {
      session_token: string;
    };

    const ctx = await browser.newContext();
    const baseURL =
      process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";
    await ctx.addCookies([
      {
        name: "ory_kratos_session",
        value: sessionToken,
        url: baseURL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await ctx.newPage();
    await page.goto("/");

    // After session injection user should land on the project list, not login.
    await expect(page.locator("h1")).toContainText("Projects", {
      timeout: 15_000,
    });

    // User avatar or user-menu trigger should be visible.
    await expect(
      page.locator('[data-testid="user-menu"]'),
    ).toBeVisible({ timeout: 10_000 });

    await ctx.close();

    // Cleanup Kratos identity.
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });

  test("logout via user menu clears session", async ({ browser }) => {

    const email = `e2e-logout-${Date.now()}@sunbeam-test.invalid`;
    const idRes = await fetch(`${KRATOS_ADMIN_URL}/admin/identities`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schema_id: "default",
        traits: { email },
        credentials: {
          password: { config: { password: `E2E-${email}-pw!` } },
        },
      }),
    });
    const identity = (await idRes.json()) as { id: string };
    const sessRes = await fetch(
      `${KRATOS_ADMIN_URL}/admin/identities/${identity.id}/sessions`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
    );
    const { session_token: sessionToken } = (await sessRes.json()) as {
      session_token: string;
    };

    const ctx = await browser.newContext();
    const baseURL =
      process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";
    await ctx.addCookies([
      {
        name: "ory_kratos_session",
        value: sessionToken,
        url: baseURL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await ctx.newPage();
    await page.goto("/");
    await expect(page.locator('[data-testid="user-menu"]')).toBeVisible({
      timeout: 10_000,
    });

    // Trigger logout via user menu.
    await page.locator('[data-testid="user-menu"]').click();
    await page.getByRole("menuitem", { name: /sign.?out|log.?out/i }).click();

    // After logout the app must redirect to / (or the login page) and clear
    // authStore.status.
    await page.waitForURL((url) => url.pathname === "/" || url.pathname.includes("/login"), {
      timeout: 10_000,
    });

    // Stage 6c must expose window.__sunbeam_auth_status.
    const authStatus = await page.evaluate(
      () => (window as unknown as Record<string, unknown>).__sunbeam_auth_status,
    );
    expect(authStatus).toBe("anonymous");

    await ctx.close();
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });
});
