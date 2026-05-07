/**
 * new/logout-cuts-stream.spec.ts
 *
 * After SignalLogout the board stream must close within 2 s with a
 * 401-ish / Unauthenticated response, and the UI must show a re-login prompt.
 *
 * Sequence:
 *   1. Authenticate and open a board (stream established).
 *   2. In a second browser context (same user, second session), call
 *      AuthService.SignalLogout for the same subject.
 *   3. Within 2 s, the first tab's stream must close and the FE must show
 *      a re-login prompt or authStore.status === "expired".
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs KRATOS_ADMIN_URL
 *   - needs Stage 6c: window.__sunbeam_auth_status + re-login UI prompt
 *   - needs Stage 6 SubscribeBoard + SignalLogout wired on the FE
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate } from "../helpers/flows";
import { waitForSubscribeBoardResponse } from "../helpers/streams";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_KRATOS = "needs KRATOS_ADMIN_URL";
const SKIP_STAGE6C = "needs Stage 6c: window.__sunbeam_auth_status and re-login prompt";

const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const BASE_URL =
  process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";

test.describe("Logout cuts active stream", () => {
  // Remove this skip once Stage 6c re-login prompt is implemented:

  test("SignalLogout closes stream within 2 s and shows re-login prompt", async ({
    browser,
  }) => {
    const email = `e2e-logout-stream-${Date.now()}@sunbeam-test.invalid`;

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
    const identity = (await idRes.json()) as { id: string };

    // Mint two sessions — tab 1 and the "second device" for SignalLogout.
    const mint = async () => {
      const r = await fetch(
        `${KRATOS_ADMIN_URL}/admin/identities/${identity.id}/sessions`,
        { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
      );
      const d = (await r.json()) as { session_token: string };
      return d.session_token;
    };
    const token1 = await mint();
    const token2 = await mint();

    const cookieFn = (token: string) => ({
      name: "ory_kratos_session",
      value: token,
      url: BASE_URL,
      httpOnly: true,
      sameSite: "Lax" as const,
    });

    // Tab 1: authenticated, open a board and establish the stream.
    const ctx1 = await browser.newContext();
    await ctx1.addCookies([cookieFn(token1)]);
    const page1 = await ctx1.newPage();

    await createBoardAndNavigate(page1, "Simple");
    // Confirm stream is open.
    await waitForSubscribeBoardResponse(page1, 10_000);

    // Second context: call SignalLogout.
    const ctx2 = await browser.newContext();
    await ctx2.addCookies([cookieFn(token2)]);
    const apiPage = await ctx2.newPage();

    // Navigate to base URL to have an active page for the request context.
    await apiPage.goto(BASE_URL);

    const logoutRes = await apiPage.evaluate(
      async (baseUrl) => {
        const r = await fetch(
          `${baseUrl}/sunbeam.kanban.v1.AuthService/SignalLogout`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json",
            },
            body: "{}",
            credentials: "include",
          },
        );
        return { status: r.status, body: await r.json() };
      },
      BASE_URL,
    );
    expect(logoutRes.status).toBe(200);
    expect(logoutRes.body).toMatchObject({ watermark_ms: expect.any(Number) });

    // Within 2 s page1's stream must be closed and a re-login prompt shown.
    // The FE detects the Unauthenticated stream close and shows a modal /
    // banner. Stage 6c must expose window.__sunbeam_auth_status = "expired"
    // when the stream is cut by a logout watermark.
    await page1.waitForFunction(
      () =>
        (window as unknown as Record<string, unknown>).__sunbeam_auth_status ===
        "expired",
      { timeout: 2_000 },
    );

    // A re-login prompt must be visible.
    await expect(
      page1
        .getByRole("dialog")
        .or(page1.getByRole("alert"))
        .or(page1.getByText(/session expired|sign in again|re-authenticate/i)),
    ).toBeVisible({ timeout: 2_000 });

    await ctx1.close();
    await ctx2.close();

    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });
});
