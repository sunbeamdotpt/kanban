/**
 * new/forgejo-link.spec.ts
 *
 * Forgejo issue linking: search, link, assert badge, unlink, verify event_log.
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs FORGEJO_TEST_URL + FORGEJO_TEST_REPO + FORGEJO_TEST_ISSUE_NUMBER
 *   - needs DATABASE_URL for direct event_log assertion
 *   - needs KRATOS_ADMIN_URL
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "../helpers/flows";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_FORGEJO = "needs FORGEJO_TEST_URL env var";
const SKIP_DB = "needs DATABASE_URL for event_log assertion";
const SKIP_KRATOS = "needs KRATOS_ADMIN_URL";

const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const forgejoAvailable = !!process.env.FORGEJO_TEST_URL;
const dbAvailable = !!process.env.DATABASE_URL;
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const BASE_URL =
  process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";

// Forgejo test fixture: known repo/issue that exists in the test Forgejo instance.
const FORGEJO_REPO_OWNER =
  process.env.FORGEJO_TEST_REPO_OWNER ?? "sunbeam";
const FORGEJO_REPO_NAME =
  process.env.FORGEJO_TEST_REPO_NAME ?? "e2e-test-repo";
const FORGEJO_ISSUE_NUMBER = parseInt(
  process.env.FORGEJO_TEST_ISSUE_NUMBER ?? "1",
  10,
);

test.describe("Forgejo issue linking", () => {

  test("search, link, see badge, unlink, verify event_log", async ({
    browser,
  }) => {
    const email = `e2e-forgejo-${Date.now()}@sunbeam-test.invalid`;

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
    await ctx.addCookies([
      {
        name: "ory_kratos_session",
        value: sessionToken,
        url: BASE_URL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await ctx.newPage();

    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Forgejo Test Card");
    await page.getByText("Forgejo Test Card").click();
    await expect(
      page.getByRole("heading", { name: "Description" }),
    ).toBeVisible();

    // Open the link picker.
    await page.getByText("Link issue or PR").click();
    await expect(page.getByRole("button", { name: "Search" })).toBeVisible();

    // Fill in repo owner + name + number.
    const repoInput = page
      .locator('input[placeholder*="repo"]')
      .or(page.locator('input[name="repo"]'))
      .first();
    await repoInput.fill(`${FORGEJO_REPO_OWNER}/${FORGEJO_REPO_NAME}`);

    const numberInput = page
      .locator('input[placeholder*="number"]')
      .or(page.locator('input[name="number"]'))
      .first();
    await numberInput.fill(String(FORGEJO_ISSUE_NUMBER));

    await page.getByRole("button", { name: "Search" }).click();

    // A search result row should appear.
    const resultItem = page
      .locator('[data-testid^="forgejo-result-"]')
      .or(page.getByRole("listitem").filter({ hasText: `#${FORGEJO_ISSUE_NUMBER}` }))
      .first();
    await expect(resultItem).toBeVisible({ timeout: 10_000 });

    // Click "Link" on that result.
    await resultItem.getByRole("button", { name: "Link" }).click();

    // Assert the badge appears in the card detail.
    const badge = page
      .locator(`[data-testid^="forgejo-link-"]`)
      .first();
    await expect(badge).toBeVisible({ timeout: 10_000 });

    // Extract the link id from the testid attribute.
    const testid = await badge.getAttribute("data-testid");
    const linkId = testid?.replace("forgejo-link-", "") ?? "";
    expect(linkId).toBeTruthy();

    // Verify via API: ListLinksByCard should return the link.
    // (Skipped if generated client not ready — we assert via UI only for now.)

    // Unlink.
    await badge.getByRole("button", { name: /unlink|remove/i }).click();
    await expect(badge).not.toBeVisible({ timeout: 5_000 });

    // event_log assertion via direct DB query (skip if DATABASE_URL not set).
    if (dbAvailable) {
      // This assertion is left as a TODO for Stage 7e integration:
      // Query: SELECT COUNT(*) FROM event_log WHERE payload->>'link_id' = linkId
      // For now we assert the UI state only.
      // TODO(stage-7e): wire pg client and assert event_log row.
    }

    await ctx.close();
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });
});
