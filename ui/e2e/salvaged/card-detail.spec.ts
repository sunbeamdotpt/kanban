/**
 * Salvaged from apps/kanban-old/ui/e2e/card-detail.spec.ts (5 tests).
 * Replaced class-regex selector ([class*="pos_fixed"]) with
 * data-testid="card-detail-modal" backdrop click pattern.
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "../helpers/flows";

const SKIP_REASON = "needs deployed kanban service (Stage 7e+)";
const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";

async function setupBoardWithCard(page: import("@playwright/test").Page) {
  await createBoardAndNavigate(page, "Simple");
  await expect(page.getByRole("heading", { name: "TO DO" })).toBeVisible();
  await addCardViaModal(page, "Feature Card");
}

test.describe("Card detail features", () => {

  test("forgejo link search UI opens", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.getByText("Link issue or PR").click();
    await expect(page.getByRole("button", { name: "Search" })).toBeVisible();

    await page.getByRole("button", { name: "Cancel" }).last().click();
    await expect(page.getByText("Link issue or PR")).toBeVisible();
  });

  test("attachment upload button exists", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();
    await expect(page.getByText("Upload file")).toBeVisible();
    await expect(page.getByText("Attachments")).toBeVisible();
  });

  test("card shows correct sections", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();

    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();
    await expect(page.getByText("Forgejo Links")).toBeVisible();
    await expect(page.getByText("Attachments")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Assignees" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Labels" })).toBeVisible();
  });

  test("close modal by clicking backdrop", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    // Click the card-detail-modal backdrop (the overlay element).
    // Stage 6 must wire data-testid="card-detail-modal" on the backdrop overlay.
    const backdrop = page.locator('[data-testid="card-detail-modal"]');
    if (await backdrop.count() > 0) {
      await backdrop.click({ position: { x: 5, y: 5 } });
    } else {
      // Fallback until Stage 6 testids land: click the outermost dialog element.
      await page.getByRole("dialog").click({ position: { x: 5, y: 5 }, force: true });
    }

    await expect(
      page.getByRole("heading", { name: "Description" }),
    ).not.toBeVisible({ timeout: 3_000 });
  });

  test("close modal by pressing Escape", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.keyboard.press("Escape");

    await expect(
      page.getByRole("heading", { name: "Description" }),
    ).not.toBeVisible({ timeout: 3_000 });
  });
});
