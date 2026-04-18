import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "./helpers";

async function setupBoardWithCard(page: import("@playwright/test").Page) {
  await createBoardAndNavigate(page, "Simple");
  await expect(page.getByText("TO DO")).toBeVisible();
  await addCardViaModal(page, "Feature Card");
}

test.describe("Card detail features", () => {
  test("forgejo link search UI opens", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.getByText("Link issue or PR").click();
    // The search form should appear with Search and Cancel buttons
    await expect(page.getByRole("button", { name: "Search" })).toBeVisible();

    await page.getByRole("button", { name: "Cancel" }).last().click();
    // The search form should be gone, replaced by the "Link issue or PR" button
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

    await page.locator('[class*="pos_fixed"]').first().click({ position: { x: 5, y: 5 } });

    await expect(page.getByRole("heading", { name: "Description" })).not.toBeVisible({ timeout: 3000 });
  });

  test("close modal by pressing Escape", async ({ page }) => {
    await setupBoardWithCard(page);
    await page.getByText("Feature Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.keyboard.press("Escape");

    await expect(page.getByRole("heading", { name: "Description" })).not.toBeVisible({ timeout: 3000 });
  });
});
