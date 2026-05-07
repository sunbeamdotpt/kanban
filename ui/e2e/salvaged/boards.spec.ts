/**
 * Salvaged from apps/kanban-old/ui/e2e/boards.spec.ts (3 tests).
 * Ported to use data-testid selectors and structured assertions.
 */

import { test, expect } from "@playwright/test";
import { newProjectButton, newBoardButton } from "../helpers/selectors";

const SKIP_REASON = "needs deployed kanban service (Stage 7e+)";
const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";

async function createProjectAndExpand(page: import("@playwright/test").Page) {
  const projectName = `Board E2E ${Date.now()}`;
  await page.goto("/");
  await newProjectButton(page).click();
  await page.locator('input[type="text"]').first().fill(projectName);
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText(projectName)).toBeVisible({ timeout: 10_000 });
  await page.getByText(projectName).click();
  await expect(newBoardButton(page)).toBeVisible();
  return projectName;
}

test.describe("Boards", () => {
  test.skip(!deployed, SKIP_REASON);

  test("create board from template", async ({ page }) => {
    await createProjectAndExpand(page);
    await newBoardButton(page).click();
    await expect(page.getByRole("heading", { name: "New Board" })).toBeVisible();

    await page.locator('input[type="text"]').first().fill("Sprint Board");

    // beam-ui Select dropdown.
    await page.locator('[data-part="trigger"]').click();
    await page
      .locator('[data-part="item"]')
      .filter({ hasText: "Kanban" })
      .first()
      .click();

    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText("Sprint Board")).toBeVisible({ timeout: 10_000 });
  });

  test("create empty board", async ({ page }) => {
    await createProjectAndExpand(page);
    await newBoardButton(page).click();
    await page.locator('input[type="text"]').first().fill("Blank Board");
    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText("Blank Board")).toBeVisible({ timeout: 10_000 });
  });

  test("navigate to board view", async ({ page }) => {
    await createProjectAndExpand(page);
    await newBoardButton(page).click();
    await page.locator('input[type="text"]').first().fill("Nav Board");

    await page.locator('[data-part="trigger"]').click();
    await page
      .locator('[data-part="item"]')
      .filter({ hasText: "Simple" })
      .first()
      .click();

    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText("Nav Board")).toBeVisible({ timeout: 10_000 });

    await page.getByText("Nav Board").click();

    // Assert on column headings — structured, not string-contains.
    await expect(page.getByRole("heading", { name: "TO DO" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "DOING" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "DONE" })).toBeVisible();
  });
});
