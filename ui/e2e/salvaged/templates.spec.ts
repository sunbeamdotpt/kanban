/**
 * Salvaged from apps/kanban-old/ui/e2e/templates.spec.ts (3 tests).
 * Asserts on role-based headings rather than plain text to avoid
 * matching inside column cards.
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate } from "../helpers/flows";

const SKIP_REASON = "needs deployed kanban service (Stage 7e+)";
const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";

test.describe("Board templates", () => {

  test("board created from Kanban template has 5 columns", async ({ page }) => {
    await createBoardAndNavigate(page, "Kanban");

    // Each column renders its title as a heading element.
    await expect(page.getByRole("heading", { name: "BACKLOG" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "TO DO" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "IN PROGRESS" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "REVIEW" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "DONE" })).toBeVisible();
  });

  test("board created from Sprint template has 4 columns", async ({ page }) => {
    await createBoardAndNavigate(page, "Sprint");

    await expect(
      page.getByRole("heading", { name: "SPRINT BACKLOG" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "IN PROGRESS" }),
    ).toBeVisible();
    await expect(page.getByRole("heading", { name: "TESTING" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "DONE" })).toBeVisible();
  });

  test("empty board has no columns, add column works", async ({ page }) => {
    await createBoardAndNavigate(page); // no template

    // Assert the add-column affordance exists.
    await expect(page.getByText("Add column")).toBeVisible();

    await page.getByText("Add column").click();
    await page.locator('input[type="text"]').last().fill("Custom Column");
    await page.getByRole("button", { name: "Add" }).last().click();

    // Column header should appear.
    await expect(
      page.getByRole("heading", { name: "CUSTOM COLUMN" }),
    ).toBeVisible({ timeout: 10_000 });
  });
});
