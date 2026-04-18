import { test, expect } from "@playwright/test";
import { createBoardAndNavigate } from "./helpers";

test.describe("Board templates", () => {
  test("board created from Kanban template has 5 columns", async ({ page }) => {
    await createBoardAndNavigate(page, "Kanban");

    await expect(page.getByText("BACKLOG")).toBeVisible();
    await expect(page.getByText("TO DO")).toBeVisible();
    await expect(page.getByText("IN PROGRESS")).toBeVisible();
    await expect(page.getByText("REVIEW")).toBeVisible();
    await expect(page.getByText("DONE")).toBeVisible();
  });

  test("board created from Sprint template has 4 columns", async ({ page }) => {
    await createBoardAndNavigate(page, "Sprint");

    await expect(page.getByText("SPRINT BACKLOG")).toBeVisible();
    await expect(page.getByText("IN PROGRESS")).toBeVisible();
    await expect(page.getByText("TESTING")).toBeVisible();
    await expect(page.getByText("DONE")).toBeVisible();
  });

  test("empty board has no columns, add column works", async ({ page }) => {
    await createBoardAndNavigate(page); // no template

    await expect(page.getByText("Add column")).toBeVisible();

    await page.getByText("Add column").click();
    await page.locator('input[type="text"]').last().fill("Custom Column");
    await page.getByRole("button", { name: "Add" }).last().click();

    await expect(page.getByText("CUSTOM COLUMN")).toBeVisible();
  });
});
