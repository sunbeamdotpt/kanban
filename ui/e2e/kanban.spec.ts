import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "./helpers";

test.describe("Kanban board", () => {
  test("board shows columns from template", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();
    await expect(page.getByText("DOING")).toBeVisible();
    await expect(page.getByText("DONE")).toBeVisible();
  });

  test("add card to column", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();

    await addCardViaModal(page, "My First Task");

    await expect(page.getByText("My First Task")).toBeVisible();
  });

  test("add column", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();

    await page.getByText("Add column").click();

    const titleInput = page.locator('input[type="text"]').last();
    await titleInput.fill("Blocked");
    await page.getByRole("button", { name: "Add" }).last().click();

    await expect(page.getByText("BLOCKED")).toBeVisible();
  });

  test("card click opens detail modal", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();

    await addCardViaModal(page, "Detail Test Card");

    await page.getByText("Detail Test Card").click();

    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Assignees" })).toBeVisible();
  });

  test("card detail — edit and save", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();

    await addCardViaModal(page, "Edit Me");

    await page.getByText("Edit Me").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.getByRole("button", { name: "Edit", exact: true }).click();

    const titleInput = page.locator("input").first();
    await titleInput.clear();
    await titleInput.fill("Edited Title");

    await page.getByRole("button", { name: "Save" }).click();
    await expect(page.getByText("Edited Title").first()).toBeVisible();
  });

  test("card detail — delete card", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();

    await addCardViaModal(page, "Delete Me");

    await page.getByText("Delete Me").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.getByRole("button", { name: "Delete", exact: true }).click();
    // Modal closes and card is removed
    await expect(page.getByRole("heading", { name: "Description" })).not.toBeVisible({ timeout: 5000 });
  });

  test("card detail — deep link via URL param", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByText("TO DO")).toBeVisible();

    await addCardViaModal(page, "Deep Link Card");

    await page.getByText("Deep Link Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    expect(page.url()).toContain("card=");

    const url = page.url();
    await page.keyboard.press("Escape");
    await page.goto("/");
    await page.goto(url);

    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible({ timeout: 10000 });
  });
});
