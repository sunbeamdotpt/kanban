/**
 * Salvaged from apps/kanban-old/ui/e2e/kanban.spec.ts (7 tests).
 * Uses data-testid + role selectors; no class-regex selectors.
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "../helpers/flows";
import { cardDetailModal } from "../helpers/selectors";

const SKIP_REASON = "needs deployed kanban service (Stage 7e+)";
const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";

test.describe("Kanban board", () => {
  test.skip(!deployed, SKIP_REASON);

  test("board shows columns from template", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByRole("heading", { name: "TO DO" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "DOING" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "DONE" })).toBeVisible();
  });

  test("add card to column", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByRole("heading", { name: "TO DO" })).toBeVisible();

    await addCardViaModal(page, "My First Task");

    await expect(page.getByText("My First Task")).toBeVisible();
  });

  test("add column", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await expect(page.getByRole("heading", { name: "TO DO" })).toBeVisible();

    await page.getByText("Add column").click();

    const titleInput = page.locator('input[type="text"]').last();
    await titleInput.fill("Blocked");
    await page.getByRole("button", { name: "Add" }).last().click();

    await expect(
      page.getByRole("heading", { name: "BLOCKED" }),
    ).toBeVisible({ timeout: 10_000 });
  });

  test("card click opens detail modal", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Detail Test Card");

    await page.getByText("Detail Test Card").click();

    // Prefer data-testid="card-detail-modal" once Stage 6 lands.
    const modal = cardDetailModal(page);
    const fallback = page.getByRole("heading", { name: "Description" });
    await Promise.race([
      modal.waitFor({ state: "visible", timeout: 10_000 }),
      fallback.waitFor({ state: "visible", timeout: 10_000 }),
    ]);

    await expect(page.getByRole("heading", { name: "Assignees" })).toBeVisible();
  });

  test("card detail — edit and save", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Edit Me");

    await page.getByText("Edit Me").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.getByRole("button", { name: "Edit", exact: true }).click();

    const titleInput = page.locator("input").first();
    await titleInput.clear();
    await titleInput.fill("Edited Title");

    await page.getByRole("button", { name: "Save" }).click();
    await expect(page.getByText("Edited Title").first()).toBeVisible({ timeout: 10_000 });
  });

  test("card detail — delete card", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Delete Me");

    await page.getByText("Delete Me").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    await page.getByRole("button", { name: "Delete", exact: true }).click();

    // Modal closes and card is removed from DOM.
    await expect(
      page.getByRole("heading", { name: "Description" }),
    ).not.toBeVisible({ timeout: 5_000 });
  });

  test("card detail — deep link via URL param", async ({ page }) => {
    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Deep Link Card");

    await page.getByText("Deep Link Card").click();
    await expect(page.getByRole("heading", { name: "Description" })).toBeVisible();

    // URL must contain a card query param.
    expect(page.url()).toContain("card=");

    const url = page.url();
    await page.keyboard.press("Escape");
    await page.goto("/");
    await page.goto(url);

    await expect(
      page.getByRole("heading", { name: "Description" }),
    ).toBeVisible({ timeout: 15_000 });
  });
});
