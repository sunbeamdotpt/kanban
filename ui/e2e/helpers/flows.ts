/**
 * UI-level flow helpers shared across salvaged + new specs.
 * These drive real browser interactions; no mocking.
 */

import { expect } from "@playwright/test";
import type { Page } from "@playwright/test";

// ── Project flows ────────────────────────────────────────────────────────────

/**
 * Creates a project via the UI and returns its generated name.
 * Assumes the page is at "/".
 */
export async function createProject(page: Page): Promise<string> {
  const name = `E2E ${Date.now()}`;
  await page.goto("/");
  await page.getByText("+ New project").click();
  await page.locator('input[type="text"]').first().fill(name);
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText(name)).toBeVisible({ timeout: 10_000 });
  return name;
}

// ── Board flows ──────────────────────────────────────────────────────────────

/**
 * Creates a project + board and navigates to the board view.
 * @param templateName  Optional beam-ui Select template to pick (e.g. "Kanban", "Simple").
 */
export async function createBoardAndNavigate(
  page: Page,
  templateName?: string,
): Promise<void> {
  const projectName = await createProject(page);
  await page.getByText(projectName).click();
  await expect(page.getByText("+ New board")).toBeVisible();

  await page.getByText("+ New board").click();
  await expect(page.getByRole("heading", { name: "New Board" })).toBeVisible();
  await page.locator('input[type="text"]').first().fill("Test Board");

  if (templateName) {
    // beam-ui Select — click the trigger (data-part="trigger"), pick option.
    await page.locator('[data-part="trigger"]').click();
    await page
      .locator('[data-part="item"]')
      .filter({ hasText: templateName })
      .first()
      .click();
  }

  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText("Test Board")).toBeVisible();

  // Wait for dialog to close, then navigate to board.
  await expect(
    page.getByRole("heading", { name: "New Board" }),
  ).not.toBeVisible();
  await page.getByText("Test Board", { exact: true }).click();

  // Board URL pattern: /p/{projectId}/b/{boardId}
  await page.waitForURL(/\/p\/.*\/b\/.*/);
  // Wait for board canvas to render (at least one column or the add-column CTA).
  await page.waitForFunction(
    () =>
      document.querySelector("[data-testid^='column-']") !== null ||
      (document.querySelector("body")?.textContent?.includes("Add c") ?? false),
    { timeout: 15_000 },
  );
}

// ── Card flows ───────────────────────────────────────────────────────────────

/**
 * Adds a card via the create modal. Assumes the current page is a board view.
 * Uses the first available "+ Add card" button (first column).
 */
export async function addCardViaModal(page: Page, title: string): Promise<void> {
  await page.getByText("+ Add card").first().click();
  await expect(page.getByText("New card in")).toBeVisible();
  await page.locator('input[type="text"]').first().fill(title);
  await page.getByRole("button", { name: "Create card" }).click();
  await expect(page.getByText(title)).toBeVisible({ timeout: 10_000 });
}

/**
 * Opens a card detail modal by clicking the card title.
 * Waits for the card detail modal to be visible.
 */
export async function openCardDetail(page: Page, title: string): Promise<void> {
  await page.getByText(title).click();
  // Prefer testid when Stage 6 has landed; fall back to heading as secondary
  // signal so tests are not brittle before testids exist.
  const modal = page.locator('[data-testid="card-detail-modal"]');
  const fallback = page.getByRole("heading", { name: "Description" });
  await Promise.race([
    modal.waitFor({ state: "visible", timeout: 10_000 }),
    fallback.waitFor({ state: "visible", timeout: 10_000 }),
  ]);
}
