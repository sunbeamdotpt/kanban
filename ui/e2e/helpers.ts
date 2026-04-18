import { expect } from "@playwright/test";

export async function createProject(page: import("@playwright/test").Page): Promise<string> {
  const name = `E2E ${Date.now()}`;
  await page.goto("/");
  await page.getByText("+ New project").click();
  await page.locator('input[type="text"]').first().fill(name);
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText(name)).toBeVisible({ timeout: 10000 });
  return name;
}

export async function createBoardAndNavigate(
  page: import("@playwright/test").Page,
  templateName?: string,
): Promise<void> {
  const projectName = await createProject(page);
  await page.getByText(projectName).click();
  await expect(page.getByText("+ New board")).toBeVisible();

  await page.getByText("+ New board").click();
  await expect(page.getByRole("heading", { name: "New Board" })).toBeVisible();
  await page.locator('input[type="text"]').first().fill("Test Board");

  if (templateName) {
    // beam-ui Select — click the trigger (has data-part="trigger"), then pick option
    await page.locator('[data-part="trigger"]').click();
    await page.locator('[data-part="item"]').filter({ hasText: templateName }).first().click();
  }

  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText("Test Board")).toBeVisible();

  // Wait for dialog to close, then navigate to board
  await expect(page.getByRole("heading", { name: "New Board" })).not.toBeVisible();
  await page.getByText("Test Board", { exact: true }).click();

  // Wait for board page URL and content to load
  await page.waitForURL(/\/p\/.*\/b\/.*/);
  // Wait for board to render
  await page.waitForFunction(
    () => document.querySelector("body")?.textContent?.includes("Add c"),
    { timeout: 10000 },
  );
}

/** Add a card via the create modal. Assumes you're already on a board page. */
export async function addCardViaModal(
  page: import("@playwright/test").Page,
  title: string,
): Promise<void> {
  await page.getByText("+ Add card").first().click();
  await expect(page.getByText("New card in")).toBeVisible();
  await page.locator('input[type="text"]').first().fill(title);
  await page.getByRole("button", { name: "Create card" }).click();
  await expect(page.getByText(title)).toBeVisible();
}
