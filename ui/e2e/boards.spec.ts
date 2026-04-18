import { test, expect } from "@playwright/test";

test.describe("Boards", () => {
  async function createProjectAndExpand(page: import("@playwright/test").Page) {
    const projectName = `Board E2E ${Date.now()}`;
    await page.goto("/");
    await page.getByText("+ New project").click();
    await page.locator('input[type="text"]').first().fill(projectName);
    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText(projectName)).toBeVisible();
    await page.getByText(projectName).click();
    await expect(page.getByText("+ New board")).toBeVisible();
    return projectName;
  }

  test("create board from template", async ({ page }) => {
    await createProjectAndExpand(page);
    await page.getByText("+ New board").click();
    await expect(page.getByRole("heading", { name: "New Board" })).toBeVisible();

    await page.locator('input[type="text"]').first().fill("Sprint Board");

    // Select Kanban template via beam-ui Select dropdown
    await page.locator('[data-part="trigger"]').click();
    await page.locator('[data-part="item"]').filter({ hasText: "Kanban" }).first().click();

    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText("Sprint Board")).toBeVisible();
  });

  test("create empty board", async ({ page }) => {
    await createProjectAndExpand(page);
    await page.getByText("+ New board").click();
    await page.locator('input[type="text"]').first().fill("Blank Board");
    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText("Blank Board")).toBeVisible();
  });

  test("navigate to board view", async ({ page }) => {
    await createProjectAndExpand(page);
    await page.getByText("+ New board").click();
    await page.locator('input[type="text"]').first().fill("Nav Board");

    await page.locator('[data-part="trigger"]').click();
    await page.locator('[data-part="item"]').filter({ hasText: "Simple" }).first().click();

    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText("Nav Board")).toBeVisible();

    // Click to navigate
    await page.getByText("Nav Board").click();

    // Should see the board view with columns
    await expect(page.getByText("TO DO")).toBeVisible();
    await expect(page.getByText("DOING")).toBeVisible();
    await expect(page.getByText("DONE")).toBeVisible();
  });
});
