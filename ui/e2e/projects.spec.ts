import { test, expect } from "@playwright/test";

test.describe("Projects page", () => {
  test("shows the projects page heading", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toContainText("Projects");
  });

  test("shows new project button", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByText("+ New project")).toBeVisible();
  });

  test("create project flow", async ({ page }) => {
    await page.goto("/");

    // Open create dialog
    await page.getByText("+ New project").click();
    await expect(page.getByRole("heading", { name: "New Project" })).toBeVisible();

    // Fill in name
    const nameInput = page.locator('input[type="text"]').first();
    await nameInput.fill(`E2E Test Project ${Date.now()}`);

    // Click create
    await page.getByRole("button", { name: "Create" }).click();

    // Should see the project card (use first() in case of duplicates from prior runs)
    await expect(page.getByText(/E2E Test Project/).first()).toBeVisible();
  });

  test("cancel create project", async ({ page }) => {
    await page.goto("/");
    await page.getByText("+ New project").click();
    await expect(page.getByRole("heading", { name: "New Project" })).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
    // The heading should be gone (dialog closed)
    await expect(page.getByRole("heading", { name: "New Project" })).not.toBeVisible();
  });

  test("expand project shows boards", async ({ page }) => {
    await page.goto("/");

    // Create a project first
    await page.getByText("+ New project").click();
    const nameInput = page.locator('input[type="text"]').first();
    const name = `Expand Test ${Date.now()}`;
    await nameInput.fill(name);
    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.getByText(name)).toBeVisible();

    // Click to expand
    await page.getByText(name).click();
    await expect(page.getByText("+ New board")).toBeVisible();
  });
});
