import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  // Exclude fixture and helper modules — they are not test files.
  testIgnore: ["**/e2e/fixtures/**", "**/e2e/helpers/**"],
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 2 : 4,
  reporter: [
    ["html", { open: "never" }],
    ["junit", { outputFile: "test-results/junit.xml" }],
  ],
  use: {
    baseURL: process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823",
    trace: "on-first-retry",
    screenshot: "on",
  },
  // No webServer — tests run against an already-deployed stack (per plan §A1.2).
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  timeout: 60_000,
  expect: { timeout: 10_000 },
});
