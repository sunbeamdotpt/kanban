/**
 * new/realtime-two-tab.spec.ts
 *
 * Two-tab DnD sync: open board in two browser contexts (same user),
 * drag card in tab 1, tab 2 must reflect the move within 2 s via the
 * SubscribeBoard stream — NOT via a page reload.
 *
 * This is the canonical wire-level streaming test that replaces the
 * deferred Stage 0c smoke test (per plan §A1.2).
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs KRATOS_ADMIN_URL (to provision the shared test user)
 *   - needs Stage 6 FE with drag-and-drop and SubscribeBoard wired
 */

import { test, expect } from "@playwright/test";
import { waitForSubscribeBoardResponse, assertStreamIsOpen } from "../helpers/streams";
import { createBoardAndNavigate, addCardViaModal } from "../helpers/flows";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_KRATOS = "needs KRATOS_ADMIN_URL";
const SKIP_STAGE6 = "needs Stage 6 FE with drag-and-drop and SubscribeBoard wired";

const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const BASE_URL =
  process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";

test.describe("Realtime two-tab synchronisation", () => {
  test.skip(!deployed, SKIP_DEPLOYED);
  test.skip(!kratosAvailable, SKIP_KRATOS);
  // Remove this skip once Stage 6 FE drag-and-drop is implemented:
  test.skip(true, SKIP_STAGE6);

  test("drag in tab 1 reflects in tab 2 within 2 s via SubscribeBoard stream", async ({
    browser,
  }) => {
    const email = `e2e-realtime-${Date.now()}@sunbeam-test.invalid`;

    // Provision identity + session.
    const idRes = await fetch(`${KRATOS_ADMIN_URL}/admin/identities`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schema_id: "default",
        traits: { email },
        credentials: {
          password: { config: { password: `E2E-${email}-pw!` } },
        },
      }),
    });
    const identity = (await idRes.json()) as { id: string };
    const sessRes = await fetch(
      `${KRATOS_ADMIN_URL}/admin/identities/${identity.id}/sessions`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
    );
    const { session_token: sessionToken } = (await sessRes.json()) as {
      session_token: string;
    };

    const cookie = {
      name: "ory_kratos_session",
      value: sessionToken,
      url: BASE_URL,
      httpOnly: true,
      sameSite: "Lax" as const,
    };

    // Open two browser contexts authenticated as the same user.
    const ctx1 = await browser.newContext();
    const ctx2 = await browser.newContext();
    await ctx1.addCookies([cookie]);
    await ctx2.addCookies([cookie]);

    const page1 = await ctx1.newPage();
    const page2 = await ctx2.newPage();

    // Tab 1: create a board and add a card.
    await createBoardAndNavigate(page1, "Simple");
    await addCardViaModal(page1, "Realtime Test Card");

    // Capture the board URL so tab 2 can navigate to the same board.
    const boardUrl = page1.url();

    // Tab 2: navigate to the same board URL.
    await page2.goto(boardUrl);
    await expect(
      page2.getByRole("heading", { name: "TO DO" }),
    ).toBeVisible({ timeout: 15_000 });

    // Wait for tab 2 to establish its SubscribeBoard stream.
    const streamResponse = await waitForSubscribeBoardResponse(page2, 10_000);
    await assertStreamIsOpen(streamResponse);

    // Tab 1: drag "Realtime Test Card" from TO DO → DOING.
    // DnD via Playwright: locate the card and the target column.
    const card = page1.getByText("Realtime Test Card");
    const targetColumn = page1.getByRole("heading", { name: "DOING" });

    const cardBox = await card.boundingBox();
    const targetBox = await targetColumn.boundingBox();

    if (!cardBox || !targetBox) {
      throw new Error("Could not locate card or target column bounding boxes");
    }

    await page1.mouse.move(
      cardBox.x + cardBox.width / 2,
      cardBox.y + cardBox.height / 2,
    );
    await page1.mouse.down();
    // Slow drag to give the browser time to register the drag.
    await page1.mouse.move(
      targetBox.x + targetBox.width / 2,
      targetBox.y + targetBox.height / 2,
      { steps: 20 },
    );
    await page1.mouse.up();

    // Tab 2 must show the card in DOING within 2 s.
    // This asserts the change came through the stream, not a reload, because
    // we never navigated page2 again after the drag.
    const doingColumn = page2.locator('[data-testid^="column-"]').filter({
      has: page2.getByRole("heading", { name: "DOING" }),
    });
    await expect(doingColumn.getByText("Realtime Test Card")).toBeVisible({
      timeout: 2_000,
    });

    await ctx1.close();
    await ctx2.close();

    // Cleanup.
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });
});
