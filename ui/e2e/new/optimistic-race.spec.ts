/**
 * new/optimistic-race.spec.ts
 *
 * 3 rapid drags before stream catches up; final state matches server.
 *
 * Sequence:
 *   1. Add card C to "TO DO".
 *   2. Drag todo → progress (drag 1).
 *   3. Drag progress → todo (drag 2, <300 ms after drag 1).
 *   4. Drag todo → done (drag 3, <300 ms after drag 2).
 *   5. Poll GetCard(C) until revision is stable (no further changes).
 *   6. Assert that the card's column_id on the server matches the
 *      on-screen position in "DONE".
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs Stage 6 FE with drag-and-drop and optimistic pending-mutation queue
 *   - needs KRATOS_ADMIN_URL
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "../helpers/flows";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_STAGE6 = "needs Stage 6 FE drag-and-drop + optimistic pending-mutation queue";
const SKIP_KRATOS = "needs KRATOS_ADMIN_URL";

const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const BASE_URL =
  process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";

async function dragCardToColumn(
  page: import("@playwright/test").Page,
  cardTitle: string,
  targetColumnHeading: string,
): Promise<void> {
  const card = page.getByText(cardTitle, { exact: true });
  const target = page.getByRole("heading", { name: targetColumnHeading });

  const cardBox = await card.boundingBox();
  const targetBox = await target.boundingBox();
  if (!cardBox || !targetBox) {
    throw new Error(`Cannot locate card "${cardTitle}" or column "${targetColumnHeading}"`);
  }

  await page.mouse.move(
    cardBox.x + cardBox.width / 2,
    cardBox.y + cardBox.height / 2,
  );
  await page.mouse.down();
  await page.mouse.move(
    targetBox.x + targetBox.width / 2,
    targetBox.y + targetBox.height + 40, // drop below the heading
    { steps: 10 },
  );
  await page.mouse.up();
}

test.describe("Optimistic race: 3 rapid drags", () => {
  test.skip(!deployed, SKIP_DEPLOYED);
  test.skip(!kratosAvailable, SKIP_KRATOS);
  // Remove this skip once Stage 6 FE optimistic queue is implemented:
  test.skip(true, SKIP_STAGE6);

  test("final card position matches server after 3 rapid drags", async ({
    browser,
  }) => {
    const email = `e2e-race-${Date.now()}@sunbeam-test.invalid`;

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

    const ctx = await browser.newContext();
    await ctx.addCookies([
      {
        name: "ory_kratos_session",
        value: sessionToken,
        url: BASE_URL,
        httpOnly: true,
        sameSite: "Lax",
      },
    ]);
    const page = await ctx.newPage();

    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Race Card");

    // Extract card id for server polling.
    const cardEl = page.locator('[data-testid^="card-"]').filter({
      hasText: "Race Card",
    });
    await expect(cardEl).toBeVisible({ timeout: 10_000 });
    const cardTestId = await cardEl.getAttribute("data-testid");
    const cardId = cardTestId?.replace("card-", "") ?? "";
    expect(cardId).toBeTruthy();

    const cookies = await ctx.cookies();
    const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join("; ");

    // 3 rapid drags (no await between them beyond mouse operations).
    await dragCardToColumn(page, "Race Card", "DOING");
    await dragCardToColumn(page, "Race Card", "TO DO");
    await dragCardToColumn(page, "Race Card", "DONE");

    // Poll GetCard until revision stabilises (max 5 s).
    let lastRevision = -1;
    let stableCard: { column_id: string; revision: number } | null = null;

    for (let attempt = 0; attempt < 20; attempt++) {
      await page.waitForTimeout(250);
      const res = await fetch(
        `${BASE_URL}/sunbeam.kanban.v1.CardService/GetCard`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            Cookie: cookieHeader,
          },
          body: JSON.stringify({ card_id: cardId }),
        },
      );
      if (!res.ok) continue;
      const card = (await res.json()) as {
        column_id: string;
        revision: number;
      };
      if (card.revision === lastRevision) {
        stableCard = card;
        break;
      }
      lastRevision = card.revision;
    }

    expect(stableCard, "Card revision never stabilised").not.toBeNull();

    // The DONE column's id — extract from UI.
    const doneColumnEl = page
      .locator('[data-testid^="column-"]')
      .filter({ has: page.getByRole("heading", { name: "DONE" }) });
    const doneTestId = await doneColumnEl.getAttribute("data-testid");
    const doneColumnId = doneTestId?.replace("column-", "") ?? "";

    // Assert server agrees: card is in the DONE column.
    expect(stableCard!.column_id).toBe(doneColumnId);

    // Assert the UI also shows the card in DONE.
    await expect(
      doneColumnEl.getByText("Race Card"),
    ).toBeVisible({ timeout: 5_000 });

    await ctx.close();
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });
});
