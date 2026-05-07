/**
 * new/attachment-upload.spec.ts
 *
 * Full attachment round-trip:
 *   1. RequestPresignedUpload → get presigned PUT URL + attachment_id
 *   2. PUT 1 KB random bytes to the presigned URL (SeaweedFS S3)
 *   3. ConfirmUpload(attachment_id)
 *   4. RequestPresignedDownload → GET same bytes
 *   5. Assert byte-identical
 *
 * This test calls the ConnectRPC endpoints directly via page.request
 * (no UI interaction for the upload flow itself — the UI is used only to
 * set up the board + card context).
 *
 * Skip reasons:
 *   - needs deployed kanban service (Stage 7e+)
 *   - needs KRATOS_ADMIN_URL
 *   - needs SEAWEEDFS_S3_URL (or the kanban service's presign URL must be
 *     reachable from the test runner — it always is in the dev stack)
 */

import { test, expect } from "@playwright/test";
import { createBoardAndNavigate, addCardViaModal } from "../helpers/flows";

const SKIP_DEPLOYED = "needs deployed kanban service (Stage 7e+)";
const SKIP_KRATOS = "needs KRATOS_ADMIN_URL";

const deployed = !!process.env.KANBAN_E2E_BASE_URL || process.env.CI === "true";
const kratosAvailable = !!process.env.KRATOS_ADMIN_URL;

const KRATOS_ADMIN_URL =
  process.env.KRATOS_ADMIN_URL ?? "http://localhost:4434";
const BASE_URL =
  process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";

/** Generate 1 KB of deterministic pseudo-random bytes. */
function makeTestPayload(): Uint8Array {
  const buf = new Uint8Array(1024);
  for (let i = 0; i < buf.length; i++) {
    buf[i] = (i * 137 + 42) & 0xff;
  }
  return buf;
}

test.describe("Attachment upload round-trip", () => {
  test.skip(!deployed, SKIP_DEPLOYED);
  test.skip(!kratosAvailable, SKIP_KRATOS);

  test("presign upload → PUT → confirm → presign download → byte-identical", async ({
    browser,
    request,
  }) => {
    const email = `e2e-attach-${Date.now()}@sunbeam-test.invalid`;

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

    // Create board + card via UI.
    await createBoardAndNavigate(page, "Simple");
    await addCardViaModal(page, "Attachment Test Card");

    // Extract the card id from the DOM (data-testid="card-{id}").
    const cardEl = page.locator('[data-testid^="card-"]').first();
    await expect(cardEl).toBeVisible({ timeout: 10_000 });
    const cardTestId = await cardEl.getAttribute("data-testid");
    const cardId = cardTestId?.replace("card-", "") ?? "";
    expect(cardId).toBeTruthy();

    // Get the current session cookies for the API requests.
    const cookies = await ctx.cookies();
    const cookieHeader = cookies
      .map((c) => `${c.name}=${c.value}`)
      .join("; ");

    const payload = makeTestPayload();

    // Step 1: RequestPresignedUpload.
    const presignRes = await fetch(
      `${BASE_URL}/sunbeam.kanban.v1.AttachmentService/RequestPresignedUpload`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Cookie: cookieHeader,
        },
        body: JSON.stringify({
          card_id: cardId,
          filename: "test-payload.bin",
          mime_type: "application/octet-stream",
          size_bytes: payload.length,
        }),
      },
    );
    expect(presignRes.status).toBe(200);
    const presignData = (await presignRes.json()) as {
      presigned_url: string;
      attachment_id: string;
      s3_key: string;
    };
    expect(presignData.presigned_url).toBeTruthy();
    expect(presignData.attachment_id).toBeTruthy();

    // Step 2: PUT the payload to the presigned URL.
    const putRes = await request.put(presignData.presigned_url, {
      data: Buffer.from(payload),
      headers: { "Content-Type": "application/octet-stream" },
    });
    expect(putRes.status()).toBe(200);

    // Step 3: ConfirmUpload.
    const confirmRes = await fetch(
      `${BASE_URL}/sunbeam.kanban.v1.AttachmentService/ConfirmUpload`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Cookie: cookieHeader,
        },
        body: JSON.stringify({ attachment_id: presignData.attachment_id }),
      },
    );
    expect(confirmRes.status).toBe(200);
    const confirmed = (await confirmRes.json()) as { id: string };
    expect(confirmed.id).toBe(presignData.attachment_id);

    // Step 4: RequestPresignedDownload.
    const downloadPresignRes = await fetch(
      `${BASE_URL}/sunbeam.kanban.v1.AttachmentService/RequestPresignedDownload`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Cookie: cookieHeader,
        },
        body: JSON.stringify({ attachment_id: presignData.attachment_id }),
      },
    );
    expect(downloadPresignRes.status).toBe(200);
    const downloadData = (await downloadPresignRes.json()) as {
      presigned_url: string;
    };
    expect(downloadData.presigned_url).toBeTruthy();

    // Step 5: GET the bytes and assert byte-identical.
    const getRes = await request.get(downloadData.presigned_url);
    expect(getRes.status()).toBe(200);
    const downloaded = new Uint8Array(await getRes.body());
    expect(downloaded.length).toBe(payload.length);
    for (let i = 0; i < payload.length; i++) {
      expect(downloaded[i]).toBe(payload[i]);
    }

    await ctx.close();
    await fetch(`${KRATOS_ADMIN_URL}/admin/identities/${identity.id}`, {
      method: "DELETE",
    });
  });
});
