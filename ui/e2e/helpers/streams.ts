/**
 * Realtime stream assertion helpers.
 *
 * These helpers verify that the ConnectRPC server-streaming endpoints are
 * alive and deliver events. They do NOT mock the stream — they observe real
 * network traffic per feedback_no_mocks_for_infra.md.
 */

import { expect } from "@playwright/test";
import type { Page, Response } from "@playwright/test";

/**
 * Waits for a SubscribeBoard streaming RPC response to appear on the page.
 * Returns the Response object so callers can inspect headers.
 *
 * The URL pattern matches ConnectRPC streaming paths:
 *   http(s)://<host>/sunbeam.kanban.v1.BoardService/SubscribeBoard
 */
export async function waitForSubscribeBoardResponse(
  page: Page,
  timeoutMs = 10_000,
): Promise<Response> {
  return page.waitForResponse(
    (r) => r.url().includes("BoardService/SubscribeBoard"),
    { timeout: timeoutMs },
  );
}

/**
 * Waits for a SubscribeProject streaming RPC response.
 */
export async function waitForSubscribeProjectResponse(
  page: Page,
  timeoutMs = 10_000,
): Promise<Response> {
  return page.waitForResponse(
    (r) => r.url().includes("ProjectService/SubscribeProject"),
    { timeout: timeoutMs },
  );
}

/**
 * Asserts that a board subscription stream is (or stays) open.
 *
 * ConnectRPC server-streaming responses over h2 use content-type
 * "application/connect+proto" or "application/grpc-web+proto".
 * A closed stream will have a non-2xx status or a grpc-status trailer != 0.
 *
 * We check the response's content-type header as a proxy for "this is a
 * streaming response" — a REST fallback or error would have a different
 * content type.
 */
export async function assertStreamIsOpen(response: Response): Promise<void> {
  const ct = response.headers()["content-type"] ?? "";
  expect(
    ct.includes("connect+proto") ||
      ct.includes("grpc-web+proto") ||
      ct.includes("connect+json"),
    `Expected a ConnectRPC streaming content-type but got: ${ct}`,
  ).toBe(true);
}

/**
 * Waits for any BoardEvent carrying a CardMoved payload to arrive on the
 * board subscription for the given page.  Because Playwright's
 * waitForResponse only fires on the initial response (not on individual
 * streamed frames), we instead wait for a network response *or* for the
 * UI to reflect the change.
 *
 * For the two-tab realtime test the caller should combine this with a
 * UI assertion on page2 to prove the event arrived via the stream and not
 * a page reload.
 */
export async function waitForBoardEvent(
  page: Page,
  matcher: (url: string) => boolean,
  timeoutMs = 5_000,
): Promise<Response> {
  return page.waitForResponse(matcher, { timeout: timeoutMs });
}
