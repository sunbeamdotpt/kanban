import { assertEquals, assertNotEquals, assertStringIncludes } from "https://deno.land/std@0.220.0/assert/mod.ts";

// KANBAN_TEST_MODE=1 is set via deno task test, so CSRF middleware passes through.
// We test the token generation functions directly.

Deno.test("generateCsrfToken — returns token and cookie", async () => {
  const { generateCsrfToken } = await import("../../server/csrf.ts");
  const { token, cookie } = await generateCsrfToken();

  assertEquals(typeof token, "string");
  assertEquals(typeof cookie, "string");
  assertStringIncludes(cookie, "kanban-csrf-token=");
  assertStringIncludes(cookie, "Path=/");
  assertStringIncludes(cookie, "HttpOnly");
  assertStringIncludes(cookie, "SameSite=Strict");
});

Deno.test("generateCsrfToken — token format is uuid.signature", async () => {
  const { generateCsrfToken } = await import("../../server/csrf.ts");
  const { token } = await generateCsrfToken();

  const parts = token.split(".");
  assertEquals(parts.length, 2);
  // UUID part should be 36 chars
  assertEquals(parts[0].length, 36);
  // Signature should be hex (64 chars for SHA-256)
  assertEquals(parts[1].length, 64);
});

Deno.test("generateCsrfToken — each call produces unique token", async () => {
  const { generateCsrfToken } = await import("../../server/csrf.ts");
  const { token: t1 } = await generateCsrfToken();
  const { token: t2 } = await generateCsrfToken();
  assertNotEquals(t1, t2);
});

Deno.test("CSRF_COOKIE_NAME is kanban-csrf-token", async () => {
  const { CSRF_COOKIE_NAME } = await import("../../server/csrf.ts");
  assertEquals(CSRF_COOKIE_NAME, "kanban-csrf-token");
});
