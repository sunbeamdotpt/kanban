import { assertEquals } from "https://deno.land/std@0.220.0/assert/mod.ts";
import { Hono } from "hono";

// KANBAN_TEST_MODE=1 is set — auth middleware injects test identity

Deno.test("auth — sessionHandler returns test identity in test mode", async () => {
  const { sessionHandler } = await import("../../server/auth.ts");

  const app = new Hono();
  app.get("/api/auth/session", sessionHandler);

  const res = await app.request("/api/auth/session");
  assertEquals(res.status, 200);

  const body = await res.json();
  assertEquals(body.user.id, "e2e-test-user-00000000");
  assertEquals(body.user.email, "e2e@test.local");
  assertEquals(body.user.name, "E2E Test User");
});

Deno.test("auth — authMiddleware injects test identity in test mode", async () => {
  const { authMiddleware } = await import("../../server/auth.ts");

  const app = new Hono();
  app.use("/*", authMiddleware);
  app.get("/api/test", (c) => {
    const identity = c.get("identity");
    return c.json({ id: identity?.id });
  });

  const res = await app.request("/api/test");
  assertEquals(res.status, 200);

  const body = await res.json();
  assertEquals(body.id, "e2e-test-user-00000000");
});

Deno.test("auth — health endpoint bypasses auth", async () => {
  const { authMiddleware } = await import("../../server/auth.ts");

  const app = new Hono();
  app.use("/*", authMiddleware);
  app.get("/health", (c) => c.json({ ok: true }));

  const res = await app.request("/health");
  assertEquals(res.status, 200);
});

Deno.test("auth — static assets bypass auth", async () => {
  const { authMiddleware } = await import("../../server/auth.ts");

  const app = new Hono();
  app.use("/*", authMiddleware);
  app.get("/assets/test.js", (c) => c.text("js"));

  const res = await app.request("/assets/test.js");
  assertEquals(res.status, 200);
});

Deno.test("getSession — returns null for empty cookie header", async () => {
  const { getSession } = await import("../../server/auth.ts");
  const result = await getSession("");
  assertEquals(result.info, null);
  assertEquals(result.needsAal2, false);
});

Deno.test("getSession — returns null for unrelated cookies", async () => {
  const { getSession } = await import("../../server/auth.ts");
  const result = await getSession("foo=bar; baz=qux");
  assertEquals(result.info, null);
  assertEquals(result.needsAal2, false);
});
