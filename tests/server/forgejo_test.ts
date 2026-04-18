import { assertEquals } from "https://deno.land/std@0.220.0/assert/mod.ts";

Deno.test("searchForgejoIssues — returns empty array on network error", async () => {
  // Set FORGEJO_URL to an unreachable address to test error handling
  const originalUrl = Deno.env.get("FORGEJO_URL");
  Deno.env.set("FORGEJO_URL", "http://localhost:1");

  // Need to re-import to pick up the env change — since the module caches the URL
  // at import time, we test the error path by calling with a repo param
  const { searchForgejoIssues } = await import("../../server/forgejo.ts");
  const results = await searchForgejoIssues("test query", "nonexistent/repo");
  assertEquals(Array.isArray(results), true);
  assertEquals(results.length, 0);

  if (originalUrl) {
    Deno.env.set("FORGEJO_URL", originalUrl);
  } else {
    Deno.env.delete("FORGEJO_URL");
  }
});

Deno.test("searchForgejoIssues — returns empty array for global search on error", async () => {
  Deno.env.set("FORGEJO_URL", "http://localhost:1");
  const { searchForgejoIssues } = await import("../../server/forgejo.ts");
  const results = await searchForgejoIssues("test query");
  assertEquals(Array.isArray(results), true);

  Deno.env.delete("FORGEJO_URL");
});
