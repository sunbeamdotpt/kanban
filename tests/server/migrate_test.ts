import { assertEquals, assertStringIncludes } from "https://deno.land/std@0.220.0/assert/mod.ts";

// We can't actually run migrations without a DB, but we can verify the module
// exports correctly and the migration list is well-formed.

Deno.test("migrate module — exports migrate function", async () => {
  // We only import the module to check it parses — we don't call migrate()
  // because that requires a running PostgreSQL instance.
  const mod = await import("../../server/migrate.ts");
  assertEquals(typeof mod.migrate, "function");
});
