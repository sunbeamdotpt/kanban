import { assertEquals, assertRejects } from "https://deno.land/std@0.220.0/assert/mod.ts";
import { RpcError } from "../../server/rpc.ts";

Deno.test("RpcError — stores code and message", () => {
  const err = new RpcError("not_found", "Card not found");
  assertEquals(err.code, "not_found");
  assertEquals(err.message, "Card not found");
  assertEquals(err instanceof Error, true);
});

Deno.test("RpcError — different codes", () => {
  const codes = [
    "invalid_argument",
    "permission_denied",
    "already_exists",
    "unauthenticated",
    "internal",
  ] as const;

  for (const code of codes) {
    const err = new RpcError(code, `test ${code}`);
    assertEquals(err.code, code);
  }
});
