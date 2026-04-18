import { assertEquals, assertNotEquals } from "https://deno.land/std@0.220.0/assert/mod.ts";
import { hmacSha256, sha256Hex, toHex, getSigningKey } from "../../server/s3.ts";

const encoder = new TextEncoder();

Deno.test("toHex — converts ArrayBuffer to hex string", () => {
  const buf = new Uint8Array([0, 1, 15, 16, 255]).buffer;
  assertEquals(toHex(buf), "00010f10ff");
});

Deno.test("toHex — empty buffer", () => {
  const buf = new Uint8Array([]).buffer;
  assertEquals(toHex(buf), "");
});

Deno.test("sha256Hex — known hash", async () => {
  const data = encoder.encode("hello");
  const hash = await sha256Hex(data);
  assertEquals(hash, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
});

Deno.test("sha256Hex — empty input", async () => {
  const data = new Uint8Array(0);
  const hash = await sha256Hex(data);
  assertEquals(hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
});

Deno.test("hmacSha256 — produces consistent output", async () => {
  const key = encoder.encode("secret");
  const result1 = await hmacSha256(key, "message");
  const result2 = await hmacSha256(key, "message");
  assertEquals(toHex(result1), toHex(result2));
});

Deno.test("hmacSha256 — different keys produce different output", async () => {
  const key1 = encoder.encode("secret1");
  const key2 = encoder.encode("secret2");
  const result1 = toHex(await hmacSha256(key1, "message"));
  const result2 = toHex(await hmacSha256(key2, "message"));
  assertNotEquals(result1, result2);
});

Deno.test("hmacSha256 — different messages produce different output", async () => {
  const key = encoder.encode("secret");
  const result1 = toHex(await hmacSha256(key, "message1"));
  const result2 = toHex(await hmacSha256(key, "message2"));
  assertNotEquals(result1, result2);
});

Deno.test("getSigningKey — returns an ArrayBuffer", async () => {
  const key = await getSigningKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20240101", "us-east-1");
  assertEquals(key instanceof ArrayBuffer, true);
  assertEquals(key.byteLength > 0, true);
});

Deno.test("getSigningKey — consistent output for same inputs", async () => {
  const key1 = await getSigningKey("secret", "20240101", "us-east-1");
  const key2 = await getSigningKey("secret", "20240101", "us-east-1");
  assertEquals(toHex(key1), toHex(key2));
});

Deno.test("getSigningKey — different dates produce different keys", async () => {
  const key1 = toHex(await getSigningKey("secret", "20240101", "us-east-1"));
  const key2 = toHex(await getSigningKey("secret", "20240102", "us-east-1"));
  assertNotEquals(key1, key2);
});
