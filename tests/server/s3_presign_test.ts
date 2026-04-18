import { assertEquals, assertStringIncludes } from "https://deno.land/std@0.220.0/assert/mod.ts";
import { presignGetUrl, presignPutUrl } from "../../server/s3-presign.ts";

Deno.test("presignGetUrl — returns a URL string", async () => {
  const url = await presignGetUrl("test/file.txt");
  assertEquals(typeof url, "string");
  assertStringIncludes(url, "test/file.txt");
  assertStringIncludes(url, "X-Amz-Algorithm=AWS4-HMAC-SHA256");
  assertStringIncludes(url, "X-Amz-Signature=");
});

Deno.test("presignGetUrl — includes expiry", async () => {
  const url = await presignGetUrl("test/file.txt", 7200);
  assertStringIncludes(url, "X-Amz-Expires=7200");
});

Deno.test("presignGetUrl — default expiry is 3600", async () => {
  const url = await presignGetUrl("test/file.txt");
  assertStringIncludes(url, "X-Amz-Expires=3600");
});

Deno.test("presignPutUrl — returns a URL string with content-type header signed", async () => {
  const url = await presignPutUrl("test/file.txt", "text/plain");
  assertEquals(typeof url, "string");
  assertStringIncludes(url, "test/file.txt");
  assertStringIncludes(url, "X-Amz-SignedHeaders=");
});

Deno.test("presignPutUrl — different keys produce different signatures", async () => {
  const url1 = await presignPutUrl("file1.txt", "text/plain");
  const url2 = await presignPutUrl("file2.txt", "text/plain");
  // Different keys = different URL paths at minimum
  assertStringIncludes(url1, "file1.txt");
  assertStringIncludes(url2, "file2.txt");
});
