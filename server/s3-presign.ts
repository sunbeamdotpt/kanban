/**
 * Pre-signed URL generation for S3 (AWS Signature V4 query-string auth).
 */

import {
  ACCESS_KEY,
  BUCKET,
  getSigningKey,
  hmacSha256,
  REGION,
  SECRET_KEY,
  SEAWEEDFS_S3_URL,
  sha256Hex,
  toHex,
} from "./s3.ts";

const encoder = new TextEncoder();

export async function presignUrl(
  method: string,
  key: string,
  expiresIn: number,
  extraSignedHeaders?: Record<string, string>,
): Promise<string> {
  const url = new URL(`/${BUCKET}/${key}`, SEAWEEDFS_S3_URL);
  const now = new Date();
  const dateStamp =
    now.toISOString().replace(/[-:]/g, "").split(".")[0] + "Z";
  const shortDate = dateStamp.slice(0, 8);
  const scope = `${shortDate}/${REGION}/s3/aws4_request`;

  url.searchParams.set("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
  url.searchParams.set("X-Amz-Credential", `${ACCESS_KEY}/${scope}`);
  url.searchParams.set("X-Amz-Date", dateStamp);
  url.searchParams.set("X-Amz-Expires", String(expiresIn));

  const headers: Record<string, string> = {
    host: url.host,
    ...extraSignedHeaders,
  };
  const signedHeaderKeys = Object.keys(headers)
    .map((k) => k.toLowerCase())
    .sort();
  const signedHeadersStr = signedHeaderKeys.join(";");
  url.searchParams.set("X-Amz-SignedHeaders", signedHeadersStr);

  const sortedParams = [...url.searchParams.entries()].sort((a, b) =>
    a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0,
  );
  const canonicalQs = sortedParams
    .map(
      ([k, v]) =>
        `${encodeURIComponent(k)}=${encodeURIComponent(v)}`,
    )
    .join("&");

  const canonicalHeaders =
    signedHeaderKeys
      .map((k) => {
        const originalKey = Object.keys(headers).find(
          (h) => h.toLowerCase() === k,
        )!;
        return `${k}:${headers[originalKey]}`;
      })
      .join("\n") + "\n";

  const canonicalRequest = [
    method,
    url.pathname,
    canonicalQs,
    canonicalHeaders,
    signedHeadersStr,
    "UNSIGNED-PAYLOAD",
  ].join("\n");

  const stringToSign = [
    "AWS4-HMAC-SHA256",
    dateStamp,
    scope,
    await sha256Hex(encoder.encode(canonicalRequest)),
  ].join("\n");

  const signingKey = await getSigningKey(SECRET_KEY, shortDate, REGION);
  const signature = toHex(await hmacSha256(signingKey, stringToSign));

  url.searchParams.set("X-Amz-Signature", signature);

  return url.toString();
}

const DEFAULT_EXPIRES = 3600;

export function presignGetUrl(
  key: string,
  expiresIn = DEFAULT_EXPIRES,
): Promise<string> {
  return presignUrl("GET", key, expiresIn);
}

export function presignPutUrl(
  key: string,
  contentType: string,
  expiresIn = DEFAULT_EXPIRES,
): Promise<string> {
  return presignUrl("PUT", key, expiresIn, {
    "content-type": contentType,
  });
}
