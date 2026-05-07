/**
 * PKCE (Proof Key for Code Exchange) utilities for the OIDC authorization code flow.
 * All functions use the Web Crypto API (globalThis.crypto.subtle) — no external deps.
 */

/** Base64url-encode a Uint8Array without padding. */
function base64url(bytes: Uint8Array): string {
  let str = "";
  for (const byte of bytes) {
    str += String.fromCharCode(byte);
  }
  return btoa(str).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
}

/**
 * Generate a cryptographically random code_verifier as per RFC 7636 §4.1.
 * 64 random bytes base64url-encoded → 86-char string (well within the 43–128 range).
 */
export function generateCodeVerifier(): string {
  const bytes = new Uint8Array(64);
  globalThis.crypto.getRandomValues(bytes);
  return base64url(bytes);
}

/**
 * Derive a code_challenge from a code_verifier using SHA-256 as per RFC 7636 §4.2.
 * challenge = BASE64URL(SHA-256(ASCII(verifier)))
 */
export async function generateCodeChallenge(verifier: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(verifier);
  const hash = await globalThis.crypto.subtle.digest("SHA-256", data);
  return base64url(new Uint8Array(hash));
}

/**
 * Generate a cryptographically random OAuth state parameter.
 * 32 random bytes base64url-encoded → 43-char string.
 */
export function randomState(): string {
  const bytes = new Uint8Array(32);
  globalThis.crypto.getRandomValues(bytes);
  return base64url(bytes);
}
