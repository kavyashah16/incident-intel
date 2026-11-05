import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

/** API key format: `ms_<8 hex prefix>_<32 hex secret>`. The prefix is the lookup handle. */
const KEY_RE = /^ms_([0-9a-f]{8})_([0-9a-f]{32})$/;

export interface GeneratedKey {
  /** Full key; shown once to the user, never stored. */
  key: string;
  prefix: string;
  hash: Buffer;
}

export function hashSecret(secret: string): Buffer {
  // SHA-256 is sufficient for 128-bit random secrets; a slow KDF would only burn CPU per request.
  return createHash("sha256").update(secret).digest();
}

export function generateApiKey(): GeneratedKey {
  const prefix = randomBytes(4).toString("hex");
  const secret = randomBytes(16).toString("hex");
  return { key: `ms_${prefix}_${secret}`, prefix, hash: hashSecret(secret) };
}

export function parseApiKey(raw: string | undefined): { prefix: string; secret: string } | null {
  if (!raw) return null;
  const m = KEY_RE.exec(raw.trim());
  return m ? { prefix: m[1]!, secret: m[2]! } : null;
}

export function verifySecret(secret: string, expectedHash: Buffer): boolean {
  const actual = hashSecret(secret);
  return actual.length === expectedHash.length && timingSafeEqual(actual, expectedHash);
}