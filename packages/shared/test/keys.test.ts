import { describe, expect, it } from "vitest";
import { generateApiKey, hashSecret, parseApiKey, verifySecret } from "../src";

describe("api keys", () => {
  it("round-trips generate, parse and verify", () => {
    const g = generateApiKey();
    const parsed = parseApiKey(g.key);
    expect(parsed?.prefix).toBe(g.prefix);
    expect(verifySecret(parsed!.secret, g.hash)).toBe(true);
  });

  it("rejects wrong secrets and malformed keys", () => {
    const g = generateApiKey();
    expect(verifySecret("0".repeat(32), g.hash)).toBe(false);
    expect(verifySecret("x", hashSecret("y"))).toBe(false);
    for (const bad of [
      undefined,
      "",
      "ms_short_x",
      "xx_abcdef12_" + "0".repeat(32),
      g.key + "0",
    ]) {
      expect(parseApiKey(bad)).toBeNull();
    }
  });
});
