import { describe, expect, it } from "vitest";
import { normalizeFilename, normalizeMessage } from "../src";

describe("normalizeMessage", () => {
  it("collapses numeric ids", () => {
    expect(normalizeMessage("User 4812 not found")).toBe(
      "User <num> not found",
    );
    expect(normalizeMessage("User 4812 not found")).toBe(
      normalizeMessage("User 9930 not found"),
    );
  });

  it("replaces uuids, emails, ips, urls and timestamps", () => {
    expect(
      normalizeMessage("order 3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90 failed"),
    ).toBe("order <uuid> failed");
    expect(normalizeMessage("cannot email bob.smith+x@example.co.uk")).toBe(
      "cannot email <email>",
    );
    expect(normalizeMessage("connect ECONNREFUSED 10.0.3.17:5432")).toBe(
      "connect ECONNREFUSED <ip>",
    );
    expect(
      normalizeMessage("GET https://api.example.com/v1/x?y=1 returned 502"),
    ).toBe("GET <url> returned <num>");
    expect(normalizeMessage("at 2026-11-01T14:03:00.123Z job died")).toBe(
      "at <ts> job died",
    );
  });

  it("replaces unix paths and long hex tokens", () => {
    expect(
      normalizeMessage("ENOENT: no such file '/var/app/data/u123/file.txt'"),
    ).toBe("ENOENT: no such file '<path>'");
    expect(
      normalizeMessage("bad object 5f4dcc3b5aa765d61d8327deb882cf99"),
    ).toBe("bad object <hex>");
  });

  it("keeps ordinary words, even ones that look like hex", () => {
    expect(normalizeMessage("decaffeinated beverage rejected")).toBe(
      "decaffeinated beverage rejected",
    );
    expect(
      normalizeMessage("Cannot read properties of undefined (reading 'name')"),
    ).toBe("Cannot read properties of undefined (reading 'name')");
  });

  it("collapses whitespace and bounds input length", () => {
    expect(normalizeMessage("a   b\n\tc")).toBe("a b c");
    expect(normalizeMessage("x".repeat(100_000)).length).toBeLessThanOrEqual(
      2048,
    );
  });
});

describe("normalizeFilename", () => {
  it("keeps the last two segments and drops hashes and queries", () => {
    expect(normalizeFilename("/srv/releases/42/src/handlers/user.js")).toBe(
      "handlers/user.js",
    );
    expect(normalizeFilename("/app/dist/main.4f3a2b1c9d.js?v=3")).toBe(
      "dist/main.js",
    );
    expect(normalizeFilename("C:\\build\\src\\a.ts")).toBe("src/a.ts");
  });
});
