import { describe, expect, it } from "vitest";
import { CachedKeyStore, type KeyRecord, type KeyStore } from "../src/keystore";

const rec = (id: string): KeyRecord => ({
  keyId: id,
  projectId: "p",
  keyHash: Buffer.from(id),
  revoked: false,
});

class CountingStore implements KeyStore {
  calls = 0;
  delayMs = 0;
  data = new Map<string, KeyRecord>();
  fail = false;
  async lookup(prefix: string): Promise<KeyRecord | null> {
    this.calls++;
    if (this.delayMs) await new Promise((r) => setTimeout(r, this.delayMs));
    if (this.fail) throw new Error("down");
    return this.data.get(prefix) ?? null;
  }
}

describe("CachedKeyStore", () => {
  const opts = (now: () => number) => ({
    maxEntries: 3,
    ttlMs: 1000,
    negativeTtlMs: 500,
    now,
  });

  it("serves repeat lookups from cache until the TTL expires", async () => {
    let t = 0;
    const inner = new CountingStore();
    inner.data.set("a", rec("a"));
    const c = new CachedKeyStore(
      inner,
      opts(() => t),
    );

    expect((await c.lookup("a"))?.keyId).toBe("a");
    expect((await c.lookup("a"))?.keyId).toBe("a");
    expect(inner.calls).toBe(1);

    t = 1001;
    await c.lookup("a");
    expect(inner.calls).toBe(2);
  });

  it("caches unknown keys for the (shorter) negative TTL", async () => {
    let t = 0;
    const inner = new CountingStore();
    const c = new CachedKeyStore(
      inner,
      opts(() => t),
    );

    expect(await c.lookup("ghost")).toBeNull();
    expect(await c.lookup("ghost")).toBeNull();
    expect(inner.calls).toBe(1);

    t = 501;
    inner.data.set("ghost", rec("ghost")); // key created after the miss was cached
    expect((await c.lookup("ghost"))?.keyId).toBe("ghost");
    expect(inner.calls).toBe(2);
  });

  it("coalesces concurrent lookups of the same prefix into one backend call", async () => {
    const inner = new CountingStore();
    inner.delayMs = 20;
    const c = new CachedKeyStore(inner, opts(Date.now));
    const results = await Promise.all(
      Array.from({ length: 50 }, () => c.lookup("flood")),
    );
    expect(results.every((r) => r === null)).toBe(true);
    expect(inner.calls).toBe(1);
  });

  it("evicts the least recently used entry beyond maxEntries", async () => {
    const inner = new CountingStore();
    for (const k of ["a", "b", "c", "d"]) inner.data.set(k, rec(k));
    const c = new CachedKeyStore(
      inner,
      opts(() => 0),
    );

    await c.lookup("a");
    await c.lookup("b");
    await c.lookup("c");
    await c.lookup("a"); // refresh a; b is now least recent
    await c.lookup("d"); // evicts b
    expect(c.size).toBe(3);

    inner.calls = 0;
    await c.lookup("a");
    await c.lookup("c");
    await c.lookup("d");
    expect(inner.calls).toBe(0);
    await c.lookup("b");
    expect(inner.calls).toBe(1);
  });

  it("does not cache backend failures", async () => {
    const inner = new CountingStore();
    inner.data.set("a", rec("a"));
    const c = new CachedKeyStore(
      inner,
      opts(() => 0),
    );

    inner.fail = true;
    await expect(c.lookup("a")).rejects.toThrow("down");
    inner.fail = false;
    expect((await c.lookup("a"))?.keyId).toBe("a");
    expect(inner.calls).toBe(2);
  });
});
