import type pg from "pg";

export interface KeyRecord {
  keyId: string;
  projectId: string;
  keyHash: Buffer;
  revoked: boolean;
}

export interface KeyStore {
  /** Resolve an API key prefix to its record, or null if unknown. Throws if the backend is down. */
  lookup(prefix: string): Promise<KeyRecord | null>;
}

export class PgKeyStore implements KeyStore {
  constructor(private readonly pool: pg.Pool) {}

  async lookup(prefix: string): Promise<KeyRecord | null> {
    const r = await this.pool.query<{
      id: string;
      project_id: string;
      key_hash: Buffer;
      revoked_at: Date | null;
    }>(
      "SELECT id, project_id, key_hash, revoked_at FROM api_keys WHERE prefix = $1",
      [prefix],
    );
    const row = r.rows[0];
    if (!row) return null;
    return {
      keyId: row.id,
      projectId: row.project_id,
      keyHash: row.key_hash,
      revoked: row.revoked_at !== null,
    };
  }
}

interface Entry {
  value: KeyRecord | null;
  expiresAt: number;
}

/**
 * In-process LRU with TTL in front of another store. Unknown keys are cached too (negative
 * caching), and concurrent lookups of the same prefix share one backend call, so a flood of
 * bad keys costs one database query per TTL, not one per request.
 * Revocation therefore takes effect within `ttlMs`.
 */
export class CachedKeyStore implements KeyStore {
  private readonly cache = new Map<string, Entry>(); // Map preserves insertion order = LRU order
  private readonly inflight = new Map<string, Promise<KeyRecord | null>>();

  constructor(
    private readonly inner: KeyStore,
    private readonly opts: {
      maxEntries: number;
      ttlMs: number;
      negativeTtlMs: number;
      now?: () => number;
    },
  ) {}

  private now(): number {
    return (this.opts.now ?? Date.now)();
  }

  async lookup(prefix: string): Promise<KeyRecord | null> {
    const hit = this.cache.get(prefix);
    if (hit && hit.expiresAt > this.now()) {
      this.cache.delete(prefix); // refresh recency
      this.cache.set(prefix, hit);
      return hit.value;
    }

    const pending = this.inflight.get(prefix);
    if (pending) return pending;

    const p = this.inner
      .lookup(prefix)
      .then((value) => {
        this.cache.delete(prefix);
        this.cache.set(prefix, {
          value,
          expiresAt:
            this.now() + (value ? this.opts.ttlMs : this.opts.negativeTtlMs),
        });
        while (this.cache.size > this.opts.maxEntries) {
          const oldest = this.cache.keys().next().value as string;
          this.cache.delete(oldest);
        }
        return value;
      })
      .finally(() => this.inflight.delete(prefix));
    this.inflight.set(prefix, p);
    return p;
  }

  get size(): number {
    return this.cache.size;
  }
}
