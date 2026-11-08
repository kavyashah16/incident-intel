import type pg from "pg";
import {
  computeFingerprint,
  LEVEL_TO_SMALLINT,
  type Envelope,
} from "@ii/shared";

export interface WriteResult {
  received: number;
  inserted: number;
  duplicates: number;
  issuesCreated: number;
  /** Wall time of the transaction, including BEGIN and COMMIT. */
  dbMs: number;
  /** received_at -> inserted_at, one value per newly inserted row. */
  freshnessSeconds: number[];
}

interface Group {
  projectId: string;
  fingerprint: string;
  version: number;
  title: string;
  level: number;
  firstMs: number;
  lastMs: number;
}

const BUCKET_MS = 5 * 60 * 1000;

/**
 * Write one batch of envelopes in a single transaction:
 *
 *   1. insert any issues we have not seen (ON CONFLICT DO NOTHING) and resolve all issue ids
 *   2. insert events (ON CONFLICT DO NOTHING ... RETURNING): the primary key makes redelivery
 *      and client retries harmless, and RETURNING tells us which rows are genuinely new
 *   3. update issue counters and 5-minute stats from the NEWLY INSERTED rows only,
 *      aggregated per issue, so a redelivered batch never inflates counts and a viral error
 *      costs one row update per batch instead of one per event
 *
 * The caller commits Kafka offsets only after this resolves.
 */
export async function writeBatch(
  pool: pg.Pool,
  envelopes: Envelope[],
): Promise<WriteResult> {
  const started = performance.now();
  if (envelopes.length === 0) {
    return {
      received: 0,
      inserted: 0,
      duplicates: 0,
      issuesCreated: 0,
      dbMs: 0,
      freshnessSeconds: [],
    };
  }

  // ---- group by fingerprint (pure, no I/O)
  const fps = envelopes.map((e) => ({
    env: e,
    fp: computeFingerprint(e.payload),
  }));
  const groups = new Map<string, Group>();
  for (const { env, fp } of fps) {
    const key = `${env.project_id}|${fp.fingerprint}`;
    const t = Date.parse(env.occurred_at);
    const g = groups.get(key);
    if (g) {
      g.firstMs = Math.min(g.firstMs, t);
      g.lastMs = Math.max(g.lastMs, t);
    } else {
      groups.set(key, {
        projectId: env.project_id,
        fingerprint: fp.fingerprint,
        version: fp.version,
        title: fp.title,
        level: LEVEL_TO_SMALLINT[env.payload.level],
        firstMs: t,
        lastMs: t,
      });
    }
  }
  // Deterministic order keeps lock acquisition order consistent across concurrent consumers.
  const sortedGroups = [...groups.values()].sort((a, b) =>
    a.projectId === b.projectId
      ? a.fingerprint < b.fingerprint
        ? -1
        : 1
      : a.projectId < b.projectId
        ? -1
        : 1,
  );

  const client = await pool.connect();
  let broken = false;
  try {
    await client.query("BEGIN");

    // ---- 1. issues
    const ins = await client.query(
      `INSERT INTO issues (project_id, fingerprint, fingerprint_version, title, level, first_seen, last_seen)
       SELECT * FROM unnest($1::uuid[], $2::text[], $3::smallint[], $4::text[], $5::smallint[], $6::timestamptz[], $7::timestamptz[])
       ON CONFLICT (project_id, fingerprint) DO NOTHING`,
      [
        sortedGroups.map((g) => g.projectId),
        sortedGroups.map((g) => g.fingerprint),
        sortedGroups.map((g) => g.version),
        sortedGroups.map((g) => g.title),
        sortedGroups.map((g) => g.level),
        sortedGroups.map((g) => new Date(g.firstMs).toISOString()),
        sortedGroups.map((g) => new Date(g.lastMs).toISOString()),
      ],
    );
    const issuesCreated = ins.rowCount ?? 0;

    const idRows = await client.query<{
      id: string;
      project_id: string;
      fingerprint: string;
    }>(
      `SELECT i.id, i.project_id, i.fingerprint
         FROM issues i
         JOIN unnest($1::uuid[], $2::text[]) AS k(project_id, fingerprint)
           ON i.project_id = k.project_id AND i.fingerprint = k.fingerprint`,
      [
        sortedGroups.map((g) => g.projectId),
        sortedGroups.map((g) => g.fingerprint),
      ],
    );
    const issueId = new Map(
      idRows.rows.map((r) => [`${r.project_id}|${r.fingerprint}`, r.id]),
    );

    // ---- 2. events
    const rows = fps.map(({ env, fp }) => ({
      env,
      title: fp.title,
      issue: issueId.get(`${env.project_id}|${fp.fingerprint}`)!,
    }));
    const inserted = await client.query<{
      issue_id: string;
      occurred_at: Date;
      received_at: Date;
      inserted_at: Date;
    }>(
      `INSERT INTO events (project_id, occurred_at, event_id, received_at, issue_id, level, environment, release, title, payload)
       SELECT project_id, occurred_at, event_id, received_at, issue_id, level, environment, release, title, payload::jsonb
         FROM unnest($1::uuid[], $2::timestamptz[], $3::uuid[], $4::timestamptz[], $5::bigint[], $6::smallint[],
                     $7::text[], $8::text[], $9::text[], $10::text[])
              AS t(project_id, occurred_at, event_id, received_at, issue_id, level, environment, release, title, payload)
       ON CONFLICT DO NOTHING
       RETURNING issue_id, occurred_at, received_at, inserted_at`,
      [
        rows.map((r) => r.env.project_id),
        rows.map((r) => r.env.occurred_at),
        rows.map((r) => r.env.event_id),
        rows.map((r) => r.env.received_at),
        rows.map((r) => r.issue),
        rows.map((r) => LEVEL_TO_SMALLINT[r.env.payload.level]),
        rows.map((r) => r.env.payload.environment ?? null),
        rows.map((r) => r.env.payload.release ?? null),
        rows.map((r) => r.title),
        rows.map((r) => JSON.stringify(r.env.payload)),
      ],
    );

    // ---- 3. counters, from newly inserted rows only
    const perIssue = new Map<
      string,
      { n: number; firstMs: number; lastMs: number; projectId: string }
    >();
    const perBucket = new Map<
      string,
      { issue: string; bucketMs: number; n: number }
    >();
    const issueProject = new Map(rows.map((r) => [r.issue, r.env.project_id]));
    const freshnessSeconds: number[] = [];
    for (const r of inserted.rows) {
      const t = r.occurred_at.getTime();
      const p = perIssue.get(r.issue_id);
      if (p) {
        p.n++;
        p.firstMs = Math.min(p.firstMs, t);
        p.lastMs = Math.max(p.lastMs, t);
      } else {
        perIssue.set(r.issue_id, {
          n: 1,
          firstMs: t,
          lastMs: t,
          projectId: issueProject.get(r.issue_id)!,
        });
      }
      const bucketMs = Math.floor(t / BUCKET_MS) * BUCKET_MS;
      const bk = `${r.issue_id}|${bucketMs}`;
      const b = perBucket.get(bk);
      if (b) b.n++;
      else perBucket.set(bk, { issue: r.issue_id, bucketMs, n: 1 });
      freshnessSeconds.push(
        Math.max(0, (r.inserted_at.getTime() - r.received_at.getTime()) / 1000),
      );
    }

    if (perIssue.size > 0) {
      const ids = [...perIssue.keys()].sort((a, b) =>
        BigInt(a) < BigInt(b) ? -1 : 1,
      );
      await client.query(
        `UPDATE issues i
            SET event_count = i.event_count + u.n,
                first_seen  = LEAST(i.first_seen, u.first_ts),
                last_seen   = GREATEST(i.last_seen, u.last_ts)
           FROM unnest($1::bigint[], $2::int[], $3::timestamptz[], $4::timestamptz[]) AS u(id, n, first_ts, last_ts)
          WHERE i.id = u.id`,
        [
          ids,
          ids.map((id) => perIssue.get(id)!.n),
          ids.map((id) => new Date(perIssue.get(id)!.firstMs).toISOString()),
          ids.map((id) => new Date(perIssue.get(id)!.lastMs).toISOString()),
        ],
      );

      const buckets = [...perBucket.values()].sort((a, b) =>
        a.issue === b.issue
          ? a.bucketMs - b.bucketMs
          : BigInt(a.issue) < BigInt(b.issue)
            ? -1
            : 1,
      );
      await client.query(
        `INSERT INTO issue_stats_5m (project_id, issue_id, bucket, count)
         SELECT * FROM unnest($1::uuid[], $2::bigint[], $3::timestamptz[], $4::int[])
         ON CONFLICT (issue_id, bucket) DO UPDATE SET count = issue_stats_5m.count + EXCLUDED.count`,
        [
          buckets.map((b) => perIssue.get(b.issue)!.projectId),
          buckets.map((b) => b.issue),
          buckets.map((b) => new Date(b.bucketMs).toISOString()),
          buckets.map((b) => b.n),
        ],
      );
    }

    await client.query("COMMIT");
    const insertedCount = inserted.rowCount ?? 0;
    return {
      received: envelopes.length,
      inserted: insertedCount,
      duplicates: envelopes.length - insertedCount,
      issuesCreated,
      dbMs: performance.now() - started,
      freshnessSeconds,
    };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {
      broken = true; // connection is unusable; do not return it to the pool
    }
    throw err;
  } finally {
    client.release(broken);
  }
}
