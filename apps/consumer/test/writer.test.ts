import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { uuidv7 } from "@ii/shared";
import { classifyError } from "../src/errors";
import { writeBatch } from "../src/writer";
import { createTestDb, DAY, makeEnvelope, type TestDb } from "./helpers";

describe("writeBatch (real Postgres)", () => {
  let db: TestDb;

  beforeAll(async () => {
    db = await createTestDb();
    await db.pool.query("SELECT ensure_events_partition($1::date)", [DAY]);
  });
  afterAll(() => db.close());
  beforeEach(async () => {
    await db.pool.query(
      "TRUNCATE events, issue_stats_5m, issues RESTART IDENTITY",
    );
  });

  const count = async (table: string): Promise<number> =>
    Number(
      (await db.pool.query(`SELECT count(*) AS n FROM ${table}`)).rows[0].n,
    );

  it("groups repeated errors into one issue and counts them per 5-minute bucket", async () => {
    const p = db.projectId;
    const result = await writeBatch(db.pool, [
      makeEnvelope(p, { occurredAt: `${DAY}T10:00:01.000Z` }),
      makeEnvelope(p, { occurredAt: `${DAY}T10:00:02.000Z` }),
      makeEnvelope(p, { occurredAt: `${DAY}T10:07:00.000Z` }),
    ]);
    expect(result).toMatchObject({
      received: 3,
      inserted: 3,
      duplicates: 0,
      issuesCreated: 1,
    });

    const issues = (await db.pool.query("SELECT * FROM issues")).rows;
    expect(issues).toHaveLength(1);
    expect(Number(issues[0].event_count)).toBe(3);
    expect(issues[0].first_seen.toISOString()).toBe(`${DAY}T10:00:01.000Z`);
    expect(issues[0].last_seen.toISOString()).toBe(`${DAY}T10:07:00.000Z`);

    const stats = (
      await db.pool.query(
        "SELECT bucket, count FROM issue_stats_5m ORDER BY bucket",
      )
    ).rows;
    expect(stats.map((s) => [s.bucket.toISOString(), s.count])).toEqual([
      [`${DAY}T10:00:00.000Z`, 2],
      [`${DAY}T10:05:00.000Z`, 1],
    ]);
  });

  it("is idempotent: writing the same batch again changes nothing", async () => {
    const batch = [
      makeEnvelope(db.projectId),
      makeEnvelope(db.projectId),
      makeEnvelope(db.projectId),
    ];
    await writeBatch(db.pool, batch);
    const again = await writeBatch(db.pool, batch);
    expect(again).toMatchObject({
      inserted: 0,
      duplicates: 3,
      issuesCreated: 0,
    });
    expect(await count("events")).toBe(3);
    expect(
      Number(
        (await db.pool.query("SELECT event_count FROM issues")).rows[0]
          .event_count,
      ),
    ).toBe(3);
    expect(
      Number(
        (await db.pool.query("SELECT sum(count) AS n FROM issue_stats_5m"))
          .rows[0].n,
      ),
    ).toBe(3);
  });

  it("skips a duplicate event id that appears twice inside one batch", async () => {
    const e = makeEnvelope(db.projectId);
    const r = await writeBatch(db.pool, [e, e]);
    expect(r).toMatchObject({ inserted: 1, duplicates: 1 });
    expect(
      Number(
        (await db.pool.query("SELECT event_count FROM issues")).rows[0]
          .event_count,
      ),
    ).toBe(1);
  });

  it("creates separate issues for different errors and different projects", async () => {
    const other = await db.addProject();
    const r = await writeBatch(db.pool, [
      makeEnvelope(db.projectId, { message: "boom" }),
      makeEnvelope(db.projectId, { message: "bang" }),
      makeEnvelope(other, { message: "boom" }),
    ]);
    expect(r.issuesCreated).toBe(3);
    expect(await count("issues")).toBe(3);
  });

  it("widens first_seen and last_seen when later batches arrive out of order", async () => {
    const p = db.projectId;
    await writeBatch(db.pool, [
      makeEnvelope(p, { occurredAt: `${DAY}T12:00:00.000Z` }),
    ]);
    await writeBatch(db.pool, [
      makeEnvelope(p, { occurredAt: `${DAY}T09:00:00.000Z` }),
    ]);
    const issue = (await db.pool.query("SELECT * FROM issues")).rows[0];
    expect(issue.first_seen.toISOString()).toBe(`${DAY}T09:00:00.000Z`);
    expect(issue.last_seen.toISOString()).toBe(`${DAY}T12:00:00.000Z`);
    expect(Number(issue.event_count)).toBe(2);
  });

  it("fails with a foreign-key error for an unknown project, which classifies as permanent", async () => {
    const err = await writeBatch(db.pool, [makeEnvelope(uuidv7())]).catch(
      (e) => e,
    );
    expect(err.code).toBe("23503");
    expect(classifyError(err)).toBe("permanent");
  });

  it("rolls back the whole batch when any row fails", async () => {
    await expect(
      writeBatch(db.pool, [
        makeEnvelope(db.projectId, { message: "good" }),
        makeEnvelope(uuidv7(), { message: "bad" }),
      ]),
    ).rejects.toThrow();
    expect(await count("events")).toBe(0);
    expect(await count("issues")).toBe(0);
  });

  it("handles an empty batch", async () => {
    expect(await writeBatch(db.pool, [])).toMatchObject({
      received: 0,
      inserted: 0,
    });
  });
});
