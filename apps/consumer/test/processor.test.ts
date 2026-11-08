import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Registry } from "prom-client";
import { uuidv7, type Envelope } from "@ii/shared";
import { BatchFailedError } from "../src/errors";
import { createMetrics } from "../src/metrics";
import {
  BatchProcessor,
  type DeadLetter,
  type DeadLetterSink,
  type ProcessorOptions,
  type RawMessage,
} from "../src/processor";
import { writeBatch } from "../src/writer";
import { createTestDb, makeEnvelope, type TestDb } from "./helpers";

class CollectingDlq implements DeadLetterSink {
  items: DeadLetter[] = [];
  async send(items: DeadLetter[]): Promise<void> {
    this.items.push(...items);
  }
}

let offset = 0;
const msg = (value: string | Buffer | null, key = "k"): RawMessage => ({
  topic: "events.raw",
  partition: 0,
  offset: String(offset++),
  key: Buffer.from(key),
  value: value === null ? null : Buffer.from(value),
});
const msgOf = (env: Envelope): RawMessage =>
  msg(JSON.stringify(env), env.event_id);

describe("BatchProcessor (real Postgres)", () => {
  let db: TestDb;
  let dlq: CollectingDlq;
  let sleeps: number[];

  beforeAll(async () => {
    db = await createTestDb();
  });
  afterAll(() => db.close());
  beforeEach(async () => {
    await db.pool.query(
      "TRUNCATE events, issue_stats_5m, issues RESTART IDENTITY",
    );
    dlq = new CollectingDlq();
    sleeps = [];
  });

  const make = (opts: ProcessorOptions = {}) =>
    new BatchProcessor(
      db.pool,
      dlq,
      createMetrics(new Registry(), { defaultMetrics: false }),
      {
        sleep: async (ms) => void sleeps.push(ms),
        log: () => {},
        ...opts,
      },
    );
  const eventCount = async (): Promise<number> =>
    Number((await db.pool.query("SELECT count(*) AS n FROM events")).rows[0].n);
  const fakeDbError = (code: string) =>
    Object.assign(new Error(`fake ${code}`), { code });

  it("writes valid messages and creates the day partition on demand", async () => {
    const p = make();
    const outcome = await p.process([
      msgOf(makeEnvelope(db.projectId)),
      msgOf(makeEnvelope(db.projectId, { message: "other" })),
    ]);
    expect(outcome).toMatchObject({
      received: 2,
      inserted: 2,
      duplicates: 0,
      issuesCreated: 2,
      deadLettered: 0,
    });
    expect(await eventCount()).toBe(2);
    expect(
      (await db.pool.query("SELECT to_regclass('events_20261101') AS t"))
        .rows[0].t,
    ).not.toBeNull();
  });

  it("dead-letters unparseable and wrongly shaped messages and still writes the good ones", async () => {
    const p = make();
    const outcome = await p.process([
      msg("{not json"),
      msg(null),
      msg(JSON.stringify({ schema_version: 1, hello: "world" })),
      msgOf(makeEnvelope(db.projectId)),
    ]);
    expect(outcome).toMatchObject({ inserted: 1, deadLettered: 3 });
    expect(dlq.items.map((d) => d.reason)).toEqual([
      "invalid_json",
      "invalid_json",
      "invalid_schema",
    ]);
    expect(await eventCount()).toBe(1);
  });

  it("isolates one permanently bad event inside a batch by bisection", async () => {
    const p = make();
    const good = Array.from({ length: 7 }, (_, i) =>
      makeEnvelope(db.projectId, { message: `err ${i}x` }),
    );
    const bad = makeEnvelope(uuidv7(), { message: "unknown project" }); // violates the issues foreign key
    const messages = [
      ...good.slice(0, 3).map(msgOf),
      msgOf(bad),
      ...good.slice(3).map(msgOf),
    ];
    const badOffset = messages[3]!.offset;

    const outcome = await p.process(messages);
    expect(outcome).toMatchObject({
      received: 8,
      inserted: 7,
      deadLettered: 1,
    });
    expect(dlq.items).toHaveLength(1);
    expect(dlq.items[0]).toMatchObject({ reason: "permanent_db_error" });
    expect(dlq.items[0]!.message.offset).toBe(badOffset);
    expect(await eventCount()).toBe(7);
  });

  it("treats a redelivered batch as duplicates", async () => {
    const p = make();
    const messages = [
      msgOf(makeEnvelope(db.projectId)),
      msgOf(makeEnvelope(db.projectId)),
    ];
    await p.process(messages);
    const again = await p.process(messages);
    expect(again).toMatchObject({
      inserted: 0,
      duplicates: 2,
      deadLettered: 0,
    });
    expect(await eventCount()).toBe(2);
  });

  it("retries transient database errors with backoff and then succeeds", async () => {
    let calls = 0;
    const p = make({
      writer: async (pool, envs) => {
        if (++calls <= 2) throw fakeDbError("40P01"); // deadlock detected
        return writeBatch(pool, envs);
      },
    });
    const outcome = await p.process([msgOf(makeEnvelope(db.projectId))]);
    expect(outcome.inserted).toBe(1);
    expect(calls).toBe(3);
    expect(sleeps).toHaveLength(2);
    expect(sleeps[1]!).toBeGreaterThan(sleeps[0]!); // exponential growth beats the +/-20% jitter
  });

  it("gives up after the attempt limit without dead-lettering, leaving the batch uncommitted", async () => {
    let calls = 0;
    const p = make({
      maxAttempts: 3,
      writer: async () => {
        calls++;
        throw fakeDbError("08006"); // connection failure
      },
    });
    const err = await p
      .process([msgOf(makeEnvelope(db.projectId))])
      .catch((e) => e);
    expect(err).toBeInstanceOf(BatchFailedError);
    expect(err.errorClass).toBe("transient");
    expect(calls).toBe(3);
    expect(dlq.items).toHaveLength(0);
  });

  it("treats an unrecognised error as transient and never dead-letters it", async () => {
    const p = make({
      maxAttempts: 2,
      writer: async () => {
        throw new Error("something odd");
      },
    });
    const err = await p
      .process([msgOf(makeEnvelope(db.projectId))])
      .catch((e) => e);
    expect(err).toBeInstanceOf(BatchFailedError);
    expect(err.errorClass).toBe("transient");
    expect(dlq.items).toHaveLength(0);
  });

  it("stops on a fatal error (a bug) instead of dead-lettering good events", async () => {
    let calls = 0;
    const p = make({
      writer: async () => {
        calls++;
        throw fakeDbError("42P01"); // undefined table
      },
    });
    const err = await p
      .process([msgOf(makeEnvelope(db.projectId))])
      .catch((e) => e);
    expect(err).toBeInstanceOf(BatchFailedError);
    expect(err.errorClass).toBe("fatal");
    expect(calls).toBe(1); // no retries for bugs
    expect(dlq.items).toHaveLength(0);
  });
});
