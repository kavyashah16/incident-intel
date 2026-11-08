import type pg from "pg";
import { EnvelopeSchema, type Envelope } from "@ii/shared";
import { BatchFailedError, classifyError } from "./errors";
import type { ConsumerMetrics } from "./metrics";
import { writeBatch, type WriteResult } from "./writer";

export interface RawMessage {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
}

export type DeadReason =
  | "invalid_json"
  | "invalid_schema"
  | "permanent_db_error";

export interface DeadLetter {
  message: RawMessage;
  reason: DeadReason;
  detail: string;
}

export interface DeadLetterSink {
  send(items: DeadLetter[]): Promise<void>;
}

export interface BatchOutcome {
  received: number;
  inserted: number;
  duplicates: number;
  issuesCreated: number;
  deadLettered: number;
}

export interface ProcessorOptions {
  /** Attempts per database operation before a transient error is surfaced. */
  maxAttempts?: number;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  log?: (msg: string, fields?: Record<string, unknown>) => void;
  /** Injectable for tests that exercise retry and bisection without a database. */
  writer?: (pool: pg.Pool, envelopes: Envelope[]) => Promise<WriteResult>;
}

interface Item {
  msg: RawMessage;
  env: Envelope;
}

function decode(msg: RawMessage): { item: Item } | { dead: DeadLetter } {
  if (!msg.value)
    return {
      dead: { message: msg, reason: "invalid_json", detail: "empty value" },
    };
  let json: unknown;
  try {
    json = JSON.parse(msg.value.toString("utf8"));
  } catch (err) {
    return {
      dead: {
        message: msg,
        reason: "invalid_json",
        detail: (err as Error).message,
      },
    };
  }
  const parsed = EnvelopeSchema.safeParse(json);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    return {
      dead: {
        message: msg,
        reason: "invalid_schema",
        detail: `${issue?.path.join(".") ?? ""}: ${issue?.message ?? "invalid"}`,
      },
    };
  }
  return { item: { msg, env: parsed.data } };
}

/**
 * Turns a chunk of Kafka messages into database rows, with the failure handling from the design:
 * poison messages are dead-lettered, a bad row inside a good batch is isolated by bisection,
 * transient database errors are retried with backoff, and anything else leaves the batch
 * uncommitted (the caller must not resolve offsets when `process` throws).
 */
export class BatchProcessor {
  private readonly knownDays = new Set<string>();
  private readonly write: NonNullable<ProcessorOptions["writer"]>;

  constructor(
    private readonly pool: pg.Pool,
    private readonly dlq: DeadLetterSink,
    private readonly metrics: ConsumerMetrics,
    private readonly opts: ProcessorOptions = {},
  ) {
    this.write = opts.writer ?? writeBatch;
  }

  /** Make sure the daily partitions for these UTC days (YYYY-MM-DD) exist. Idempotent and cached. */
  async ensurePartitions(days: Iterable<string>): Promise<void> {
    for (const day of days) {
      if (this.knownDays.has(day)) continue;
      await this.withRetry(() =>
        this.pool.query("SELECT ensure_events_partition($1::date)", [day]),
      );
      this.knownDays.add(day);
    }
  }

  async process(messages: RawMessage[]): Promise<BatchOutcome> {
    this.metrics.batchSize.observe(messages.length);

    const items: Item[] = [];
    const dead: DeadLetter[] = [];
    for (const m of messages) {
      const d = decode(m);
      if ("dead" in d) dead.push(d.dead);
      else items.push(d.item);
    }

    const outcome: BatchOutcome = {
      received: messages.length,
      inserted: 0,
      duplicates: 0,
      issuesCreated: 0,
      deadLettered: 0,
    };

    if (items.length > 0) {
      try {
        await this.ensurePartitions(
          new Set(items.map((i) => i.env.occurred_at.slice(0, 10))),
        );
      } catch (err) {
        throw new BatchFailedError(
          "could not ensure partitions",
          classifyError(err),
          err,
        );
      }
      await this.writeWithBisect(items, outcome, dead);
    }

    // Offsets are resolved by the caller only after this returns, so a crash between the
    // database commit and the dead-letter send re-delivers (and re-dead-letters) the message.
    if (dead.length > 0) {
      await this.dlq.send(dead);
      for (const d of dead) this.metrics.dlq.inc({ reason: d.reason });
    }
    outcome.deadLettered = dead.length;
    return outcome;
  }

  private async writeWithBisect(
    items: Item[],
    outcome: BatchOutcome,
    dead: DeadLetter[],
  ): Promise<void> {
    try {
      const r = await this.withRetry(() =>
        this.write(
          this.pool,
          items.map((i) => i.env),
        ),
      );
      outcome.inserted += r.inserted;
      outcome.duplicates += r.duplicates;
      outcome.issuesCreated += r.issuesCreated;
      this.metrics.inserted.inc(r.inserted);
      this.metrics.duplicates.inc(r.duplicates);
      this.metrics.issuesCreated.inc(r.issuesCreated);
      this.metrics.batchDuration.observe(r.dbMs / 1000);
      for (const s of r.freshnessSeconds) this.metrics.freshness.observe(s);
      return;
    } catch (err) {
      if (err instanceof BatchFailedError) throw err;
      const cls = classifyError(err);
      if (cls !== "permanent") {
        throw new BatchFailedError(
          `batch write failed (${cls}): ${(err as Error).message}`,
          cls,
          err,
        );
      }
      if (items.length === 1) {
        dead.push({
          message: items[0]!.msg,
          reason: "permanent_db_error",
          detail: (err as Error).message.slice(0, 500),
        });
        this.log("message isolated and dead-lettered", {
          offset: items[0]!.msg.offset,
          error: (err as Error).message,
        });
        return;
      }
      // Something in this batch can never be written. Split and recurse to find it; the good
      // halves still commit (writes are idempotent, so a later redelivery is harmless).
    }
    const mid = items.length >> 1;
    await this.writeWithBisect(items.slice(0, mid), outcome, dead);
    await this.writeWithBisect(items.slice(mid), outcome, dead);
  }

  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    const max = this.opts.maxAttempts ?? 5;
    const base = this.opts.backoffBaseMs ?? 100;
    const cap = this.opts.backoffMaxMs ?? 5_000;
    const sleep =
      this.opts.sleep ??
      ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
    const random = this.opts.random ?? Math.random;
    for (let attempt = 1; ; attempt++) {
      try {
        return await fn();
      } catch (err) {
        if (classifyError(err) !== "transient" || attempt >= max) throw err;
        this.metrics.retries.inc();
        const delay =
          Math.min(cap, base * 2 ** (attempt - 1)) * (0.8 + 0.4 * random()); // +/-20% jitter
        this.log("transient database error, retrying", {
          attempt,
          delayMs: Math.round(delay),
          error: (err as Error).message,
        });
        await sleep(delay);
      }
    }
  }

  private log(msg: string, fields?: Record<string, unknown>): void {
    (
      this.opts.log ??
      ((m, f) => console.warn(JSON.stringify({ level: "warn", msg: m, ...f })))
    )(msg, fields);
  }
}
