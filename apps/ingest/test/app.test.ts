import { gzipSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Registry } from "prom-client";
import {
  EnvelopeSchema,
  generateApiKey,
  uuidv7,
  type Envelope,
} from "@ii/shared";
import { buildApp } from "../src/app";
import type { IngestConfig } from "../src/config";
import type { KeyRecord, KeyStore } from "../src/keystore";
import type { EventProducer, ProducerMessage } from "../src/producer";

const PROJECT = "0b6f6a3e-61a7-4cc6-9c43-5a13b8cbf6d1";
const NOW = new Date("2026-11-01T10:00:00.000Z");

const config: IngestConfig = {
  port: 0,
  databaseUrl: "",
  brokers: [],
  eventsTopic: "events.raw",
  nodeName: "ingest-test",
  keyCacheTtlMs: 30_000,
  keyNegativeTtlMs: 30_000,
  keyCacheMaxEntries: 100,
  maxEventAgeMs: 72 * 3600_000,
  sendTimeoutMs: 1000,
  logLevel: "silent",
};

class FakeKeys implements KeyStore {
  records = new Map<string, KeyRecord>();
  fail = false;
  async lookup(prefix: string): Promise<KeyRecord | null> {
    if (this.fail) throw new Error("db down");
    return this.records.get(prefix) ?? null;
  }
}

class FakeProducer implements EventProducer {
  sent: { topic: string; messages: ProducerMessage[] }[] = [];
  fail = false;
  async send(topic: string, messages: ProducerMessage[]): Promise<void> {
    if (this.fail) throw new Error("broker down");
    this.sent.push({ topic, messages });
  }
  async disconnect(): Promise<void> {}
  get all(): Envelope[] {
    return this.sent.flatMap((s) =>
      s.messages.map((m) => JSON.parse(m.value) as Envelope),
    );
  }
}

describe("POST /v1/events", () => {
  let keys: FakeKeys;
  let producer: FakeProducer;
  let registry: Registry;
  let app: ReturnType<typeof buildApp>;
  let apiKey: string;

  beforeEach(async () => {
    keys = new FakeKeys();
    producer = new FakeProducer();
    registry = new Registry();
    const k = generateApiKey();
    apiKey = k.key;
    keys.records.set(k.prefix, {
      keyId: "k1",
      projectId: PROJECT,
      keyHash: k.hash,
      revoked: false,
    });
    app = buildApp({
      keyStore: keys,
      producer,
      config,
      registry,
      defaultMetrics: false,
      now: () => NOW,
    });
    await app.ready();
  });
  afterEach(() => app.close());

  const post = (payload: unknown, headers: Record<string, string> = {}) =>
    app.inject({
      method: "POST",
      url: "/v1/events",
      headers: {
        "content-type": "application/json",
        "x-api-key": apiKey,
        ...headers,
      },
      payload: payload as never,
    });

  describe("authentication", () => {
    it("rejects missing, malformed, unknown, wrong-secret and revoked keys with 401", async () => {
      expect(
        (await post({ message: "x" }, { "x-api-key": "" })).statusCode,
      ).toBe(401);
      expect(
        (await post({ message: "x" }, { "x-api-key": "nope" })).statusCode,
      ).toBe(401);
      expect(
        (await post({ message: "x" }, { "x-api-key": generateApiKey().key }))
          .statusCode,
      ).toBe(401);

      const prefix = apiKey.split("_")[1]!;
      expect(
        (
          await post(
            { message: "x" },
            { "x-api-key": `ms_${prefix}_${"0".repeat(32)}` },
          )
        ).statusCode,
      ).toBe(401);

      keys.records.set(prefix, { ...keys.records.get(prefix)!, revoked: true });
      expect((await post({ message: "x" })).statusCode).toBe(401);
      expect(producer.sent).toHaveLength(0);
    });

    it("returns 503 (not 401) when the key store is unavailable", async () => {
      keys.fail = true;
      const res = await post({ message: "x" });
      expect(res.statusCode).toBe(503);
      expect(res.headers["retry-after"]).toBe("1");
    });
  });

  describe("accepting events", () => {
    it("accepts a single event and produces one valid envelope keyed by event_id", async () => {
      const res = await post({
        message: "boom",
        level: "fatal",
        environment: "prod",
      });
      expect(res.statusCode).toBe(202);
      const body = res.json() as { accepted: string[]; rejected: unknown[] };
      expect(body.accepted).toHaveLength(1);
      expect(body.rejected).toEqual([]);

      expect(producer.sent).toHaveLength(1);
      expect(producer.sent[0]!.topic).toBe("events.raw");
      const msg = producer.sent[0]!.messages[0]!;
      const env = EnvelopeSchema.parse(JSON.parse(msg.value));
      expect(msg.key).toBe(env.event_id);
      expect(msg.headers).toEqual({ ingest_node: "ingest-test" });
      expect(env.project_id).toBe(PROJECT); // taken from the key, never from the body
      expect(env.payload.level).toBe("fatal");
    });

    it("ignores a project_id supplied in the body", async () => {
      const res = await post({
        message: "x",
        project_id: "11111111-1111-4111-8111-111111111111",
      });
      expect(res.statusCode).toBe(202);
      expect(producer.all[0]!.project_id).toBe(PROJECT);
    });

    it("accepts a batch, produces only the valid events, and reports the rest", async () => {
      const good = uuidv7();
      const res = await post({
        events: [
          { message: "a", event_id: good },
          { nope: true },
          { message: "c" },
          { message: "x", level: "bad" },
        ],
      });
      expect(res.statusCode).toBe(202);
      const body = res.json() as {
        accepted: string[];
        rejected: { index: number; reason: string }[];
      };
      expect(body.accepted).toHaveLength(2);
      expect(body.accepted[0]).toBe(good);
      expect(body.rejected.map((r) => [r.index, r.reason])).toEqual([
        [1, "invalid"],
        [3, "invalid"],
      ]);
      expect(producer.sent).toHaveLength(1); // one produce call per request
      expect(producer.all).toHaveLength(2);
    });

    it("answers 400 and produces nothing when no event is valid", async () => {
      const res = await post({ events: [{}, { level: "error" }] });
      expect(res.statusCode).toBe(400);
      expect(producer.sent).toHaveLength(0);
    });

    it("rejects an empty batch and an oversized batch", async () => {
      expect((await post({ events: [] })).statusCode).toBe(400);
      const many = Array.from({ length: 101 }, () => ({ message: "x" }));
      expect((await post({ events: many })).statusCode).toBe(413);
      expect(producer.sent).toHaveLength(0);
    });

    it("rejects too-old and too-large events individually", async () => {
      const res = await post({
        events: [
          { message: "old", timestamp: "2026-10-01T00:00:00.000Z" },
          { message: "big", extra: { blob: "z".repeat(70_000) } },
          { message: "ok" },
        ],
      });
      expect(res.statusCode).toBe(202);
      const body = res.json() as {
        rejected: { index: number; reason: string }[];
      };
      expect(body.rejected.map((r) => r.reason)).toEqual([
        "too_old",
        "too_large",
      ]);
      expect(producer.all).toHaveLength(1);
    });

    it("returns 400 for malformed JSON", async () => {
      const res = await post("{not json", {});
      expect(res.statusCode).toBe(400);
    });
  });

  describe("idempotent retries", () => {
    it("gives a retried event the same dedupe key even if accepted later", async () => {
      const event = {
        event_id: uuidv7(),
        message: "flaky",
        timestamp: "2026-11-01T09:58:00.000Z",
      };
      await post(event);

      const later = new Date("2026-11-01T10:00:09.000Z");
      const app2 = buildApp({
        keyStore: keys,
        producer,
        config,
        registry: new Registry(),
        defaultMetrics: false,
        now: () => later,
      });
      await app2.ready();
      await app2.inject({
        method: "POST",
        url: "/v1/events",
        headers: { "content-type": "application/json", "x-api-key": apiKey },
        payload: event,
      });
      await app2.close();

      const [first, retry] = producer.all;
      expect(first!.event_id).toBe(retry!.event_id);
      expect(first!.occurred_at).toBe(retry!.occurred_at); // same primary key in Postgres
      expect(first!.received_at).not.toBe(retry!.received_at);
    });
  });

  describe("failure handling", () => {
    it("answers 503 with Retry-After when the broker does not acknowledge", async () => {
      producer.fail = true;
      const res = await post({ events: [{ message: "a" }, { message: "b" }] });
      expect(res.statusCode).toBe(503);
      expect(res.headers["retry-after"]).toBe("1");
      const metrics = await registry.getSingleMetricAsString(
        "events_rejected_total",
      );
      expect(metrics).toContain(
        'events_rejected_total{reason="unavailable"} 2',
      );
    });
  });

  describe("gzip request bodies", () => {
    it("decompresses gzip bodies", async () => {
      const body = gzipSync(
        JSON.stringify({ events: [{ message: "a" }, { message: "b" }] }),
      );
      const res = await post(body as never, { "content-encoding": "gzip" });
      expect(res.statusCode).toBe(202);
      expect(producer.all).toHaveLength(2);
    });

    it("returns 400 for a corrupt gzip body", async () => {
      const res = await post(Buffer.from("this is not gzip") as never, {
        "content-encoding": "gzip",
      });
      expect(res.statusCode).toBe(400);
    });

    it("returns 413 when the decompressed body exceeds the limit (zip bomb)", async () => {
      const bomb = gzipSync(
        JSON.stringify({
          message: "x",
          extra: { pad: "a".repeat(3 * 1024 * 1024) },
        }),
      );
      expect(bomb.length).toBeLessThan(20_000); // tiny on the wire, 3 MB once inflated
      const res = await post(bomb as never, { "content-encoding": "gzip" });
      expect(res.statusCode).toBe(413);
      expect(producer.sent).toHaveLength(0);
    });
  });

  describe("observability endpoints", () => {
    it("serves health and Prometheus metrics that reflect traffic", async () => {
      await post({ events: [{ message: "a" }, { message: "b" }, { nope: 1 }] });
      expect(
        (await app.inject({ method: "GET", url: "/healthz" })).statusCode,
      ).toBe(200);

      const text = (await app.inject({ method: "GET", url: "/metrics" })).body;
      expect(text).toContain("events_accepted_total 2");
      expect(text).toContain('events_rejected_total{reason="invalid"} 1');
      expect(text).toContain('ingest_requests_total{status="202"} 1');
      expect(text).toMatch(/ingest_request_duration_seconds_count 1/);
    });
  });
});
