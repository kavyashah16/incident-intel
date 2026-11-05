import { createGunzip } from "node:zlib";
import type { Readable } from "node:stream";
import Fastify, {
  LogController,
  type FastifyInstance,
  type FastifyReply,
  type FastifyRequest,
} from "fastify";
import { Registry } from "prom-client";
import {
  buildEnvelope,
  MAX_EVENTS_PER_REQUEST,
  MAX_REQUEST_BYTES,
  parseApiKey,
  verifySecret,
  type Envelope,
  type RejectReason,
} from "@ii/shared";
import type { IngestConfig } from "./config";
import type { KeyStore } from "./keystore";
import { createMetrics } from "./metrics";
import type { EventProducer } from "./producer";

declare module "fastify" {
  interface FastifyRequest {
    projectId: string | null;
  }
}

export interface AppDeps {
  keyStore: KeyStore;
  producer: EventProducer;
  config: IngestConfig;
  registry?: Registry;
  /** Disable prom-client default metrics (tests create many apps in one process). */
  defaultMetrics?: boolean;
  now?: () => Date;
}

interface Rejection {
  index: number;
  reason: RejectReason;
  detail?: string;
}

export function buildApp(deps: AppDeps): FastifyInstance {
  const { keyStore, producer, config } = deps;
  const registry = deps.registry ?? new Registry();
  const metrics = createMetrics(registry, {
    defaultMetrics: deps.defaultMetrics ?? true,
  });
  const now = deps.now ?? (() => new Date());

  const app = Fastify({
    logger: config.logLevel === "silent" ? false : { level: config.logLevel },
    // Per-request log lines would distort the throughput benchmark.
    logController: new LogController({ disableRequestLogging: true }),
    bodyLimit: MAX_REQUEST_BYTES, // applies to the decompressed body, which also bounds zip bombs
    forceCloseConnections: "idle",
  });
  app.decorateRequest("projectId", null);

  // Transparent gzip request bodies. The wrapper stream reports how many encoded bytes it
  // consumed so Fastify can still validate Content-Length against what actually arrived.
  app.addHook("preParsing", async (request, _reply, payload) => {
    if (request.headers["content-encoding"] !== "gzip") return payload;
    const gunzip = createGunzip() as ReturnType<typeof createGunzip> & {
      receivedEncodedLength: number;
    };
    gunzip.receivedEncodedLength = 0;
    payload.on("data", (chunk: Buffer) => {
      gunzip.receivedEncodedLength += chunk.length;
    });
    payload.on("error", (err) => gunzip.destroy(err));
    (payload as Readable).pipe(gunzip);
    return gunzip;
  });

  app.setErrorHandler(
    (err: Error & { statusCode?: number; code?: string }, request, reply) => {
      if (err.code?.startsWith("Z_")) {
        return reply.code(400).send({ error: "invalid_gzip" });
      }
      const status =
        err.statusCode && err.statusCode >= 400 && err.statusCode < 500
          ? err.statusCode
          : 500;
      if (status === 500) request.log.error(err);
      return reply
        .code(status)
        .send({
          error:
            status === 413
              ? "payload_too_large"
              : status === 500
                ? "internal"
                : "bad_request",
        });
    },
  );

  app.addHook("onResponse", async (request, reply) => {
    if (request.routeOptions.url !== "/v1/events") return;
    metrics.requests.inc({ status: String(reply.statusCode) });
    metrics.duration.observe(reply.elapsedTime / 1000);
  });

  async function authenticate(
    request: FastifyRequest,
    reply: FastifyReply,
  ): Promise<void> {
    const header = request.headers["x-api-key"];
    const parsed = parseApiKey(Array.isArray(header) ? header[0] : header);
    if (!parsed) {
      return reply.code(401).send({ error: "invalid_api_key" });
    }
    let record;
    try {
      record = await keyStore.lookup(parsed.prefix);
    } catch (err) {
      request.log.error(err, "key lookup failed");
      return reply
        .code(503)
        .header("Retry-After", "1")
        .send({ error: "auth_unavailable" });
    }
    // Unknown, revoked and wrong-secret all look identical to the caller.
    if (
      !record ||
      record.revoked ||
      !verifySecret(parsed.secret, record.keyHash)
    ) {
      return reply.code(401).send({ error: "invalid_api_key" });
    }
    request.projectId = record.projectId;
  }

  app.post(
    "/v1/events",
    { onRequest: authenticate },
    async (request, reply) => {
      const projectId = request.projectId as string;
      const body = request.body as unknown;

      let raw: unknown[];
      if (Array.isArray(body)) raw = body;
      else if (
        body &&
        typeof body === "object" &&
        Array.isArray((body as { events?: unknown }).events)
      ) {
        raw = (body as { events: unknown[] }).events;
      } else raw = [body];

      if (raw.length === 0) return reply.code(400).send({ error: "no_events" });
      if (raw.length > MAX_EVENTS_PER_REQUEST) {
        return reply
          .code(413)
          .send({ error: "too_many_events", max: MAX_EVENTS_PER_REQUEST });
      }

      const ctx = { projectId, now: now(), maxAgeMs: config.maxEventAgeMs };
      const envelopes: Envelope[] = [];
      const rejected: Rejection[] = [];
      raw.forEach((item, index) => {
        const r = buildEnvelope(item, ctx);
        if (r.ok) envelopes.push(r.envelope);
        else {
          rejected.push({ index, reason: r.reason, detail: r.detail });
          metrics.rejected.inc({ reason: r.reason });
        }
      });

      if (envelopes.length === 0) {
        return reply
          .code(400)
          .send({ error: "no_valid_events", accepted: [], rejected });
      }

      const t0 = performance.now();
      try {
        await producer.send(
          config.eventsTopic,
          envelopes.map((e) => ({
            key: e.event_id,
            value: JSON.stringify(e),
            headers: { ingest_node: config.nodeName },
          })),
        );
      } catch (err) {
        request.log.error(err, "produce failed");
        metrics.rejected.inc({ reason: "unavailable" }, envelopes.length);
        // Not durable, so not accepted: the SDK retries with the same event ids.
        return reply
          .code(503)
          .header("Retry-After", "1")
          .send({ error: "unavailable" });
      } finally {
        metrics.sendDuration.observe((performance.now() - t0) / 1000);
      }

      metrics.accepted.inc(envelopes.length);
      return reply
        .code(202)
        .send({ accepted: envelopes.map((e) => e.event_id), rejected });
    },
  );

  app.get("/healthz", async () => ({ ok: true }));

  app.get("/metrics", async (_request, reply) => {
    reply.type(registry.contentType);
    return registry.metrics();
  });

  return app;
}
