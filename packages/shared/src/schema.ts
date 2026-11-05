import { z } from "zod";
import { uuidv7 } from "./uuid";

export const LEVELS = ["debug", "info", "warning", "error", "fatal"] as const;
export type Level = (typeof LEVELS)[number];

export const LEVEL_TO_SMALLINT: Record<Level, number> = {
  debug: 10,
  info: 20,
  warning: 30,
  error: 40,
  fatal: 50,
};

export const MAX_EVENT_BYTES = 64 * 1024;
export const MAX_EVENTS_PER_REQUEST = 100;
export const MAX_REQUEST_BYTES = 1024 * 1024;
export const MAX_CLOCK_SKEW_MS = 5 * 60 * 1000;

/** Frames are ordered most-recent-call first, like Error.stack in Node. */
export const FrameSchema = z.object({
  filename: z.string().max(512).optional(),
  function: z.string().max(256).optional(),
  lineno: z.number().int().nonnegative().optional(),
  colno: z.number().int().nonnegative().optional(),
  in_app: z.boolean().optional(),
});
export type Frame = z.infer<typeof FrameSchema>;

export const ExceptionSchema = z.object({
  type: z.string().min(1).max(256),
  value: z.string().max(8192).default(""),
  frames: z.array(FrameSchema).max(200).optional(),
});
export type ExceptionInfo = z.infer<typeof ExceptionSchema>;

/** The part of an event that is stored as-is (jsonb) once accepted. */
export const EventPayloadSchema = z.object({
  level: z.enum(LEVELS).default("error"),
  message: z.string().max(8192).optional(),
  exception: ExceptionSchema.optional(),
  environment: z.string().max(64).optional(),
  release: z.string().max(128).optional(),
  tags: z.record(z.string().max(64), z.string().max(256)).optional(),
  extra: z.record(z.string(), z.unknown()).optional(),
});
export type EventPayload = z.infer<typeof EventPayloadSchema>;

/** What an SDK sends. `timestamp` is an ISO-8601 string or epoch milliseconds. */
export const ClientEventSchema = EventPayloadSchema.extend({
  event_id: z.uuid().optional(),
  timestamp: z.union([z.iso.datetime({ offset: true }), z.number()]).optional(),
}).refine((e) => e.message !== undefined || e.exception !== undefined, {
  message: "event needs a message or an exception",
});
export type ClientEvent = z.infer<typeof ClientEventSchema>;

/** The Kafka message value on `events.raw`. */
export const EnvelopeSchema = z.object({
  schema_version: z.literal(1),
  event_id: z.uuid(),
  project_id: z.uuid(),
  /** Partition key in Postgres. Derived from the client timestamp so retries dedupe. */
  occurred_at: z.iso.datetime(),
  /** When ingest accepted the event; used for freshness metrics, not for dedupe. */
  received_at: z.iso.datetime(),
  payload: EventPayloadSchema,
});
export type Envelope = z.infer<typeof EnvelopeSchema>;

export type RejectReason = "invalid" | "too_large" | "too_old";

export type BuildResult =
  | { ok: true; envelope: Envelope }
  | { ok: false; reason: RejectReason; detail?: string };

export interface BuildContext {
  projectId: string;
  now: Date;
  maxAgeMs: number;
}

function parseTimestamp(ts: string | number | undefined): number | undefined {
  if (ts === undefined) return undefined;
  const ms = typeof ts === "number" ? ts : Date.parse(ts);
  return Number.isFinite(ms) ? ms : undefined;
}

export function buildEnvelope(raw: unknown, ctx: BuildContext): BuildResult {
  let size: number;
  try {
    size = Buffer.byteLength(JSON.stringify(raw) ?? "");
  } catch {
    return { ok: false, reason: "invalid", detail: "not serializable" };
  }
  if (size > MAX_EVENT_BYTES) {
    return {
      ok: false,
      reason: "too_large",
      detail: `${size} bytes > ${MAX_EVENT_BYTES}`,
    };
  }

  const parsed = ClientEventSchema.safeParse(raw);
  if (!parsed.success) {
    return {
      ok: false,
      reason: "invalid",
      detail: parsed.error.issues[0]?.message,
    };
  }
  const { event_id, timestamp, ...payload } = parsed.data;

  const nowMs = ctx.now.getTime();
  let occurredMs = parseTimestamp(timestamp);
  if (occurredMs === undefined || occurredMs > nowMs + MAX_CLOCK_SKEW_MS) {
    occurredMs = nowMs;
  }
  if (occurredMs < nowMs - ctx.maxAgeMs) {
    return {
      ok: false,
      reason: "too_old",
      detail: new Date(occurredMs).toISOString(),
    };
  }

  return {
    ok: true,
    envelope: {
      schema_version: 1,
      event_id: event_id ?? uuidv7(nowMs),
      project_id: ctx.projectId,
      occurred_at: new Date(occurredMs).toISOString(),
      received_at: ctx.now.toISOString(),
      payload,
    },
  };
}
