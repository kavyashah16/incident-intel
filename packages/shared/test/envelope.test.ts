import { describe, expect, it } from "vitest";
import { buildEnvelope, EnvelopeSchema, uuidv7 } from "../src";

const projectId = "0b6f6a3e-61a7-4cc6-9c43-5a13b8cbf6d1";
const now = new Date("2026-11-01T10:00:00.000Z");
const ctx = { projectId, now, maxAgeMs: 72 * 3600_000 };

describe("uuidv7", () => {
  it("is a valid, time-ordered uuid", () => {
    const a = uuidv7(1_700_000_000_000);
    const b = uuidv7(1_700_000_001_000);
    expect(a).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(a < b).toBe(true);
  });
});

describe("buildEnvelope", () => {
  it("accepts a minimal message event and assigns ids and times", () => {
    const r = buildEnvelope({ message: "boom" }, ctx);
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(EnvelopeSchema.safeParse(r.envelope).success).toBe(true);
    expect(r.envelope.project_id).toBe(projectId);
    expect(r.envelope.occurred_at).toBe(now.toISOString());
    expect(r.envelope.received_at).toBe(now.toISOString());
    expect(r.envelope.payload.level).toBe("error");
  });

  it("keeps the client event_id and derives occurred_at from the client timestamp (retry-stable)", () => {
    const raw = {
      event_id: uuidv7(),
      message: "x",
      timestamp: "2026-11-01T09:59:00.000Z",
    };
    const a = buildEnvelope(raw, ctx);
    const b = buildEnvelope(raw, {
      ...ctx,
      now: new Date("2026-11-01T10:00:07.000Z"),
    });
    expect(a.ok && b.ok).toBe(true);
    if (!a.ok || !b.ok) return;
    expect(a.envelope.event_id).toBe(raw.event_id);
    expect(a.envelope.occurred_at).toBe("2026-11-01T09:59:00.000Z");
    // A retry accepted seconds later has the same dedupe key (project, event_id, occurred_at).
    expect(b.envelope.occurred_at).toBe(a.envelope.occurred_at);
    expect(b.envelope.received_at).not.toBe(a.envelope.received_at);
  });

  it("accepts epoch-millisecond timestamps", () => {
    const r = buildEnvelope(
      { message: "x", timestamp: now.getTime() - 1000 },
      ctx,
    );
    expect(r.ok && r.envelope.occurred_at).toBe("2026-11-01T09:59:59.000Z");
  });

  it("falls back to server time for timestamps beyond the allowed clock skew", () => {
    const r = buildEnvelope(
      { message: "x", timestamp: "2026-11-02T10:00:00.000Z" },
      ctx,
    );
    expect(r.ok && r.envelope.occurred_at).toBe(now.toISOString());
  });

  it("rejects events older than the retention window", () => {
    const r = buildEnvelope(
      { message: "x", timestamp: "2026-10-01T10:00:00.000Z" },
      ctx,
    );
    expect(r).toMatchObject({ ok: false, reason: "too_old" });
  });

  it("rejects invalid shapes and oversize events", () => {
    expect(buildEnvelope({}, ctx)).toMatchObject({
      ok: false,
      reason: "invalid",
    });
    expect(buildEnvelope({ message: "x", level: "nope" }, ctx)).toMatchObject({
      ok: false,
      reason: "invalid",
    });
    expect(
      buildEnvelope({ message: "x", event_id: "not-a-uuid" }, ctx),
    ).toMatchObject({ ok: false, reason: "invalid" });
    expect(buildEnvelope("string", ctx)).toMatchObject({
      ok: false,
      reason: "invalid",
    });
    const big = { message: "x", extra: { blob: "y".repeat(70_000) } };
    expect(buildEnvelope(big, ctx)).toMatchObject({
      ok: false,
      reason: "too_large",
    });
  });
});
