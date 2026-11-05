import { describe, expect, it } from "vitest";
import {
  computeFingerprint,
  FINGERPRINT_VERSION,
  type EventPayload,
} from "../src";

const base = (over: Partial<EventPayload> = {}): EventPayload => ({
  level: "error",
  exception: {
    type: "TypeError",
    value: "Cannot read properties of undefined (reading 'id') for user 4812",
    frames: [
      {
        filename: "/app/src/handlers/user.js",
        function: "getUser",
        lineno: 42,
        in_app: true,
      },
      {
        filename: "/app/node_modules/express/lib/router.js",
        function: "handle",
        lineno: 10,
      },
      { filename: "/app/src/server.js", function: "dispatch", lineno: 7 },
    ],
  },
  ...over,
});

describe("computeFingerprint", () => {
  it("is stable across ids, line numbers and deploy paths", () => {
    const a = computeFingerprint(base());
    const b = computeFingerprint(
      base({
        exception: {
          type: "TypeError",
          value:
            "Cannot read properties of undefined (reading 'id') for user 9930",
          frames: [
            {
              filename: "/srv/releases/77/src/handlers/user.js",
              function: "getUser",
              lineno: 99,
              in_app: true,
            },
            {
              filename: "/srv/releases/77/src/server.js",
              function: "dispatch",
              lineno: 8,
            },
          ],
        },
      }),
    );
    expect(a.fingerprint).toBe(b.fingerprint);
    expect(a.version).toBe(FINGERPRINT_VERSION);
  });

  it("separates different exception types and different call sites", () => {
    const a = computeFingerprint(base());
    const otherType = computeFingerprint(
      base({
        exception: {
          type: "RangeError",
          value: base().exception!.value,
          frames: base().exception!.frames,
        },
      }),
    );
    const otherSite = computeFingerprint(
      base({
        exception: {
          type: "TypeError",
          value: base().exception!.value,
          frames: [
            {
              filename: "/app/src/handlers/order.js",
              function: "getOrder",
              in_app: true,
            },
          ],
        },
      }),
    );
    expect(otherType.fingerprint).not.toBe(a.fingerprint);
    expect(otherSite.fingerprint).not.toBe(a.fingerprint);
  });

  it("ignores node_modules frames when grouping", () => {
    const withLib = computeFingerprint(base());
    const noLib = computeFingerprint(
      base({
        exception: {
          type: "TypeError",
          value: base().exception!.value,
          frames: [
            {
              filename: "/app/src/handlers/user.js",
              function: "getUser",
              in_app: true,
            },
            { filename: "/app/src/server.js", function: "dispatch" },
          ],
        },
      }),
    );
    expect(withLib.fingerprint).toBe(noLib.fingerprint);
  });

  it("groups message-only events by normalized text", () => {
    const a = computeFingerprint({
      level: "error",
      message: "payment 123 declined",
    });
    const b = computeFingerprint({
      level: "error",
      message: "payment 456 declined",
    });
    const c = computeFingerprint({
      level: "error",
      message: "payment refused",
    });
    expect(a.fingerprint).toBe(b.fingerprint);
    expect(a.fingerprint).not.toBe(c.fingerprint);
  });

  it("builds a bounded, human-readable title", () => {
    expect(
      computeFingerprint(base()).title.startsWith("TypeError: Cannot read"),
    ).toBe(true);
    expect(
      computeFingerprint({ level: "error", message: "x".repeat(1000) }).title
        .length,
    ).toBe(200);
    expect(computeFingerprint({ level: "error", message: "" }).title).toBe(
      "(no message)",
    );
  });
});
