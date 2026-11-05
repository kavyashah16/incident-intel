import { createHash } from "node:crypto";
import type { EventPayload, Frame } from "./schema";
import { normalizeFilename, normalizeMessage } from "./normalize";

/** Bump when grouping rules change; stored on each issue so regroupings are traceable. */
export const FINGERPRINT_VERSION = 1;
const TOP_FRAMES = 5;
const TITLE_MAX = 200;

export interface FingerprintResult {
  fingerprint: string;
  title: string;
  version: number;
}

function isInApp(f: Frame): boolean {
  if (f.in_app === false) return false;
  const name = f.filename ?? "";
  return !(name.includes("node_modules") || name.startsWith("node:"));
}

function frameKey(f: Frame): string {
  return `${normalizeFilename(f.filename ?? "?")}:${f.function ?? "?"}`;
}

export function computeFingerprint(p: EventPayload): FingerprintResult {
  let type = "message";
  let text = p.message ?? "";
  let frames: string[] = [];
  let title = p.message ?? "";

  if (p.exception) {
    type = p.exception.type;
    text = p.exception.value;
    frames = (p.exception.frames ?? []).filter(isInApp).slice(0, TOP_FRAMES).map(frameKey);
    const firstLine = p.exception.value.split("\n")[0] ?? "";
    title = firstLine ? `${type}: ${firstLine}` : type;
  }

  const material = [`v${FINGERPRINT_VERSION}`, type, normalizeMessage(text), frames.join(">")].join("|");
  const fingerprint = createHash("sha256").update(material).digest("hex").slice(0, 32);

  return {
    fingerprint,
    title: title.slice(0, TITLE_MAX) || "(no message)",
    version: FINGERPRINT_VERSION,
  };
}