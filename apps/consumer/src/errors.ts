/**
 * How the consumer reacts to a database error.
 *
 *  - transient: the same write may succeed later (connection loss, deadlock, serialization
 *    failure, resource exhaustion, admin shutdown). Retry with backoff; never drop data.
 *  - permanent: this data can never be written (class 22 bad value, class 23 constraint
 *    violation such as an unknown project). Isolate the offending message and dead-letter it.
 *  - fatal: a bug or a broken schema (class 42 syntax/undefined table, XX internal). Dead-lettering
 *    here would silently discard good events, so stop loudly instead.
 *
 * Anything unrecognised is treated as transient: retried a bounded number of times, then the
 * batch is left uncommitted so it is redelivered. It is never dead-lettered.
 */
export type ErrorClass = "transient" | "permanent" | "fatal";

const NETWORK_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE", "ENOTFOUND", "EAI_AGAIN"]);
const TRANSIENT_CLASSES = new Set(["08", "40", "53", "57", "58"]);
const PERMANENT_CLASSES = new Set(["22", "23"]);
const FATAL_CLASSES = new Set(["42", "XX", "0A"]);

export function classifyError(err: unknown): ErrorClass {
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code !== "string") return "transient";
  if (NETWORK_CODES.has(code)) return "transient";
  if (/^[0-9A-Z]{5}$/.test(code)) {
    const klass = code.slice(0, 2);
    if (PERMANENT_CLASSES.has(klass)) return "permanent";
    if (FATAL_CLASSES.has(klass)) return "fatal";
    if (TRANSIENT_CLASSES.has(klass)) return "transient";
  }
  return "transient";
}

/** Thrown when a batch cannot be completed and must stay uncommitted (redelivered later). */
export class BatchFailedError extends Error {
  constructor(
    message: string,
    readonly errorClass: ErrorClass,
    override readonly cause: unknown,
  ) {
    super(message);
    this.name = "BatchFailedError";
  }
}