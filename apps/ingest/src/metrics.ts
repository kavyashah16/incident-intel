import {
  collectDefaultMetrics,
  Counter,
  Histogram,
  type Registry,
} from "prom-client";

export function createMetrics(
  registry: Registry,
  opts: { defaultMetrics?: boolean } = {},
) {
  if (opts.defaultMetrics ?? true) {
    // Includes nodejs_eventloop_lag_* which is the first thing to watch in the 10x test.
    collectDefaultMetrics({ register: registry });
  }
  return {
    requests: new Counter({
      name: "ingest_requests_total",
      help: "POST /v1/events requests by HTTP status",
      labelNames: ["status"],
      registers: [registry],
    }),
    duration: new Histogram({
      name: "ingest_request_duration_seconds",
      help: "Server-side latency of POST /v1/events",
      buckets: [
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
      ],
      registers: [registry],
    }),
    accepted: new Counter({
      name: "events_accepted_total",
      help: "Events written to the broker and acknowledged",
      registers: [registry],
    }),
    rejected: new Counter({
      name: "events_rejected_total",
      help: "Events rejected at ingest",
      labelNames: ["reason"],
      registers: [registry],
    }),
    sendDuration: new Histogram({
      name: "producer_send_duration_seconds",
      help: "Time for the broker to acknowledge a produce request",
      buckets: [
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
      ],
      registers: [registry],
    }),
  };
}

export type IngestMetrics = ReturnType<typeof createMetrics>;
