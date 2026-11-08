import {
  collectDefaultMetrics,
  Counter,
  Gauge,
  Histogram,
  type Registry,
} from "prom-client";

export function createMetrics(
  registry: Registry,
  opts: { defaultMetrics?: boolean } = {},
) {
  if (opts.defaultMetrics ?? true)
    collectDefaultMetrics({ register: registry });
  return {
    batchSize: new Histogram({
      name: "consumer_batch_size",
      help: "Messages per processed batch",
      buckets: [1, 10, 50, 100, 250, 500, 1000, 2000],
      registers: [registry],
    }),
    batchDuration: new Histogram({
      name: "consumer_batch_duration_seconds",
      help: "Database transaction time per batch",
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers: [registry],
    }),
    inserted: new Counter({
      name: "consumer_rows_inserted_total",
      help: "Event rows newly written",
      registers: [registry],
    }),
    duplicates: new Counter({
      name: "consumer_duplicates_total",
      help: "Events skipped because their primary key already existed (redelivery or client retry)",
      registers: [registry],
    }),
    issuesCreated: new Counter({
      name: "consumer_issues_created_total",
      help: "New issues (distinct fingerprints) created",
      registers: [registry],
    }),
    dlq: new Counter({
      name: "consumer_dlq_total",
      help: "Messages sent to the dead-letter topic",
      labelNames: ["reason"],
      registers: [registry],
    }),
    retries: new Counter({
      name: "consumer_db_retries_total",
      help: "Database attempts retried after a transient error",
      registers: [registry],
    }),
    lag: new Gauge({
      name: "consumer_lag_messages",
      help: "Messages between the partition high watermark and this consumer's position",
      labelNames: ["partition"],
      registers: [registry],
    }),
    freshness: new Histogram({
      name: "event_freshness_seconds",
      help: "Time from ingest acceptance (received_at) to the row being written (inserted_at)",
      buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300],
      registers: [registry],
    }),
  };
}

export type ConsumerMetrics = ReturnType<typeof createMetrics>;
