export interface ConsumerConfig {
  databaseUrl: string;
  brokers: string[];
  eventsTopic: string;
  dlqTopic: string;
  groupId: string;
  nodeName: string;
  /** Upper bound on messages per database transaction. */
  batchMaxMessages: number;
  /** Max time the broker holds a fetch open waiting for data (latency bound at low load). */
  batchMaxWaitMs: number;
  dbPoolSize: number;
  /** Partitions processed in parallel inside this process. */
  concurrency: number;
  metricsPort: number;
}

const int = (v: string | undefined, d: number): number => {
  const n = v === undefined ? NaN : Number(v);
  return Number.isFinite(n) ? n : d;
};

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ConsumerConfig {
  return {
    databaseUrl: env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/incidents",
    brokers: (env.KAFKA_BROKERS ?? "localhost:19092").split(",").map((s) => s.trim()),
    eventsTopic: env.EVENTS_TOPIC ?? "events.raw",
    dlqTopic: env.DLQ_TOPIC ?? "events.dlq",
    groupId: env.CONSUMER_GROUP ?? "event-writers",
    nodeName: env.CONSUMER_NODE ?? "consumer-1",
    batchMaxMessages: int(env.BATCH_MAX_MESSAGES, 1000),
    batchMaxWaitMs: int(env.BATCH_MAX_WAIT_MS, 200),
    dbPoolSize: int(env.DB_POOL_SIZE, 8),
    concurrency: int(env.CONSUMER_CONCURRENCY, 1),
    metricsPort: int(env.CONSUMER_METRICS_PORT, 9101),
  };
}