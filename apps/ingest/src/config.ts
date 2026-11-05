export interface IngestConfig {
  port: number;
  databaseUrl: string;
  brokers: string[];
  eventsTopic: string;
  nodeName: string;
  keyCacheTtlMs: number;
  keyNegativeTtlMs: number;
  keyCacheMaxEntries: number;
  maxEventAgeMs: number;
  sendTimeoutMs: number;
  logLevel: string;
}

const int = (v: string | undefined, d: number): number => {
  const n = v === undefined ? NaN : Number(v);
  return Number.isFinite(n) ? n : d;
};

export function loadConfig(env: NodeJS.ProcessEnv = process.env): IngestConfig {
  return {
    port: int(env.INGEST_PORT, 8080),
    databaseUrl:
      env.DATABASE_URL ??
      "postgres://postgres:postgres@localhost:5432/incidents",
    brokers: (env.KAFKA_BROKERS ?? "localhost:19092")
      .split(",")
      .map((s) => s.trim()),
    eventsTopic: env.EVENTS_TOPIC ?? "events.raw",
    nodeName: env.INGEST_NODE ?? "ingest-1",
    keyCacheTtlMs: int(env.KEY_CACHE_TTL_MS, 30_000),
    keyNegativeTtlMs: int(env.KEY_NEGATIVE_TTL_MS, 30_000),
    keyCacheMaxEntries: int(env.KEY_CACHE_MAX_ENTRIES, 10_000),
    maxEventAgeMs: int(env.MAX_EVENT_AGE_HOURS, 72) * 3600_000,
    sendTimeoutMs: int(env.SEND_TIMEOUT_MS, 5_000),
    logLevel: env.LOG_LEVEL ?? "info",
  };
}
