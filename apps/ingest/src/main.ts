import pg from "pg";
import { buildApp } from "./app";
import { loadConfig } from "./config";
import { CachedKeyStore, PgKeyStore } from "./keystore";
import { KafkaEventProducer } from "./producer";

async function main(): Promise<void> {
  const config = loadConfig();

  const pool = new pg.Pool({ connectionString: config.databaseUrl, max: 5 });
  const keyStore = new CachedKeyStore(new PgKeyStore(pool), {
    maxEntries: config.keyCacheMaxEntries,
    ttlMs: config.keyCacheTtlMs,
    negativeTtlMs: config.keyNegativeTtlMs,
  });

  const producer = new KafkaEventProducer(config.brokers, {
    clientId: config.nodeName,
    sendTimeoutMs: config.sendTimeoutMs,
  });
  await producer.connect();

  const app = buildApp({ keyStore, producer, config });
  await app.listen({ port: config.port, host: "0.0.0.0" });

  // Graceful shutdown: stop accepting, let in-flight requests finish, flush the producer.
  let stopping = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    app.log.info({ signal }, "shutting down");
    try {
      await app.close();
      await producer.disconnect();
      await pool.end();
      process.exit(0);
    } catch (err) {
      app.log.error(err, "shutdown failed");
      process.exit(1);
    }
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
