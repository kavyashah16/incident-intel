import { createServer } from "node:http";
import { Kafka, logLevel, Partitioners } from "kafkajs";
import pg from "pg";
import { Registry } from "prom-client";
import { loadConfig } from "./config";
import { KafkaDlqSink } from "./dlq";
import { createMetrics } from "./metrics";
import { BatchProcessor, type RawMessage } from "./processor";

const log = (
  level: string,
  msg: string,
  fields: Record<string, unknown> = {},
): void => console.log(JSON.stringify({ level, msg, ...fields }));

/** UTC days (YYYY-MM-DD) from `back` days ago to `ahead` days from now. */
function utcDays(back: number, ahead: number): string[] {
  const out: string[] = [];
  for (let d = -back; d <= ahead; d++)
    out.push(new Date(Date.now() + d * 86_400_000).toISOString().slice(0, 10));
  return out;
}

async function main(): Promise<void> {
  const config = loadConfig();
  const registry = new Registry();
  const metrics = createMetrics(registry);

  const pool = new pg.Pool({
    connectionString: config.databaseUrl,
    max: config.dbPoolSize,
  });
  pool.on("error", (err) =>
    log("error", "idle database client error", { error: err.message }),
  );

  const kafka = new Kafka({
    clientId: config.nodeName,
    brokers: config.brokers,
    logLevel: logLevel.WARN,
  });
  const dlqProducer = kafka.producer({
    idempotent: true,
    retry: { retries: 8 },
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await dlqProducer.connect();
  const processor = new BatchProcessor(
    pool,
    new KafkaDlqSink(dlqProducer, config.dlqTopic),
    metrics,
    {
      log: (msg, fields) => log("warn", msg, fields),
    },
  );

  // Partitions ahead of time, so the common path never creates one mid-batch.
  await processor.ensurePartitions(utcDays(3, 3));
  const partitionTimer = setInterval(() => {
    processor
      .ensurePartitions(utcDays(3, 3))
      .catch((err) =>
        log("error", "partition pre-creation failed", { error: String(err) }),
      );
  }, 3_600_000);
  partitionTimer.unref();

  const consumer = kafka.consumer({
    groupId: config.groupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
    maxWaitTimeInMs: config.batchMaxWaitMs,
    maxBytesPerPartition: 4 * 1024 * 1024,
  });
  consumer.on(consumer.events.CRASH, (e) =>
    log("error", "consumer crashed", {
      error: e.payload.error.message,
      restart: e.payload.restart,
    }),
  );
  await consumer.connect();
  await consumer.subscribe({
    topics: [config.eventsTopic],
    fromBeginning: true,
  });

  await consumer.run({
    partitionsConsumedConcurrently: config.concurrency,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      isRunning,
      isStale,
    }) => {
      for (let i = 0; i < batch.messages.length; i += config.batchMaxMessages) {
        if (!isRunning() || isStale()) return;
        const chunk = batch.messages.slice(i, i + config.batchMaxMessages);
        const raw = chunk.map(
          (m): RawMessage => ({
            topic: batch.topic,
            partition: batch.partition,
            offset: m.offset,
            key: m.key,
            value: m.value,
          }),
        );

        // If this throws, offsets are NOT resolved: the chunk is redelivered after kafkajs backs off.
        await processor.process(raw);

        const last = chunk[chunk.length - 1]!.offset;
        resolveOffset(last); // only after the database transaction has committed
        metrics.lag.set(
          { partition: String(batch.partition) },
          Math.max(0, Number(batch.highWatermark) - (Number(last) + 1)),
        );
        await heartbeat();
        await commitOffsetsIfNecessary();
      }
    },
  });

  const server = createServer(async (req, res) => {
    if (req.url === "/healthz") {
      res
        .writeHead(200, { "content-type": "application/json" })
        .end('{"ok":true}');
    } else if (req.url === "/metrics") {
      res
        .writeHead(200, { "content-type": registry.contentType })
        .end(await registry.metrics());
    } else {
      res.writeHead(404).end();
    }
  });
  server.listen(config.metricsPort, "0.0.0.0");
  log("info", "consumer running", {
    group: config.groupId,
    topic: config.eventsTopic,
    metricsPort: config.metricsPort,
  });

  // Graceful shutdown: stop fetching, let the in-flight batch commit, then close everything.
  let stopping = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    log("info", "shutting down", { signal });
    try {
      await consumer.disconnect();
      await dlqProducer.disconnect();
      server.close();
      await pool.end();
      process.exit(0);
    } catch (err) {
      log("error", "shutdown failed", { error: String(err) });
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
