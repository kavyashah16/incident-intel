/**
 * Kafka hands-on check. Creates a throwaway topic, produces 5 keyed messages, reads them back
 * with a fresh consumer group and prints where each one landed. It never touches events.raw.
 */
import { Kafka, logLevel } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:19092").split(",");
const topic = "smoke.test";
const runId = Date.now().toString(36);
const keys = ["a", "b", "a", "c", "a"];

async function main(): Promise<void> {
  const kafka = new Kafka({
    clientId: "smoke",
    brokers,
    logLevel: logLevel.WARN,
  });

  const admin = kafka.admin();
  await admin.connect();
  const created = await admin.createTopics({
    topics: [{ topic, numPartitions: 3 }],
  });
  console.log(
    created
      ? `created topic ${topic} (3 partitions)`
      : `topic ${topic} already exists`,
  );
  await admin.disconnect();

  const producer = kafka.producer({ idempotent: true });
  await producer.connect();
  for (const [i, key] of keys.entries()) {
    const [meta] = await producer.send({
      topic,
      acks: -1,
      messages: [{ key, value: `${runId} message ${i}` }],
    });
    console.log(
      `sent     key=${key} -> partition ${meta?.partition}, offset ${meta?.baseOffset}`,
    );
  }
  await producer.disconnect();

  const consumer = kafka.consumer({ groupId: `smoke-${runId}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  let seen = 0;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("timed out waiting for messages")),
      20_000,
    );
    consumer
      .run({
        eachMessage: async ({ partition, message }) => {
          const value = message.value?.toString() ?? "";
          if (!value.startsWith(runId)) return; // ignore messages from earlier runs
          console.log(
            `received key=${message.key?.toString()} <- partition ${partition}, offset ${message.offset}: ${value}`,
          );
          if (++seen === keys.length) {
            clearTimeout(timer);
            resolve();
          }
        },
      })
      .catch(reject);
  });
  await consumer.disconnect();
  console.log("kafka ok");
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
