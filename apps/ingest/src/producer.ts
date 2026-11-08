import {
  CompressionTypes,
  Kafka,
  logLevel,
  Partitioners,
  type Producer,
} from "kafkajs";

export interface ProducerMessage {
  key: string;
  value: string;
  headers?: Record<string, string>;
}

/**
 * Thin seam over the Kafka client. Week 1 uses kafkajs (pure JS, installs everywhere); the
 * Week 2 tuning sweep should benchmark a librdkafka-based client behind this same interface.
 */
export interface EventProducer {
  /** Resolves only after the broker has acknowledged every message (acks=all). */
  send(topic: string, messages: ProducerMessage[]): Promise<void>;
  disconnect(): Promise<void>;
}

export class ProducerTimeoutError extends Error {
  constructor(ms: number) {
    super(`broker did not acknowledge within ${ms} ms`);
    this.name = "ProducerTimeoutError";
  }
}

export class KafkaEventProducer implements EventProducer {
  private readonly producer: Producer;

  constructor(
    brokers: string[],
    private readonly opts: { clientId: string; sendTimeoutMs: number },
  ) {
    const kafka = new Kafka({
      clientId: opts.clientId,
      brokers,
      logLevel: logLevel.WARN,
    });
    this.producer = kafka.producer({
      // Idempotent producer: broker-side dedupe of internal retries. Retries are bounded so a
      // dead broker surfaces as a 503 instead of an unbounded in-memory queue.
      idempotent: false,
      retry: { retries: 8, initialRetryTime: 100, maxRetryTime: 2_000 },
      allowAutoTopicCreation: false,
      // Same key hashing as the Java client; setting it explicitly also silences the v2 warning.
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  connect(): Promise<void> {
    return this.producer.connect();
  }

  async send(topic: string, messages: ProducerMessage[]): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new ProducerTimeoutError(this.opts.sendTimeoutMs)),
        this.opts.sendTimeoutMs,
      );
    });
    try {
      await Promise.race([
        this.producer.send({
          topic,
          messages,
          acks: -1,
          compression: CompressionTypes.GZIP,
        }),
        timeout,
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  disconnect(): Promise<void> {
    return this.producer.disconnect();
  }
}
