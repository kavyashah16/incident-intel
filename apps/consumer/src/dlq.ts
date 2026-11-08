import type { Producer } from "kafkajs";
import type { DeadLetter, DeadLetterSink } from "./processor";

/** Publishes failed messages unchanged to the dead-letter topic with the failure in headers. */
export class KafkaDlqSink implements DeadLetterSink {
  constructor(
    private readonly producer: Producer,
    private readonly topic: string,
  ) {}

  async send(items: DeadLetter[]): Promise<void> {
    if (items.length === 0) return;
    await this.producer.send({
      topic: this.topic,
      acks: -1,
      messages: items.map((i) => ({
        key: i.message.key,
        value: i.message.value,
        headers: {
          error_reason: i.reason,
          error_detail: i.detail.slice(0, 500),
          source_topic: i.message.topic,
          source_partition: String(i.message.partition),
          source_offset: i.message.offset,
        },
      })),
    });
  }
}
