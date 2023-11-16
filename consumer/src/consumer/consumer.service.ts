import { Injectable } from '@nestjs/common';
import {
  Consumer,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  Kafka,
} from 'kafkajs';

// 对于单播消息，如果多个消费者在同一个消费组，那么只有一个消费者可以收到消息。
// 对于多播，也就是存在多个消费组，那么多个消费者都可以 consume 一个 topic，但是，每个消费组只有一个消费者能收到。
// mac: /usr/local/var/lib/kafka-logs/
// 000000.log 保存的就是消息本身
@Injectable()
export class ConsumerService {
  private consumer0: Consumer;
  private consumer1: Consumer;
  private consumer2: Consumer;
  private consumer3: Consumer;

  constructor() {
    const kafka = new Kafka({
      clientId: 'nestjs-kafka-example',
      brokers: ['localhost:9092'],
    });

    this.consumer0 = kafka.consumer({ groupId: 'consumer-group' });
    this.consumer1 = kafka.consumer({ groupId: 'consumer-group' });
    this.consumer2 = kafka.consumer({ groupId: 'consumer-group' });
    this.consumer3 = kafka.consumer({ groupId: 'consumer-group' });

    this.startListening();
  }

  async startListening() {
    await this.consumer0.connect();
    await this.consumer1.connect();
    await this.consumer2.connect();
    await this.consumer3.connect();

    await this.consumer0.subscribe({
      topic: 'topic1',
    });
    await this.consumer1.subscribe({ topic: 'topic1' });
    await this.consumer2.subscribe({ topic: 'topic1' });
    await this.consumer3.subscribe({ topic: 'topic1' });

    await this.consumer0.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Consumer 0 - Received message on partition ${partition} of topic ${topic}: ${message.value}`,
        );
      },
    });

    await this.consumer1.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Consumer 1 - Received message on partition ${partition} of topic ${topic}: ${message.value}`,
        );
      },
    });

    await this.consumer2.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Consumer 2 - Received message on partition ${partition} of topic ${topic}: ${message.value}`,
        );
      },
    });

    await this.consumer3.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `Consumer 3 - Received message on partition ${partition} of topic ${topic}: ${message.value}`,
        );
      },
    });
  }
}
