import { Injectable } from '@nestjs/common';
import {
  Consumer,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  Kafka,
} from 'kafkajs';

@Injectable()
export class ConsumerService {
  private consumer0: Consumer;
  private consumer1: Consumer;

  constructor() {
    const kafka = new Kafka({
      clientId: 'nestjs-kafka-example',
      brokers: ['localhost:9092'],
    });

    this.consumer0 = kafka.consumer({ groupId: 'consumer-group' });
    this.consumer1 = kafka.consumer({ groupId: 'consumer-group' });

    this.startListening();
  }

  async startListening() {
    await this.consumer0.connect();
    await this.consumer1.connect();

    await this.consumer0.subscribe({
      topic: 'topic1',
    });
    await this.consumer1.subscribe({ topic: 'topic1' });

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
  }
}
