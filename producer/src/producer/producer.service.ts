import { Injectable } from '@nestjs/common';
import { Admin, Kafka, Producer } from 'kafkajs';

@Injectable()
export class ProducerService {
  producer: Producer;
  admin: Admin;

  constructor() {
    const kafka = new Kafka({
      clientId: 'nestjs-kafka-example',
      brokers: ['localhost:9092'],
    });

    this.producer = kafka.producer();
    this.admin = kafka.admin();
  }

  async getAllTopics() {
    await this.admin.connect();
    const topics = await this.admin.listTopics();
    console.log('[p1.0] topics', topics);
    await this.admin.disconnect();
    return topics;
  }

  async createTopic(topic: string) {
    await this.admin.connect();
    await this.admin.createTopics({
      topics: [{ topic }],
    });
    await this.admin.disconnect();
  }

  async sendToTopic1(message: string) {
    await this.producer.connect();
    await this.producer.send({
      topic: 'topic1',
      messages: [{ value: message }],
    });
    await this.producer.disconnect();
  }
}
