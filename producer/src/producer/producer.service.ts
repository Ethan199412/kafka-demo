import { Injectable } from '@nestjs/common';
import { Admin, Kafka, Producer, ProducerRecord } from 'kafkajs';

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

  async createTopic(topic: string, partition: number, replicas: number) {
    await this.admin.connect();
    await this.admin.createTopics({
      topics: [
        { topic, numPartitions: partition, replicationFactor: replicas },
      ],
    });
    await this.admin.disconnect();
  }

  async deleteTopic(topic: string) {
    await this.admin.connect();
    await this.admin.deleteTopics({
      topics: [topic],
    });
    await this.admin.disconnect();
  }

  async sendToTopic1(message: string) {
    await this.producer.connect();
    await this.producer.send({
      topic: 'topic1',
      messages: [{ value: message, partition: 0 }],
    } as ProducerRecord);
    await this.producer.disconnect();
  }

  async send(topic: string, message: string, partition) {
    await this.producer.connect();
    await this.producer.send({
      topic,
      messages: [{ value: message, partition }],
    } as ProducerRecord);
    await this.producer.disconnect();
  }
}
