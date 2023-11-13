import { Body, Controller, Delete, Get, Param, Post } from '@nestjs/common';
import { ProducerService } from './producer.service';

@Controller('producer')
export class ProducerController {
  constructor(private readonly producerService: ProducerService) {}

  @Get('topics')
  async topics() {
    return await this.producerService.getAllTopics();
  }

  @Post('topic/:topic_name')
  async topic(@Param('topic_name') topic_name: string, @Body() body: any) {
    const { partition, replicas } = body;
    await this.producerService.createTopic(topic_name, partition, replicas);
  }

  @Delete('topic/:topic_name')
  async deleteTopic(@Param('topic_name') topic_name: string) {
    await this.producerService.deleteTopic(topic_name);
  }

  @Post('send')
  async send(@Body() body: any) {
    const { partition, topic_name, message } = body;
    await this.producerService.send(topic_name, message, partition);
  }
}
