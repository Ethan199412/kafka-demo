import { Controller, Get, Param, Post } from '@nestjs/common';
import { ProducerService } from './producer.service';

@Controller('producer')
export class ProducerController {
  constructor(private readonly producerService: ProducerService) {}

  @Get('topics')
  async topics() {
    return await this.producerService.getAllTopics();
  }

  @Post('topic/:topic_name')
  async topic(@Param('topic_name') topic_name: string) {
    await this.producerService.createTopic(topic_name);
  }
}
