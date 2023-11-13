import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { KafkaModule } from './kafka/kafka.module';
import { ProducerModule } from './producer/producer.module';

@Module({
  imports: [KafkaModule, ProducerModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
