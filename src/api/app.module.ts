import { Module } from '@nestjs/common';
import { ApiController, HealthController, MetricsController } from './controllers';
import { WageringService } from '../application/wagering.service';
import { Workers } from '../infrastructure/workers';

@Module({controllers:[ApiController,HealthController,MetricsController],providers:[WageringService,Workers]})
export class AppModule {}
