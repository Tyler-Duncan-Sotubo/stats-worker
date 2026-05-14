// src/infrastructure/health/health.module.ts
import { Module } from '@nestjs/common';
import { WarmupService } from './warmup.service';

@Module({
  providers: [WarmupService],
  exports: [WarmupService],
})
export class HealthModule {}
