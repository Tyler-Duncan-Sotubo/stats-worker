// jobs/cache/cache-warm.job.ts
import { Injectable, Logger } from '@nestjs/common';
import { CacheWarmerService } from 'src/infrastructure/cache/cache-warmer.service';

@Injectable()
export class CacheWarmJob {
  private readonly logger = new Logger(CacheWarmJob.name);

  constructor(private readonly cacheWarmer: CacheWarmerService) {}

  async run(): Promise<void> {
    this.logger.log('Cache warm job starting');
    await this.cacheWarmer.warmAll();
    this.logger.log('Cache warm job complete');
  }
}
