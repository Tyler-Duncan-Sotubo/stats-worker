import { Injectable, Logger, Inject } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from '../drizzle/drizzle.module';
import type { DrizzleDB } from '../drizzle/drizzle.module';

@Injectable()
export class WarmupService {
  private readonly logger = new Logger(WarmupService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  @Cron('*/5 * * * *', { timeZone: 'Europe/London' })
  async ping() {
    try {
      await this.db.execute(sql`SELECT 1`);
      this.logger.log('Warmup ping OK'); // change debug → log
    } catch (err) {
      this.logger.warn(`Warmup ping failed: ${(err as Error).message}`);
    }
  }
}
