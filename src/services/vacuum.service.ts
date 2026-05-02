import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { sql } from 'drizzle-orm';

@Injectable()
export class VacuumService {
  private readonly logger = new Logger(VacuumService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  async run(): Promise<void> {
    const tables = [
      'song_stats_snapshots',
      'artist_stats_snapshots',
      'artist_monthly_listener_snapshots',
      'songs',
      'artists',
    ];

    this.logger.log('Vacuum starting...');

    for (const table of tables) {
      try {
        const start = Date.now();
        await this.db.execute(sql.raw(`VACUUM ANALYZE ${table}`));
        const duration = Date.now() - start;
        this.logger.log(`VACUUM ANALYZE ${table} — ${duration}ms`);
      } catch (err) {
        this.logger.error(
          `VACUUM failed for ${table}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }

    this.logger.log('Vacuum complete');
  }
}
