// src/modules/cleanup/cleanup.service.ts
import { Injectable, Logger, Inject } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';

@Injectable()
export class CleanupService {
  private readonly logger = new Logger(CleanupService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  async cleanupMonthlyListenerSnapshots(): Promise<void> {
    this.logger.log('Cleaning up monthly listener snapshots...');

    const result = await this.db.execute(sql`
      DELETE FROM artist_monthly_listener_snapshots
      WHERE id NOT IN (
        SELECT DISTINCT ON (artist_id) id
        FROM artist_monthly_listener_snapshots
        ORDER BY artist_id, snapshot_date DESC
      )
    `);

    this.logger.log(
      `Monthly listener cleanup complete — ${result.rowCount} rows deleted`,
    );
  }

  async cleanupSongStatsSnapshots(): Promise<void> {
    this.logger.log('Cleaning up song stats snapshots...');

    const result = await this.db.execute(sql`
    DELETE FROM song_stats_snapshots
    WHERE snapshot_date < CURRENT_DATE - INTERVAL '90 days'
  `);

    this.logger.log(`Song stats cleanup — ${result.rowCount} rows deleted`);
  }

  async cleanupArtistStatsSnapshots(): Promise<void> {
    this.logger.log('Cleaning up artist stats snapshots...');

    const result = await this.db.execute(sql`
    DELETE FROM artist_stats_snapshots
    WHERE snapshot_date < CURRENT_DATE - INTERVAL '90 days'
  `);

    this.logger.log(`Artist stats cleanup — ${result.rowCount} rows deleted`);
  }
}
