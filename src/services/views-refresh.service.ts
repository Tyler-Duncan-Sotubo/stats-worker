import { Injectable, Logger, Inject } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';

const VIEWS = [
  'song_chart_summary',
  'chart_latest_leaderboard',
  'artist_stream_summary',
  'song_stream_summary',
  'artist_certification_summary',
  'artist_chart_summary',
  'artist_growth_summary',
  'artist_trending_summary',
  'song_growth_summary',
  'song_trending_summary',
  'artist_country_summary',
  'artist_recent_chart_summary',
] as const;

@Injectable()
export class ViewsRefreshService {
  private readonly logger = new Logger(ViewsRefreshService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  async refreshAll(): Promise<void> {
    for (const view of VIEWS) {
      const start = Date.now();

      try {
        await this.db.execute(
          sql.raw(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`),
        );

        this.logger.log(`Refreshed ${view} in ${Date.now() - start}ms`);
      } catch (err) {
        this.logger.error(
          `Failed to refresh ${view}: ${(err as Error).message}`,
        );
      }
    }
  }

  async refreshOne(view: (typeof VIEWS)[number]): Promise<void> {
    await this.db.execute(
      sql.raw(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`),
    );

    this.logger.log(`Manually refreshed ${view}`);
  }
}
