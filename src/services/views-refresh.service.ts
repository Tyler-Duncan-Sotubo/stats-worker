import { Injectable, Logger, Inject } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';

// Views with no dependencies — refreshed sequentially to control memory
const GROUP_1_VIEWS = [
  'artist_stream_summary', // largest — do first
  'song_stream_summary', // second largest
  'artist_monthly_listener_summary',
  'artist_chart_summary',
  'song_chart_summary',
  'chart_latest_leaderboard',
  'artist_certification_summary',
  'artist_awards_summary',
  'artist_records_summary',
  'artist_recent_chart_summary',
  'artist_growth_summary',
  'song_growth_summary',
] as const;

// Views that depend on group 1 — run after group 1 completes
const GROUP_2_VIEWS = [
  'artist_trending_summary', // depends on artist_growth_summary
  'song_trending_summary', // depends on song_growth_summary
  'artist_country_summary', // depends on artist_stream_summary
  'song_search_summary', // depends on song_stream_summary
] as const;

type Group1View = (typeof GROUP_1_VIEWS)[number];
type Group2View = (typeof GROUP_2_VIEWS)[number];
type AnyView = Group1View | Group2View;

@Injectable()
export class ViewsRefreshService {
  private readonly logger = new Logger(ViewsRefreshService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  // ─── Refresh all views in dependency order ───────────────────
  async refreshAll(): Promise<void> {
    const totalStart = Date.now();
    this.logger.log('Starting full materialized view refresh...');

    // Group 1: sequential to keep memory under control
    this.logger.log(
      `Refreshing ${GROUP_1_VIEWS.length} base views sequentially...`,
    );
    for (const view of GROUP_1_VIEWS) {
      await this.refreshView(view);
    }

    // Group 2: sequential after group 1 completes
    this.logger.log(
      `Refreshing ${GROUP_2_VIEWS.length} dependent views sequentially...`,
    );
    for (const view of GROUP_2_VIEWS) {
      await this.refreshView(view);
    }

    this.logger.log(`Full refresh completed in ${Date.now() - totalStart}ms`);
  }

  // ─── Refresh a single view by name ───────────────────────────
  async refreshOne(view: AnyView): Promise<void> {
    await this.refreshView(view);
  }

  // ─── Internal refresh with timing + error handling ───────────
  private async refreshView(view: string): Promise<void> {
    const start = Date.now();
    try {
      await this.db.execute(
        sql.raw(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`),
      );
      this.logger.log(`✓ ${view} (${Date.now() - start}ms)`);
    } catch (err) {
      this.logger.error(
        `✗ ${view} failed after ${Date.now() - start}ms: ${(err as Error).message}`,
      );
    }
  }
}
