import { Injectable, Logger, Inject } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from '../drizzle/drizzle.module';
import type { DrizzleDB } from '../drizzle/drizzle.module';
import { CacheService } from './cache.service';

@Injectable()
export class CacheWarmerService {
  private readonly logger = new Logger(CacheWarmerService.name);

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly cache: CacheService,
  ) {}

  async warmAll(): Promise<void> {
    this.logger.log('Cache warming started');
    const start = Date.now();

    await this.warmSitemapIndex();

    this.logger.log(`Cache warming complete in ${Date.now() - start}ms`);
  }

  private async warmSitemapIndex(): Promise<void> {
    const [songCount, artistCount, albumCount, milestoneCount] =
      await Promise.all([
        this.db.execute(sql`
          SELECT COUNT(*)::int AS total
          FROM song_stream_summary ss
          JOIN songs s ON s.id = ss.song_id
          WHERE ss.total_spotify_streams >= 1000000
            AND s.entity_status = 'canonical'
            AND s.slug IS NOT NULL
            AND s.merged_into_song_id IS NULL
        `),
        this.db.execute(sql`
          SELECT COUNT(*)::int AS total
          FROM artists
          WHERE entity_status = 'canonical'
            AND slug IS NOT NULL
        `),
        this.db.execute(sql`
          SELECT COUNT(*)::int AS total
          FROM albums
          WHERE slug IS NOT NULL
        `),
        this.db.execute(sql`
          SELECT COUNT(*)::int AS total
          FROM milestone_events me
          JOIN artists a ON a.id = me.artist_id
          WHERE a.slug IS NOT NULL
        `),
      ]);

    await Promise.all([
      this.cache.set(
        'public:songs:indexable:count',
        (songCount.rows[0] as any).total,
        CacheService.TTL.DAY,
      ),
      this.cache.set(
        'public:artists:indexable:count',
        (artistCount.rows[0] as any).total,
        CacheService.TTL.DAY,
      ),
      this.cache.set(
        'public:albums:indexable:count',
        (albumCount.rows[0] as any).total,
        CacheService.TTL.DAY,
      ),
      this.cache.set(
        'public:milestones:facts:indexable:count',
        (milestoneCount.rows[0] as any).total,
        CacheService.TTL.DAY,
      ),
    ]);

    this.logger.log(
      `Sitemap index warmed — songs: ${(songCount.rows[0] as any).total}, artists: ${(artistCount.rows[0] as any).total}, albums: ${(albumCount.rows[0] as any).total}, milestones: ${(milestoneCount.rows[0] as any).total}`,
    );
  }
}
