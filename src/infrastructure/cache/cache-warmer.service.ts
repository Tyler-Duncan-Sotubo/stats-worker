// cache-warmer.service.ts
import { Injectable, Logger, Inject } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from '../drizzle/drizzle.module';
import type { DrizzleDB } from '../drizzle/drizzle.module';
import { CacheService } from './cache.service';

const BASE_URL = 'https://tooxclusive.com/stats';
const PAGE_SIZE = 5000;

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

    await Promise.all([
      this.warmSitemapSongPages(),
      this.warmSitemapArtistPages(),
      this.warmSitemapAlbumPages(),
    ]);

    this.logger.log(`Cache warming complete in ${Date.now() - start}ms`);
  }

  // ---------- songs ----------

  private async warmSitemapSongPages(): Promise<void> {
    // Refresh the materialized view first
    await this.db.execute(
      sql`REFRESH MATERIALIZED VIEW CONCURRENTLY sitemap_songs`,
    );

    const countResult = await this.db.execute(sql`
    SELECT COUNT(*)::int AS total FROM sitemap_songs
  `);

    const total = (countResult.rows[0] as any).total as number;
    const totalPages = Math.ceil(total / PAGE_SIZE);

    this.logger.log(
      `Warming ${totalPages} song sitemap pages (${total} songs)`,
    );

    let lastRn = 0;

    for (let page = 1; page <= totalPages; page++) {
      try {
        const result = await this.db.execute(sql`
        SELECT rn, slug, "updatedAt", "totalStreams"
        FROM sitemap_songs
        WHERE rn > ${lastRn}
        ORDER BY rn
        LIMIT ${PAGE_SIZE}
      `);

        if (!result.rows.length) break;

        const rows = result.rows as {
          rn: number;
          slug: string;
          updatedAt: string;
          totalStreams: number;
        }[];
        lastRn = Number(rows[rows.length - 1].rn);

        const xml = this.buildSongSitemapXml(rows);
        await this.cache.setRaw(
          `sitemap:songs:${page}`,
          xml,
          CacheService.TTL.DAY,
        );
        this.logger.log(`  Song page ${page} — ${rows.length} songs ✓`);
      } catch (err: any) {
        this.logger.error(
          `  Song page ${page} — FAILED: ${err.message}`,
          err.stack,
        );
      }
    }

    this.logger.log(`Song sitemap pages done — ${totalPages} pages`);
  }

  private buildSongSitemapXml(
    songs: { slug: string; updatedAt: string; totalStreams: number }[],
  ): string {
    const urls = songs
      .filter((s) => s.slug)
      .map((s) => {
        const streams = Number(s.totalStreams ?? 0);
        const priority =
          streams >= 1_000_000_000
            ? '0.9'
            : streams >= 100_000_000
              ? '0.8'
              : streams >= 10_000_000
                ? '0.7'
                : '0.6';

        return `
  <url>
    <loc>${BASE_URL}/songs/${s.slug}</loc>
    <lastmod>${new Date(s.updatedAt).toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>${priority}</priority>
  </url>`;
      })
      .join('');

    return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;
  }

  // ---------- artists ----------

  private async warmSitemapArtistPages(): Promise<void> {
    const countResult = await this.db.execute(sql`
      SELECT COUNT(*)::int AS total
      FROM artists
      WHERE entity_status = 'canonical'
        AND slug IS NOT NULL
    `);

    const total = (countResult.rows[0] as any).total as number;
    const totalPages = Math.ceil(total / PAGE_SIZE);

    this.logger.log(
      `Warming ${totalPages} artist sitemap pages (${total} artists)`,
    );

    let lastUpdatedAt: string | null = null;
    let lastSlug: string | null = null;

    for (let page = 1; page <= totalPages; page++) {
      try {
        const result = await this.db.execute(sql`
          SELECT
            slug,
            updated_at AS "updatedAt"
          FROM artists
          WHERE entity_status = 'canonical'
            AND slug IS NOT NULL
            ${lastUpdatedAt !== null ? sql`AND (updated_at, slug) < (${lastUpdatedAt}::timestamptz, ${lastSlug})` : sql``}
          ORDER BY updated_at DESC, slug DESC
          LIMIT ${PAGE_SIZE}
        `);

        if (!result.rows.length) break;

        const rows = result.rows as { slug: string; updatedAt: string }[];
        const last = rows[rows.length - 1];
        lastUpdatedAt = last.updatedAt;
        lastSlug = last.slug;

        const urls = rows
          .filter((a) => a.slug)
          .map(
            (a) => `
  <url>
    <loc>${BASE_URL}/artists/${a.slug}</loc>
    <lastmod>${new Date(a.updatedAt).toISOString()}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>`,
          )
          .join('');

        const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

        await this.cache.setRaw(
          `sitemap:artists:${page}`,
          xml,
          CacheService.TTL.DAY,
        );
        this.logger.log(`  Artist page ${page} — ${rows.length} artists ✓`);
      } catch (err: any) {
        this.logger.error(
          `  Artist page ${page} — FAILED: ${err.message}`,
          err.stack,
        );
      }
    }

    this.logger.log(`Artist sitemap pages done — ${totalPages} pages`);
  }

  // ---------- albums ----------

  private async warmSitemapAlbumPages(): Promise<void> {
    const countResult = await this.db.execute(sql`
      SELECT COUNT(*)::int AS total
      FROM albums
      WHERE slug IS NOT NULL
    `);

    const total = (countResult.rows[0] as any).total as number;
    const totalPages = Math.ceil(total / PAGE_SIZE);

    this.logger.log(
      `Warming ${totalPages} album sitemap pages (${total} albums)`,
    );

    let lastCreatedAt: string | null = null;
    let lastSlug: string | null = null;

    for (let page = 1; page <= totalPages; page++) {
      try {
        const result = await this.db.execute(sql`
          SELECT
            slug,
            created_at AS "updatedAt"
          FROM albums
          WHERE slug IS NOT NULL
            ${lastCreatedAt !== null ? sql`AND (created_at, slug) < (${lastCreatedAt}::timestamptz, ${lastSlug})` : sql``}
          ORDER BY created_at DESC, slug DESC
          LIMIT ${PAGE_SIZE}
        `);

        if (!result.rows.length) break;

        const rows = result.rows as { slug: string; updatedAt: string }[];
        const last = rows[rows.length - 1];
        lastCreatedAt = last.updatedAt;
        lastSlug = last.slug;

        const urls = rows
          .filter((a) => a.slug)
          .map(
            (a) => `
  <url>
    <loc>${BASE_URL}/albums/${a.slug}</loc>
    <lastmod>${new Date(a.updatedAt).toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>`,
          )
          .join('');

        const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

        await this.cache.setRaw(
          `sitemap:albums:${page}`,
          xml,
          CacheService.TTL.DAY,
        );
        this.logger.log(`  Album page ${page} — ${rows.length} albums ✓`);
      } catch (err: any) {
        this.logger.error(
          `  Album page ${page} — FAILED: ${err.message}`,
          err.stack,
        );
      }
    }

    this.logger.log(`Album sitemap pages done — ${totalPages} pages`);
  }

  // ---------- milestones ----------

  private async warmSitemapMilestonePages(): Promise<void> {
    const countResult = await this.db.execute(sql`
      SELECT COUNT(*)::int AS total
      FROM milestone_events me
      JOIN artists a ON a.id = me.artist_id
      WHERE a.slug IS NOT NULL
    `);

    const total = (countResult.rows[0] as any).total as number;
    const totalPages = Math.ceil(total / PAGE_SIZE);

    this.logger.log(
      `Warming ${totalPages} milestone sitemap pages (${total} milestones)`,
    );

    let lastCreatedAt: string | null = null;

    for (let page = 1; page <= totalPages; page++) {
      try {
        const result = await this.db.execute(sql`
          SELECT
            a.slug                              AS "artistSlug",
            s.slug                              AS "songSlug",
            me.metric,
            me.threshold::bigint                AS "threshold",
            me.created_at                       AS "updatedAt",
            CASE
              WHEN me.song_id IS NOT NULL THEN
                CONCAT(a.slug, '-', s.slug, '-',
                  CASE
                    WHEN me.threshold >= 1000000000 THEN CONCAT((me.threshold / 1000000000)::text, 'b')
                    WHEN me.threshold >= 1000000 THEN CONCAT((me.threshold / 1000000)::text, 'm')
                    ELSE me.threshold::text
                  END,
                  '-streams-spotify')
              WHEN me.metric = 'monthly_listeners' THEN
                CONCAT(a.slug, '-',
                  CASE
                    WHEN me.threshold >= 1000000000 THEN CONCAT((me.threshold / 1000000000)::text, 'b')
                    WHEN me.threshold >= 1000000 THEN CONCAT((me.threshold / 1000000)::text, 'm')
                    ELSE me.threshold::text
                  END,
                  '-monthly-listeners-spotify')
              ELSE
                CONCAT(a.slug, '-',
                  CASE
                    WHEN me.threshold >= 1000000000 THEN CONCAT((me.threshold / 1000000000)::text, 'b')
                    WHEN me.threshold >= 1000000 THEN CONCAT((me.threshold / 1000000)::text, 'm')
                    ELSE me.threshold::text
                  END,
                  '-streams-spotify')
            END AS "slug"
          FROM milestone_events me
          JOIN artists a ON a.id = me.artist_id
          LEFT JOIN songs s ON s.id = me.song_id
          WHERE a.slug IS NOT NULL
            ${lastCreatedAt !== null ? sql`AND me.created_at > ${lastCreatedAt}::timestamptz` : sql``}
          ORDER BY me.created_at ASC
          LIMIT ${PAGE_SIZE}
        `);

        if (!result.rows.length) break;

        const rows = result.rows as { slug: string; updatedAt: string }[];
        lastCreatedAt = rows[rows.length - 1].updatedAt;

        const urls = rows
          .filter((f) => f.slug)
          .map(
            (f) => `
  <url>
    <loc>${BASE_URL}/milestones/facts/${f.slug}</loc>
    <lastmod>${new Date(f.updatedAt).toISOString()}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>`,
          )
          .join('');

        const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

        await this.cache.setRaw(
          `sitemap:milestones:${page}`,
          xml,
          CacheService.TTL.DAY,
        );
        this.logger.log(
          `  Milestone page ${page} — ${rows.length} milestones ✓`,
        );
      } catch (err: any) {
        this.logger.error(
          `  Milestone page ${page} — FAILED: ${err.message}`,
          err.stack,
        );
      }
    }

    this.logger.log(`Milestone sitemap pages done — ${totalPages} pages`);
  }

  // ---------- sitemap index ----------

  private async warmSitemapIndex(): Promise<void> {
    const [songCount, artistCount, albumCount] =
      await Promise.all([
        this.db.execute(sql`
          SELECT COUNT(*)::int AS total
          FROM song_stream_summary ss
          JOIN songs s ON s.id = ss.song_id
          WHERE ss.total_spotify_streams >= 10000000
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
    ]);

    this.logger.log(
      `Sitemap index warmed — songs: ${(songCount.rows[0] as any).total}, artists: ${(artistCount.rows[0] as any).total}, albums: ${(albumCount.rows[0] as any).total}`,
    );
  }
}
