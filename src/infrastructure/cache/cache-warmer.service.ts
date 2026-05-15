/* eslint-disable @typescript-eslint/no-unsafe-return */
import { Injectable, Logger, Inject } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from '../drizzle/drizzle.module';
import type { DrizzleDB } from '../drizzle/drizzle.module';
import { CacheService } from './cache.service';

@Injectable()
export class CacheWarmerService {
  private readonly logger = new Logger(CacheWarmerService.name);

  // Top N to warm for each category
  private readonly TOP_ARTISTS = 200;
  private readonly TOP_SONGS = 200;

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly cache: CacheService,
  ) {}

  async warmAll(): Promise<void> {
    this.logger.log('Cache warming started');
    const start = Date.now();

    await Promise.all([
      this.warmTopArtistsByStreams(),
      this.warmTopArtistsByListeners(),
      this.warmTopSongsByStreams(),
      this.warmCharts(),
      this.warmAvailableCharts(),
      this.warmTrendingArtists(),
      this.warmTrendingSongs(),
    ]);

    this.logger.log(`Cache warming complete in ${Date.now() - start}ms`);
  }

  // ── Top artists by streams ────────────────────────────────────────────

  private async warmTopArtistsByStreams(): Promise<void> {
    const result = await this.db.execute(sql`
      SELECT
        ROW_NUMBER() OVER (ORDER BY s.total_streams DESC NULLS LAST)::int AS rank,
        a.id                    AS "artistId",
        a.name                  AS "artistName",
        a.slug                  AS "artistSlug",
        a.image_url             AS "artistImageUrl",
        a.origin_country        AS "originCountry",
        a.is_afrobeats          AS "isAfrobeats",
        a.spotify_id            AS "spotifyId",
        s.total_streams::bigint AS "totalStreams",
        s.daily_streams::bigint AS "dailyStreams",
        s.track_count::int      AS "trackCount",
        s.snapshot_date         AS "snapshotDate"
      FROM artist_stream_summary s
      JOIN artists a ON a.id = s.artist_id
      WHERE a.entity_status = 'canonical'
      ORDER BY s.total_streams DESC NULLS LAST
      LIMIT ${this.TOP_ARTISTS}
    `);

    const all = (result.rows as any[]).map((row) => ({
      ...row,
      totalStreams: row.totalStreams ? Number(row.totalStreams) : null,
      dailyStreams: row.dailyStreams ? Number(row.dailyStreams) : null,
      trackCount: row.trackCount ? Number(row.trackCount) : null,
    }));

    // Global top
    await this.cache.set(
      `public:leaderboard:streams:${JSON.stringify({})}`,
      { data: all, meta: { total: all.length } },
      CacheService.TTL.DAY,
    );

    // Afrobeats only
    const afrobeats = all.filter((r) => r.isAfrobeats === true);
    await this.cache.set(
      `public:leaderboard:streams:${JSON.stringify({ isAfrobeats: true })}`,
      { data: afrobeats, meta: { total: afrobeats.length } },
      CacheService.TTL.DAY,
    );

    // Non-afrobeats
    const nonAfrobeats = all.filter((r) => r.isAfrobeats === false);
    await this.cache.set(
      `public:leaderboard:streams:${JSON.stringify({ isAfrobeats: false })}`,
      { data: nonAfrobeats, meta: { total: nonAfrobeats.length } },
      CacheService.TTL.DAY,
    );

    // By country — slice from the full result in memory
    const countries = ['NG', 'GH', 'ZA', 'TZ', 'KE', 'CI', 'CM'];
    for (const country of countries) {
      const filtered = all.filter((r) => r.originCountry === country);
      await this.cache.set(
        `public:leaderboard:streams:${JSON.stringify({ country })}`,
        { data: filtered, meta: { total: filtered.length } },
        CacheService.TTL.DAY,
      );
    }

    // Common limit variants — slice from memory, no extra DB queries
    for (const limit of [10, 50, 100, 200]) {
      await this.cache.set(
        `public:leaderboard:streams:${JSON.stringify({ limit })}`,
        {
          data: all.slice(0, limit),
          meta: { total: all.slice(0, limit).length },
        },
        CacheService.TTL.DAY,
      );
      await this.cache.set(
        `public:leaderboard:streams:${JSON.stringify({ isAfrobeats: true, limit })}`,
        {
          data: afrobeats.slice(0, limit),
          meta: { total: afrobeats.slice(0, limit).length },
        },
        CacheService.TTL.DAY,
      );
    }

    this.logger.log(`Top artists by streams warmed: ${all.length} rows`);
  }

  // ── Top artists by listeners ──────────────────────────────────────────

  private async warmTopArtistsByListeners(): Promise<void> {
    const result = await this.db.execute(sql`
      SELECT
        ROW_NUMBER() OVER (ORDER BY ml.monthly_listeners DESC NULLS LAST)::int AS rank,
        ml.global_rank::int               AS "globalRank",
        a.id                              AS "artistId",
        a.name                            AS "artistName",
        a.slug                            AS "artistSlug",
        a.image_url                       AS "artistImageUrl",
        a.origin_country                  AS "originCountry",
        a.is_afrobeats                    AS "isAfrobeats",
        a.spotify_id                      AS "spotifyId",
        ml.monthly_listeners::bigint      AS "monthlyListeners",
        ml.daily_change::bigint           AS "dailyChange",
        ml.peak_listeners::bigint         AS "peakListeners",
        ml.snapshot_date                  AS "snapshotDate"
      FROM artist_monthly_listener_summary ml
      JOIN artists a ON a.id = ml.artist_id
      WHERE a.entity_status = 'canonical'
      ORDER BY ml.monthly_listeners DESC NULLS LAST
      LIMIT ${this.TOP_ARTISTS}
    `);

    const all = (result.rows as any[]).map((row) => ({
      ...row,
      monthlyListeners: row.monthlyListeners
        ? Number(row.monthlyListeners)
        : null,
      dailyChange: row.dailyChange ? Number(row.dailyChange) : null,
      peakListeners: row.peakListeners ? Number(row.peakListeners) : null,
      globalRank: row.globalRank ? Number(row.globalRank) : null,
    }));

    // Global
    await this.cache.set(
      `public:leaderboard:listeners:${JSON.stringify({})}`,
      { data: all, meta: { total: all.length } },
      CacheService.TTL.DAY,
    );

    // Afrobeats
    const afrobeats = all.filter((r) => r.isAfrobeats === true);
    await this.cache.set(
      `public:leaderboard:listeners:${JSON.stringify({ isAfrobeats: true })}`,
      { data: afrobeats, meta: { total: afrobeats.length } },
      CacheService.TTL.DAY,
    );

    // Non-afrobeats
    const nonAfrobeats = all.filter((r) => r.isAfrobeats === false);
    await this.cache.set(
      `public:leaderboard:listeners:${JSON.stringify({ isAfrobeats: false })}`,
      { data: nonAfrobeats, meta: { total: nonAfrobeats.length } },
      CacheService.TTL.DAY,
    );

    // By country
    const countries = ['NG', 'GH', 'ZA', 'TZ', 'KE', 'CI', 'CM'];
    for (const country of countries) {
      const filtered = all.filter((r) => r.originCountry === country);
      await this.cache.set(
        `public:leaderboard:listeners:${JSON.stringify({ country })}`,
        { data: filtered, meta: { total: filtered.length } },
        CacheService.TTL.DAY,
      );
    }

    // Limit variants
    for (const limit of [10, 50, 100, 200]) {
      await this.cache.set(
        `public:leaderboard:listeners:${JSON.stringify({ limit })}`,
        {
          data: all.slice(0, limit),
          meta: { total: all.slice(0, limit).length },
        },
        CacheService.TTL.DAY,
      );
      await this.cache.set(
        `public:leaderboard:listeners:${JSON.stringify({ isAfrobeats: true, limit })}`,
        {
          data: afrobeats.slice(0, limit),
          meta: { total: afrobeats.slice(0, limit).length },
        },
        CacheService.TTL.DAY,
      );
    }

    this.logger.log(`Top artists by listeners warmed: ${all.length} rows`);
  }

  // ── Top songs by streams ──────────────────────────────────────────────

  private async warmTopSongsByStreams(): Promise<void> {
    const result = await this.db.execute(sql`
      SELECT
        ROW_NUMBER() OVER (ORDER BY s.total_spotify_streams DESC NULLS LAST)::int AS rank,
        sg.id                             AS "songId",
        sg.title                          AS "songTitle",
        sg.slug                           AS "songSlug",
        sg.image_url                      AS "songImageUrl",
        sg.spotify_track_id               AS "spotifyTrackId",
        sg.is_afrobeats                   AS "isAfrobeats",
        a.id                              AS "artistId",
        a.name                            AS "artistName",
        a.slug                            AS "artistSlug",
        a.image_url                       AS "artistImageUrl",
        s.total_spotify_streams::bigint   AS "totalStreams",
        s.daily_streams::bigint           AS "dailyStreams",
        s.snapshot_date                   AS "snapshotDate"
      FROM song_stream_summary s
      JOIN songs sg ON sg.id = s.song_id
      JOIN artists a ON a.id = sg.artist_id
      WHERE sg.entity_status IN ('canonical', 'provisional')
      ORDER BY s.total_spotify_streams DESC NULLS LAST
      LIMIT ${this.TOP_SONGS}
    `);

    const all = (result.rows as any[]).map((row) => ({
      ...row,
      totalStreams: row.totalStreams ? Number(row.totalStreams) : null,
      dailyStreams: row.dailyStreams ? Number(row.dailyStreams) : null,
    }));

    // Global
    await this.cache.set(
      `public:leaderboard:songs:${JSON.stringify({})}`,
      { data: all, meta: { total: all.length } },
      CacheService.TTL.DAY,
    );

    // Afrobeats
    const afrobeats = all.filter((r) => r.isAfrobeats === true);
    await this.cache.set(
      `public:leaderboard:songs:${JSON.stringify({ isAfrobeats: true })}`,
      { data: afrobeats, meta: { total: afrobeats.length } },
      CacheService.TTL.DAY,
    );

    // Non-afrobeats
    const nonAfrobeats = all.filter((r) => r.isAfrobeats === false);
    await this.cache.set(
      `public:leaderboard:songs:${JSON.stringify({ isAfrobeats: false })}`,
      { data: nonAfrobeats, meta: { total: nonAfrobeats.length } },
      CacheService.TTL.DAY,
    );

    // Limit variants
    for (const limit of [100, 200]) {
      await this.cache.set(
        `public:leaderboard:songs:${JSON.stringify({ limit })}`,
        {
          data: all.slice(0, limit),
          meta: { total: all.slice(0, limit).length },
        },
        CacheService.TTL.DAY,
      );
      await this.cache.set(
        `public:leaderboard:songs:${JSON.stringify({ isAfrobeats: true, limit })}`,
        {
          data: afrobeats.slice(0, limit),
          meta: { total: afrobeats.slice(0, limit).length },
        },
        CacheService.TTL.DAY,
      );
    }

    this.logger.log(`Top songs by streams warmed: ${all.length} rows`);
  }

  // ── Charts ────────────────────────────────────────────────────────────

  private async warmCharts(): Promise<void> {
    const charts = [
      { name: 'tooxclusive_top_100', territory: 'NG' },
      { name: 'spotify_nigeria', territory: 'NG' },
      { name: 'spotify_south_africa', territory: 'ZA' },
      { name: 'spotify_ghana', territory: 'GH' },
      { name: 'spotify_kenya', territory: 'KE' },
      { name: 'uk_afrobeats', territory: 'GB' },
      { name: 'billboard_hot_100', territory: 'US' },
      { name: 'uk_official_singles', territory: 'GB' },
      { name: 'apple_music_nigeria', territory: 'NG' },
      { name: 'apple_music_ghana', territory: 'GH' },
      { name: 'apple_music_kenya', territory: 'KE' },
      { name: 'east_africa_top_50', territory: 'EA' },
    ];

    await Promise.all(
      charts.map(async ({ name, territory }) => {
        const result = await this.db.execute(sql`
          SELECT
            position,
            peak_position                     AS "peakPosition",
            weeks_on_chart                    AS "weeksOnChart",
            chart_week                        AS "chartWeek",
            chart_name                        AS "chartName",
            chart_territory                   AS "chartTerritory",
            song_id                           AS "songId",
            song_title                        AS "songTitle",
            song_slug                         AS "songSlug",
            song_image_url                    AS "songImageUrl",
            spotify_track_id                  AS "spotifyTrackId",
            artist_id                         AS "artistId",
            artist_name                       AS "artistName",
            artist_slug                       AS "artistSlug",
            artist_image_url                  AS "artistImageUrl",
            is_afrobeats                      AS "isAfrobeats",
            prev_rank                         AS "prevRank",
            delta,
            trend
          FROM chart_latest_leaderboard
          WHERE chart_name      = ${name}
            AND chart_territory = ${territory}
          ORDER BY position ASC
          LIMIT 100
        `);

        if (!result.rows.length) return;

        const data = result.rows as any[];

        await this.cache.set(
          `public:charts:${name}:${territory}:100`,
          {
            chartName: name,
            chartTerritory: territory.toUpperCase(),
            chartWeek: data[0]?.chartWeek ?? null,
            data,
            meta: { total: data.length },
          },
          CacheService.TTL.DAY,
        );
      }),
    );

    this.logger.log('Charts warmed');
  }

  // ── Trending ──────────────────────────────────────────────────────────

  private async warmTrendingArtists(): Promise<void> {
    const result = await this.db.execute(sql`
      SELECT
        a.id,
        a.name,
        a.slug,
        a.image_url                               AS "imageUrl",
        a.origin_country                          AS "originCountry",
        a.is_afrobeats                            AS "isAfrobeats",
        a.spotify_id                              AS "spotifyId",
        t.snapshot_date                           AS "snapshotDate",
        t.daily_streams::bigint                   AS "dailyStreams",
        t.daily_growth::bigint                    AS "dailyGrowth",
        t.growth_7d::bigint                       AS "growth7d",
        t.momentum_score::float                   AS "momentumScore",
        s.total_streams::bigint                   AS "totalStreams",
        ml.monthly_listeners::bigint              AS "monthlyListeners",
        chart.best_peak_position::int             AS "bestChartPeak",
        chart.chart_name                          AS "bestChartName",
        chart.chart_territory                     AS "bestChartTerritory"
      FROM artist_trending_summary t
      JOIN artists a ON a.id = t.artist_id
      LEFT JOIN artist_stream_summary s ON s.artist_id = t.artist_id
      LEFT JOIN artist_monthly_listener_summary ml ON ml.artist_id = t.artist_id
      LEFT JOIN LATERAL (
        SELECT arc.best_peak_position, arc.chart_name, arc.chart_territory
        FROM artist_chart_summary arc
        WHERE arc.artist_id = t.artist_id
        ORDER BY arc.best_peak_position ASC NULLS LAST
        LIMIT 1
      ) chart ON true
      WHERE t.snapshot_date = (SELECT MAX(snapshot_date) FROM artist_trending_summary)
        AND a.entity_status = 'canonical'
      ORDER BY t.momentum_score DESC NULLS LAST
      LIMIT 100
    `);

    const all = result.rows as any[];

    // One DB query, slice everything in memory
    const filterSets = [
      { filters: {}, data: all },
      {
        filters: { isAfrobeats: true },
        data: all.filter((r) => r.isAfrobeats),
      },
      {
        filters: { isAfrobeats: false },
        data: all.filter((r) => !r.isAfrobeats),
      },
      {
        filters: { country: 'NG' },
        data: all.filter((r) => r.originCountry === 'NG'),
      },
      {
        filters: { country: 'ZA' },
        data: all.filter((r) => r.originCountry === 'ZA'),
      },
      {
        filters: { country: 'GH' },
        data: all.filter((r) => r.originCountry === 'GH'),
      },
      {
        filters: { isAfrobeats: true, limit: 10 },
        data: all.filter((r) => r.isAfrobeats).slice(0, 10),
      },
      {
        filters: { isAfrobeats: true, limit: 20 },
        data: all.filter((r) => r.isAfrobeats).slice(0, 20),
      },
      { filters: { limit: 50 }, data: all.slice(0, 50) },
    ];

    await Promise.all(
      filterSets.map(({ filters, data }) =>
        this.cache.set(
          `public:trending:artists:${JSON.stringify(filters)}`,
          {
            data,
            meta: {
              total: data.length,
              snapshotDate: data[0]?.snapshotDate ?? null,
            },
          },
          CacheService.TTL.DAY,
        ),
      ),
    );

    this.logger.log(`Trending artists warmed: ${all.length} rows`);
  }

  private async warmTrendingSongs(): Promise<void> {
    const result = await this.db.execute(sql`
      SELECT
        sg.id,
        sg.title,
        sg.slug,
        sg.image_url                              AS "imageUrl",
        sg.spotify_track_id                       AS "spotifyTrackId",
        sg.is_afrobeats                           AS "isAfrobeats",
        sg.artist_id                              AS "artistId",
        a.name                                    AS "artistName",
        a.slug                                    AS "artistSlug",
        a.image_url                               AS "artistImageUrl",
        t.snapshot_date                           AS "snapshotDate",
        t.daily_streams::bigint                   AS "dailyStreams",
        t.daily_growth::bigint                    AS "dailyGrowth",
        t.growth_7d::bigint                       AS "growth7d",
        t.momentum_score::float                   AS "momentumScore",
        s.total_spotify_streams::bigint           AS "totalStreams",
        chart.peak_position::int                  AS "bestChartPeak",
        chart.chart_name                          AS "bestChartName",
        chart.chart_territory                     AS "bestChartTerritory"
      FROM song_trending_summary t
      JOIN songs sg ON sg.id = t.song_id
      JOIN artists a ON a.id = sg.artist_id
      LEFT JOIN song_stream_summary s ON s.song_id = t.song_id
      LEFT JOIN LATERAL (
        SELECT scs.peak_position, scs.chart_name, scs.chart_territory
        FROM song_chart_summary scs
        WHERE scs.song_id = t.song_id
        ORDER BY scs.peak_position ASC NULLS LAST
        LIMIT 1
      ) chart ON true
      WHERE t.snapshot_date = (SELECT MAX(snapshot_date) FROM song_trending_summary)
        AND sg.entity_status = 'canonical'
      ORDER BY t.momentum_score DESC NULLS LAST
      LIMIT 100
    `);

    const all = result.rows as any[];

    const filterSets = [
      { filters: {}, data: all },
      {
        filters: { isAfrobeats: true },
        data: all.filter((r) => r.isAfrobeats),
      },
      {
        filters: { isAfrobeats: false },
        data: all.filter((r) => !r.isAfrobeats),
      },
      { filters: { limit: 50 }, data: all.slice(0, 50) },
      {
        filters: { isAfrobeats: true, limit: 20 },
        data: all.filter((r) => r.isAfrobeats).slice(0, 20),
      },
    ];

    await Promise.all(
      filterSets.map(({ filters, data }) =>
        this.cache.set(
          `public:trending:songs:${JSON.stringify(filters)}`,
          {
            data,
            meta: {
              total: data.length,
              snapshotDate: data[0]?.snapshotDate ?? null,
            },
          },
          CacheService.TTL.DAY,
        ),
      ),
    );

    this.logger.log(`Trending songs warmed: ${all.length} rows`);
  }

  // ── Available charts ──────────────────────────────────────────────────

  private async warmAvailableCharts(): Promise<void> {
    const result = await this.db.execute(sql`
      SELECT DISTINCT
        chart_name        AS "chartName",
        chart_territory   AS "chartTerritory"
      FROM chart_latest_leaderboard
      ORDER BY chart_name, chart_territory
    `);

    await this.cache.set(
      'public:charts:available',
      result.rows,
      CacheService.TTL.DAY,
    );

    this.logger.log(`Available charts warmed: ${result.rows.length}`);
  }
}
