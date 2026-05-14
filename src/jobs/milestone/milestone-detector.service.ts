import { Inject, Injectable, Logger } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { milestoneEvents } from 'src/infrastructure/drizzle/schema';

const ARTIST_STREAM_THRESHOLDS = [
  100_000_000, 500_000_000, 1_000_000_000, 2_000_000_000, 5_000_000_000,
  10_000_000_000, 15_000_000_000, 20_000_000_000,
];

const SONG_STREAM_THRESHOLDS = [
  50_000_000, 100_000_000, 250_000_000, 500_000_000, 1_000_000_000,
  2_000_000_000,
];

const LISTENER_THRESHOLDS = [
  1_000_000,
  5_000_000,
  10_000_000,
  25_000_000,
  50_000_000,
  ...Array.from({ length: 21 }, (_, i) => 100_000_000 + i * 5_000_000),
];

export interface DetectedMilestone {
  type: 'artist_streams' | 'song_streams' | 'monthly_listeners';
  artistId?: string;
  songId?: string;
  artistName: string;
  songTitle?: string;
  artistSlug: string;
  songSlug?: string;
  threshold: number;
  actualValue: number;
  crossedAt: string;
  isAfrobeats: boolean;
}

@Injectable()
export class MilestoneDetectorService {
  private readonly logger = new Logger(MilestoneDetectorService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  // ── Main entry point ──────────────────────────────────────────────────

  async detect(): Promise<DetectedMilestone[]> {
    const today = new Date().toISOString().split('T')[0];
    const detected: DetectedMilestone[] = [];

    const [artistStreamMilestones, songStreamMilestones, listenerMilestones] =
      await Promise.all([
        this.detectArtistStreamMilestones(today),
        this.detectSongStreamMilestones(today),
        this.detectListenerMilestones(today),
      ]);

    detected.push(
      ...artistStreamMilestones,
      ...songStreamMilestones,
      ...listenerMilestones,
    );

    // Bootstrap pass — catch artists/songs that have crossed thresholds
    // but have no snapshot history to compare against
    const bootstrapped = await this.bootstrapMissingMilestones(today);
    detected.push(...bootstrapped);

    this.logger.log(`Detected ${detected.length} new milestones for ${today}`);
    return detected;
  }

  // ── Artist stream milestones ──────────────────────────────────────────

  private async detectArtistStreamMilestones(
    today: string,
  ): Promise<DetectedMilestone[]> {
    const results: DetectedMilestone[] = [];

    for (const threshold of ARTIST_STREAM_THRESHOLDS) {
      const crossed = await this.db.execute(sql`
        SELECT
          a.id            AS "artistId",
          a.name          AS "artistName",
          a.slug          AS "artistSlug",
          a.is_afrobeats  AS "isAfrobeats",
          today.total_streams AS "actualValue"
        FROM artist_stats_snapshots today
        JOIN artists a ON a.id = today.artist_id
        LEFT JOIN artist_stats_snapshots yesterday
          ON yesterday.artist_id = today.artist_id
          AND yesterday.snapshot_date = ${today}::date - INTERVAL '1 day'
        LEFT JOIN milestone_events me
          ON me.artist_id = a.id
          AND me.song_id IS NULL
          AND me.metric = 'spotify_streams'
          AND me.threshold = ${threshold}
        WHERE today.snapshot_date = ${today}
          AND today.total_streams >= ${threshold}
          AND (yesterday.total_streams IS NULL OR yesterday.total_streams < ${threshold})
          AND me.id IS NULL
          AND a.entity_status = 'canonical'
      `);

      for (const row of crossed.rows as any[]) {
        await this.db
          .insert(milestoneEvents)
          .values({
            artistId: row.artistId,
            metric: 'spotify_streams',
            threshold,
            crossedAt: today,
            streamValueAtCrossing: row.actualValue,
            isAfrobeats: row.isAfrobeats,
          })
          .onConflictDoNothing();

        results.push({
          type: 'artist_streams',
          artistId: row.artistId,
          artistName: row.artistName,
          artistSlug: row.artistSlug,
          threshold,
          actualValue: Number(row.actualValue),
          crossedAt: today,
          isAfrobeats: row.isAfrobeats,
        });

        this.logger.log(
          `🎯 Artist milestone: ${row.artistName} crossed ${this.formatNumber(threshold)} streams`,
        );
      }
    }

    return results;
  }

  // ── Song stream milestones ────────────────────────────────────────────

  private async detectSongStreamMilestones(
    today: string,
  ): Promise<DetectedMilestone[]> {
    const results: DetectedMilestone[] = [];

    for (const threshold of SONG_STREAM_THRESHOLDS) {
      const crossed = await this.db.execute(sql`
        SELECT
          s.id            AS "songId",
          s.title         AS "songTitle",
          s.slug          AS "songSlug",
          s.is_afrobeats  AS "isAfrobeats",
          a.id            AS "artistId",
          a.name          AS "artistName",
          a.slug          AS "artistSlug",
          today.spotify_streams AS "actualValue"
        FROM song_stats_snapshots today
        JOIN songs s ON s.id = today.song_id
        JOIN artists a ON a.id = s.artist_id
        LEFT JOIN song_stats_snapshots yesterday
          ON yesterday.song_id = today.song_id
          AND yesterday.snapshot_date = ${today}::date - INTERVAL '1 day'
        LEFT JOIN milestone_events me
          ON me.song_id = s.id
          AND me.metric = 'spotify_streams'
          AND me.threshold = ${threshold}
        WHERE today.snapshot_date = ${today}
          AND today.spotify_streams >= ${threshold}
          AND (yesterday.spotify_streams IS NULL OR yesterday.spotify_streams < ${threshold})
          AND me.id IS NULL
          AND s.entity_status = 'canonical'
          AND s.merged_into_song_id IS NULL
      `);

      for (const row of crossed.rows as any[]) {
        await this.db
          .insert(milestoneEvents)
          .values({
            artistId: row.artistId,
            songId: row.songId,
            metric: 'spotify_streams',
            threshold,
            crossedAt: today,
            streamValueAtCrossing: row.actualValue,
            isAfrobeats: row.isAfrobeats,
          })
          .onConflictDoNothing();

        results.push({
          type: 'song_streams',
          artistId: row.artistId,
          songId: row.songId,
          artistName: row.artistName,
          artistSlug: row.artistSlug,
          songTitle: row.songTitle,
          songSlug: row.songSlug,
          threshold,
          actualValue: Number(row.actualValue),
          crossedAt: today,
          isAfrobeats: row.isAfrobeats,
        });

        this.logger.log(
          `🎯 Song milestone: "${row.songTitle}" by ${row.artistName} crossed ${this.formatNumber(threshold)} streams`,
        );
      }
    }

    return results;
  }

  // ── Monthly listener milestones ───────────────────────────────────────

  private async detectListenerMilestones(
    today: string,
  ): Promise<DetectedMilestone[]> {
    const results: DetectedMilestone[] = [];

    for (const threshold of LISTENER_THRESHOLDS) {
      const crossed = await this.db.execute(sql`
        SELECT
          a.id            AS "artistId",
          a.name          AS "artistName",
          a.slug          AS "artistSlug",
          a.is_afrobeats  AS "isAfrobeats",
          today.monthly_listeners AS "actualValue"
        FROM artist_monthly_listener_snapshots today
        JOIN artists a ON a.id = today.artist_id
        LEFT JOIN artist_monthly_listener_snapshots yesterday
          ON yesterday.artist_id = today.artist_id
          AND yesterday.snapshot_date = ${today}::date - INTERVAL '1 day'
        LEFT JOIN milestone_events me
          ON me.artist_id = a.id
          AND me.song_id IS NULL
          AND me.metric = 'monthly_listeners'
          AND me.threshold = ${threshold}
        WHERE today.snapshot_date = ${today}
          AND today.monthly_listeners >= ${threshold}
          AND (yesterday.monthly_listeners IS NULL OR yesterday.monthly_listeners < ${threshold})
          AND me.id IS NULL
          AND a.entity_status = 'canonical'
      `);

      for (const row of crossed.rows as any[]) {
        await this.db
          .insert(milestoneEvents)
          .values({
            artistId: row.artistId,
            metric: 'monthly_listeners',
            threshold,
            crossedAt: today,
            streamValueAtCrossing: row.actualValue,
            isAfrobeats: row.isAfrobeats,
          })
          .onConflictDoNothing();

        results.push({
          type: 'monthly_listeners',
          artistId: row.artistId,
          artistName: row.artistName,
          artistSlug: row.artistSlug,
          threshold,
          actualValue: Number(row.actualValue),
          crossedAt: today,
          isAfrobeats: row.isAfrobeats,
        });

        this.logger.log(
          `🎯 Listener milestone: ${row.artistName} crossed ${this.formatNumber(threshold)} monthly listeners`,
        );
      }
    }

    return results;
  }

  // ── Bootstrap missing milestones ──────────────────────────────────────

  private async bootstrapMissingMilestones(
    today: string,
  ): Promise<DetectedMilestone[]> {
    const results: DetectedMilestone[] = [];

    // Artist stream bootstrap — uses artist_stream_summary MV
    for (const threshold of ARTIST_STREAM_THRESHOLDS) {
      const crossed = await this.db.execute(sql`
        SELECT
          a.id            AS "artistId",
          a.name          AS "artistName",
          a.slug          AS "artistSlug",
          a.is_afrobeats  AS "isAfrobeats",
          s.total_streams AS "actualValue"
        FROM artist_stream_summary s
        JOIN artists a ON a.id = s.artist_id
        LEFT JOIN milestone_events me
          ON me.artist_id = a.id
          AND me.song_id IS NULL
          AND me.metric = 'spotify_streams'
          AND me.threshold = ${threshold}
        WHERE s.total_streams >= ${threshold}
          AND me.id IS NULL
          AND a.entity_status = 'canonical'
      `);

      for (const row of crossed.rows as any[]) {
        await this.db
          .insert(milestoneEvents)
          .values({
            artistId: row.artistId,
            metric: 'spotify_streams',
            threshold,
            crossedAt: today,
            streamValueAtCrossing: row.actualValue,
            isAfrobeats: row.isAfrobeats,
          })
          .onConflictDoNothing();

        results.push({
          type: 'artist_streams',
          artistId: row.artistId,
          artistName: row.artistName,
          artistSlug: row.artistSlug,
          threshold,
          actualValue: Number(row.actualValue),
          crossedAt: today,
          isAfrobeats: row.isAfrobeats,
        });

        this.logger.log(
          `🔁 Bootstrap artist: ${row.artistName} — ${this.formatNumber(threshold)} streams`,
        );
      }
    }

    // Song stream bootstrap — uses song_stream_summary MV
    for (const threshold of SONG_STREAM_THRESHOLDS) {
      const crossed = await this.db.execute(sql`
        SELECT
          s.id                      AS "songId",
          s.title                   AS "songTitle",
          s.slug                    AS "songSlug",
          s.is_afrobeats            AS "isAfrobeats",
          a.id                      AS "artistId",
          a.name                    AS "artistName",
          a.slug                    AS "artistSlug",
          ss.total_spotify_streams  AS "actualValue"
        FROM song_stream_summary ss
        JOIN songs s ON s.id = ss.song_id
        JOIN artists a ON a.id = s.artist_id
        LEFT JOIN milestone_events me
          ON me.song_id = s.id
          AND me.metric = 'spotify_streams'
          AND me.threshold = ${threshold}
        WHERE ss.total_spotify_streams >= ${threshold}
          AND me.id IS NULL
          AND s.entity_status = 'canonical'
          AND s.merged_into_song_id IS NULL
      `);

      for (const row of crossed.rows as any[]) {
        await this.db
          .insert(milestoneEvents)
          .values({
            artistId: row.artistId,
            songId: row.songId,
            metric: 'spotify_streams',
            threshold,
            crossedAt: today,
            streamValueAtCrossing: row.actualValue,
            isAfrobeats: row.isAfrobeats,
          })
          .onConflictDoNothing();

        results.push({
          type: 'song_streams',
          artistId: row.artistId,
          songId: row.songId,
          artistName: row.artistName,
          artistSlug: row.artistSlug,
          songTitle: row.songTitle,
          songSlug: row.songSlug,
          threshold,
          actualValue: Number(row.actualValue),
          crossedAt: today,
          isAfrobeats: row.isAfrobeats,
        });

        this.logger.log(
          `🔁 Bootstrap song: "${row.songTitle}" by ${row.artistName} — ${this.formatNumber(threshold)}`,
        );
      }
    }

    if (results.length) {
      this.logger.log(
        `Bootstrap complete — inserted ${results.length} missing milestones`,
      );
    }

    return results;
  }

  // ── Tweet text generator ──────────────────────────────────────────────

  generateTweetText(milestone: DetectedMilestone): string {
    const num = this.formatNumber(milestone.threshold);
    const url = `https://tooxclusive.com/stats/artists/${milestone.artistSlug}`;

    if (milestone.type === 'artist_streams') {
      const afrobeatsTag = milestone.isAfrobeats ? ' #Afrobeats' : '';
      return `🚨 ${milestone.artistName} has crossed ${num} streams on Spotify!${afrobeatsTag}\n\n📊 Full stats: ${url}`;
    }

    if (milestone.type === 'song_streams') {
      const songUrl = `https://tooxclusive.com/stats/songs/${milestone.songSlug}`;
      return `🚨 "${milestone.songTitle}" by ${milestone.artistName} has crossed ${num} streams on Spotify!\n\n📊 Full stats: ${songUrl}`;
    }

    if (milestone.type === 'monthly_listeners') {
      return `🚨 ${milestone.artistName} now has ${num} monthly listeners on Spotify!\n\n📊 Full stats: ${url}`;
    }

    return '';
  }

  // ── Fetch unnotified milestones ───────────────────────────────────────

  async getUnnotified(): Promise<any[]> {
    const result = await this.db.execute(sql`
      SELECT
        me.*,
        a.name    AS "artistName",
        a.slug    AS "artistSlug",
        s.title   AS "songTitle",
        s.slug    AS "songSlug"
      FROM milestone_events me
      LEFT JOIN artists a ON a.id = me.artist_id
      LEFT JOIN songs s ON s.id = me.song_id
      WHERE me.notified_at IS NULL
      ORDER BY me.crossed_at DESC
    `);

    return result.rows as any[];
  }

  async markNotified(
    id: string,
    tweetId: string,
    tweetText: string,
  ): Promise<void> {
    await this.db.execute(sql`
      UPDATE milestone_events
      SET notified_at = NOW(),
          tweet_id    = ${tweetId},
          tweet_text  = ${tweetText}
      WHERE id = ${id}
    `);
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  private formatNumber(n: number): string {
    if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(0)}B`;
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(0)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
    return n.toString();
  }
}
