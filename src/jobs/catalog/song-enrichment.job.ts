import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { SongScraperService } from 'src/services/song-scraper.service';
import { SongsRepository } from 'src/repository/songs.repository';

const BATCH_SIZE = 50; // matches Spotify's batch limit — 1 API call per run
const REDIS_CURSOR_KEY = 'job:song_enrichment:cursor';
const REDIS_LOCK_KEY = 'job:song_enrichment:lock';
const REDIS_LOCK_TTL_SECONDS = 60 * 5;

@Injectable()
export class SongEnrichmentJob {
  private readonly logger = new Logger(SongEnrichmentJob.name);

  constructor(
    private readonly songScraperService: SongScraperService,
    private readonly songsRepository: SongsRepository,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async runBatch(): Promise<void> {
    const locked = await this.acquireLock();
    if (!locked) {
      this.logger.warn('Song enrichment already running — skipping');
      return;
    }

    try {
      this.logger.log('Song enrichment job starting');

      const pending =
        await this.songsRepository.findSongsNeedingEnrichment(10_000);

      if (!pending.length) {
        await this.redis.del(REDIS_CURSOR_KEY);
        this.logger.log('No songs pending enrichment');
        return;
      }

      const cursorStr = await this.redis.get(REDIS_CURSOR_KEY);
      let cursor = cursorStr ? parseInt(cursorStr, 10) : 0;

      if (cursor >= pending.length) {
        cursor = 0;
        this.logger.log(
          `Cursor reset — all ${pending.length} pending songs processed, starting over`,
        );
      }

      const batch = pending.slice(cursor, cursor + BATCH_SIZE);
      const nextCursor = cursor + batch.length;

      this.logger.log(
        `Processing songs ${cursor + 1}–${nextCursor} of ${pending.length} pending`,
      );

      // Separate enrichable from skippable upfront
      const enrichable = batch.filter((s) => !!s.spotifyTrackId);
      const skipped = batch.filter((s) => !s.spotifyTrackId);

      skipped.forEach((s) =>
        this.logger.warn(`Skipping "${s.title}" — no spotifyTrackId`),
      );

      let synced = 0;
      let failed = 0;

      if (enrichable.length) {
        // Group by artistId so enrichMany can batch album upserts per artist
        const byArtist = new Map<string, typeof enrichable>();

        for (const song of enrichable) {
          const group = byArtist.get(song.artistId) ?? [];
          group.push(song);
          byArtist.set(song.artistId, group);
        }

        for (const [artistId, songs] of byArtist) {
          const spotifyTrackIds = songs.map((s) => s.spotifyTrackId as string);

          try {
            const results = await this.songScraperService.enrichMany(
              artistId,
              spotifyTrackIds,
            );
            synced += results.length;
            failed += songs.length - results.length;
          } catch (err) {
            failed += songs.length;
            this.logger.error(
              `[Artist ${artistId}] enrichMany failed: ${(err as Error).message}`,
            );
          }
        }
      }

      await this.redis.set(REDIS_CURSOR_KEY, String(nextCursor));

      this.logger.log(
        `Batch complete — ${synced} synced, ${skipped.length} skipped, ${failed} failed. ` +
          `Next run starts at song ${nextCursor + 1}`,
      );
    } finally {
      await this.releaseLock();
    }
  }

  async triggerManually(resetCursor = false): Promise<void> {
    if (resetCursor) {
      await this.redis.del(REDIS_CURSOR_KEY);
      this.logger.log('Song enrichment cursor reset manually');
    }

    await this.runBatch();
  }

  async getStatus(): Promise<{
    cursor: number;
    totalPending: number;
    percentComplete: number;
    nextBatch: string;
  }> {
    const pending =
      await this.songsRepository.findSongsNeedingEnrichment(10_000);

    const cursorStr = await this.redis.get(REDIS_CURSOR_KEY);
    const cursor = cursorStr ? parseInt(cursorStr, 10) : 0;

    return {
      cursor,
      totalPending: pending.length,
      percentComplete: pending.length
        ? Math.round((cursor / pending.length) * 100)
        : 100,
      nextBatch: `Songs ${cursor + 1}–${Math.min(cursor + BATCH_SIZE, pending.length)}`,
    };
  }

  private async acquireLock(): Promise<boolean> {
    const result = await this.redis.set(
      REDIS_LOCK_KEY,
      '1',
      'EX',
      REDIS_LOCK_TTL_SECONDS,
      'NX',
    );

    return result === 'OK';
  }

  private async releaseLock(): Promise<void> {
    await this.redis.del(REDIS_LOCK_KEY);
  }
}
