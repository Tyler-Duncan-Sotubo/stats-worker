import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { SongScraperService } from 'src/services/song-scraper.service';
import { SongsRepository } from 'src/repository/songs.repository';

const BATCH_SIZE = 100; // total songs picked per run
const CONCURRENCY = 10; // never exceed 10 Spotify requests at once
const DELAY_BETWEEN_GROUPS_MS = 500;
const REDIS_CURSOR_KEY = 'job:song_enrichment:cursor';
const REDIS_LOCK_KEY = 'job:song_enrichment:lock';
const REDIS_LOCK_TTL_SECONDS = 60 * 15;

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
        this.logger.log('No songs need enrichment — skipping');
        await this.redis.del(REDIS_CURSOR_KEY);
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

      let synced = 0;
      let skipped = 0;
      let failed = 0;

      for (let i = 0; i < batch.length; i += CONCURRENCY) {
        const group = batch.slice(i, i + CONCURRENCY);

        const results = await Promise.allSettled(
          group.map(async (song) => {
            if (!song.spotifyTrackId) {
              return { status: 'skipped' as const, song };
            }

            await this.songScraperService.enrichOne(
              song.artistId,
              song.spotifyTrackId,
            );

            return { status: 'synced' as const, song };
          }),
        );

        for (const result of results) {
          if (result.status === 'fulfilled') {
            if (result.value.status === 'synced') {
              synced += 1;
            } else {
              skipped += 1;
              this.logger.warn(
                `Skipping song "${result.value.song.title}" — no spotifyTrackId`,
              );
            }
          } else {
            failed += 1;
            this.logger.error(
              `Song enrichment failed — ${
                result.reason instanceof Error
                  ? result.reason.message
                  : String(result.reason)
              }`,
            );
          }
        }

        if (i + CONCURRENCY < batch.length) {
          await this.sleep(DELAY_BETWEEN_GROUPS_MS);
        }
      }

      await this.redis.set(REDIS_CURSOR_KEY, String(nextCursor));

      this.logger.log(
        `Batch complete — ${synced} synced, ${skipped} skipped, ${failed} failed. ` +
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

  private async sleep(ms: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}
