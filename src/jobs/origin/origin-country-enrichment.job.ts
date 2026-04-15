import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { ArtistsRepository } from 'src/repository/artists.repository';
import { OriginCountryEnrichmentService } from 'src/scraper/origin/origin-country-enrichment.service';

const BATCH_SIZE = 40;
const DELAY_BETWEEN_MS = 1200;
const REDIS_CURSOR_KEY = 'job:origin_country_enrichment:cursor';
const REDIS_LOCK_KEY = 'job:origin_country_enrichment:lock';
const REDIS_LOCK_TTL_SECONDS = 60 * 10; // 10 mins

@Injectable()
export class OriginCountryEnrichmentJob {
  private readonly logger = new Logger(OriginCountryEnrichmentJob.name);

  constructor(
    private readonly artistsRepository: ArtistsRepository,
    private readonly originCountryEnrichmentService: OriginCountryEnrichmentService,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async runBatch(): Promise<void> {
    const lock = await this.acquireLock();
    if (!lock) {
      this.logger.warn('Origin country enrichment already running — skipping');
      return;
    }

    try {
      this.logger.log('Origin country enrichment batch starting');

      const allArtists =
        await this.artistsRepository.findMissingOriginCountryBasic();
      if (!allArtists.length) {
        return;
      }

      const cursorStr = await this.redis.get(REDIS_CURSOR_KEY);
      let cursor = cursorStr ? parseInt(cursorStr, 10) : 0;

      if (cursor >= allArtists.length) {
        cursor = 0;
        this.logger.log(
          `Cursor reset — full list of ${allArtists.length} artists completed, starting over`,
        );
      }

      const batch = allArtists.slice(cursor, cursor + BATCH_SIZE);
      const nextCursor = cursor + batch.length;

      this.logger.log(
        `Processing artists ${cursor + 1}–${nextCursor} of ${allArtists.length}`,
      );

      let enriched = 0;
      let skipped = 0;
      let failed = 0;

      for (const artist of batch) {
        try {
          if (artist.originCountry) {
            skipped += 1;
            continue;
          }

          const country =
            await this.originCountryEnrichmentService.enrichArtistIfMissing(
              artist.id,
              artist.name,
            );

          if (country) {
            enriched += 1;
          } else {
            skipped += 1;
          }
        } catch (err) {
          failed += 1;
          this.logger.error(
            `Failed: "${artist.name}" (${artist.id}) — ${(err as Error).message}`,
          );
        }

        await new Promise((resolve) => setTimeout(resolve, DELAY_BETWEEN_MS));
      }

      await this.redis.set(REDIS_CURSOR_KEY, String(nextCursor));

      this.logger.log(
        `Batch complete — ${enriched} enriched, ${skipped} skipped, ${failed} failed. ` +
          `Next run starts at artist ${nextCursor + 1}`,
      );
    } finally {
      await this.releaseLock();
    }
  }

  async triggerManually(resetCursor = false): Promise<void> {
    if (resetCursor) {
      await this.redis.del(REDIS_CURSOR_KEY);
      this.logger.log('Origin country cursor reset manually');
    }

    await this.runBatch();
  }

  async getStatus(): Promise<{
    cursor: number;
    totalArtists: number;
    percentComplete: number;
    nextBatch: string;
  }> {
    const allArtists = await this.artistsRepository.findAllBasic();
    const cursorStr = await this.redis.get(REDIS_CURSOR_KEY);
    const cursor = cursorStr ? parseInt(cursorStr, 10) : 0;

    return {
      cursor,
      totalArtists: allArtists.length,
      percentComplete: allArtists.length
        ? Math.round((cursor / allArtists.length) * 100)
        : 0,
      nextBatch: `Artists ${cursor + 1}–${Math.min(
        cursor + BATCH_SIZE,
        allArtists.length,
      )}`,
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
