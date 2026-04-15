import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { ArtistsRepository } from 'src/repository/artists.repository';
import { ArtistsService } from 'src/services/artists.service';

const BATCH_SIZE = 100;
const REDIS_CURSOR_KEY = 'job:artist_enrichment:last_artist_id';
const REDIS_LOCK_KEY = 'job:artist_enrichment:lock';
const REDIS_LOCK_TTL_SECONDS = 60 * 10;

@Injectable()
export class ArtistEnrichmentJob {
  private readonly logger = new Logger(ArtistEnrichmentJob.name);

  constructor(
    private readonly artistsRepository: ArtistsRepository,
    private readonly artistsService: ArtistsService,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async runBatch(): Promise<void> {
    const locked = await this.acquireLock();
    if (!locked) {
      this.logger.warn('Artist enrichment already running — skipping');
      return;
    }

    try {
      this.logger.log('Artist enrichment batch starting');

      const cursor = await this.redis.get(REDIS_CURSOR_KEY);

      let batch = await this.artistsRepository.findUnenrichedBatch(
        BATCH_SIZE,
        cursor ?? undefined,
      );

      if (!batch.length && cursor) {
        await this.redis.del(REDIS_CURSOR_KEY);
        this.logger.log(
          'Cursor reset — reached end of ID range, restarting from earliest unenriched artist',
        );

        batch = await this.artistsRepository.findUnenrichedBatch(BATCH_SIZE);
      }

      if (!batch.length) {
        this.logger.log('No unenriched artists found — enrichment complete');
        return;
      }

      const spotifyIds = batch
        .map((a) => a.spotifyId)
        .filter((id): id is string => !!id);

      if (!spotifyIds.length) {
        const lastArtist = batch[batch.length - 1];
        await this.redis.set(REDIS_CURSOR_KEY, lastArtist.id);

        this.logger.log(
          `Batch had no Spotify IDs — advanced cursor after "${lastArtist.name}" (${lastArtist.id})`,
        );
        return;
      }

      await this.artistsService.enrichAndUpsert(spotifyIds);

      const lastArtist = batch[batch.length - 1];
      await this.redis.set(REDIS_CURSOR_KEY, lastArtist.id);

      this.logger.log(
        `Artist enrichment batch complete — ${spotifyIds.length} attempted. ` +
          `Next batch starts after "${lastArtist.name}" (${lastArtist.id})`,
      );
    } finally {
      await this.releaseLock();
    }
  }

  async triggerManually(resetCursor = false): Promise<void> {
    if (resetCursor) {
      await this.redis.del(REDIS_CURSOR_KEY);
      this.logger.log('Artist enrichment cursor reset manually');
    }

    await this.runBatch();
  }

  async getStatus(): Promise<{
    lastArtistId: string | null;
    nextBatchSize: number;
    cursorActive: boolean;
  }> {
    const lastArtistId = await this.redis.get(REDIS_CURSOR_KEY);

    const nextBatch = await this.artistsRepository.findUnenrichedBatch(
      BATCH_SIZE,
      lastArtistId ?? undefined,
    );

    return {
      lastArtistId,
      nextBatchSize: nextBatch.length,
      cursorActive: !!lastArtistId,
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
