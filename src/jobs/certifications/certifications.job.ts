import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { CertificationsService } from 'src/services/certifications.service';
import { ArtistsRepository } from 'src/repository/artists.repository';

const BATCH_SIZE = 10;
const DELAY_BETWEEN_MS = 2000;
const REDIS_CURSOR_KEY = 'cron:riaa_sync:cursor';

@Injectable()
export class CertificationsJob {
  private readonly logger = new Logger(CertificationsJob.name);

  constructor(
    private readonly certificationsService: CertificationsService,
    private readonly artistsRepository: ArtistsRepository,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async runBatch(): Promise<void> {
    this.logger.log('RIAA batch sync starting');

    const allArtists = await this.artistsRepository.findAllBasic();
    if (!allArtists.length) {
      this.logger.log('No artists found — skipping');
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

    let synced = 0;
    let failed = 0;

    for (const artist of batch) {
      try {
        await this.certificationsService.syncRiaaForArtist(
          artist.id,
          artist.name,
        );
        synced++;
      } catch (err) {
        failed++;
        this.logger.error(
          `Failed: "${artist.name}" (${artist.id}) — ${(err as Error).message}`,
        );
      }

      await new Promise((resolve) => setTimeout(resolve, DELAY_BETWEEN_MS));
    }

    await this.redis.set(REDIS_CURSOR_KEY, String(nextCursor));

    this.logger.log(
      `Batch complete — ${synced} synced, ${failed} failed. Next run starts at artist ${nextCursor + 1}`,
    );
  }

  async triggerManually(resetCursor = false): Promise<void> {
    if (resetCursor) {
      await this.redis.del(REDIS_CURSOR_KEY);
      this.logger.log('Cursor reset manually');
    }

    await this.runBatch();
  }

  async getStatus(): Promise<{
    cursor: number;
    totalArtists: number;
    percentComplete: number;
    nextBatch: string;
  }> {
    const allArtists = await this.artistsRepository.findAllWithSpotifyId();
    const cursorStr = await this.redis.get(REDIS_CURSOR_KEY);
    const cursor = cursorStr ? parseInt(cursorStr, 10) : 0;

    return {
      cursor,
      totalArtists: allArtists.length,
      percentComplete: allArtists.length
        ? Math.round((cursor / allArtists.length) * 100)
        : 0,
      nextBatch: `Artists ${cursor + 1}–${Math.min(cursor + BATCH_SIZE, allArtists.length)}`,
    };
  }
}
