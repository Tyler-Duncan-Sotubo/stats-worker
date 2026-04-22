import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { CertificationsService } from 'src/services/certifications.service';
import { ArtistsRepository } from 'src/repository/artists.repository';

const BATCH_SIZE = 50;
const STAGGER_MS = 100;
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
    const allArtists = await this.artistsRepository.findAllBasic();
    if (!allArtists.length) return;

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

    const results = await Promise.allSettled(
      batch.map(async (artist, i) => {
        await new Promise((r) => setTimeout(r, i * STAGGER_MS));
        await this.certificationsService.syncRiaaForArtist(
          artist.id,
          artist.name,
        );
      }),
    );

    const synced = results.filter((r) => r.status === 'fulfilled').length;
    const failed = results.filter((r) => r.status === 'rejected').length;

    results.forEach((r, i) => {
      if (r.status === 'rejected') {
        this.logger.error(
          `Failed: "${batch[i].name}" (${batch[i].id}) — ${(r.reason as Error).message}`,
        );
      }
    });

    await this.redis.set(REDIS_CURSOR_KEY, String(nextCursor));

    this.logger.log(
      `Batch complete — ${synced} synced, ${failed} failed. Next starts at ${nextCursor + 1}/${allArtists.length}`,
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
    const allArtists = await this.artistsRepository.findAllBasic();
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
