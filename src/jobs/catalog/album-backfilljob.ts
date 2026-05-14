/* eslint-disable @typescript-eslint/no-unsafe-return */
import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import { SongScraperService } from 'src/services/song-scraper.service';
import { SongsRepository } from 'src/repository/songs.repository';

@Injectable()
export class AlbumBackfillJob {
  private readonly logger = new Logger(AlbumBackfillJob.name);

  private readonly BATCH_SIZE = 5; // was 50
  private readonly LOCK_TTL = 60 * 2; // drop to 2 min since batch is tiny now
  private readonly CURSOR_KEY = 'job:album_backfill:cursor';
  private readonly LOCK_KEY = 'job:album_backfill:lock';

  constructor(
    private readonly songsRepository: SongsRepository,
    private readonly songScraperService: SongScraperService,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async runBatch(): Promise<void> {
    const locked = await this.acquireLock();
    if (!locked) {
      this.logger.warn('Album backfill already running — skipping');
      return;
    }

    try {
      // Songs that are enriched but have no albumId yet
      const pending =
        await this.songsRepository.findEnrichedSongsWithoutAlbum(10_000);

      if (!pending.length) {
        this.logger.log('No songs missing album data');
        return;
      }

      const cursorStr = await this.redis.get(this.CURSOR_KEY);
      let cursor = cursorStr ? parseInt(cursorStr, 10) : 0;

      if (cursor >= pending.length) {
        cursor = 0;
        this.logger.log('Album backfill cursor reset');
      }

      const batch = pending.slice(cursor, cursor + this.BATCH_SIZE);
      const nextCursor = cursor + batch.length;

      // Group by artist — same pattern as your enrichment job
      const byArtist = new Map<string, typeof batch>();
      for (const song of batch) {
        const group = byArtist.get(song.artistId) ?? [];
        group.push(song);
        byArtist.set(song.artistId, group);
      }

      let synced = 0;
      let failed = 0;

      for (const [artistId, songs] of byArtist) {
        const trackIds = songs
          .map((s) => s.spotifyTrackId)
          .filter(Boolean) as string[];

        try {
          const results = await this.songScraperService.enrichMany(
            artistId,
            trackIds,
          );
          synced += results.length;
          failed += songs.length - results.length;
        } catch (err) {
          failed += songs.length;
          this.logger.error(
            `[Artist ${artistId}] album backfill failed: ${(err as Error).message}`,
          );
        }

        // gentle delay between artist groups
        await new Promise((r) => setTimeout(r, 500));
      }

      await this.redis.set(this.CURSOR_KEY, String(nextCursor));

      this.logger.log(
        `Album backfill — ${synced} synced, ${failed} failed. ` +
          `Next run starts at ${nextCursor + 1} of ${pending.length}`,
      );
    } finally {
      await this.releaseLock();
    }
  }

  // mirror your existing lock helpers
  private async acquireLock(): Promise<boolean> {
    const result = await this.redis.set(
      this.LOCK_KEY,
      '1',
      'EX',
      this.LOCK_TTL,
      'NX',
    );
    return result === 'OK';
  }

  private async releaseLock(): Promise<void> {
    await this.redis.del(this.LOCK_KEY);
  }
}
