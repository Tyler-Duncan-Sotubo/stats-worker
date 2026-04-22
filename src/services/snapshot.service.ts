/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/prefer-promise-reject-errors */
import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import axios from 'axios';
import { KworbTotalsService } from '../scraper/services/kworb-totals.service';
import { SnapshotRepository } from 'src/repository/snapshot.repository';
import { ArtistsRepository } from 'src/repository/artists.repository';
import { SongScraperService } from './song-scraper.service';

const TIER_DAILY_MIN = 8_000_000;
const TIER_HIGH_MIN = 4_800_000;
const TIER_MID_MIN = 2_500_000;

// Tune per tier — daily can be aggressive, low should be gentle
const TIER_CONFIG: Record<TierFilter, { batchSize: number; sleepMs: number }> =
  {
    daily: { batchSize: 15, sleepMs: 1_000 },
    high: { batchSize: 15, sleepMs: 1_000 },
    mid: { batchSize: 10, sleepMs: 1_500 },
    low: { batchSize: 5, sleepMs: 3_000 },
    all: { batchSize: 5, sleepMs: 3_000 },
  };

const redisKey = (spotifyId: string, date: string) =>
  `snapshot:done:${spotifyId}:${date}`;

const TTL_SECONDS = 30 * 60 * 60;

export type TierFilter = 'daily' | 'high' | 'mid' | 'low' | 'all';

function tierOf(monthlyListeners: number): 'daily' | 'high' | 'mid' | 'low' {
  if (monthlyListeners >= TIER_DAILY_MIN) return 'daily';
  if (monthlyListeners >= TIER_HIGH_MIN) return 'high';
  if (monthlyListeners >= TIER_MID_MIN) return 'mid';
  return 'low';
}

function shouldRun(monthlyListeners: number, filter: TierFilter): boolean {
  if (filter === 'all') return true;
  return tierOf(monthlyListeners) === filter;
}

@Injectable()
export class SnapshotService {
  private readonly logger = new Logger(SnapshotService.name);

  constructor(
    private readonly kworbTotals: KworbTotalsService,
    private readonly snapshotRepository: SnapshotRepository,
    private readonly artistsRepository: ArtistsRepository,
    private readonly songScraperService: SongScraperService,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async runAll(filter: TierFilter = 'all', date?: string): Promise<void> {
    const snapshotDate = date ?? new Date().toISOString().split('T')[0];
    const allArtists = await this.artistsRepository.findAllWithSpotifyId();

    const artists = allArtists.filter((a) =>
      shouldRun(a.monthlyListeners ?? 0, filter),
    );

    const { batchSize, sleepMs } = TIER_CONFIG[filter];

    this.logger.log(
      `Snapshot starting — filter=${filter}, date=${snapshotDate}, candidates=${artists.length}/${allArtists.length}, batchSize=${batchSize}, sleepMs=${sleepMs}`,
    );

    let succeeded = 0;
    let skipped = 0;
    let failed = 0;
    let totalSongs = 0;
    let batchNum = 0;

    for (let i = 0; i < artists.length; i += batchSize) {
      const batch = artists.slice(i, i + batchSize);
      batchNum++;

      const results = await Promise.allSettled(
        batch.map((artist) =>
          this.snapshotArtist(
            artist as { id: string; spotifyId: string; name: string },
            snapshotDate,
          ),
        ),
      );

      for (let j = 0; j < results.length; j++) {
        const result = results[j];
        const artist = batch[j];

        if (result.status === 'rejected') {
          if (result.reason === 'SKIP') {
            skipped++;
          } else {
            if (axios.isAxiosError(result.reason)) {
              const status = result.reason.response?.status;
              if (status === 404) {
                await this.artistsRepository.markKworbNotFound(artist.id);
              }
            }
            failed++;
            this.logger.error(
              `Snapshot failed ${artist.spotifyId}: ${result.reason instanceof Error ? result.reason.message : String(result.reason)}`,
            );
          }
        } else {
          succeeded++;
          totalSongs += result.value ?? 0;
        }
      }

      if (batchNum % 100 === 0) {
        this.logger.log(
          `Snapshot progress — ${succeeded} done, ${skipped} skipped, ${failed} failed`,
        );
      }

      if (i + batchSize < artists.length) {
        await this.sleep(sleepMs);
      }
    }

    this.logger.log(
      `Snapshot complete — ${succeeded} artists, ${totalSongs} songs, ${skipped} skipped, ${failed} failed`,
    );
  }

  private async snapshotArtist(
    artist: { id: string; spotifyId: string; name: string },
    snapshotDate: string,
  ): Promise<number> {
    const key = redisKey(artist.spotifyId, snapshotDate);

    // Check Redis first — cheapest check
    const alreadyDone = await this.redis.get(key);
    if (alreadyDone) return Promise.reject('SKIP');

    // DB check second
    const existsInDb =
      await this.snapshotRepository.artistSnapshotExistsForDate(
        artist.id,
        snapshotDate,
      );
    if (existsInDb) {
      await this.redis.set(key, '1', 'EX', TTL_SECONDS);
      return Promise.reject('SKIP');
    }

    const payload = await this.kworbTotals.fetchArtistTotals(artist.spotifyId);

    await this.snapshotRepository.upsertArtistSnapshot({
      artistId: artist.id,
      snapshotDate,
      totalStreams: payload.totals.totalStreams,
      totalStreamsAsLead: payload.totals.totalStreamsAsLead,
      totalStreamsSolo: payload.totals.totalStreamsSolo,
      totalStreamsAsFeature: payload.totals.totalStreamsAsFeature,
      dailyStreams: payload.totals.dailyStreams,
      dailyStreamsAsLead: payload.totals.dailyStreamsAsLead,
      dailyStreamsAsFeature: payload.totals.dailyStreamsAsFeature,
      trackCount: payload.totals.trackCount,
      sourceUpdatedAt: this.normalizeKworbDate(payload.totals.lastUpdated),
    });

    // Process songs concurrently instead of sequentially
    const songResults = await Promise.allSettled(
      payload.songs.map((song) =>
        this.snapshotSong(song, artist.id, snapshotDate),
      ),
    );

    const songCount = songResults.filter(
      (r) => r.status === 'fulfilled' && r.value,
    ).length;

    await this.redis.set(key, '1', 'EX', TTL_SECONDS);
    return songCount;
  }

  private async snapshotSong(
    song: {
      title: string;
      spotifyTrackId: string;
      streams: number;
      dailyStreams: number;
      isFeature: boolean;
    },
    artistId: string,
    snapshotDate: string,
  ): Promise<boolean> {
    try {
      if (song.isFeature) {
        const existing = await this.snapshotRepository.findBySpotifyTrackId(
          song.spotifyTrackId,
        );
        if (!existing) return false;

        await Promise.all([
          this.snapshotRepository.upsertSongSnapshot({
            songId: existing.id,
            snapshotDate,
            spotifyStreams: song.streams,
            dailyStreams: song.dailyStreams,
          }),
          this.snapshotRepository.ensureFeatureLink(existing.id, artistId),
        ]);
      } else {
        const dbSong = await this.songScraperService.findOrCreate({
          artistId,
          title: song.title,
          spotifyTrackId: song.spotifyTrackId,
        });

        await this.snapshotRepository.upsertSongSnapshot({
          songId: dbSong.id,
          snapshotDate,
          spotifyStreams: song.streams,
          dailyStreams: song.dailyStreams,
        });
      }
      return true;
    } catch {
      return false;
    }
  }
  private normalizeKworbDate(value?: string | null): string | null {
    if (!value) return null;
    const normalized = value.replace(/\//g, '-');
    return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : null;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
