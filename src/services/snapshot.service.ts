/* eslint-disable @typescript-eslint/prefer-promise-reject-errors */
import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import axios from 'axios';
import { KworbTotalsService } from '../scraper/services/kworb-totals.service';
import { SnapshotRepository } from 'src/repository/snapshot.repository';
import { ArtistsRepository } from 'src/repository/artists.repository';
import { SongScraperService } from './song-scraper.service';

const TIER_DAILY_MIN = 5_000_000;
const TIER_HIGH_MIN = 3_000_000;
const TIER_MID_MIN = 2_000_000;
const TIER_MID2_MIN = 1_500_000;
const TIER_MID3_MIN = 1_250_000;
const TIER_MID4_MIN = 1_000_000;
const TIER_LOW_MIN = 875_000;
const TIER_LOW2_MIN = 750_000;
const TIER_LOW3_MIN = 625_000;

const TIER_CONFIG: Record<TierFilter, { batchSize: number; sleepMs: number }> =
  {
    daily: { batchSize: 15, sleepMs: 1_000 },
    high: { batchSize: 15, sleepMs: 1_000 },
    mid: { batchSize: 10, sleepMs: 1_500 },
    mid2: { batchSize: 10, sleepMs: 1_500 },
    mid3: { batchSize: 10, sleepMs: 1_500 },
    mid4: { batchSize: 10, sleepMs: 1_500 },
    low: { batchSize: 5, sleepMs: 2_000 },
    low2: { batchSize: 5, sleepMs: 2_000 },
    low3: { batchSize: 5, sleepMs: 2_500 },
    low4: { batchSize: 5, sleepMs: 3_000 },
    all: { batchSize: 5, sleepMs: 3_000 },
  };

const redisKey = (spotifyId: string, date: string) =>
  `snapshot:done:${spotifyId}:${date}`;

const TTL_SECONDS = 30 * 60 * 60;

export type TierFilter =
  | 'daily'
  | 'high'
  | 'mid'
  | 'mid2'
  | 'mid3'
  | 'mid4'
  | 'low'
  | 'low2'
  | 'low3'
  | 'low4'
  | 'all';

function tierOf(monthlyListeners: number): Exclude<TierFilter, 'all'> {
  if (monthlyListeners >= TIER_DAILY_MIN) return 'daily';
  if (monthlyListeners >= TIER_HIGH_MIN) return 'high';
  if (monthlyListeners >= TIER_MID_MIN) return 'mid';
  if (monthlyListeners >= TIER_MID2_MIN) return 'mid2';
  if (monthlyListeners >= TIER_MID3_MIN) return 'mid3';
  if (monthlyListeners >= TIER_MID4_MIN) return 'mid4';
  if (monthlyListeners >= TIER_LOW_MIN) return 'low';
  if (monthlyListeners >= TIER_LOW2_MIN) return 'low2';
  if (monthlyListeners >= TIER_LOW3_MIN) return 'low3';
  return 'low4';
}

function shouldRun(monthlyListeners: number, filter: TierFilter): boolean {
  if (filter === 'all') return true;
  return tierOf(monthlyListeners) === filter;
}

const MIN_STREAMS_TO_SNAPSHOT = 1_000_000;

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

    const alreadyDone = await this.redis.get(key);
    if (alreadyDone) return Promise.reject('SKIP');

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

    const featureSongs = payload.songs.filter((s) => s.isFeature);
    const ownSongs = payload.songs.filter((s) => !s.isFeature);

    const featureMap = await this.snapshotRepository.findManyBySpotifyTrackIds(
      featureSongs.map((s) => s.spotifyTrackId),
    );

    const songSnapshots: {
      songId: string;
      snapshotDate: string;
      spotifyStreams: number | null;
      dailyStreams: number | null;
    }[] = [];

    const featureLinks: { songId: string; featuredArtistId: string }[] = [];

    for (const song of featureSongs) {
      const existing = featureMap.get(song.spotifyTrackId);
      if (!existing) continue;

      songSnapshots.push({
        songId: existing.id,
        snapshotDate,
        spotifyStreams: song.streams,
        dailyStreams: song.dailyStreams,
      });

      featureLinks.push({ songId: existing.id, featuredArtistId: artist.id });
    }

    for (const song of ownSongs) {
      try {
        const dbSong = await this.songScraperService.findOrCreate({
          artistId: artist.id,
          title: song.title,
          spotifyTrackId: song.spotifyTrackId,
        });

        songSnapshots.push({
          songId: dbSong.id,
          snapshotDate,
          spotifyStreams: song.streams,
          dailyStreams: song.dailyStreams,
        });
      } catch {
        // skip failed songs
      }
    }

    const significantSnapshots = songSnapshots.filter(
      (s) => (s.spotifyStreams ?? 0) >= MIN_STREAMS_TO_SNAPSHOT,
    );
    await Promise.all([
      this.snapshotRepository.bulkUpsertSongSnapshots(significantSnapshots),
      this.snapshotRepository.bulkEnsureFeatureLinks(featureLinks),
    ]);

    await this.redis.set(key, '1', 'EX', TTL_SECONDS);
    return significantSnapshots.length;
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
