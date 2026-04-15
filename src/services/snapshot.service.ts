// src/modules/snapshots/snapshot.service.ts

import { Injectable, Logger } from '@nestjs/common';
import { KworbTotalsService } from '../scraper/services/kworb-totals.service';
import axios from 'axios';
import { SnapshotRepository } from 'src/repository/snapshot.repository';
import { ArtistsRepository } from 'src/repository/artists.repository';
import { SongScraperService } from './song-scraper.service';

@Injectable()
export class SnapshotService {
  private readonly logger = new Logger(SnapshotService.name);

  constructor(
    private readonly kworbTotals: KworbTotalsService,
    private readonly snapshotRepository: SnapshotRepository,
    private readonly artistsRepository: ArtistsRepository,
    private readonly songScraperService: SongScraperService,
  ) {}

  async snapshotAllArtists(): Promise<void> {
    const today = new Date().toISOString().split('T')[0];
    const allArtists = await this.artistsRepository.findAllWithSpotifyId();
    const batchSize = 5;
    let succeeded = 0;
    let failed = 0;

    this.logger.log(
      `[Artist snapshot] Starting for ${allArtists.length} artists`,
    );

    for (let i = 0; i < allArtists.length; i += batchSize) {
      const batch = allArtists.slice(i, i + batchSize);

      const results = await Promise.allSettled(
        batch.map((artist) =>
          this.snapshotArtistTotalsOnly(
            artist as { id: string; spotifyId: string; name: string },
            today,
          ),
        ),
      );

      for (let j = 0; j < results.length; j++) {
        const result = results[j];

        if (result.status === 'rejected') {
          const artist = batch[j];
          const reason =
            result.reason instanceof Error
              ? result.reason.message
              : String(result.reason);

          if (axios.isAxiosError(result.reason)) {
            const status = result.reason.response?.status;
            if (status === 404) {
              await this.artistsRepository.markKworbNotFound(artist.id);
            }
          }

          failed++;
          this.logger.error(
            `[Artist snapshot] Failed ${artist.spotifyId}: ${reason}`,
          );
        } else {
          succeeded++;
        }
      }

      if (i + batchSize < allArtists.length) {
        await this.sleep(5000);
      }
    }

    this.logger.log(
      `[Artist snapshot] Complete — ${succeeded} succeeded, ${failed} failed`,
    );
  }

  async snapshotAllSongs(): Promise<void> {
    const today = new Date().toISOString().split('T')[0];
    const allArtists = await this.artistsRepository.findAllWithSpotifyId();
    const batchSize = 3;
    let succeeded = 0;
    let failed = 0;
    let totalSongs = 0;

    this.logger.log(
      `[Song snapshot] Starting for ${allArtists.length} artists`,
    );

    for (let i = 0; i < allArtists.length; i += batchSize) {
      const batch = allArtists.slice(i, i + batchSize);

      const results = await Promise.allSettled(
        batch.map((artist) =>
          this.snapshotArtistSongs(
            artist as { id: string; spotifyId: string; name: string },
            today,
          ),
        ),
      );

      for (let j = 0; j < results.length; j++) {
        const result = results[j];

        if (result.status === 'rejected') {
          const artist = batch[j];
          const reason =
            result.reason instanceof Error
              ? result.reason.message
              : String(result.reason);

          if (axios.isAxiosError(result.reason)) {
            const status = result.reason.response?.status;
            if (status === 404) {
              await this.artistsRepository.markKworbNotFound(artist.id);
            }
          }

          failed++;
          this.logger.error(
            `[Song snapshot] Failed ${artist.spotifyId}: ${reason}`,
          );
        } else {
          totalSongs += result.value ?? 0;
          succeeded++;
        }
      }

      if (i + batchSize < allArtists.length) {
        await this.sleep(6000);
      }
    }

    this.logger.log(
      `[Song snapshot] Complete — ${succeeded} artists, ${totalSongs} songs, ${failed} failed`,
    );
  }

  async snapshotArtist(spotifyId: string): Promise<void> {
    const today = new Date().toISOString().split('T')[0];

    const artist = await this.artistsRepository.findBySpotifyId(spotifyId);
    if (!artist) {
      this.logger.warn(`Artist not found for spotifyId=${spotifyId}`);
      return;
    }
    if (!artist.spotifyId) return;

    const a = artist as { id: string; spotifyId: string; name: string };
    await this.snapshotArtistTotalsOnly(a, today);
    await this.snapshotArtistSongs(a, today);
  }

  private async snapshotArtistTotalsOnly(
    artist: { id: string; spotifyId: string; name: string },
    snapshotDate: string,
  ): Promise<void> {
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

    this.logger.log(
      `[Artist snapshot] ${artist.name} — ${payload.totals.totalStreams.toLocaleString()} streams`,
    );
  }

  private async snapshotArtistSongs(
    artist: { id: string; spotifyId: string; name: string },
    snapshotDate: string,
  ): Promise<number> {
    const payload = await this.kworbTotals.fetchArtistTotals(artist.spotifyId);

    await Promise.all(
      payload.songs.map(async (song) => {
        const dbSong = await this.songScraperService.findOrCreate({
          artistId: artist.id,
          title: song.title,
          spotifyTrackId: song.spotifyTrackId,
        });

        await this.snapshotRepository.upsertSongSnapshot({
          songId: dbSong.id,
          snapshotDate,
          spotifyStreams: song.streams,
          dailyStreams: song.dailyStreams,
        });
      }),
    );

    this.logger.log(
      `[Song snapshot] ${artist.name} — ${payload.songs.length} songs`,
    );

    return payload.songs.length;
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
