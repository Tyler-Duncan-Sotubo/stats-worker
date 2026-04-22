/* eslint-disable @typescript-eslint/no-unnecessary-type-assertion */
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosResponse } from 'axios';
import { SpotifyAuthService } from './spotify-auth.service';

export interface SpotifyTrackMetadata {
  spotifyTrackId: string;
  title: string;
  spotifyAlbumId: string;
  albumName: string;
  albumType: string;
  albumImageUrl: string | null;
  releaseDate: string;
  totalTracks: number;
  durationMs: number;
  explicit: boolean;
}

@Injectable()
export class SongMetadataService {
  private readonly logger = new Logger(SongMetadataService.name);
  private accessToken: string | null = null;
  private tokenExpiresAt = 0;

  constructor(
    private readonly config: ConfigService,
    private readonly spotifyAuth: SpotifyAuthService,
  ) {}

  // ── Auth ──────────────────────────────────────────────────────────────
  private async get<T>(path: string, retry = true): Promise<T> {
    const token = await this.spotifyAuth.getAccessToken();

    try {
      const response: AxiosResponse<T> = await axios.get(
        `https://api.spotify.com/v1${path}`,
        {
          headers: { Authorization: `Bearer ${token}` },
          timeout: 10_000,
        },
      );
      return response.data as T;
    } catch (err) {
      if (
        retry &&
        axios.isAxiosError(err) &&
        (err.response?.status === 401 || err.response?.status === 403)
      ) {
        this.logger.warn(
          'Spotify token rejected — forcing refresh and retrying',
        );
        this.spotifyAuth.invalidate();
        return this.get<T>(path, false);
      }
      throw err;
    }
  }

  // ── Single track ──────────────────────────────────────────────────────

  async fetchTrack(spotifyTrackId: string): Promise<SpotifyTrackMetadata> {
    const data = await this.get<any>(`/tracks/${spotifyTrackId}`);
    return this.mapTrack(data);
  }

  // ── Batch tracks — up to 50 per Spotify request ───────────────────────

  async fetchTracks(
    spotifyTrackIds: string[],
  ): Promise<SpotifyTrackMetadata[]> {
    const results: SpotifyTrackMetadata[] = [];
    const chunks = this.chunk(spotifyTrackIds, 50);

    for (const chunk of chunks) {
      try {
        const data = await this.get<any>(`/tracks?ids=${chunk.join(',')}`);

        for (const track of data.tracks ?? []) {
          if (!track) continue;
          results.push(this.mapTrack(track));
        }

        if (chunks.length > 1) {
          await this.sleep(300);
        }
      } catch (err) {
        this.logger.error(
          `Failed to fetch track batch [${chunk.join(',')}]: ${(err as Error).message}`,
        );
      }
    }

    return results;
  }

  // ── Mapping ───────────────────────────────────────────────────────────

  private mapTrack(track: any): SpotifyTrackMetadata {
    return {
      spotifyTrackId: track.id,
      title: track.name,
      spotifyAlbumId: track.album?.id ?? '',
      albumName: track.album?.name ?? '',
      albumType: track.album?.album_type ?? 'album',
      albumImageUrl: track.album?.images?.[0]?.url ?? null,
      releaseDate: track.album?.release_date ?? '',
      totalTracks: track.album?.total_tracks ?? 0,
      durationMs: track.duration_ms ?? 0,
      explicit: track.explicit ?? false,
    };
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  private chunk<T>(arr: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(arr.length / size) }, (_, i) =>
      arr.slice(i * size, i * size + size),
    );
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
