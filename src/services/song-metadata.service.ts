import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosResponse } from 'axios';
import { SpotifyOfficialChartsService } from 'src/scraper/chart/spotify-official-charts.service';

export interface SpotifyTrackMetadata {
  spotifyTrackId: string;
  title: string;
  spotifyAlbumId: string;
  albumName: string;
  albumType: string;
  albumImageUrl: string | null;
  releaseDate: string | null; // was string
  totalTracks: number;
  durationMs: number;
  explicit: boolean;
}

@Injectable()
export class SongMetadataService {
  private readonly logger = new Logger(SongMetadataService.name);
  private tokenCache: { token: string; expiresAt: number } | null = null;

  constructor(
    private readonly spotifyOfficialChartsService: SpotifyOfficialChartsService,
  ) {}

  // ── Token management ──────────────────────────────────────────────────

  private async getFreshToken(): Promise<string> {
    if (
      this.tokenCache &&
      Date.now() < this.tokenCache.expiresAt - 5 * 60 * 1000
    ) {
      return this.tokenCache.token;
    }

    this.logger.log('Fetching Spotify Bearer token via Playwright...');
    const token = await this.spotifyOfficialChartsService.getBearerToken('ng');

    this.tokenCache = {
      token,
      expiresAt: Date.now() + 50 * 60 * 1000,
    };

    this.logger.log('Bearer token acquired ✓');
    return token;
  }

  // ── Auth ──────────────────────────────────────────────────────────────

  private async get<T>(path: string, retry = true): Promise<T> {
    const token = await this.getFreshToken();

    try {
      const response: AxiosResponse<T> = await axios.get(
        `https://api.spotify.com/v1${path}`,
        {
          headers: { Authorization: `Bearer ${token}` },
          timeout: 10_000,
        },
      );
      return response.data;
    } catch (err) {
      if (
        retry &&
        axios.isAxiosError(err) &&
        (err.response?.status === 401 || err.response?.status === 403)
      ) {
        this.logger.warn(
          'Spotify token rejected — forcing refresh and retrying',
        );
        this.tokenCache = null;
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
        if (axios.isAxiosError(err)) {
          this.logger.error(
            `Spotify /tracks error — ` +
              `status: ${err.response?.status}, ` +
              `body: ${JSON.stringify(err.response?.data)}, ` +
              `headers: ${JSON.stringify(err.response?.headers)}`,
          );
        } else {
          this.logger.error(
            `Failed to fetch track batch [${chunk.join(',')}]: ${(err as Error).message}`,
          );
        }
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
      releaseDate: this.normalizeReleaseDate(track.album?.release_date),
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

  private normalizeReleaseDate(date: string | undefined): string | null {
    if (!date) return null;
    if (/^\d{4}$/.test(date)) return `${date}-01-01`;
    if (/^\d{4}-\d{2}$/.test(date)) return `${date}-01`;
    return date;
  }
}
