import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosResponse } from 'axios';
import { SpotifyOfficialChartsService } from 'src/scraper/chart/spotify-official-charts.service';

export interface SpotifyArtistMetadata {
  spotifyId: string;
  name: string;
  imageUrl: string | null;
  followers: number;
  popularity: number;
  genres: string[];
}

@Injectable()
export class SpotifyMetadataService {
  private readonly logger = new Logger(SpotifyMetadataService.name);
  private tokenCache: { token: string; expiresAt: number } | null = null;

  constructor(
    private readonly config: ConfigService,
    private readonly spotifyOfficialChartsService: SpotifyOfficialChartsService,
  ) {}

  // ── Auth ──────────────────────────────────────────────────────────────
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
      return response.data as unknown as T;
    } catch (err) {
      if (
        retry &&
        axios.isAxiosError(err) &&
        (err.response?.status === 401 || err.response?.status === 403)
      ) {
        this.logger.warn('Token rejected — refreshing and retrying');
        return this.get<T>(path, false);
      }
      throw err;
    }
  }

  // ── Single artist ─────────────────────────────────────────────────────

  async fetchArtistMetadata(spotifyId: string): Promise<SpotifyArtistMetadata> {
    const data = await this.get<any>(`/artists/${spotifyId}`);
    return this.mapArtist(data);
  }

  // ── Batch artists — up to 50 per Spotify request ──────────────────────

  async fetchMultipleArtists(
    spotifyIds: string[],
  ): Promise<SpotifyArtistMetadata[]> {
    const results: SpotifyArtistMetadata[] = [];
    const chunks = this.chunk(spotifyIds, 50);

    for (const chunk of chunks) {
      try {
        const data = await this.get<any>(`/artists?ids=${chunk.join(',')}`);

        for (const artist of data.artists ?? []) {
          if (!artist) continue;
          results.push(this.mapArtist(artist));
        }

        if (chunks.length > 1) {
          await this.sleep(300);
        }
      } catch (err) {
        this.logger.error(
          `Failed to fetch artist batch: ${(err as Error).message}`,
        );
      }
    }

    return results;
  }

  // ── Mapping ───────────────────────────────────────────────────────────

  private mapArtist(data: any): SpotifyArtistMetadata {
    return {
      spotifyId: data.id,
      name: data.name ?? '',
      imageUrl: data.images?.[0]?.url ?? null,
      followers: data.followers?.total ?? 0,
      popularity: data.popularity ?? 0,
      genres: data.genres ?? [],
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
