import { Inject, Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { eq, isNull } from 'drizzle-orm';

import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { songs, artists } from 'src/infrastructure/drizzle/schema';
import { SpotifyOfficialChartsService } from '../chart/spotify-official-charts.service';

export interface BackfillResult {
  total: number;
  matched: number;
  skipped: number;
  needsReview: number;
}

interface SpotifyTrack {
  id: string;
  name: string;
  artists: { id: string; name: string }[];
  external_urls: { spotify: string };
}

interface TokenCache {
  token: string;
  expiresAt: number;
}

@Injectable()
export class SpotifyTrackBackfillService {
  private readonly logger = new Logger(SpotifyTrackBackfillService.name);
  private tokenCache: TokenCache | null = null;

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly spotifyOfficialChartsService: SpotifyOfficialChartsService,
  ) {}

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

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

  // ── Normalisation helpers ─────────────────────────────────────────────

  private normalizeForMatch(value: string): string {
    return value
      .toLowerCase()
      .trim()
      .replace(/\s*\(feat\.?.*?\)/gi, '')
      .replace(/\s*\(ft\.?.*?\)/gi, '')
      .replace(/\s*\[feat\.?.*?\]/gi, '')
      .replace(/\s*featuring\s+.*/gi, '')
      .replace(/[^\p{L}\p{N}\s]/gu, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private titlesMatch(dbTitle: string, spotifyTitle: string): boolean {
    return (
      this.normalizeForMatch(dbTitle) === this.normalizeForMatch(spotifyTitle)
    );
  }

  private artistMatches(
    dbArtistName: string,
    spotifyArtists: { id: string; name: string }[],
  ): boolean {
    const normalizedDb = this.normalizeForMatch(dbArtistName);
    return spotifyArtists.some(
      (a) => this.normalizeForMatch(a.name) === normalizedDb,
    );
  }

  // ── Spotify search ────────────────────────────────────────────────────

  private async searchSpotify(
    title: string,
    artistName: string,
    token: string,
  ): Promise<SpotifyTrack[]> {
    const query = encodeURIComponent(
      `track:${this.normalizeForMatch(title)} artist:${this.normalizeForMatch(artistName)}`,
    );

    const url = `https://api.spotify.com/v1/search?q=${query}&type=track&limit=3`;

    const response = await axios.get(url, {
      timeout: 10_000,
      headers: {
        Authorization: `Bearer ${token}`,
        'App-Platform': 'WebPlayer',
        'Content-Type': 'application/json',
      },
      validateStatus: () => true,
    });

    if (response.status === 429) throw new Error('429');
    if (response.status === 401) throw new Error('401');
    if (response.status !== 200) {
      throw new Error(`Spotify returned ${response.status}`);
    }

    return (response.data?.tracks?.items ?? []) as SpotifyTrack[];
  }

  // ── Find best match ───────────────────────────────────────────────────

  private findMatch(
    dbTitle: string,
    dbArtistName: string,
    dbArtistSpotifyId: string | null,
    results: SpotifyTrack[],
  ): SpotifyTrack | null {
    // Priority 1 — title + Spotify artist ID (most confident)
    if (dbArtistSpotifyId) {
      const byId = results.find(
        (r) =>
          this.titlesMatch(dbTitle, r.name) &&
          r.artists.some((a) => a.id === dbArtistSpotifyId),
      );
      if (byId) return byId;
    }

    // Priority 2 — title + artist name
    const byName = results.find(
      (r) =>
        this.titlesMatch(dbTitle, r.name) &&
        this.artistMatches(dbArtistName, r.artists),
    );
    if (byName) return byName;

    return null;
  }

  // ── Write match to DB ─────────────────────────────────────────────────

  private async writeMatch(
    songId: string,
    spotifyTrackId: string,
    artistName: string,
    title: string,
    result: BackfillResult,
  ): Promise<void> {
    try {
      await this.db
        .update(songs)
        .set({ spotifyTrackId })
        .where(eq(songs.id, songId));

      this.logger.debug(
        `Matched: "${artistName} — ${title}" → ${spotifyTrackId}`,
      );
      result.matched++;
    } catch (dbErr) {
      const dbMsg = (dbErr as Error).message;

      if (dbMsg.includes('unique') || dbMsg.includes('duplicate')) {
        // spotifyTrackId already assigned to another song — likely a duplicate song row
        this.logger.warn(
          `Duplicate spotifyTrackId ${spotifyTrackId} for "${artistName} — ${title}" — flagging for review`,
        );
        await this.db
          .update(songs)
          .set({ needsReview: true })
          .where(eq(songs.id, songId));
        result.needsReview++;
      } else {
        throw dbErr;
      }
    }
  }

  // ── Main backfill ─────────────────────────────────────────────────────

  async backfill(
    options: {
      batchSize?: number;
      delayMs?: number;
      dryRun?: boolean;
    } = {},
  ): Promise<BackfillResult> {
    const { batchSize = 8247, delayMs = 1000, dryRun = false } = options;

    this.logger.log(
      `Starting backfill — dryRun: ${dryRun}, delayMs: ${delayMs}, batchSize: ${batchSize}`,
    );

    const rows = await this.db
      .select({
        id: songs.id,
        title: songs.title,
        artistId: songs.artistId,
        artistName: artists.name,
        artistSpotifyId: artists.spotifyId,
      })
      .from(songs)
      .innerJoin(artists, eq(songs.artistId, artists.id))
      .where(isNull(songs.spotifyTrackId))
      .limit(batchSize);

    this.logger.log(`Found ${rows.length} songs without spotifyTrackId`);

    const result: BackfillResult = {
      total: rows.length,
      matched: 0,
      skipped: 0,
      needsReview: 0,
    };

    let token = await this.getFreshToken();
    let requestCount = 0;

    for (const row of rows) {
      let retries = 0;
      let processed = false;

      while (!processed && retries < 3) {
        try {
          if (requestCount > 0 && requestCount % 200 === 0) {
            this.tokenCache = null;
            token = await this.getFreshToken();
            this.logger.log(`Token refreshed at request ${requestCount}`);
          }

          const results = await this.searchSpotify(
            row.title,
            row.artistName,
            token,
          );

          requestCount++;

          const match = this.findMatch(
            row.title,
            row.artistName,
            row.artistSpotifyId,
            results,
          );

          if (match) {
            if (!dryRun) {
              await this.writeMatch(
                row.id,
                match.id,
                row.artistName,
                row.title,
                result,
              );
            } else {
              this.logger.debug(
                `[dryRun] Would match: "${row.artistName} — ${row.title}" → ${match.id}`,
              );
              result.matched++;
            }
          } else if (results.length > 0) {
            if (!dryRun) {
              await this.db
                .update(songs)
                .set({ needsReview: true })
                .where(eq(songs.id, row.id));
            }
            this.logger.debug(
              `No confident match: "${row.artistName} — ${row.title}" — flagged`,
            );
            result.needsReview++;
          } else {
            this.logger.debug(
              `No results: "${row.artistName} — ${row.title}" — skipped`,
            );
            result.skipped++;
          }

          processed = true;
          await this.sleep(delayMs);
        } catch (err) {
          const msg = (err as Error).message;

          if (msg.includes('401')) {
            this.logger.warn('Token expired — refreshing...');
            this.tokenCache = null;
            token = await this.getFreshToken();
            retries++;
            await this.sleep(2000);
            continue;
          }

          if (msg.includes('429')) {
            const backoff = 30_000 * (retries + 1);
            this.logger.warn(
              `Rate limited — backing off ${backoff / 1000}s (retry ${retries + 1}/3)...`,
            );
            await this.sleep(backoff);
            retries++;
            continue;
          }

          this.logger.error(
            `Error on "${row.artistName} — ${row.title}": ${msg}`,
          );
          result.skipped++;
          processed = true;
        }
      }

      if (!processed) {
        this.logger.warn(
          `Skipping "${row.artistName} — ${row.title}" after 3 retries`,
        );
        result.skipped++;
      }
    }

    this.logger.log(
      `Backfill complete — matched: ${result.matched}, needsReview: ${result.needsReview}, skipped: ${result.skipped}`,
    );

    return result;
  }
}
