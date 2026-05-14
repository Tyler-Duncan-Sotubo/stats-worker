/* eslint-disable @typescript-eslint/no-unnecessary-type-assertion */
/* eslint-disable @typescript-eslint/no-unsafe-return */
import { Inject, Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import * as cheerio from 'cheerio';
import slugify from 'slugify';
import { and, eq, inArray, isNotNull, isNull, sql } from 'drizzle-orm';

import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import {
  albums,
  songs,
  songAlbums,
  songFeatures,
  artists,
} from 'src/infrastructure/drizzle/schema';
import { EntityResolutionService } from 'src/services/entity-resolution.service';

export interface MissingTrack {
  spotifyTrackId: string;
  title: string;
  trackNumber: number;
  explicit: boolean;
  featuredArtists: { name: string; spotifyId: string | null }[];
}

export interface ScrapedAlbum {
  spotifyAlbumId: string;
  title: string;
  albumType: string;
  releaseDate: string | null;
  imageUrl: string | null;
  label: string | null;
  totalTracks: number;
  artistName: string;
  artistSpotifyId: null;
  tracks: ScrapedTrack[];
}

export interface ScrapedTrack {
  spotifyTrackId: string | null;
  title: string;
  trackNumber: number;
  durationMs: number | null;
  explicit: boolean;
  featuredArtists: { name: string; spotifyId: string | null }[];
}

export interface AlbumIngestResult {
  album: typeof albums.$inferSelect;
  linked: number;
  fuzzyLinked: number;
  created: number;
  missing: MissingTrack[];
}

@Injectable()
export class SpotifyAlbumScraperService {
  private readonly logger = new Logger(SpotifyAlbumScraperService.name);

  private readonly httpHeaders = {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    Accept:
      'text/html,application/xhtml+xml,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  };

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly entityResolutionService: EntityResolutionService,
  ) {}

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

  private makeSlug(value: string): string {
    return slugify(value, { lower: true, strict: true, trim: true });
  }

  private normalizeArtistName(name: string): string {
    return name
      .toLowerCase()
      .trim()
      .replace(/&/g, ' and ')
      .replace(/\bfeat\.?\b/gi, 'featuring')
      .replace(/[^\p{L}\p{N}\s]/gu, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private normalizeTitle(value: string): string {
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

  // ── Scrape ────────────────────────────────────────────────────────────

  async scrapeAlbum(albumId: string, debugMode = false): Promise<ScrapedAlbum> {
    const url = `https://open.spotify.com/embed/album/${albumId}`;

    this.logger.log(`Scraping album: ${url}`);

    const { data } = await axios.get<string>(url, {
      timeout: 15_000,
      headers: this.httpHeaders,
    });

    const $ = cheerio.load(data);
    const raw = $('#__NEXT_DATA__').text();

    if (!raw) {
      throw new Error(
        `__NEXT_DATA__ not found on album page ${albumId} — HTML length: ${data.length}`,
      );
    }

    const json = JSON.parse(raw);

    if (debugMode) {
      this.logger.warn(`Full __NEXT_DATA__:\n${JSON.stringify(json, null, 2)}`);
    }

    const entity = json?.props?.pageProps?.state?.data?.entity;

    if (!entity || entity.type !== 'album') {
      throw new Error(
        `Could not find album entity in __NEXT_DATA__ for ${albumId} — run with debug=true to inspect`,
      );
    }

    const tracks: ScrapedTrack[] = (entity.trackList ?? []).map(
      (track: any, index: number) => {
        const spotifyTrackId = track.uri?.split(':').pop() ?? null;

        const subtitleParts: string[] = (track.subtitle ?? '')
          .split(',')
          .map((s: string) => s.trim())
          .filter(Boolean);

        const featuredArtistNames = subtitleParts.slice(1);

        return {
          spotifyTrackId,
          title: track.title,
          trackNumber: index + 1,
          durationMs: track.duration ?? null,
          explicit:
            track.isExplicit === true ||
            (track.contentRatings?.labels ?? []).includes('EXPLICIT'),
          featuredArtists: featuredArtistNames.map((name: string) => ({
            name,
            spotifyId: null,
          })),
        };
      },
    );

    const images: { url: string; maxHeight: number }[] =
      entity.visualIdentity?.image ?? [];
    const imageUrl =
      images.sort((a, b) => b.maxHeight - a.maxHeight)[0]?.url ?? null;

    const mainArtistName: string = entity.subtitle ?? '';

    return {
      spotifyAlbumId: albumId,
      title: entity.name,
      albumType: 'album',
      releaseDate: entity.releaseDate?.isoString?.split('T')[0] ?? null,
      imageUrl,
      label: null,
      totalTracks: tracks.length,
      artistName: mainArtistName,
      artistSpotifyId: null,
      tracks,
    };
  }

  // ── Link song to album ────────────────────────────────────────────────

  private async linkSongToAlbum(
    songId: string,
    albumId: string,
    trackNumber: number,
    isPrimary: boolean,
  ): Promise<void> {
    await this.db
      .insert(songAlbums)
      .values({ songId, albumId, trackNumber, isPrimary })
      .onConflictDoNothing();

    if (isPrimary) {
      await this.db.update(songs).set({ albumId }).where(eq(songs.id, songId));
    }
  }

  // ── Resolve featured artists ──────────────────────────────────────────

  private async resolveFeaturedArtists(
    songId: string,
    primaryArtistId: string,
    featuredArtists: { name: string; spotifyId: string | null }[],
  ): Promise<void> {
    for (const featured of featuredArtists) {
      if (!featured.name) continue;

      const featuredArtist = await this.entityResolutionService.resolveArtist({
        name: featured.name,
        spotifyId: featured.spotifyId ?? undefined,
        source: 'spotify',
        allowCreate: false,
        markProvisionalIfCreated: false,
      });

      if (!featuredArtist || featuredArtist.id === primaryArtistId) continue;

      await this.db
        .insert(songFeatures)
        .values({ songId, featuredArtistId: featuredArtist.id })
        .onConflictDoNothing();
    }
  }

  // ── Ingest one album ──────────────────────────────────────────────────

  async ingestAlbum(
    albumId: string,
    options: {
      debugMode?: boolean;
      isPrimary?: boolean;
      createMissing?: boolean;
    } = {},
  ): Promise<AlbumIngestResult> {
    const {
      debugMode = false,
      isPrimary = true,
      createMissing = false,
    } = options;

    const scraped = await this.scrapeAlbum(albumId, debugMode);

    this.logger.log(
      `Scraped "${scraped.title}" — ${scraped.tracks.length} tracks`,
    );

    // 1. Resolve artist by normalizedName
    const normalizedName = this.normalizeArtistName(scraped.artistName);

    const [artist] = await this.db
      .select()
      .from(artists)
      .where(eq(artists.normalizedName, normalizedName))
      .limit(1);

    if (!artist) {
      throw new Error(
        `Artist "${scraped.artistName}" (normalized: "${normalizedName}") not found in DB. Ingest the artist first.`,
      );
    }

    this.logger.log(`Resolved artist "${artist.name}" (${artist.id})`);

    // 2. Upsert album
    const slug = this.makeSlug(`${artist.slug}-${scraped.title}`);

    const [album] = await this.db
      .insert(albums)
      .values({
        artistId: artist.id,
        title: scraped.title,
        slug,
        spotifyAlbumId: scraped.spotifyAlbumId,
        albumType: scraped.albumType,
        releaseDate: scraped.releaseDate,
        imageUrl: scraped.imageUrl,
        totalTracks: scraped.totalTracks,
        isAfrobeats: artist.isAfrobeats,
      })
      .onConflictDoUpdate({
        target: albums.spotifyAlbumId,
        set: {
          title: scraped.title,
          imageUrl: scraped.imageUrl,
          totalTracks: scraped.totalTracks,
        },
      })
      .returning();

    this.logger.log(`Upserted album "${album.title}" (${album.id})`);

    // 3. Pass 1 — match by spotifyTrackId
    const trackIds = scraped.tracks
      .map((t) => t.spotifyTrackId)
      .filter((id): id is string => !!id);

    const existingSongs = trackIds.length
      ? await this.db
          .select({ id: songs.id, spotifyTrackId: songs.spotifyTrackId })
          .from(songs)
          .where(inArray(songs.spotifyTrackId, trackIds))
      : [];

    const existingMap = new Map(
      existingSongs.map((s) => [s.spotifyTrackId, s.id]),
    );

    const firstPassMissing: ScrapedTrack[] = [];
    let linked = 0;

    for (const track of scraped.tracks) {
      if (!track.spotifyTrackId) {
        firstPassMissing.push(track);
        continue;
      }

      const songId = existingMap.get(track.spotifyTrackId);

      if (!songId) {
        firstPassMissing.push(track);
        this.logger.debug(
          `Pass 1 miss: "${track.title}" (${track.spotifyTrackId})`,
        );
        continue;
      }

      await this.linkSongToAlbum(
        songId,
        album.id,
        track.trackNumber,
        isPrimary,
      );
      await this.resolveFeaturedArtists(
        songId,
        artist.id,
        track.featuredArtists,
      );
      linked++;
    }

    // 4. Pass 2 — fuzzy match by normalized title against artist's songs
    const stillMissing: MissingTrack[] = [];
    let fuzzyLinked = 0;

    for (const track of firstPassMissing) {
      if (!track.spotifyTrackId) {
        // No track ID at all — skip fuzzy, can't create either
        this.logger.warn(
          `Track "${track.title}" has no spotifyTrackId — skipping`,
        );
        continue;
      }

      const normalizedTrackTitle = this.normalizeTitle(track.title);

      const [byTitle] = await this.db
        .select({ id: songs.id, title: songs.title })
        .from(songs)
        .where(
          and(
            eq(songs.artistId, artist.id),
            eq(songs.normalizedTitle, normalizedTrackTitle),
          ),
        )
        .limit(1);

      if (byTitle) {
        // Fuzzy matched — backfill spotifyTrackId and link
        try {
          await this.db
            .update(songs)
            .set({ spotifyTrackId: track.spotifyTrackId })
            .where(eq(songs.id, byTitle.id));

          await this.linkSongToAlbum(
            byTitle.id,
            album.id,
            track.trackNumber,
            isPrimary,
          );
          await this.resolveFeaturedArtists(
            byTitle.id,
            artist.id,
            track.featuredArtists,
          );

          this.logger.log(
            `Pass 2 fuzzy match: "${track.title}" → "${byTitle.title}" (${byTitle.id})`,
          );
          fuzzyLinked++;
        } catch (err) {
          const msg = (err as Error).message;
          if (msg.includes('unique') || msg.includes('duplicate')) {
            // spotifyTrackId already on another song — flag and skip
            this.logger.warn(
              `Duplicate spotifyTrackId ${track.spotifyTrackId} on fuzzy match "${track.title}" — skipping`,
            );
            continue;
          }
          throw err;
        }
        continue;
      }

      // Truly missing
      stillMissing.push({
        spotifyTrackId: track.spotifyTrackId,
        title: track.title,
        trackNumber: track.trackNumber,
        explicit: track.explicit,
        featuredArtists: track.featuredArtists,
      });
    }

    // 5. Pass 3 — create missing songs via EntityResolutionService
    let created = 0;

    if (createMissing && stillMissing.length) {
      this.logger.log(
        `Pass 3 — creating ${stillMissing.length} missing songs...`,
      );

      for (const track of stillMissing) {
        try {
          const song = await this.entityResolutionService.resolveSong({
            artistId: artist.id,
            artistSlug: artist.slug,
            title: track.title,
            spotifyTrackId: track.spotifyTrackId,
            source: 'spotify',
            allowCreate: true,
            markProvisionalIfCreated: true,
          });

          if (!song) continue;

          // Update explicit and durationMs if created
          await this.db
            .update(songs)
            .set({ explicit: track.explicit })
            .where(eq(songs.id, song.id));

          await this.linkSongToAlbum(
            song.id,
            album.id,
            track.trackNumber,
            isPrimary,
          );

          await this.resolveFeaturedArtists(
            song.id,
            artist.id,
            track.featuredArtists,
          );

          this.logger.log(
            `Pass 3 created: "${track.title}" (${track.spotifyTrackId})`,
          );
          created++;
        } catch (err) {
          this.logger.error(
            `Failed to create song "${track.title}": ${(err as Error).message}`,
          );
        }
      }
    }

    this.logger.log(
      `Album "${album.title}" — linked: ${linked}, fuzzyLinked: ${fuzzyLinked}, created: ${created}, missing: ${stillMissing.length - created}`,
    );

    return {
      album,
      linked,
      fuzzyLinked,
      created,
      missing: createMissing ? [] : stillMissing,
    };
  }

  // ── Ingest multiple albums ────────────────────────────────────────────

  async ingestAlbums(
    albumIds: string[],
    options: {
      debugMode?: boolean;
      isPrimary?: boolean;
      delayMs?: number;
      createMissing?: boolean;
    } = {},
  ): Promise<AlbumIngestResult[]> {
    const { delayMs = 2000, ...ingestOptions } = options;
    const results: AlbumIngestResult[] = [];

    for (const [index, albumId] of albumIds.entries()) {
      try {
        const result = await this.ingestAlbum(albumId, ingestOptions);
        results.push(result);
      } catch (err) {
        this.logger.error(
          `Failed to ingest album ${albumId}: ${(err as Error).message}`,
        );
      }

      if (index < albumIds.length - 1) {
        await this.sleep(delayMs);
      }
    }

    return results;
  }

  async scrapeArtistAlbumIds(artistSpotifyId: string): Promise<string[]> {
    const url = `https://kworb.net/spotify/artist/${artistSpotifyId}_albums.html`;

    this.logger.log(`Fetching discography from Kworb: ${url}`);

    const { data } = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent': 'tooXclusiveStatsBot/1.0 (+https://tooxclusive.com)',
        Accept: 'text/html,application/xhtml+xml',
      },
    });

    const $ = cheerio.load(data);
    const albumIds: string[] = [];

    // Filter compilations — Kworb marks them with ^ in the row text
    $('table tr').each((_, row) => {
      const rowText = $(row).text().trim();
      if (rowText.startsWith('^')) return;

      const link = $(row).find('a[href*="open.spotify.com/album/"]');
      if (!link.length) return;

      const href = link.attr('href') ?? '';
      const match = href.match(/open\.spotify\.com\/album\/([A-Za-z0-9]+)/);
      if (match) albumIds.push(match[1]);
    });

    this.logger.log(
      `Found ${albumIds.length} albums for artist ${artistSpotifyId}`,
    );

    return albumIds;
  }

  async backfillAfrobeatsAlbums(
    options: {
      includeGroups?: string[];
      createMissing?: boolean;
      delayMs?: number;
      limitArtists?: number;
    } = {},
  ): Promise<{
    artistsProcessed: number;
    albumsIngested: number;
    errors: string[];
  }> {
    const { createMissing = false, delayMs = 3000, limitArtists } = options;

    // Query afrobeats artists with spotifyId but no albums yet
    const afrobeatsArtists = await this.db
      .select({
        id: artists.id,
        name: artists.name,
        spotifyId: artists.spotifyId,
      })
      .from(artists)
      .leftJoin(albums, eq(albums.artistId, artists.id))
      .where(
        and(
          eq(artists.isAfrobeats, true),
          eq(artists.entityStatus, 'canonical'),
          isNotNull(artists.spotifyId),
          isNull(albums.id), // no albums yet
        ),
      )
      .groupBy(artists.id, artists.name, artists.spotifyId)
      .limit(limitArtists ?? 1000);

    this.logger.log(
      `Found ${afrobeatsArtists.length} afrobeats artists without albums`,
    );

    const result = {
      artistsProcessed: 0,
      albumsIngested: 0,
      errors: [] as string[],
    };

    for (const artist of afrobeatsArtists) {
      try {
        this.logger.log(`Processing "${artist.name}" (${artist.spotifyId})...`);

        const albumIds = await this.scrapeArtistAlbumIds(artist.spotifyId!);

        if (!albumIds.length) {
          this.logger.warn(`No albums found for "${artist.name}"`);
          result.artistsProcessed++;
          continue;
        }

        this.logger.log(
          `Found ${albumIds.length} albums for "${artist.name}" — ingesting...`,
        );

        const ingestResults = await this.ingestAlbums(albumIds, {
          isPrimary: true,
          createMissing,
          delayMs: 1500,
        });

        result.albumsIngested += ingestResults.length;
        result.artistsProcessed++;

        await this.sleep(delayMs);
      } catch (err) {
        const msg = `"${artist.name}": ${(err as Error).message}`;
        this.logger.error(`Failed — ${msg}`);
        result.errors.push(msg);
      }
    }

    this.logger.log(
      `Backfill complete — ${result.artistsProcessed} artists, ${result.albumsIngested} albums`,
    );

    return result;
  }

  async backfillTopArtistAlbums(
    options: {
      createMissing?: boolean;
      delayMs?: number;
      limitArtists?: number;
      minMonthlyListeners?: number;
    } = {},
  ): Promise<{
    artistsProcessed: number;
    albumsIngested: number;
    errors: string[];
  }> {
    const {
      createMissing = false,
      delayMs = 3000,
      limitArtists = 100,
      minMonthlyListeners = 1_000_000,
    } = options;

    // Query artists with high monthly listeners but no albums yet
    const topArtists = await this.db.execute(sql`
    SELECT
      a.id,
      a.name,
      a.spotify_id AS "spotifyId",
      ml.monthly_listeners AS "monthlyListeners"
    FROM artists a
    JOIN artist_monthly_listener_summary ml ON ml.artist_id = a.id
    LEFT JOIN albums al ON al.artist_id = a.id
    WHERE a.entity_status = 'canonical'
      AND a.spotify_id IS NOT NULL
      AND ml.monthly_listeners >= ${minMonthlyListeners}
      AND al.id IS NULL
    GROUP BY a.id, a.name, a.spotify_id, ml.monthly_listeners
    ORDER BY ml.monthly_listeners DESC
    LIMIT ${limitArtists}
  `);

    const artistRows = topArtists.rows as {
      id: string;
      name: string;
      spotifyId: string;
      monthlyListeners: number;
    }[];

    this.logger.log(
      `Found ${artistRows.length} artists with ${minMonthlyListeners.toLocaleString()}+ monthly listeners without albums`,
    );

    const result = {
      artistsProcessed: 0,
      albumsIngested: 0,
      errors: [] as string[],
    };

    for (const artist of artistRows) {
      try {
        this.logger.log(
          `Processing "${artist.name}" — ${artist.monthlyListeners.toLocaleString()} listeners...`,
        );

        const albumIds = await this.scrapeArtistAlbumIds(artist.spotifyId);

        if (!albumIds.length) {
          this.logger.warn(`No albums found for "${artist.name}"`);
          result.artistsProcessed++;
          continue;
        }

        this.logger.log(
          `Found ${albumIds.length} albums for "${artist.name}" — ingesting...`,
        );

        const ingestResults = await this.ingestAlbums(albumIds, {
          isPrimary: true,
          createMissing,
          delayMs: 1500,
        });

        result.albumsIngested += ingestResults.length;
        result.artistsProcessed++;

        await this.sleep(delayMs);
      } catch (err) {
        const msg = `"${artist.name}": ${(err as Error).message}`;
        this.logger.error(`Failed — ${msg}`);
        result.errors.push(msg);
      }
    }

    this.logger.log(
      `Backfill complete — ${result.artistsProcessed} artists, ${result.albumsIngested} albums`,
    );

    return result;
  }
}
