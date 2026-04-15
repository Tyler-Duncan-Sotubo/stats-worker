// src/modules/catalog/entity-resolution.service.ts

import { Inject, Injectable, Logger } from '@nestjs/common';
import { and, eq, sql } from 'drizzle-orm';
import slugify from 'slugify';

import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';

import {
  artists,
  songs,
  artistAliases,
  songAliases,
  artistExternalIds,
  songExternalIds,
} from 'src/infrastructure/drizzle/schema';

type EntitySource =
  | 'spotify'
  | 'kworb'
  | 'billboard'
  | 'official_charts'
  | 'riaa'
  | 'apple_music'
  | 'manual';

type ResolveArtistInput = {
  name: string;
  spotifyId?: string | null;
  artistSlug?: string;
  externalIds?: Array<{
    source: string;
    externalId: string;
    externalUrl?: string | null;
  }>;
  source: EntitySource;
  allowCreate?: boolean;
  markProvisionalIfCreated?: boolean;
};

type ResolveSongInput = {
  artistId: string;
  artistSlug?: string; // pass from caller to avoid re-query
  title: string;
  spotifyTrackId?: string | null;
  externalIds?: Array<{
    source: string;
    externalId: string;
    externalUrl?: string | null;
  }>;
  source: EntitySource;
  allowCreate?: boolean;
  markProvisionalIfCreated?: boolean;
};

type ArtistRow = typeof artists.$inferSelect;
type SongRow = typeof songs.$inferSelect;
type DbExecutor = DrizzleDB;

@Injectable()
export class EntityResolutionService {
  private readonly logger = new Logger(EntityResolutionService.name);

  // ── In-memory caches — populated by warmCache() ───────────────────────
  // Keyed multiple ways so any lookup hits without a DB round trip
  private readonly artistCache = new Map<string, ArtistRow>();
  private readonly songCache = new Map<string, SongRow>();

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  // ── Cache management ──────────────────────────────────────────────────

  async warmCache(): Promise<void> {
    const [artistRows, songRows] = await Promise.all([
      this.db.select().from(artists),
      this.db.select().from(songs),
    ]);

    for (const a of artistRows) {
      this.artistCache.set(a.slug, a);
      if (a.normalizedName) {
        this.artistCache.set(`norm::${a.normalizedName}`, a);
      }
      if (a.spotifyId) {
        this.artistCache.set(`spotify::${a.spotifyId}`, a);
      }
    }

    for (const s of songRows) {
      this.songCache.set(s.slug, s);
      if (s.normalizedTitle) {
        this.songCache.set(`${s.artistId}::${s.normalizedTitle}`, s);
      }
      if (s.spotifyTrackId) {
        this.songCache.set(`spotify::${s.spotifyTrackId}`, s);
      }
    }

    this.logger.log(
      `Cache warmed — ${artistRows.length} artists, ${songRows.length} songs`,
    );
  }

  clearCache(): void {
    this.artistCache.clear();
    this.songCache.clear();
    this.logger.log('Entity resolution cache cleared');
  }

  // ── Artist resolution ─────────────────────────────────────────────────

  async resolveArtist(
    input: ResolveArtistInput,
    db?: DbExecutor,
  ): Promise<ArtistRow | null> {
    const executor = db ?? this.db;
    const allowCreate = input.allowCreate ?? true;
    const normalizedName = this.normalizeName(input.name);
    const cleanName = this.cleanDisplayName(input.name);
    const slug = this.makeSlug(cleanName);

    // ── 1. Cache hits — zero DB cost ──────────────────────────────────
    if (input.spotifyId) {
      const cached = this.artistCache.get(`spotify::${input.spotifyId}`);
      if (cached) return cached;
    }

    const cachedBySlug = this.artistCache.get(slug);
    if (cachedBySlug) return cachedBySlug;

    const cachedByNorm = this.artistCache.get(`norm::${normalizedName}`);
    if (cachedByNorm) return cachedByNorm;

    // ── 2. DB lookups — only on cache miss ────────────────────────────

    if (input.spotifyId) {
      const [bySpotifyId] = await executor
        .select()
        .from(artists)
        .where(eq(artists.spotifyId, input.spotifyId))
        .limit(1);

      if (bySpotifyId) {
        this.populateArtistCache(bySpotifyId);
        await this.ensureArtistAlias(
          executor,
          bySpotifyId.id,
          cleanName,
          input.source,
        );
        await this.ensureArtistExternalIds(executor, bySpotifyId.id, [
          { source: 'spotify', externalId: input.spotifyId },
          ...(input.externalIds ?? []),
        ]);
        return bySpotifyId;
      }
    }

    for (const ext of input.externalIds ?? []) {
      const [byExternalId] = await executor
        .select({ artist: artists })
        .from(artistExternalIds)
        .innerJoin(artists, eq(artistExternalIds.artistId, artists.id))
        .where(
          and(
            eq(artistExternalIds.source, ext.source),
            eq(artistExternalIds.externalId, ext.externalId),
          ),
        )
        .limit(1);

      if (byExternalId?.artist) {
        this.populateArtistCache(byExternalId.artist);
        await this.ensureArtistAlias(
          executor,
          byExternalId.artist.id,
          cleanName,
          input.source,
        );
        if (input.spotifyId) {
          await this.ensureArtistExternalIds(executor, byExternalId.artist.id, [
            { source: 'spotify', externalId: input.spotifyId },
          ]);
        }
        await this.ensureArtistExternalIds(
          executor,
          byExternalId.artist.id,
          input.externalIds ?? [],
        );
        return byExternalId.artist;
      }
    }

    const [bySlug] = await executor
      .select()
      .from(artists)
      .where(eq(artists.slug, slug))
      .limit(1);

    if (bySlug) {
      this.populateArtistCache(bySlug);
      await this.ensureArtistAlias(
        executor,
        bySlug.id,
        cleanName,
        input.source,
      );
      if (input.spotifyId) {
        await this.ensureArtistExternalIds(executor, bySlug.id, [
          { source: 'spotify', externalId: input.spotifyId },
        ]);
      }
      await this.ensureArtistExternalIds(
        executor,
        bySlug.id,
        input.externalIds ?? [],
      );
      return bySlug;
    }

    const [byNormalizedName] = await executor
      .select()
      .from(artists)
      .where(eq(artists.normalizedName, normalizedName))
      .limit(1);

    if (byNormalizedName) {
      this.populateArtistCache(byNormalizedName);
      await this.ensureArtistAlias(
        executor,
        byNormalizedName.id,
        cleanName,
        input.source,
      );
      if (input.spotifyId) {
        await this.ensureArtistExternalIds(executor, byNormalizedName.id, [
          { source: 'spotify', externalId: input.spotifyId },
        ]);
      }
      await this.ensureArtistExternalIds(
        executor,
        byNormalizedName.id,
        input.externalIds ?? [],
      );
      return byNormalizedName;
    }

    const [byAlias] = await executor
      .select({ artist: artists })
      .from(artistAliases)
      .innerJoin(artists, eq(artistAliases.artistId, artists.id))
      .where(eq(artistAliases.normalizedAlias, normalizedName))
      .limit(1);

    if (byAlias?.artist) {
      this.populateArtistCache(byAlias.artist);
      await this.ensureArtistAlias(
        executor,
        byAlias.artist.id,
        cleanName,
        input.source,
      );
      if (input.spotifyId) {
        await this.ensureArtistExternalIds(executor, byAlias.artist.id, [
          { source: 'spotify', externalId: input.spotifyId },
        ]);
      }
      await this.ensureArtistExternalIds(
        executor,
        byAlias.artist.id,
        input.externalIds ?? [],
      );
      return byAlias.artist;
    }

    if (!allowCreate) return null;

    // ── 3. Create ─────────────────────────────────────────────────────
    const [created] = await executor
      .insert(artists)
      .values({
        name: cleanName,
        normalizedName,
        canonicalName: cleanName,
        spotifyId: input.spotifyId ?? null,
        slug,
        isAfrobeats: false,
        isAfrobeatsOverride: false,
        entityStatus: input.markProvisionalIfCreated
          ? 'provisional'
          : 'canonical',
        sourceOfTruth: input.source,
        needsReview: !!input.markProvisionalIfCreated,
      })
      .onConflictDoUpdate({
        target: artists.slug,
        set: {
          name: cleanName,
          normalizedName,
          spotifyId: input.spotifyId ?? undefined,
          updatedAt: sql`now()`,
        },
      })
      .returning();

    if (!created) return null;

    this.populateArtistCache(created);

    await this.ensureArtistAlias(executor, created.id, cleanName, input.source);

    const extIds = [
      ...(input.spotifyId
        ? [{ source: 'spotify', externalId: input.spotifyId }]
        : []),
      ...(input.externalIds ?? []),
    ];
    await this.ensureArtistExternalIds(executor, created.id, extIds);
    return created;
  }

  // ── Song resolution ───────────────────────────────────────────────────

  async resolveSong(
    input: ResolveSongInput,
    db?: DbExecutor,
  ): Promise<SongRow | null> {
    const executor = db ?? this.db;
    const allowCreate = input.allowCreate ?? true;
    const normalizedTitle = this.normalizeTitle(input.title);
    const cleanTitle = this.cleanDisplayTitle(input.title);

    // ── 1. Cache hits — zero DB cost ──────────────────────────────────
    if (input.spotifyTrackId) {
      const cached = this.songCache.get(`spotify::${input.spotifyTrackId}`);
      if (cached) return cached;
    }

    const cachedByArtistTitle = this.songCache.get(
      `${input.artistId}::${normalizedTitle}`,
    );
    if (cachedByArtistTitle) return cachedByArtistTitle;

    // ── 2. Resolve artist slug — use passed value to avoid re-query ───
    let artistSlug = input.artistSlug;
    if (!artistSlug) {
      const [a] = await executor
        .select({ slug: artists.slug })
        .from(artists)
        .where(eq(artists.id, input.artistId))
        .limit(1);
      artistSlug = a?.slug;
    }

    if (!artistSlug) {
      this.logger.warn(`resolveSong: artist ${input.artistId} not found`);
      return null;
    }

    const slug = this.makeSlug(`${artistSlug}-${cleanTitle}`);

    const cachedBySlug = this.songCache.get(slug);
    if (cachedBySlug) return cachedBySlug;

    // ── 3. DB lookups — only on cache miss ────────────────────────────

    if (input.spotifyTrackId) {
      const [bySpotifyTrackId] = await executor
        .select()
        .from(songs)
        .where(eq(songs.spotifyTrackId, input.spotifyTrackId))
        .limit(1);

      if (bySpotifyTrackId) {
        this.populateSongCache(bySpotifyTrackId);
        await this.ensureSongAlias(
          executor,
          bySpotifyTrackId.id,
          cleanTitle,
          input.source,
        );
        await this.ensureSongExternalIds(executor, bySpotifyTrackId.id, [
          { source: 'spotify', externalId: input.spotifyTrackId },
          ...(input.externalIds ?? []),
        ]);
        return bySpotifyTrackId;
      }
    }

    for (const ext of input.externalIds ?? []) {
      const [byExternalId] = await executor
        .select({ song: songs })
        .from(songExternalIds)
        .innerJoin(songs, eq(songExternalIds.songId, songs.id))
        .where(
          and(
            eq(songExternalIds.source, ext.source),
            eq(songExternalIds.externalId, ext.externalId),
          ),
        )
        .limit(1);

      if (byExternalId?.song) {
        this.populateSongCache(byExternalId.song);
        await this.ensureSongAlias(
          executor,
          byExternalId.song.id,
          cleanTitle,
          input.source,
        );
        if (input.spotifyTrackId) {
          await this.ensureSongExternalIds(executor, byExternalId.song.id, [
            { source: 'spotify', externalId: input.spotifyTrackId },
          ]);
        }
        await this.ensureSongExternalIds(
          executor,
          byExternalId.song.id,
          input.externalIds ?? [],
        );
        return byExternalId.song;
      }
    }

    const [byArtistAndTitle] = await executor
      .select()
      .from(songs)
      .where(
        and(
          eq(songs.artistId, input.artistId),
          eq(songs.normalizedTitle, normalizedTitle),
        ),
      )
      .limit(1);

    if (byArtistAndTitle) {
      this.populateSongCache(byArtistAndTitle);
      await this.ensureSongAlias(
        executor,
        byArtistAndTitle.id,
        cleanTitle,
        input.source,
      );
      if (input.spotifyTrackId) {
        await this.ensureSongExternalIds(executor, byArtistAndTitle.id, [
          { source: 'spotify', externalId: input.spotifyTrackId },
        ]);
      }
      await this.ensureSongExternalIds(
        executor,
        byArtistAndTitle.id,
        input.externalIds ?? [],
      );
      return byArtistAndTitle;
    }

    const [bySlug] = await executor
      .select()
      .from(songs)
      .where(eq(songs.slug, slug))
      .limit(1);

    if (bySlug) {
      this.populateSongCache(bySlug);
      await this.ensureSongAlias(executor, bySlug.id, cleanTitle, input.source);
      if (input.spotifyTrackId) {
        await this.ensureSongExternalIds(executor, bySlug.id, [
          { source: 'spotify', externalId: input.spotifyTrackId },
        ]);
      }
      await this.ensureSongExternalIds(
        executor,
        bySlug.id,
        input.externalIds ?? [],
      );
      return bySlug;
    }

    const [byAlias] = await executor
      .select({ song: songs })
      .from(songAliases)
      .innerJoin(songs, eq(songAliases.songId, songs.id))
      .where(
        and(
          eq(songAliases.normalizedAlias, normalizedTitle),
          eq(songs.artistId, input.artistId),
        ),
      )
      .limit(1);

    if (byAlias?.song) {
      this.populateSongCache(byAlias.song);
      await this.ensureSongAlias(
        executor,
        byAlias.song.id,
        cleanTitle,
        input.source,
      );
      if (input.spotifyTrackId) {
        await this.ensureSongExternalIds(executor, byAlias.song.id, [
          { source: 'spotify', externalId: input.spotifyTrackId },
        ]);
      }
      await this.ensureSongExternalIds(
        executor,
        byAlias.song.id,
        input.externalIds ?? [],
      );
      return byAlias.song;
    }

    if (!allowCreate) return null;

    // ── 4. Create ─────────────────────────────────────────────────────
    const [created] = await executor
      .insert(songs)
      .values({
        artistId: input.artistId,
        title: cleanTitle,
        normalizedTitle,
        canonicalTitle: cleanTitle,
        slug,
        spotifyTrackId: input.spotifyTrackId ?? null,
        isAfrobeats: false,
        explicit: false,
        entityStatus: input.markProvisionalIfCreated
          ? 'provisional'
          : 'canonical',
        sourceOfTruth: input.source,
        needsReview: !!input.markProvisionalIfCreated,
      })
      .onConflictDoUpdate({
        target: songs.slug,
        set: {
          title: cleanTitle,
          normalizedTitle,
        },
      })
      .returning();

    if (!created) return null;

    this.populateSongCache(created);

    await this.ensureSongAlias(executor, created.id, cleanTitle, input.source);

    const extIds = [
      ...(input.spotifyTrackId
        ? [{ source: 'spotify', externalId: input.spotifyTrackId }]
        : []),
      ...(input.externalIds ?? []),
    ];
    await this.ensureSongExternalIds(executor, created.id, extIds);
    return created;
  }

  // ── Public alias/external ID helpers ─────────────────────────────────

  async attachArtistAlias(
    db: DbExecutor,
    artistId: string,
    alias: string,
    source: string,
  ) {
    await this.ensureArtistAlias(db, artistId, alias, source);
  }

  async attachSongAlias(
    db: DbExecutor,
    songId: string,
    alias: string,
    source: string,
  ) {
    await this.ensureSongAlias(db, songId, alias, source);
  }

  // ── Private cache population helpers ─────────────────────────────────

  private populateArtistCache(a: ArtistRow): void {
    this.artistCache.set(a.slug, a);
    if (a.normalizedName) this.artistCache.set(`norm::${a.normalizedName}`, a);
    if (a.spotifyId) this.artistCache.set(`spotify::${a.spotifyId}`, a);
  }

  private populateSongCache(s: SongRow): void {
    this.songCache.set(s.slug, s);
    if (s.normalizedTitle) {
      this.songCache.set(`${s.artistId}::${s.normalizedTitle}`, s);
    }
    if (s.spotifyTrackId) {
      this.songCache.set(`spotify::${s.spotifyTrackId}`, s);
    }
  }

  // ── Private alias/externalId writers ─────────────────────────────────

  private async ensureArtistAlias(
    db: DbExecutor,
    artistId: string,
    alias: string,
    source: string,
  ): Promise<void> {
    const cleanAlias = this.cleanDisplayName(alias);
    const normalizedAlias = this.normalizeName(cleanAlias);

    await db
      .insert(artistAliases)
      .values({
        artistId,
        alias: cleanAlias,
        normalizedAlias,
        source,
        isPrimary: false,
      })
      .onConflictDoNothing();
  }

  private async ensureSongAlias(
    db: DbExecutor,
    songId: string,
    alias: string,
    source: string,
  ): Promise<void> {
    const cleanAlias = this.cleanDisplayTitle(alias);
    const normalizedAlias = this.normalizeTitle(cleanAlias);

    await db
      .insert(songAliases)
      .values({
        songId,
        alias: cleanAlias,
        normalizedAlias,
        source,
        isPrimary: false,
      })
      .onConflictDoNothing();
  }

  private async ensureArtistExternalIds(
    db: DbExecutor,
    artistId: string,
    externalIds: Array<{
      source: string;
      externalId: string;
      externalUrl?: string | null;
    }>,
  ): Promise<void> {
    for (const ext of externalIds) {
      if (!ext?.source || !ext?.externalId) continue;
      await db
        .insert(artistExternalIds)
        .values({
          artistId,
          source: ext.source,
          externalId: ext.externalId,
          externalUrl: ext.externalUrl ?? null,
        })
        .onConflictDoNothing();
    }
  }

  private async ensureSongExternalIds(
    db: DbExecutor,
    songId: string,
    externalIds: Array<{
      source: string;
      externalId: string;
      externalUrl?: string | null;
    }>,
  ): Promise<void> {
    for (const ext of externalIds) {
      if (!ext?.source || !ext?.externalId) continue;
      await db
        .insert(songExternalIds)
        .values({
          songId,
          source: ext.source,
          externalId: ext.externalId,
          externalUrl: ext.externalUrl ?? null,
        })
        .onConflictDoNothing();
    }
  }

  // ── Normalisation ─────────────────────────────────────────────────────

  normalizeName(value: string): string {
    return value
      .toLowerCase()
      .trim()
      .replace(/&/g, ' and ')
      .replace(/\bfeat\.?\b/gi, 'featuring')
      .replace(/[^\p{L}\p{N}\s]/gu, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  normalizeTitle(value: string): string {
    return value
      .toLowerCase()
      .trim()
      .replace(/\s*\(feat\.?.*?\)/gi, '')
      .replace(/\s*\(ft\.?.*?\)/gi, '')
      .replace(/\s*\[feat\.?.*?\]/gi, '')
      .replace(/[^\p{L}\p{N}\s]/gu, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private cleanDisplayName(value: string): string {
    return value.replace(/\s+/g, ' ').trim();
  }

  private cleanDisplayTitle(value: string): string {
    return value.replace(/\s+/g, ' ').trim();
  }

  private makeSlug(value: string): string {
    return slugify(value, { lower: true, strict: true, trim: true });
  }
}
