import { Inject, Injectable } from '@nestjs/common';
import { eq, inArray, sql, isNull, isNotNull, and, gt } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import {
  artists,
  artistGenres,
  artistMonthlyListenerSnapshots,
} from '../infrastructure/drizzle/schema';

@Injectable()
export class ArtistsRepository {
  constructor(@Inject(DRIZZLE) private db: DrizzleDB) {}

  async upsertManyDiscovered(
    data: { name: string; spotifyId: string; slug: string }[],
  ) {
    if (!data.length) return [];

    const values = data.map((item) => ({
      name: item.name,
      normalizedName: this.normaliseName(item.name),
      canonicalName: item.name,
      spotifyId: item.spotifyId,
      slug: item.slug,
      entityStatus: 'canonical' as const,
      sourceOfTruth: 'kworb',
      needsReview: false,
    }));

    return this.db
      .insert(artists)
      .values(values)
      .onConflictDoUpdate({
        target: artists.spotifyId,
        set: {
          name: sql`excluded.name`,
          normalizedName: sql`excluded.normalized_name`,
          updatedAt: sql`now()`,
        },
      })
      .returning({ id: artists.id, spotifyId: artists.spotifyId });
  }

  async findUnenriched(limit = 100) {
    return this.db
      .select({ id: artists.id, spotifyId: artists.spotifyId })
      .from(artists)
      .where(isNull(artists.imageUrl))
      .limit(limit);
  }

  async upsertBySpotifyId(data: typeof artists.$inferInsert) {
    const [result] = await this.db
      .insert(artists)
      .values({
        ...data,
        normalizedName: data.normalizedName ?? this.normaliseName(data.name),
        canonicalName: data.canonicalName ?? data.name,
        entityStatus: data.entityStatus ?? 'canonical',
        sourceOfTruth: data.sourceOfTruth ?? 'spotify',
        needsReview: data.needsReview ?? false,
      })
      .onConflictDoUpdate({
        target: artists.spotifyId,
        set: {
          name: sql`excluded.name`,
          normalizedName: sql`excluded.normalized_name`,
          canonicalName: sql`excluded.canonical_name`,
          imageUrl: sql`excluded.image_url`,
          popularity: sql`excluded.popularity`,
          isAfrobeats: sql`excluded.is_afrobeats`,
          sourceOfTruth: sql`excluded.source_of_truth`,
          updatedAt: sql`now()`,
        },
      })
      .returning();

    return result;
  }

  upsertManyBySpotifyId(data: (typeof artists.$inferInsert)[]) {
    if (!data.length) return [];

    const values = data.map((item) => ({
      ...item,
      normalizedName: item.normalizedName ?? this.normaliseName(item.name),
      canonicalName: item.canonicalName ?? item.name,
      entityStatus: item.entityStatus ?? 'canonical',
      sourceOfTruth: item.sourceOfTruth ?? 'spotify',
      needsReview: item.needsReview ?? false,
    }));

    return this.db
      .insert(artists)
      .values(values)
      .onConflictDoUpdate({
        target: artists.spotifyId,
        set: {
          name: sql`excluded.name`,
          normalizedName: sql`excluded.normalized_name`,
          canonicalName: sql`excluded.canonical_name`,
          imageUrl: sql`excluded.image_url`,
          popularity: sql`excluded.popularity`,
          isAfrobeats: sql`excluded.is_afrobeats`,
          sourceOfTruth: sql`excluded.source_of_truth`,
          updatedAt: sql`now()`,
        },
      })
      .returning();
  }

  async findBySpotifyId(spotifyId: string) {
    const [result] = await this.db
      .select()
      .from(artists)
      .where(eq(artists.spotifyId, spotifyId))
      .limit(1);

    return result ?? null;
  }

  async findBySpotifyIds(spotifyIds: string[]) {
    if (!spotifyIds.length) return [];

    return this.db
      .select()
      .from(artists)
      .where(inArray(artists.spotifyId, spotifyIds));
  }

  async findAllWithSpotifyId() {
    return this.db
      .select({
        id: artists.id,
        name: artists.name,
        spotifyId: artists.spotifyId,
      })
      .from(artists)
      .where(
        sql`
          ${artists.spotifyId} IS NOT NULL
          AND (${artists.kworbStatus} IS NULL OR ${artists.kworbStatus} != 'not_found')
          AND ${artists.entityStatus} != 'merged'
        `,
      );
  }

  async findBySlug(slug: string) {
    const [result] = await this.db
      .select()
      .from(artists)
      .where(eq(artists.slug, slug))
      .limit(1);

    return result ?? null;
  }

  async findById(id: string) {
    const [result] = await this.db
      .select()
      .from(artists)
      .where(eq(artists.id, id))
      .limit(1);

    return result ?? null;
  }

  async findByNormalizedName(normalizedName: string) {
    const [result] = await this.db
      .select()
      .from(artists)
      .where(eq(artists.normalizedName, normalizedName))
      .limit(1);

    return result ?? null;
  }

  async upsertGenres(
    artistId: string,
    genres: { genre: string; isPrimary: boolean }[],
  ) {
    if (!genres.length) return;

    const values = genres.map((g) => ({
      artistId,
      genre: g.genre,
      isPrimary: g.isPrimary,
    }));

    await this.db
      .insert(artistGenres)
      .values(values)
      .onConflictDoUpdate({
        target: [artistGenres.artistId, artistGenres.genre],
        set: { isPrimary: sql`excluded.is_primary` },
      });
  }

  async getExistingSpotifyIds(spotifyIds: string[]): Promise<string[]> {
    if (!spotifyIds.length) return [];

    const rows = await this.db
      .select({ spotifyId: artists.spotifyId })
      .from(artists)
      .where(
        and(
          isNotNull(artists.spotifyId),
          inArray(artists.spotifyId, spotifyIds),
        ),
      );

    return rows
      .map((r) => r.spotifyId)
      .filter((id): id is string => id !== null);
  }

  async updateById(id: string, data: Partial<typeof artists.$inferInsert>) {
    const patch: Partial<typeof artists.$inferInsert> = {
      ...data,
      updatedAt: new Date(),
    };

    if (data.name && !data.normalizedName) {
      patch.normalizedName = this.normaliseName(data.name);
    }

    if (data.name && !data.canonicalName) {
      patch.canonicalName = data.name;
    }

    const [updated] = await this.db
      .update(artists)
      .set(patch)
      .where(eq(artists.id, id))
      .returning();

    return updated;
  }

  async markKworbNotFound(artistId: string) {
    await this.db
      .update(artists)
      .set({
        kworbStatus: 'not_found',
        kworbLastCheckedAt: new Date(),
      })
      .where(eq(artists.id, artistId));
  }

  async updateSpotifyId(id: string, spotifyId: string): Promise<void> {
    await this.db
      .update(artists)
      .set({ spotifyId, updatedAt: new Date() })
      .where(eq(artists.id, id));
  }

  async findAllBasic() {
    return this.db
      .select({
        id: artists.id,
        name: artists.name,
        normalizedName: artists.normalizedName,
        slug: artists.slug,
        spotifyId: artists.spotifyId,
        originCountry: artists.originCountry,
      })
      .from(artists);
  }

  async upsertMonthlyListenerSnapshot(input: {
    artistId: string;
    spotifyId: string;
    snapshotDate: string;
    monthlyListeners: number;
    dailyChange: number | null;
    peakRank: number | null;
    peakListeners: number | null;
  }): Promise<void> {
    await this.db
      .insert(artistMonthlyListenerSnapshots)
      .values({
        artistId: input.artistId,
        spotifyId: input.spotifyId,
        snapshotDate: input.snapshotDate,
        monthlyListeners: input.monthlyListeners,
        dailyChange: input.dailyChange,
        peakRank: input.peakRank,
        peakListeners: input.peakListeners,
        source: 'kworb',
      })
      .onConflictDoUpdate({
        target: [
          artistMonthlyListenerSnapshots.artistId,
          artistMonthlyListenerSnapshots.snapshotDate,
        ],
        set: {
          monthlyListeners: input.monthlyListeners,
          dailyChange: input.dailyChange,
          peakRank: input.peakRank,
          peakListeners: input.peakListeners,
          updatedAt: new Date(),
        },
      });
  }

  async findMissingOriginCountry(limit = 200) {
    return this.db
      .select({
        id: artists.id,
        name: artists.name,
        spotifyId: artists.spotifyId,
      })
      .from(artists)
      .where(isNull(artists.originCountry))
      .limit(limit);
  }

  async findMissingOriginCountryBasic() {
    return this.db
      .select({
        id: artists.id,
        name: artists.name,
        normalizedName: artists.normalizedName,
        slug: artists.slug,
        spotifyId: artists.spotifyId,
        originCountry: artists.originCountry,
      })
      .from(artists)
      .where(isNull(artists.originCountry));
  }

  async findUnenrichedBatch(limit = 100, afterId?: string) {
    const conditions = [isNull(artists.imageUrl)];

    if (afterId) {
      conditions.push(gt(artists.id, afterId));
    }

    return this.db
      .select({
        id: artists.id,
        name: artists.name,
        spotifyId: artists.spotifyId,
        imageUrl: artists.imageUrl,
      })
      .from(artists)
      .where(and(...conditions))
      .orderBy(artists.id)
      .limit(limit);
  }

  private normaliseName(value: string): string {
    return value
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/&/g, 'and')
      .replace(/['"]/g, '')
      .replace(/[^a-z0-9\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }
}
