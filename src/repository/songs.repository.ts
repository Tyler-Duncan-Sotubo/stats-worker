import { Inject, Injectable } from '@nestjs/common';
import { eq, inArray, sql, and } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { songs } from 'src/infrastructure/drizzle/schema';

@Injectable()
export class SongsRepository {
  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  async findById(id: string) {
    const [row] = await this.db
      .select()
      .from(songs)
      .where(eq(songs.id, id))
      .limit(1);

    return row ?? null;
  }

  async findBySpotifyTrackId(spotifyTrackId: string) {
    const [row] = await this.db
      .select()
      .from(songs)
      .where(eq(songs.spotifyTrackId, spotifyTrackId))
      .limit(1);

    return row ?? null;
  }

  async findBySpotifyTrackIds(spotifyTrackIds: string[]) {
    if (!spotifyTrackIds.length) return [];

    return this.db
      .select()
      .from(songs)
      .where(inArray(songs.spotifyTrackId, spotifyTrackIds));
  }

  async findByArtistId(artistId: string) {
    return this.db
      .select({
        id: songs.id,
        title: songs.title,
        normalizedTitle: songs.normalizedTitle,
        spotifyTrackId: songs.spotifyTrackId,
        slug: songs.slug,
      })
      .from(songs)
      .where(eq(songs.artistId, artistId));
  }

  async findByArtistIdAndNormalizedTitle(
    artistId: string,
    normalizedTitle: string,
  ) {
    const [row] = await this.db
      .select()
      .from(songs)
      .where(
        and(
          eq(songs.artistId, artistId),
          eq(songs.normalizedTitle, normalizedTitle),
        ),
      )
      .limit(1);

    return row ?? null;
  }

  async findSongsNeedingEnrichment(limit = 100) {
    return this.db
      .select({
        id: songs.id,
        artistId: songs.artistId,
        spotifyTrackId: songs.spotifyTrackId,
        title: songs.title,
        normalizedTitle: songs.normalizedTitle,
        albumId: songs.albumId,
        releaseDate: songs.releaseDate,
        durationMs: songs.durationMs,
        imageUrl: songs.imageUrl,
      })
      .from(songs)
      .where(
        sql`(
          ${songs.spotifyTrackId} is not null
          and (
            ${songs.albumId} is null
            or ${songs.releaseDate} is null
            or ${songs.durationMs} is null
            or ${songs.imageUrl} is null
          )
        )`,
      )
      .limit(limit);
  }

  async upsertBySpotifyTrackId(data: typeof songs.$inferInsert) {
    const [row] = await this.db
      .insert(songs)
      .values({
        ...data,
      })
      .onConflictDoUpdate({
        target: songs.spotifyTrackId,
        set: {
          artistId: sql`excluded.artist_id`,
          albumId: sql`excluded.album_id`,
          title: sql`excluded.title`,
          normalizedTitle: sql`excluded.normalized_title`,
          canonicalTitle: sql`excluded.canonical_title`,
          releaseDate: sql`excluded.release_date`,
          durationMs: sql`excluded.duration_ms`,
          explicit: sql`excluded.explicit`,
          imageUrl: sql`excluded.image_url`,
          sourceOfTruth: sql`excluded.source_of_truth`,
          needsReview: sql`excluded.needs_review`,
        },
      })
      .returning();

    return row;
  }

  async upsertManyBySpotifyTrackId(data: (typeof songs.$inferInsert)[]) {
    if (!data.length) return [];

    return this.db
      .insert(songs)
      .values(data)
      .onConflictDoUpdate({
        target: songs.spotifyTrackId,
        set: {
          artistId: sql`excluded.artist_id`,
          albumId: sql`excluded.album_id`,
          title: sql`excluded.title`,
          normalizedTitle: sql`excluded.normalized_title`,
          canonicalTitle: sql`excluded.canonical_title`,
          releaseDate: sql`excluded.release_date`,
          durationMs: sql`excluded.duration_ms`,
          explicit: sql`excluded.explicit`,
          imageUrl: sql`excluded.image_url`,
          sourceOfTruth: sql`excluded.source_of_truth`,
          needsReview: sql`excluded.needs_review`,
        },
      })
      .returning();
  }

  async upsertAllFields(data: typeof songs.$inferInsert) {
    const [row] = await this.db
      .insert(songs)
      .values(data)
      .onConflictDoUpdate({
        target: songs.spotifyTrackId,
        set: {
          artistId: sql`excluded.artist_id`,
          albumId: sql`excluded.album_id`,
          title: sql`excluded.title`,
          normalizedTitle: sql`excluded.normalized_title`,
          canonicalTitle: sql`excluded.canonical_title`,
          releaseDate: sql`excluded.release_date`,
          durationMs: sql`excluded.duration_ms`,
          explicit: sql`excluded.explicit`,
          imageUrl: sql`excluded.image_url`,
          isAfrobeats: sql`excluded.is_afrobeats`,
          sourceOfTruth: sql`excluded.source_of_truth`,
          entityStatus: sql`excluded.entity_status`,
          needsReview: sql`excluded.needs_review`,
        },
      })
      .returning();

    return row;
  }

  async updateById(id: string, data: Partial<typeof songs.$inferInsert>) {
    const [row] = await this.db
      .update(songs)
      .set(data)
      .where(eq(songs.id, id))
      .returning();

    return row;
  }
}
