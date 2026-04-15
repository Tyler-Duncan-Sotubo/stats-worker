import { Inject, Injectable } from '@nestjs/common';
import { eq, inArray, sql } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { albums } from 'src/infrastructure/drizzle/schema';

@Injectable()
export class AlbumsRepository {
  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  async findBySpotifyAlbumId(spotifyAlbumId: string) {
    const [row] = await this.db
      .select()
      .from(albums)
      .where(eq(albums.spotifyAlbumId, spotifyAlbumId))
      .limit(1);

    return row ?? null;
  }

  async findBySpotifyAlbumIds(spotifyAlbumIds: string[]) {
    if (!spotifyAlbumIds.length) return [];

    return this.db
      .select()
      .from(albums)
      .where(inArray(albums.spotifyAlbumId, spotifyAlbumIds));
  }

  async upsertBySpotifyAlbumId(data: typeof albums.$inferInsert) {
    const [row] = await this.db
      .insert(albums)
      .values(data)
      .onConflictDoUpdate({
        target: albums.spotifyAlbumId,
        set: {
          artistId: sql`excluded.artist_id`,
          title: sql`excluded.title`,
          slug: sql`excluded.slug`,
          albumType: sql`excluded.album_type`,
          releaseDate: sql`excluded.release_date`,
          imageUrl: sql`excluded.image_url`,
          totalTracks: sql`excluded.total_tracks`,
          isAfrobeats: sql`excluded.is_afrobeats`,
        },
      })
      .returning();

    return row;
  }

  async upsertManyBySpotifyAlbumId(data: (typeof albums.$inferInsert)[]) {
    if (!data.length) return [];

    return this.db
      .insert(albums)
      .values(data)
      .onConflictDoUpdate({
        target: albums.spotifyAlbumId,
        set: {
          artistId: sql`excluded.artist_id`,
          title: sql`excluded.title`,
          slug: sql`excluded.slug`,
          albumType: sql`excluded.album_type`,
          releaseDate: sql`excluded.release_date`,
          imageUrl: sql`excluded.image_url`,
          totalTracks: sql`excluded.total_tracks`,
          isAfrobeats: sql`excluded.is_afrobeats`,
        },
      })
      .returning();
  }

  // albums.repository.ts — add these three methods

  // ── Scraper writes — never touches isAfrobeats ────────────────────────

  async upsertScraperFields(data: {
    artistId: string;
    spotifyAlbumId: string;
    title: string;
    slug: string;
    albumType: string;
    releaseDate: string | null;
    imageUrl: string | null;
    totalTracks: number | null;
  }) {
    const [row] = await this.db
      .insert(albums)
      .values({ ...data, isAfrobeats: false })
      .onConflictDoUpdate({
        target: albums.spotifyAlbumId,
        set: {
          artistId: sql`excluded.artist_id`,
          title: sql`excluded.title`,
          slug: sql`excluded.slug`,
          albumType: sql`excluded.album_type`,
          releaseDate: sql`excluded.release_date`,
          imageUrl: sql`excluded.image_url`,
          totalTracks: sql`excluded.total_tracks`,
          // isAfrobeats deliberately excluded — scraper never touches it
        },
      })
      .returning();

    return row;
  }

  async upsertManyScraperFields(
    data: {
      artistId: string;
      spotifyAlbumId: string;
      title: string;
      slug: string;
      albumType: string;
      releaseDate: string | null;
      imageUrl: string | null;
      totalTracks: number | null;
    }[],
  ) {
    if (!data.length) return [];

    return this.db
      .insert(albums)
      .values(data.map((d) => ({ ...d, isAfrobeats: false })))
      .onConflictDoUpdate({
        target: albums.spotifyAlbumId,
        set: {
          artistId: sql`excluded.artist_id`,
          title: sql`excluded.title`,
          slug: sql`excluded.slug`,
          albumType: sql`excluded.album_type`,
          releaseDate: sql`excluded.release_date`,
          imageUrl: sql`excluded.image_url`,
          totalTracks: sql`excluded.total_tracks`,
          // isAfrobeats deliberately excluded
        },
      })
      .returning();
  }

  // ── Dashboard writes — full control including editorial fields ─────────

  async upsertAllFields(data: typeof albums.$inferInsert) {
    const [row] = await this.db
      .insert(albums)
      .values(data)
      .onConflictDoUpdate({
        target: albums.spotifyAlbumId,
        set: {
          artistId: sql`excluded.artist_id`,
          title: sql`excluded.title`,
          slug: sql`excluded.slug`,
          albumType: sql`excluded.album_type`,
          releaseDate: sql`excluded.release_date`,
          imageUrl: sql`excluded.image_url`,
          totalTracks: sql`excluded.total_tracks`,
          isAfrobeats: sql`excluded.is_afrobeats`,
        },
      })
      .returning();

    return row;
  }

  async updateById(id: string, data: Partial<typeof albums.$inferInsert>) {
    const [row] = await this.db
      .update(albums)
      .set({ ...data })
      .where(eq(albums.id, id))
      .returning();

    return row;
  }

  async findById(id: string) {
    const [row] = await this.db
      .select()
      .from(albums)
      .where(eq(albums.id, id))
      .limit(1);

    return row ?? null;
  }
}
