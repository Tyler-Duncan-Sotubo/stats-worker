import { Inject, Injectable } from '@nestjs/common';
import { and, eq, inArray, sql } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import {
  artistStatsSnapshots,
  songFeatures,
  songs,
  songStatsSnapshots,
} from 'src/infrastructure/drizzle/schema';

@Injectable()
export class SnapshotRepository {
  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  // ── Artist snapshots ──────────────────────────────────────────────────

  async upsertArtistSnapshot(data: {
    artistId: string;
    snapshotDate: string;
    totalStreams?: number | null;
    totalStreamsAsLead?: number | null;
    totalStreamsSolo?: number | null;
    totalStreamsAsFeature?: number | null;
    dailyStreams?: number | null;
    dailyStreamsAsLead?: number | null;
    dailyStreamsAsFeature?: number | null;
    trackCount?: number | null;
    sourceUpdatedAt?: string | null;
  }) {
    const [row] = await this.db
      .insert(artistStatsSnapshots)
      .values({
        artistId: data.artistId,
        snapshotDate: data.snapshotDate,
        totalStreams: data.totalStreams ?? null,
        totalStreamsAsLead: data.totalStreamsAsLead ?? null,
        totalStreamsSolo: data.totalStreamsSolo ?? null,
        totalStreamsAsFeature: data.totalStreamsAsFeature ?? null,
        dailyStreams: data.dailyStreams ?? null,
        dailyStreamsAsLead: data.dailyStreamsAsLead ?? null,
        dailyStreamsAsFeature: data.dailyStreamsAsFeature ?? null,
        trackCount: data.trackCount ?? null,
        sourceUpdatedAt: data.sourceUpdatedAt ?? null,
      } as typeof artistStatsSnapshots.$inferInsert)
      .onConflictDoUpdate({
        target: [
          artistStatsSnapshots.artistId,
          artistStatsSnapshots.snapshotDate,
        ],
        set: {
          totalStreams: data.totalStreams ?? null,
          totalStreamsAsLead: data.totalStreamsAsLead ?? null,
          totalStreamsSolo: data.totalStreamsSolo ?? null,
          totalStreamsAsFeature: data.totalStreamsAsFeature ?? null,
          dailyStreams: data.dailyStreams ?? null,
          dailyStreamsAsLead: data.dailyStreamsAsLead ?? null,
          dailyStreamsAsFeature: data.dailyStreamsAsFeature ?? null,
          trackCount: data.trackCount ?? null,
          sourceUpdatedAt: data.sourceUpdatedAt ?? null,
        } as Partial<typeof artistStatsSnapshots.$inferInsert>,
      })
      .returning();

    return row;
  }

  async artistSnapshotExistsForDate(
    artistId: string,
    snapshotDate: string,
  ): Promise<boolean> {
    const [row] = await this.db
      .select({ id: artistStatsSnapshots.id })
      .from(artistStatsSnapshots)
      .where(
        and(
          eq(artistStatsSnapshots.artistId, artistId),
          eq(artistStatsSnapshots.snapshotDate, snapshotDate),
        ),
      )
      .limit(1);

    return !!row;
  }

  async findArtistSnapshot(artistId: string, snapshotDate: string) {
    const [row] = await this.db
      .select()
      .from(artistStatsSnapshots)
      .where(
        and(
          eq(artistStatsSnapshots.artistId, artistId),
          eq(artistStatsSnapshots.snapshotDate, snapshotDate),
        ),
      )
      .limit(1);

    return row ?? null;
  }

  // ── Song snapshots ────────────────────────────────────────────────────

  async findBySpotifyTrackId(spotifyTrackId: string) {
    if (!spotifyTrackId) return null;

    const [song] = await this.db
      .select()
      .from(songs)
      .where(eq(songs.spotifyTrackId, spotifyTrackId))
      .limit(1);

    return song ?? null;
  }

  // ── NEW: batch lookup by multiple spotifyTrackIds ─────────────────────
  async findManyBySpotifyTrackIds(
    spotifyTrackIds: string[],
  ): Promise<Map<string, typeof songs.$inferSelect>> {
    if (!spotifyTrackIds.length) return new Map();

    const rows = await this.db
      .select()
      .from(songs)
      .where(inArray(songs.spotifyTrackId, spotifyTrackIds));

    return new Map(rows.map((s) => [s.spotifyTrackId!, s]));
  }

  async ensureFeatureLink(
    songId: string,
    featuredArtistId: string,
  ): Promise<void> {
    await this.db
      .insert(songFeatures)
      .values({ songId, featuredArtistId })
      .onConflictDoNothing();
  }

  // ── NEW: batch ensure feature links ──────────────────────────────────
  async bulkEnsureFeatureLinks(
    links: { songId: string; featuredArtistId: string }[],
  ): Promise<void> {
    if (!links.length) return;

    await this.db.insert(songFeatures).values(links).onConflictDoNothing();
  }

  async upsertSongSnapshot(data: {
    songId: string;
    snapshotDate: string;
    spotifyStreams?: number | null;
    dailyStreams?: number | null;
  }) {
    const [row] = await this.db
      .insert(songStatsSnapshots)
      .values({
        songId: data.songId,
        snapshotDate: data.snapshotDate,
        spotifyStreams: data.spotifyStreams ?? null,
        dailyStreams: data.dailyStreams ?? null,
      } as typeof songStatsSnapshots.$inferInsert)
      .onConflictDoUpdate({
        target: [songStatsSnapshots.songId, songStatsSnapshots.snapshotDate],
        set: {
          spotifyStreams: data.spotifyStreams ?? null,
          dailyStreams: data.dailyStreams ?? null,
        } as Partial<typeof songStatsSnapshots.$inferInsert>,
      })
      .returning();

    return row;
  }

  // ── NEW: bulk upsert song snapshots in one query ──────────────────────
  async bulkUpsertSongSnapshots(
    rows: {
      songId: string;
      snapshotDate: string;
      spotifyStreams?: number | null;
      dailyStreams?: number | null;
    }[],
  ): Promise<void> {
    if (!rows.length) return;

    const deduped = [
      ...new Map(
        rows.map((r) => [`${r.songId}:${r.snapshotDate}`, r]),
      ).values(),
    ];

    await this.db
      .insert(songStatsSnapshots)
      .values(
        deduped.map((r) => ({
          songId: r.songId,
          snapshotDate: r.snapshotDate,
          spotifyStreams: r.spotifyStreams ?? null,
          dailyStreams: r.dailyStreams ?? null,
        })) as (typeof songStatsSnapshots.$inferInsert)[],
      )
      .onConflictDoUpdate({
        target: [songStatsSnapshots.songId, songStatsSnapshots.snapshotDate],
        set: {
          spotifyStreams: sql`excluded.spotify_streams`,
          dailyStreams: sql`excluded.daily_streams`,
        },
      });
  }

  async findSongSnapshot(songId: string, snapshotDate: string) {
    const [row] = await this.db
      .select()
      .from(songStatsSnapshots)
      .where(
        and(
          eq(songStatsSnapshots.songId, songId),
          eq(songStatsSnapshots.snapshotDate, snapshotDate),
        ),
      )
      .limit(1);

    return row ?? null;
  }
}
