import { Inject, Injectable } from '@nestjs/common';
import { and, eq } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import {
  artistStatsSnapshots,
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
