// src/modules/certifications/certifications.repository.ts

import { Inject, Injectable } from '@nestjs/common';
import { and, eq } from 'drizzle-orm';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { certifications } from 'src/infrastructure/drizzle/schema';

type UpsertCertificationInput = {
  artistId?: string | null;
  songId?: string | null;
  albumId?: string | null;
  territory: string;
  body: string;
  title: string;
  level: string;
  units?: number | null;
  certifiedAt?: string | null;
  sourceUrl?: string | null;
  rawArtistName?: string | null;
  rawTitle?: string | null;
  resolutionStatus?: 'matched' | 'artist_only' | 'unresolved';
};

@Injectable()
export class CertificationsRepository {
  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  // ── Write ─────────────────────────────────────────────────────────────

  async upsert(input: UpsertCertificationInput) {
    const [row] = await this.db
      .insert(certifications)
      .values({
        artistId: input.artistId ?? null,
        songId: input.songId ?? null,
        albumId: input.albumId ?? null,
        territory: input.territory,
        body: input.body,
        title: input.title, // include title
        level: input.level,
        units: input.units ?? null,
        certifiedAt: input.certifiedAt ?? null,
        sourceUrl: input.sourceUrl ?? null,
      } as typeof certifications.$inferInsert)
      .onConflictDoUpdate({
        target: [
          certifications.artistId,
          certifications.territory,
          certifications.body,
          certifications.title, // conflict on title now
        ],
        set: {
          level: input.level, // level can upgrade (gold → platinum)
          units: input.units ?? null,
          certifiedAt: input.certifiedAt ?? null,
          sourceUrl: input.sourceUrl ?? null,
        },
      })
      .returning();

    return row;
  }

  // ── Read ──────────────────────────────────────────────────────────────

  async findByArtistId(artistId: string) {
    return this.db
      .select()
      .from(certifications)
      .where(eq(certifications.artistId, artistId));
  }

  async findBySongId(songId: string) {
    return this.db
      .select()
      .from(certifications)
      .where(eq(certifications.songId, songId));
  }

  async findByAlbumId(albumId: string) {
    return this.db
      .select()
      .from(certifications)
      .where(eq(certifications.albumId, albumId));
  }

  async findByArtistAndTerritory(artistId: string, territory: string) {
    return this.db
      .select()
      .from(certifications)
      .where(
        and(
          eq(certifications.artistId, artistId),
          eq(certifications.territory, territory.toUpperCase()),
        ),
      );
  }

  async findById(id: string) {
    const [row] = await this.db
      .select()
      .from(certifications)
      .where(eq(certifications.id, id))
      .limit(1);

    return row ?? null;
  }

  async deleteById(id: string) {
    const [row] = await this.db
      .delete(certifications)
      .where(eq(certifications.id, id))
      .returning();

    return row ?? null;
  }
}
