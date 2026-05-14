/* eslint-disable @typescript-eslint/no-unnecessary-type-assertion */
/* eslint-disable @typescript-eslint/no-unsafe-return */
import { Injectable, Logger } from '@nestjs/common';
import { SongsRepository } from 'src/repository/songs.repository';
import { AlbumScraperService } from './album-scraper.service';
import { EntityResolutionService } from './entity-resolution.service';
import {
  SongMetadataService,
  SpotifyTrackMetadata,
} from './song-metadata.service';

export interface MinimalSongInput {
  artistId: string;
  spotifyTrackId: string;
  title: string;
}

@Injectable()
export class SongScraperService {
  private readonly logger = new Logger(SongScraperService.name);

  constructor(
    private readonly songsRepository: SongsRepository,
    private readonly songMetadataService: SongMetadataService,
    private readonly albumScraperService: AlbumScraperService,
    private readonly entityResolutionService: EntityResolutionService,
  ) {}

  // ── Called by snapshot pipeline ───────────────────────────────────────
  // Minimal resolve/create from Kworb-style data

  async findOrCreate(input: MinimalSongInput) {
    const resolved = await this.entityResolutionService.resolveSong({
      artistId: input.artistId,
      title: input.title,
      spotifyTrackId: input.spotifyTrackId,
      source: 'kworb',
      allowCreate: true,
      markProvisionalIfCreated: false,
    });

    if (!resolved) {
      throw new Error(
        `Failed to resolve song "${input.title}" (${input.spotifyTrackId})`,
      );
    }

    return resolved;
  }

  // ── Called by enrichment cron (single) ───────────────────────────────

  async enrichOne(artistId: string, spotifyTrackId: string) {
    const metadata = await this.songMetadataService.fetchTrack(spotifyTrackId);
    return this.enrichFromMetadata(artistId, metadata);
  }

  // ── Called by enrichment cron (batch) ────────────────────────────────

  async enrichMany(artistId: string, spotifyTrackIds: string[]) {
    if (!spotifyTrackIds.length) return [];

    const uniqueIds = [...new Set(spotifyTrackIds)];

    // Batch Spotify call — handles chunking into 50s internally
    const metadataRows = await this.songMetadataService.fetchTracks(uniqueIds);

    if (!metadataRows.length) {
      this.logger.warn(
        `[enrichMany] No metadata returned for ${uniqueIds.length} tracks`,
      );
      return [];
    }

    const existingSongs =
      await this.songsRepository.findBySpotifyTrackIds(uniqueIds);

    const existingMap = new Map(
      existingSongs
        .filter((s) => s.spotifyTrackId)
        .map((s) => [s.spotifyTrackId!, s]),
    );

    // Build updates directly from existingMap — no resolveSong needed
    const updates = metadataRows
      .map((track) => {
        const existing = existingMap.get(track.spotifyTrackId);
        if (!existing) {
          this.logger.warn(
            `[enrichMany] No existing song for "${track.title}" (${track.spotifyTrackId}) — skipping`,
          );
          return null;
        }

        return {
          id: existing.id,
          artistId,
          albumId: existing.albumId ?? null, // preserve — set by album ingestion pipeline
          title: track.title,
          normalizedTitle: this.normalizeTitle(track.title),
          canonicalTitle: track.title,
          spotifyTrackId: track.spotifyTrackId,
          releaseDate: track.releaseDate || null,
          durationMs: track.durationMs,
          explicit: track.explicit,
          imageUrl: track.albumImageUrl,
          isAfrobeats: existing.isAfrobeats ?? false,
          sourceOfTruth: 'spotify' as const,
          entityStatus: (existing.entityStatus ?? 'canonical') as any,
          needsReview: false,
        };
      })
      .filter((u): u is NonNullable<typeof u> => u !== null);

    if (!updates.length) {
      this.logger.warn(`[enrichMany] No updates to apply`);
      return [];
    }

    const results = await this.songsRepository.updateManyById(updates);

    this.logger.log(
      `[enrichMany] ${results.length} enriched, ${metadataRows.length - results.length} skipped/failed`,
    );

    return results as any[];
  }

  // ── Called by enrichPending ───────────────────────────────────────────

  async enrichPending(spotifyTrackIds: string[], artistId: string) {
    const results: Awaited<ReturnType<typeof this.enrichOne>>[] = [];
    let failed = 0;

    for (const id of spotifyTrackIds) {
      try {
        const enriched = await this.enrichOne(artistId, id);
        results.push(enriched);
      } catch (err) {
        failed += 1;
        this.logger.warn(
          `[enrichPending] Failed to enrich ${id}: ${(err as Error).message}`,
        );
      }
    }

    this.logger.log(
      `[enrichPending] ${results.length} enriched, ${failed} failed`,
    );

    return results;
  }

  // ── Shared enrichment logic ───────────────────────────────────────────

  private async enrichFromMetadata(
    artistId: string,
    metadata: SpotifyTrackMetadata,
  ) {
    let albumId: string | null = null;

    if (metadata.spotifyAlbumId) {
      const album = await this.albumScraperService.upsert({
        artistId,
        spotifyAlbumId: metadata.spotifyAlbumId,
        title: metadata.albumName,
        albumType: metadata.albumType,
        releaseDate: metadata.releaseDate || null,
        imageUrl: metadata.albumImageUrl,
        totalTracks: metadata.totalTracks,
      });

      albumId = album.id;
    }

    const resolved = await this.entityResolutionService.resolveSong({
      artistId,
      title: metadata.title,
      spotifyTrackId: metadata.spotifyTrackId,
      source: 'spotify',
      allowCreate: true,
      markProvisionalIfCreated: false,
      externalIds: [
        {
          source: 'spotify',
          externalId: metadata.spotifyTrackId,
        },
      ],
    });

    if (!resolved) {
      throw new Error(
        `Failed to resolve enriched song "${metadata.title}" (${metadata.spotifyTrackId})`,
      );
    }

    const existing = await this.songsRepository.findById(resolved.id);
    if (!existing) {
      throw new Error(
        `Resolved song ${resolved.id} not found after resolution`,
      );
    }

    return this.songsRepository.updateById(resolved.id, {
      artistId,
      albumId,
      title: metadata.title,
      normalizedTitle: this.normalizeTitle(metadata.title),
      canonicalTitle: metadata.title,
      spotifyTrackId: metadata.spotifyTrackId,
      releaseDate: metadata.releaseDate || null,
      durationMs: metadata.durationMs,
      explicit: metadata.explicit,
      imageUrl: metadata.albumImageUrl,
      isAfrobeats: existing.isAfrobeats ?? false,
      sourceOfTruth: 'spotify',
      entityStatus: existing.entityStatus ?? 'canonical',
      needsReview: false,
    });
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  private normalizeTitle(value: string): string {
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
}
