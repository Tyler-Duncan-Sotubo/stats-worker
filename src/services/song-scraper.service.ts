import { Injectable, Logger } from '@nestjs/common';
import { SpotifyMetadataService } from '../scraper/services/spotify-metadata.service';
import { SongsRepository } from 'src/repository/songs.repository';
import { AlbumScraperService } from './album-scraper.service';
import { EntityResolutionService } from './entity-resolution.service';

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
    private readonly spotifyMetadataService: SpotifyMetadataService,
    private readonly albumScraperService: AlbumScraperService,
    private readonly entityResolutionService: EntityResolutionService,
  ) {}

  // Called by snapshot pipeline
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

  // Called by enrichment cron
  async enrichOne(artistId: string, spotifyTrackId: string) {
    const metadata =
      await this.spotifyMetadataService.fetchTrackMetadata(spotifyTrackId);

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

  async enrichMany(artistId: string, spotifyTrackIds: string[]) {
    if (!spotifyTrackIds.length) return [];

    const uniqueIds = [...new Set(spotifyTrackIds)];
    const metadataRows =
      await this.spotifyMetadataService.fetchMultipleTracks(uniqueIds);

    const existingSongs =
      await this.songsRepository.findBySpotifyTrackIds(uniqueIds);

    const existingMap = new Map(
      existingSongs
        .filter((s) => s.spotifyTrackId)
        .map((s) => [s.spotifyTrackId!, s]),
    );

    const albumInputs = Array.from(
      new Map(
        metadataRows
          .filter((t) => t.spotifyAlbumId)
          .map((t) => [
            t.spotifyAlbumId,
            {
              artistId,
              spotifyAlbumId: t.spotifyAlbumId,
              title: t.albumName,
              albumType: t.albumType,
              releaseDate: t.releaseDate || null,
              imageUrl: t.albumImageUrl,
              totalTracks: t.totalTracks,
            },
          ]),
      ).values(),
    );

    const upsertedAlbums =
      await this.albumScraperService.upsertMany(albumInputs);
    const albumMap = new Map(upsertedAlbums.map((a) => [a.spotifyAlbumId, a]));

    const results: Awaited<ReturnType<typeof this.enrichOne>>[] = [];

    for (const track of metadataRows) {
      const album = track.spotifyAlbumId
        ? albumMap.get(track.spotifyAlbumId)
        : null;

      const resolved = await this.entityResolutionService.resolveSong({
        artistId,
        title: track.title,
        spotifyTrackId: track.spotifyTrackId,
        source: 'spotify',
        allowCreate: true,
        markProvisionalIfCreated: false,
        externalIds: [
          {
            source: 'spotify',
            externalId: track.spotifyTrackId,
          },
        ],
      });

      if (!resolved) {
        this.logger.warn(
          `[Scraper] Failed to resolve song ${track.title} (${track.spotifyTrackId})`,
        );
        continue;
      }

      const existing = existingMap.get(track.spotifyTrackId);

      const saved = await this.songsRepository.updateById(resolved.id, {
        artistId,
        albumId: album?.id ?? null,
        title: track.title,
        normalizedTitle: this.normalizeTitle(track.title),
        canonicalTitle: track.title,
        spotifyTrackId: track.spotifyTrackId,
        releaseDate: track.releaseDate || null,
        durationMs: track.durationMs,
        explicit: track.explicit,
        imageUrl: track.albumImageUrl,
        isAfrobeats: existing?.isAfrobeats ?? false,
        sourceOfTruth: 'spotify',
        entityStatus: existing?.entityStatus ?? 'canonical',
        needsReview: false,
      });

      results.push(saved);
    }

    return results;
  }

  async enrichPending(spotifyTrackIds: string[], artistId: string) {
    const results: Awaited<ReturnType<typeof this.enrichOne>>[] = [];
    let failed = 0;

    for (const id of spotifyTrackIds) {
      try {
        const enriched = await this.enrichOne(artistId, id);
        results.push(enriched);
      } catch (err) {
        failed++;
        this.logger.warn(
          `[Scraper] Failed to enrich ${id}: ${(err as Error).message}`,
        );
      }
    }

    this.logger.log(
      `[Scraper] Enriched ${results.length} songs, ${failed} failed`,
    );

    return results;
  }

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
