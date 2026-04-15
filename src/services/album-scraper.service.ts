import { Injectable, Logger } from '@nestjs/common';
import { AlbumsRepository } from 'src/repository/albums.repository';

export interface ScraperAlbumInput {
  artistId: string;
  spotifyAlbumId: string;
  title: string;
  albumType?: string | null;
  releaseDate?: string | null;
  imageUrl?: string | null;
  totalTracks?: number | null;
}

@Injectable()
export class AlbumScraperService {
  private readonly logger = new Logger(AlbumScraperService.name);

  constructor(private readonly albumsRepository: AlbumsRepository) {}

  // Used by snapshot pipeline — finds existing or creates a minimal record.
  // Never overwrites isAfrobeats or any editorial field.
  async findOrCreate(input: ScraperAlbumInput) {
    const existing = await this.albumsRepository.findBySpotifyAlbumId(
      input.spotifyAlbumId,
    );

    if (existing) return existing;

    const slug = this.buildSlug(input.title, input.spotifyAlbumId);

    const created = await this.albumsRepository.upsertScraperFields({
      artistId: input.artistId,
      spotifyAlbumId: input.spotifyAlbumId,
      title: input.title,
      slug,
      albumType: input.albumType ?? 'album',
      releaseDate: input.releaseDate ?? null,
      imageUrl: input.imageUrl ?? null,
      totalTracks: input.totalTracks ?? null,
    });

    return created;
  }

  // Used when scraper has fresher metadata — updates only scraper-owned fields.
  // Safe to call repeatedly without risking editorial data loss.
  async upsert(input: ScraperAlbumInput) {
    const existing = await this.albumsRepository.findBySpotifyAlbumId(
      input.spotifyAlbumId,
    );

    const slug =
      existing?.slug ?? this.buildSlug(input.title, input.spotifyAlbumId);

    const saved = await this.albumsRepository.upsertScraperFields({
      artistId: input.artistId,
      spotifyAlbumId: input.spotifyAlbumId,
      title: input.title,
      slug,
      albumType: input.albumType ?? 'album',
      releaseDate: input.releaseDate ?? null,
      imageUrl: input.imageUrl ?? null,
      totalTracks: input.totalTracks ?? null,
    });

    return saved;
  }

  // Batch upsert — used when enriching many albums at once from Spotify/Kworb.
  async upsertMany(inputs: ScraperAlbumInput[]) {
    if (!inputs.length) return [];

    const rows = inputs.map((input) => ({
      artistId: input.artistId,
      spotifyAlbumId: input.spotifyAlbumId,
      title: input.title,
      slug: this.buildSlug(input.title, input.spotifyAlbumId),
      albumType: input.albumType ?? 'album',
      releaseDate: input.releaseDate ?? null,
      imageUrl: input.imageUrl ?? null,
      totalTracks: input.totalTracks ?? null,
    }));

    const saved = await this.albumsRepository.upsertManyScraperFields(rows);

    this.logger.log(`[Scraper] Upserted ${saved.length} albums`);

    return saved;
  }

  private buildSlug(title: string, spotifyAlbumId: string): string {
    return `${this.slugify(title)}-${spotifyAlbumId.slice(0, 8)}`;
  }

  private slugify(value: string): string {
    return value
      .toLowerCase()
      .trim()
      .replace(/['"]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .replace(/-{2,}/g, '-');
  }
}
