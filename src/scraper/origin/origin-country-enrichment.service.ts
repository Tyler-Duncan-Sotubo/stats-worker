import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { ArtistsRepository } from 'src/repository/artists.repository';

type MusicBrainzArtistSearchResponse = {
  artists?: Array<{
    id: string;
    name: string;
    score?: number | string;
    country?: string;
    area?: {
      name?: string;
      'iso-3166-1-codes'?: string[];
    };
  }>;
};

@Injectable()
export class OriginCountryEnrichmentService {
  private readonly logger = new Logger(OriginCountryEnrichmentService.name);

  constructor(private readonly artistsRepository: ArtistsRepository) {}

  async enrichArtistIfMissing(
    artistId: string,
    artistName: string,
  ): Promise<string | null> {
    const existing = await this.artistsRepository.findById(artistId);
    if (!existing) return null;
    if (existing.originCountry) return existing.originCountry;

    const country = await this.resolveOriginCountry(artistName);
    if (!country) return null;

    await this.artistsRepository.updateById(artistId, {
      originCountry: country,
    });

    return country;
  }

  private async resolveOriginCountry(name: string): Promise<string | null> {
    const { data } = await axios.get<MusicBrainzArtistSearchResponse>(
      'https://musicbrainz.org/ws/2/artist',
      {
        params: {
          query: `artist:"${name}"`,
          limit: 5,
          fmt: 'json',
        },
        timeout: 15000,
        headers: {
          Accept: 'application/json',
          'User-Agent':
            'tooXclusiveStatsBot/1.0 (contact: engineering@tooxclusive.com)',
        },
      },
    );

    const candidates = (data.artists ?? [])
      .map((artist) => ({
        ...artist,
        numericScore:
          typeof artist.score === 'string'
            ? parseInt(artist.score, 10)
            : (artist.score ?? 0),
      }))
      .sort((a, b) => b.numericScore - a.numericScore);

    const best = candidates[0];
    if (!best) return null;

    const exactish =
      this.normalize(best.name) === this.normalize(name) ||
      best.numericScore >= 95;

    if (!exactish) {
      this.logger.warn(
        `Low-confidence MusicBrainz match for "${name}" -> "${best.name}" (${best.numericScore})`,
      );
      return null;
    }

    const code =
      best.area?.['iso-3166-1-codes']?.[0]?.trim().toUpperCase() ??
      best.country?.trim().toUpperCase() ??
      null;

    if (!code || !/^[A-Z]{2}$/.test(code)) return null;

    return code;
  }

  private normalize(value: string): string {
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
