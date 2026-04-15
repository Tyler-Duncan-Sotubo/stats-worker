import { Injectable, Logger } from '@nestjs/common';
import slugify from 'slugify';
import { ArtistsRepository } from 'src/repository/artists.repository';
import { SpotifyMetadataService } from 'src/scraper/services/spotify-metadata.service';
import { EntityResolutionService } from './entity-resolution.service';
import { DiscoveredArtist } from 'src/scraper/services/kworb-artist-discovery.service';

@Injectable()
export class ArtistsService {
  private readonly logger = new Logger(ArtistsService.name);

  constructor(
    private readonly artistsRepository: ArtistsRepository,
    private readonly spotifyMetadata: SpotifyMetadataService,
    private readonly entityResolution: EntityResolutionService,
  ) {}

  // src/modules/artists/artists.service.ts — seedFromDiscovery updated

  async seedFromDiscovery(discovered: DiscoveredArtist[]): Promise<void> {
    if (!discovered.length) return;

    // Load all existing artists once for name matching
    const existingArtists = await this.artistsRepository.findAllBasic();

    const bySpotifyId = new Map(
      existingArtists.filter((a) => a.spotifyId).map((a) => [a.spotifyId!, a]),
    );
    const byNormName = new Map(
      existingArtists.map((a) => [this.normaliseName(a.name), a]),
    );
    const bySlug = new Map(existingArtists.map((a) => [a.slug, a]));

    const today = new Date().toISOString().split('T')[0];

    let linked = 0;
    let created = 0;
    let enriched = 0;
    let skipped = 0;

    for (const artist of discovered) {
      const normName = this.normaliseName(artist.name);
      const slug = this.buildSlug(artist.name);

      // ── Case 1: already have this Spotify ID → just write listener snapshot
      const existingBySpotifyId = bySpotifyId.get(artist.spotifyId);
      if (existingBySpotifyId) {
        if (artist.monthlyListeners != null) {
          await this.artistsRepository.upsertMonthlyListenerSnapshot({
            artistId: existingBySpotifyId.id,
            spotifyId: artist.spotifyId,
            snapshotDate: today,
            monthlyListeners: artist.monthlyListeners,
            dailyChange: artist.dailyChange ?? null,
            peakRank: artist.peakRank ?? null,
            peakListeners: artist.peakListeners ?? null,
          });
          enriched++;
        } else {
          skipped++;
        }
        continue;
      }

      // ── Case 2: artist exists by name but no Spotify ID yet
      // (seeded by Billboard) → link the Spotify ID + write snapshot
      const existingByName = byNormName.get(normName) || bySlug.get(slug);
      if (existingByName && !existingByName.spotifyId) {
        await this.artistsRepository.updateSpotifyId(
          existingByName.id,
          artist.spotifyId,
        );

        // Update local maps so subsequent iterations don't re-link
        bySpotifyId.set(artist.spotifyId, {
          ...existingByName,
          spotifyId: artist.spotifyId,
        });

        if (artist.monthlyListeners != null) {
          await this.artistsRepository.upsertMonthlyListenerSnapshot({
            artistId: existingByName.id,
            spotifyId: artist.spotifyId,
            snapshotDate: today,
            monthlyListeners: artist.monthlyListeners,
            dailyChange: artist.dailyChange ?? null,
            peakRank: artist.peakRank ?? null,
            peakListeners: artist.peakListeners ?? null,
          });
        }

        this.logger.log(
          `Linked Spotify ID to existing artist "${existingByName.name}" → ${artist.spotifyId}`,
        );
        linked++;
        continue;
      }

      // ── Case 3: genuinely new artist → create via entity resolution
      if (!existingByName) {
        const resolved = await this.entityResolution.resolveArtist({
          name: artist.name,
          spotifyId: artist.spotifyId,
          source: 'kworb',
          allowCreate: true,
          markProvisionalIfCreated: false,
          externalIds: [
            { source: 'spotify', externalId: artist.spotifyId },
            {
              source: 'kworb',
              externalId: artist.spotifyId,
              externalUrl: `https://kworb.net/spotify/artist/${artist.spotifyId}.html`,
            },
          ],
        });

        if (resolved && artist.monthlyListeners != null) {
          await this.artistsRepository.upsertMonthlyListenerSnapshot({
            artistId: resolved.id,
            spotifyId: artist.spotifyId,
            snapshotDate: today,
            monthlyListeners: artist.monthlyListeners,
            dailyChange: artist.dailyChange ?? null,
            peakRank: artist.peakRank ?? null,
            peakListeners: artist.peakListeners ?? null,
          });
        }

        created++;
      }
    }

    this.logger.log(
      `Discovery seed complete — ${linked} linked, ${created} created, ` +
        `${enriched} listener snapshots updated, ${skipped} skipped`,
    );
  }

  async syncListenerSnapshots(discovered: DiscoveredArtist[]): Promise<void> {
    if (!discovered.length) return;

    const existingArtists = await this.artistsRepository.findAllBasic();
    const bySpotifyId = new Map(
      existingArtists
        .filter((a) => a.spotifyId)
        .map((a) => [a.spotifyId as string, a]),
    );

    const today = new Date().toISOString().split('T')[0];

    let updated = 0;
    let skippedNoListenerData = 0;
    let skippedNotFound = 0;

    for (const artist of discovered) {
      const existing = bySpotifyId.get(artist.spotifyId);

      if (!existing) {
        skippedNotFound += 1;
        continue;
      }

      if (artist.monthlyListeners == null) {
        skippedNoListenerData += 1;
        continue;
      }

      await this.artistsRepository.upsertMonthlyListenerSnapshot({
        artistId: existing.id,
        spotifyId: artist.spotifyId,
        snapshotDate: today,
        monthlyListeners: artist.monthlyListeners,
        dailyChange: artist.dailyChange ?? null,
        peakRank: artist.peakRank ?? null,
        peakListeners: artist.peakListeners ?? null,
      });

      updated += 1;
    }

    this.logger.log(
      `Listener snapshot sync complete — ${updated} updated, ` +
        `${skippedNotFound} skipped (artist not in catalog), ` +
        `${skippedNoListenerData} skipped (no listener data)`,
    );
  }

  async enrichAndUpsert(spotifyIds: string[]): Promise<void> {
    if (!spotifyIds.length) return;

    const metadata =
      await this.spotifyMetadata.fetchMultipleArtists(spotifyIds);

    const rows = metadata.map((m) => ({
      name: m.name,
      normalizedName: this.normaliseName(m.name),
      canonicalName: m.name,
      spotifyId: m.spotifyId,
      slug: this.buildSlug(m.name),
      imageUrl: m.imageUrl,
      sourceOfTruth: 'spotify',
      entityStatus: 'canonical' as const,
      needsReview: false,
    }));

    const upserted = await this.artistsRepository.upsertManyBySpotifyId(rows);
    this.logger.log(`Upserted ${upserted.length} artists`);
  }

  private buildSlug(name: string): string {
    const result = slugify(name, { lower: true, strict: true });
    if (typeof result === 'string') return result;
    throw new Error(`Failed to slugify artist name: ${name}`);
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
