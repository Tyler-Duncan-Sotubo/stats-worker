import { Injectable, Logger } from '@nestjs/common';
import { KworbArtistDiscoveryService } from 'src/scraper/services/kworb-artist-discovery.service';
import { ArtistsService } from 'src/services/artists.service';

@Injectable()
export class DiscoveryJob {
  private readonly logger = new Logger(DiscoveryJob.name);

  constructor(
    private readonly discovery: KworbArtistDiscoveryService,
    private readonly artistsService: ArtistsService,
  ) {}

  async runDiscoveryAndSeed(): Promise<void> {
    this.logger.log('Artist discovery job starting');

    const discovered = await this.discovery.discoverAll();
    await this.artistsService.seedFromDiscovery(discovered.artists);

    this.logger.log('Artist discovery job complete');
  }

  async runListenerSnapshotSync(): Promise<void> {
    this.logger.log('Artist listener snapshot sync starting');

    const discovered = await this.discovery.discoverFromListenerPages([
      1, 2, 3, 4,
    ]);
    await this.artistsService.syncListenerSnapshots(discovered.artists);

    this.logger.log('Artist listener snapshot sync complete');
  }
}
