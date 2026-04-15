import { Injectable, Logger } from '@nestjs/common';
import { SnapshotService } from 'src/services/snapshot.service';

@Injectable()
export class ArtistSnapshotJob {
  private readonly logger = new Logger(ArtistSnapshotJob.name);

  constructor(private readonly snapshotService: SnapshotService) {}

  async run(): Promise<void> {
    this.logger.log('Daily artist snapshot starting');

    try {
      await this.snapshotService.snapshotAllArtists();
      this.logger.log('Daily artist snapshot complete');
    } catch (err) {
      this.logger.error(
        `Daily artist snapshot failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      throw err;
    }
  }
}
