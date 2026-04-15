import { Injectable, Logger } from '@nestjs/common';
import { SnapshotService } from 'src/services/snapshot.service';

@Injectable()
export class SongSnapshotJob {
  private readonly logger = new Logger(SongSnapshotJob.name);

  constructor(private readonly snapshotService: SnapshotService) {}

  async run(): Promise<void> {
    this.logger.log('Weekly song snapshot starting');

    try {
      await this.snapshotService.snapshotAllSongs();
      this.logger.log('Weekly song snapshot complete');
    } catch (err) {
      this.logger.error(
        `Weekly song snapshot failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      throw err;
    }
  }
}
