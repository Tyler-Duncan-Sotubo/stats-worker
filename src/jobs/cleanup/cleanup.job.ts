// src/modules/cleanup/cleanup.job.ts
import { Injectable, Logger } from '@nestjs/common';
import { CleanupService } from 'src/services/cleanup.service';

@Injectable()
export class CleanupJob {
  private readonly logger = new Logger(CleanupJob.name);

  constructor(private readonly cleanupService: CleanupService) {}

  async run(): Promise<void> {
    this.logger.log('Cleanup job starting...');
    try {
      await this.cleanupService.cleanupMonthlyListenerSnapshots();
      await this.cleanupService.cleanupArtistStatsSnapshots();
      await this.cleanupService.cleanupSongStatsSnapshots();
      this.logger.log('Cleanup job complete');
    } catch (err) {
      this.logger.error(
        `Cleanup job failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      throw err;
    }
  }
}
