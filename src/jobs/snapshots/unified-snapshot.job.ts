// src/modules/snapshots/unified-snapshot.job.ts
import { Injectable, Logger } from '@nestjs/common';
import { SnapshotService, TierFilter } from 'src/services/snapshot.service';

@Injectable()
export class UnifiedSnapshotJob {
  private readonly logger = new Logger(UnifiedSnapshotJob.name);

  constructor(private readonly snapshotService: SnapshotService) {}

  async run(filter: TierFilter = 'all'): Promise<void> {
    this.logger.log(`Unified snapshot starting — filter=${filter}`);
    try {
      await this.snapshotService.runAll(filter);
      this.logger.log(`Unified snapshot complete — filter=${filter}`);
    } catch (err) {
      this.logger.error(
        `Unified snapshot failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      throw err;
    }
  }
}
