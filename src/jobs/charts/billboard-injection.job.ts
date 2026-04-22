import { Injectable, Logger } from '@nestjs/common';
import { BillboardBackfillService } from 'src/scraper/billboard/billboard-backfill.service';
@Injectable()
export class BillboardIngestionJob {
  private readonly logger = new Logger(BillboardIngestionJob.name);

  constructor(
    private readonly billboardBackfillService: BillboardBackfillService,
  ) {}

  async run(): Promise<void> {
    this.logger.log('Billboard ingestion job starting');

    await this.billboardBackfillService.runLatest();

    this.logger.log('Billboard ingestion job complete');
  }
}
