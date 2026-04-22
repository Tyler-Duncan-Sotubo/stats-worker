import { Injectable, Logger } from '@nestjs/common';
import { OfficialChartsBackfillService } from 'src/scraper/uk-chart/official-charts-backfill.service';

@Injectable()
export class OfficialChartsIngestionJob {
  private readonly logger = new Logger(OfficialChartsIngestionJob.name);

  constructor(
    private readonly officialChartsBackfillService: OfficialChartsBackfillService,
  ) {}

  async run(): Promise<void> {
    this.logger.log('Official Charts ingestion job starting');

    await Promise.allSettled([
      // UK Singles Chart
      this.officialChartsBackfillService.runLatest({
        chartPath: 'singles-chart',
        chartName: 'uk_official_singles',
        chartId: '7501',
      }),

      // Afrobeats Chart
      this.officialChartsBackfillService.runLatest({
        chartPath: 'afrobeats-chart',
        chartName: 'official_afrobeats_chart',
        chartId: 'afrobeat', // replace with actual ID from URL
      }),
    ]);

    this.logger.log('Official Charts ingestion job complete');
  }
}
