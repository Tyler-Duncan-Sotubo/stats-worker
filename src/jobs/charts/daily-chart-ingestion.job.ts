import { Injectable, Logger } from '@nestjs/common';
import { DailyChartIngestionService } from 'src/scraper/chart/daily-chart-ingestion.service';

@Injectable()
export class DailyChartIngestionJob {
  private readonly logger = new Logger(DailyChartIngestionJob.name);

  constructor(
    private readonly dailyChartIngestionService: DailyChartIngestionService,
  ) {}

  async run(): Promise<void> {
    this.logger.log('Daily chart ingestion job starting');

    await this.dailyChartIngestionService.runDailyIngestion();

    this.logger.log('Daily chart ingestion job complete');
  }
}
