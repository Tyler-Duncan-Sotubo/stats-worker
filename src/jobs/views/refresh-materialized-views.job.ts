import { Injectable, Logger } from '@nestjs/common';
import { ViewsRefreshService } from 'src/services/views-refresh.service';

@Injectable()
export class RefreshMaterializedViewsJob {
  private readonly logger = new Logger(RefreshMaterializedViewsJob.name);

  constructor(private readonly viewsRefreshService: ViewsRefreshService) {}

  async run(): Promise<void> {
    this.logger.log('Materialized view refresh starting');

    await this.viewsRefreshService.refreshAll();

    this.logger.log('Materialized view refresh complete');
  }
}
