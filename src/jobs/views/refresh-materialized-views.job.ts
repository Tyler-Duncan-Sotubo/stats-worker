import { Injectable, Logger } from '@nestjs/common';
import { VacuumService } from 'src/services/vacuum.service';
import { ViewsRefreshService } from 'src/services/views-refresh.service';

@Injectable()
export class RefreshMaterializedViewsJob {
  private readonly logger = new Logger(RefreshMaterializedViewsJob.name);

  constructor(
    private readonly viewsRefreshService: ViewsRefreshService,
    private readonly vacuumService: VacuumService,
  ) {}

  async run(): Promise<void> {
    this.logger.log('Materialized view refresh starting');

    await this.viewsRefreshService.refreshAll();
    await this.vacuumService.run();
    this.logger.log('Materialized view refresh complete');
  }
}
