// src/modules/backfill/processors/official-charts-backfill.processor.ts

import { Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Job, WorkerOptions } from 'bullmq';
import { OfficialChartsBackfillService } from './official-charts-backfill.service';

@Processor('officialChartsBackfillQueue', {
  concurrency: 1,
  limiter: {
    max: 1,
    duration: 1000,
  },
} as WorkerOptions)
@Injectable()
export class OfficialChartsBackfillProcessor extends WorkerHost {
  private readonly logger = new Logger(OfficialChartsBackfillProcessor.name);

  constructor(
    private readonly officialChartsBackfillService: OfficialChartsBackfillService,
  ) {
    super();
  }

  @OnWorkerEvent('ready')
  onReady() {
    this.logger.log('official charts worker ready');
  }

  @OnWorkerEvent('completed')
  onCompleted(job: Job) {
    this.logger.log({
      op: 'official_charts.worker.completed',
      jobId: job.id,
      jobName: job.name,
    });
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job, err: Error) {
    this.logger.error(
      {
        op: 'official_charts.worker.failed',
        jobId: job?.id,
        jobName: job?.name,
        err: err?.message,
      },
      err?.stack,
    );
  }

  process(job: Job): any {
    switch (job.name) {
      case 'official-charts-backfill-all':
        return this.officialChartsBackfillService.run(job);

      case 'official-charts-backfill-range':
        return this.officialChartsBackfillService.run(job, {
          fromDate: job.data.fromDate,
          toDate: job.data.toDate,
        });

      default:
        this.logger.warn(`Unhandled job: ${job.name}`);
        return;
    }
  }
}
