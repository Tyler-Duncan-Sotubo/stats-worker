import { Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Job, WorkerOptions } from 'bullmq';
import { BillboardBackfillService } from './billboard-backfill.service';

@Processor('billboardBackfillQueue', {
  concurrency: 1,
  limiter: {
    max: 1,
    duration: 1000,
  },
} as WorkerOptions)
@Injectable()
export class BillboardBackfillProcessor extends WorkerHost {
  private readonly logger = new Logger(BillboardBackfillProcessor.name);

  constructor(
    private readonly billboardBackfillService: BillboardBackfillService,
  ) {
    super();
  }

  @OnWorkerEvent('ready')
  onReady() {
    this.logger.log('billboard worker ready');
  }

  @OnWorkerEvent('completed')
  onCompleted(job: Job) {
    this.logger.log({
      op: 'billboard.worker.completed',
      jobId: job.id,
      jobName: job.name,
    });
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job, err: Error) {
    this.logger.error(
      {
        op: 'billboard.worker.failed',
        jobId: job?.id,
        jobName: job?.name,
        err: err?.message,
      },
      err?.stack,
    );
  }

  async process(job: Job): Promise<any> {
    switch (job.name) {
      case 'billboard-backfill-all':
        return this.billboardBackfillService.run(job);

      default:
        this.logger.warn(`Unhandled job: ${job.name}`);
        return;
    }
  }
}
