// src/modules/backfill/producers/official-charts-backfill.producer.ts

import { InjectQueue } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { Queue } from 'bullmq';

@Injectable()
export class OfficialChartsBackfillProducer {
  constructor(
    @InjectQueue('officialChartsBackfillQueue')
    private readonly queue: Queue,
  ) {}

  // Full historical backfill — all weeks from 2000 to now
  async enqueueFullBackfill() {
    return this.queue.add(
      'official-charts-backfill-all',
      {},
      {
        jobId: 'official-charts-backfill-all',
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 5000,
        },
        removeOnComplete: false,
        removeOnFail: false,
      },
    );
  }

  // Targeted range — useful for incremental updates or re-running a bad week
  async enqueueRangeBackfill(fromDate: string, toDate: string) {
    return this.queue.add(
      'official-charts-backfill-range',
      { fromDate, toDate },
      {
        jobId: `official-charts-backfill-${fromDate}-${toDate}`,
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 5000,
        },
        removeOnComplete: false,
        removeOnFail: false,
      },
    );
  }
}
