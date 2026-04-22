import { InjectQueue } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { Queue } from 'bullmq';

@Injectable()
export class BillboardBackfillProducer {
  constructor(
    @InjectQueue('billboardBackfillQueue')
    private readonly queue: Queue,
  ) {}

  async enqueueFullBackfill() {
    return this.queue.add(
      'billboard-backfill-all',
      {
        fromDate: '1990-01-01',
        toDate: new Date().toISOString().split('T')[0],
      },
      {
        jobId: 'billboard-backfill-all',
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
