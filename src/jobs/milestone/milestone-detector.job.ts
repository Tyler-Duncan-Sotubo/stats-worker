import { Injectable, Logger } from '@nestjs/common';
import { MilestoneDetectorService } from './milestone-detector.service';

@Injectable()
export class MilestoneDetectionJob {
  private readonly logger = new Logger(MilestoneDetectionJob.name);

  constructor(private readonly milestoneDetector: MilestoneDetectorService) {}

  async runDetection(): Promise<void> {
    this.logger.log('Milestone detection job starting');

    const milestones = await this.milestoneDetector.detect();

    if (!milestones.length) {
      this.logger.log('No new milestones detected');
      return;
    }

    this.logger.log(`Detected ${milestones.length} new milestones:`);

    for (const milestone of milestones) {
      const tweet = this.milestoneDetector.generateTweetText(milestone);
      this.logger.log(`  → ${tweet}`);
    }

    this.logger.log('Milestone detection job complete');
  }
}
