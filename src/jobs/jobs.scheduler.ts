import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { DiscoveryJob } from './discovery/discovery.job';
import { CertificationsJob } from './certifications/certifications.job';
import { DailyChartIngestionJob } from './charts/daily-chart-ingestion.job';
import { RefreshMaterializedViewsJob } from './views/refresh-materialized-views.job';
import { UnifiedSnapshotJob } from './snapshots/unified-snapshot.job';
import { OfficialChartsIngestionJob } from './charts/official-charts-injecton.job';
import { BillboardIngestionJob } from './charts/billboard-injection.job';
import { MilestoneDetectionJob } from './milestone/milestone-detector.job';
import { CacheWarmJob } from 'src/infrastructure/cache/cache-warm.job';

@Injectable()
export class JobsScheduler {
  constructor(
    private readonly discoveryJob: DiscoveryJob,
    private readonly certificationsJob: CertificationsJob,
    private readonly dailyChartIngestionJob: DailyChartIngestionJob,
    private readonly refreshMaterializedViewsJob: RefreshMaterializedViewsJob,
    private readonly unifiedSnapshotJob: UnifiedSnapshotJob,
    private readonly officialChartsIngestionJob: OfficialChartsIngestionJob,
    private readonly billboardIngestionJob: BillboardIngestionJob,
    private readonly milestoneDetectionJob: MilestoneDetectionJob,
    private readonly cacheWarmJob: CacheWarmJob,
  ) {}

  // ───────────────────────────────────────────────────────────────────────────
  // DISCOVERY
  // Weekly on Monday at 2:00 AM — discover new artists and seed catalog
  // Early enough to complete before snapshots start at 4:00 AM
  // ───────────────────────────────────────────────────────────────────────────

  // Monday 2AM — discover new artists and seed catalog
  @Cron('0 2 * * 1', { timeZone: 'Europe/London' })
  async runArtistDiscovery(): Promise<void> {
    await this.discoveryJob.runDiscoveryAndSeed();
  }

  // Tuesday–Sunday 11AM — sync listener snapshots for existing artists
  @Cron('0 17 * * 2,3,4,5,6,0', { timeZone: 'Europe/London' })
  async runListenerSnapshotSync(): Promise<void> {
    await this.discoveryJob.runListenerSnapshotSync();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // CERTIFICATIONS
  // Weekly on Sunday at 2:00 AM — low priority, once a week is enough
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('*/30 * * * 0', { timeZone: 'Europe/London' })
  async runCertifications(): Promise<void> {
    await this.certificationsJob.runBatch();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // LISTENER SNAPSHOTS
  // Kworb update frequency:
  //   daily tier  (≥5M)       — every day at 04:00
  //   high tier   (3M–5M)     — every day at 05:00
  //   mid  tier   (2M–3M)     — Tue, Thu, Sat at 06:00
  //   mid2 tier   (1.5M–2M)   — Tue, Thu, Sat at 07:00
  //   mid3 tier   (1.25M–1.5M)— Mon, Thu at 08:00
  //   mid4 tier   (1M–1.25M)  — Mon, Thu at 09:00
  //   low  tier   (875K–1M)   — Wed at 10:00
  //   low2 tier   (750K–875K) — Wed at 11:00
  //   low3 tier   (625K–750K) — Sat at 10:00
  //   low4 tier   (<625K)     — Sat at 11:00
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 4 * * *', { timeZone: 'Europe/London' })
  async runDailyTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('daily');
  }

  @Cron('0 5 * * *', { timeZone: 'Europe/London' })
  async runHighTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('high');
  }

  @Cron('0 6 * * 2,4,6', { timeZone: 'Europe/London' })
  async runMidTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid');
  }

  @Cron('0 7 * * 2,4,6', { timeZone: 'Europe/London' })
  async runMid2TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid2');
  }

  @Cron('0 8 * * 1,4', { timeZone: 'Europe/London' })
  async runMid3TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid3');
  }

  @Cron('0 9 * * 1,4', { timeZone: 'Europe/London' })
  async runMid4TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid4');
  }

  @Cron('0 10 * * 3', { timeZone: 'Europe/London' })
  async runLowTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low');
  }

  @Cron('0 11 * * 3', { timeZone: 'Europe/London' })
  async runLow2TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low2');
  }

  @Cron('0 10 * * 6', { timeZone: 'Europe/London' })
  async runLow3TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low3');
  }

  @Cron('0 11 * * 6', { timeZone: 'Europe/London' })
  async runLow4TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low4');
  }

  // ───────────────────────────────────────────────────────────────────────────
  // CHART INGESTION
  // Daily charts at 6:00 PM — after kworb data has settled
  // Official Charts every Friday at 7:00 PM — after UK chart release
  // Billboard every Saturday at 7:00 PM — after Billboard release
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 18 * * *', { timeZone: 'Europe/London' })
  async runDailyChartIngestion(): Promise<void> {
    await this.dailyChartIngestionJob.run();
  }

  @Cron('0 19 * * 5', { timeZone: 'Europe/London' })
  async runOfficialChartsIngestion(): Promise<void> {
    await this.officialChartsIngestionJob.run();
  }

  @Cron('0 19 * * 6', { timeZone: 'Europe/London' })
  async runBillboardIngestion(): Promise<void> {
    await this.billboardIngestionJob.run();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // MATERIALIZED VIEW REFRESH
  // Once daily at 8:00 PM — after all chart ingestion is done for the day
  // Late enough to capture daily charts (6PM), Official (7PM Fri),
  // Billboard (7PM Sat) before refreshing
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 20 * * *', { timeZone: 'Europe/London' })
  async runDailyMaterializedViewRefresh(): Promise<void> {
    await this.refreshMaterializedViewsJob.run();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // MILESTONE DETECTION
  // Once daily at 9:00 PM — after MV refresh completes
  // MVs must be fresh before detecting new milestones
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 21 * * *', { timeZone: 'Europe/London' })
  async runMilestoneDetection(): Promise<void> {
    await this.milestoneDetectionJob.runDetection();
  }

  // after runMilestoneDetection
  // ───────────────────────────────────────────────────────────────────────────
  // CACHE WARM
  // Once daily at 10:00 PM — after milestone detection completes
  // MVs and milestones must be fresh before warming sitemap cache
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 22 * * *', { timeZone: 'Europe/London' })
  async runCacheWarm(): Promise<void> {
    await this.cacheWarmJob.run();
  }
}
