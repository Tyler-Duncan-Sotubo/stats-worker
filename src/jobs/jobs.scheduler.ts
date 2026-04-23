import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { DiscoveryJob } from './discovery/discovery.job';
import { CertificationsJob } from './certifications/certifications.job';
import { DailyChartIngestionJob } from './charts/daily-chart-ingestion.job';
import { RefreshMaterializedViewsJob } from './views/refresh-materialized-views.job';
import { OriginCountryEnrichmentJob } from './origin/origin-country-enrichment.job';
import { ArtistEnrichmentJob } from './catalog/artist-enrichment.job';
import { SongEnrichmentJob } from './catalog/song-enrichment.job';
import { UnifiedSnapshotJob } from './snapshots/unified-snapshot.job';
import { OfficialChartsIngestionJob } from './charts/official-charts-injecton.job';
import { BillboardIngestionJob } from './charts/billboard-injection.job';

@Injectable()
export class JobsScheduler {
  constructor(
    private readonly discoveryJob: DiscoveryJob,
    private readonly certificationsJob: CertificationsJob,
    private readonly dailyChartIngestionJob: DailyChartIngestionJob,
    private readonly refreshMaterializedViewsJob: RefreshMaterializedViewsJob,
    private readonly originCountryEnrichmentJob: OriginCountryEnrichmentJob,
    private readonly artistEnrichmentJob: ArtistEnrichmentJob,
    private readonly songEnrichmentJob: SongEnrichmentJob,
    private readonly unifiedSnapshotJob: UnifiedSnapshotJob,
    private readonly officialChartsIngestionJob: OfficialChartsIngestionJob,
    private readonly billboardIngestionJob: BillboardIngestionJob,
  ) {}

  // ───────────────────────────────────────────────────────────────────────────
  // DISCOVERY
  // ───────────────────────────────────────────────────────────────────────────

  // Weekly on Monday at 1:00 AM — discover artists and seed catalog
  @Cron('0 1 * * 1', { timeZone: 'Europe/London' })
  async runArtistDiscovery(): Promise<void> {
    await this.discoveryJob.runDiscoveryAndSeed();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // ENRICHMENT
  // Artist + song enrichment disabled until Spotify rate limit issue resolved
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 12 * * 2,3,4,5,6,0', { timeZone: 'Europe/London' })
  async runListenerSnapshotSync(): Promise<void> {
    await this.discoveryJob.runListenerSnapshotSync();
  }
  // TODO: Done for now

  // Certifications — every 4 minutes
  // @Cron('* * * * *', { timeZone: 'Europe/London' })
  // async runCertificationsBatch(): Promise<void> {
  //   await this.certificationsJob.runBatch();
  // }

  // Origin country enrichment — every 5 minutes
  // @Cron('*/5 * * * *', { timeZone: 'Europe/London' })
  // async runOriginCountryEnrichment(): Promise<void> {
  //   await this.originCountryEnrichmentJob.runBatch();
  // }

  // ───────────────────────────────────────────────────────────────────────────
  // CHART INGESTION
  // Daily charts run 3x per day to catch updates throughout the day
  // Official + Billboard run weekly on their respective release days
  // ───────────────────────────────────────────────────────────────────────────

  // 7:00 PM — midday run
  @Cron('00 19 * * *', { timeZone: 'Europe/London' })
  async runDailyChartIngestionMidday(): Promise<void> {
    await this.dailyChartIngestionJob.run();
    await this.refreshMaterializedViewsJob.run();
  }

  // Every Friday at 8:00 PM — UK Official Charts
  @Cron('0 20 * * 5', { timeZone: 'Europe/London' })
  async runOfficialChartsIngestion(): Promise<void> {
    await this.officialChartsIngestionJob.run();
    await this.refreshMaterializedViewsJob.run();
  }

  // Every Saturday at 8:00 PM — Billboard
  @Cron('0 20 * * 6', { timeZone: 'Europe/London' })
  async runBillboardIngestion(): Promise<void> {
    await this.billboardIngestionJob.run();
    await this.refreshMaterializedViewsJob.run();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // LISTENER SNAPSHOTS
  // Kworb update frequency observed:
  //   daily tier  (≥5M)         — updates daily
  //   high tier   (3M–5M)       — updates daily
  //   mid  tier   (2M–3M)       — updates every 2–3 days
  //   mid2 tier   (1.5M–2M)     — updates every 2–3 days
  //   mid3+       (<1.5M)       — updates weekly or less
  // All times Europe/London — data source updates by ~04:00
  // ───────────────────────────────────────────────────────────────────────────

  // daily ≥ 5M — every day at 04:00
  @Cron('0 4 * * *', { timeZone: 'Europe/London' })
  async runDailyTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('daily');
  }

  // high 3M–5M — every day at 05:00
  @Cron('0 5 * * *', { timeZone: 'Europe/London' })
  async runHighTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('high');
  }

  // mid 2M–3M — Tue, Thu, Sat at 06:00
  @Cron('0 6 * * 2,4,6', { timeZone: 'Europe/London' })
  async runMidTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid');
  }

  // mid2 1.5M–2M — Tue, Thu, Sat at 08:00
  @Cron('0 8 * * 2,4,6', { timeZone: 'Europe/London' })
  async runMid2TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid2');
  }

  // mid3 1.25M–1.5M — Mon, Thu at 10:00
  @Cron('0 10 * * 1,4', { timeZone: 'Europe/London' })
  async runMid3TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid3');
  }

  // mid4 1M–1.25M — Mon, Thu at 12:00
  @Cron('0 12 * * 1,4', { timeZone: 'Europe/London' })
  async runMid4TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('mid4');
  }

  // low 875K–1M — Wed at 14:00
  @Cron('0 14 * * 3', { timeZone: 'Europe/London' })
  async runLowTierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low');
  }

  // low2 750K–875K — Wed at 16:00
  @Cron('0 16 * * 3', { timeZone: 'Europe/London' })
  async runLow2TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low2');
  }

  // low3 625K–750K — Sat at 14:00
  @Cron('0 14 * * 6', { timeZone: 'Europe/London' })
  async runLow3TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low3');
  }

  // low4 <625K — Sat at 16:00
  @Cron('0 16 * * 6', { timeZone: 'Europe/London' })
  async runLow4TierSnapshot(): Promise<void> {
    await this.unifiedSnapshotJob.run('low4');
  }
  // ───────────────────────────────────────────────────────────────────────────
  // MATERIALIZED VIEW REFRESH
  // Mon–Sat at 9:00 AM — after overnight jobs settle
  // Sunday at 10:00 AM — gives Tier 4 (starts 3AM, ~6hrs) time to finish
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 9 * * 1,2,3,4,5,6', { timeZone: 'Europe/London' })
  async runWeekdayMaterializedViewRefresh(): Promise<void> {
    await this.refreshMaterializedViewsJob.run();
  }

  @Cron('0 10 * * 0', { timeZone: 'Europe/London' })
  async runSundayMaterializedViewRefresh(): Promise<void> {
    await this.refreshMaterializedViewsJob.run();
  }
}
