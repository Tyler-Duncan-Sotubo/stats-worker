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
import { CleanupJob } from './cleanup/cleanup.job';
import { VacuumService } from 'src/services/vacuum.service';

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
    private readonly cleanupJob: CleanupJob,
    private readonly vacuumService: VacuumService,
  ) {}

  // ───────────────────────────────────────────────────────────────────────────
  // VACUUM
  // 12:00 AM — clean slate before scraping starts
  // 12:00 PM — midday clean during scraping
  // 4:00 PM  — afternoon clean before chart ingestion
  // 9:00 PM  — MV refresh handles post-write vacuum automatically
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 0 * * *', { timeZone: 'Europe/London' })
  async runMidnightVacuum(): Promise<void> {
    await this.vacuumService.run();
  }

  @Cron('0 12 * * *', { timeZone: 'Europe/London' })
  async runMiddayVacuum(): Promise<void> {
    await this.vacuumService.run();
  }

  @Cron('0 16 * * *', { timeZone: 'Europe/London' })
  async runAfternoonVacuum(): Promise<void> {
    await this.vacuumService.run();
  }
  // ───────────────────────────────────────────────────────────────────────────
  // DISCOVERY
  // Weekly on Monday at 3:00 AM — discover artists and seed catalog
  // Runs after cleanup, before snapshots start
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('0 3 * * 1', { timeZone: 'Europe/London' })
  async runArtistDiscovery(): Promise<void> {
    await this.discoveryJob.runDiscoveryAndSeed();
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

  // daily ≥5M — every day at 04:00
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
  // CHART INGESTION
  // Daily charts at 6:00 PM — after kworb data has settled for the day
  // Official Charts every Friday at 7:00 PM — after UK chart release
  // Billboard every Saturday at 7:00 PM — after Billboard release
  // ───────────────────────────────────────────────────────────────────────────

  @Cron('00 19 * * *', { timeZone: 'Europe/London' })
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

  @Cron('00 21 * * *', { timeZone: 'Europe/London' })
  async runDailyMaterializedViewRefresh(): Promise<void> {
    await this.refreshMaterializedViewsJob.run();
  }
}
