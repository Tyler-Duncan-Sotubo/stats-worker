import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { DiscoveryJob } from './discovery/discovery.job';
import { CertificationsJob } from './certifications/certifications.job';
import { ArtistSnapshotJob } from './snapshots/artist-snapshot.job';
import { SongSnapshotJob } from './snapshots/song-snapshot.job';
import { DailyChartIngestionJob } from './charts/daily-chart-ingestion.job';
import { RefreshMaterializedViewsJob } from './views/refresh-materialized-views.job';
import { OriginCountryEnrichmentJob } from './origin/origin-country-enrichment.job';
import { ArtistEnrichmentJob } from './catalog/artist-enrichment.job';
import { SongEnrichmentJob } from './catalog/song-enrichment.job';

@Injectable()
export class JobsScheduler {
  constructor(
    private readonly discoveryJob: DiscoveryJob,
    private readonly certificationsJob: CertificationsJob,
    private readonly artistSnapshotJob: ArtistSnapshotJob,
    private readonly songSnapshotJob: SongSnapshotJob,
    private readonly dailyChartIngestionJob: DailyChartIngestionJob,
    private readonly refreshMaterializedViewsJob: RefreshMaterializedViewsJob,
    private readonly originCountryEnrichmentJob: OriginCountryEnrichmentJob,
    private readonly artistEnrichmentJob: ArtistEnrichmentJob,
    private readonly songEnrichmentJob: SongEnrichmentJob,
  ) {}

  // ───────────────────────────────────────────────────────────────────────────
  // DISCOVERY + LISTENER SYNC
  // ───────────────────────────────────────────────────────────────────────────

  // Weekly on Monday at 1:00 AM — discover artists and seed catalog
  @Cron('0 1 * * 1', { timeZone: 'Europe/London' })
  async runArtistDiscovery(): Promise<void> {
    await this.discoveryJob.runDiscoveryAndSeed();
  }

  // Daily at 8:00 PM — sync listener snapshots only
  @Cron('0 20 * * *', { timeZone: 'Europe/London' })
  async runListenerSnapshotSync(): Promise<void> {
    await this.discoveryJob.runListenerSnapshotSync();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // ENRICHMENT
  // ───────────────────────────────────────────────────────────────────────────

  // Monthly on the 1st at 2:00 AM — enrich artists from Spotify
  @Cron('0 2 1 * *', { timeZone: 'Europe/London' })
  async runArtistEnrichment(): Promise<void> {
    await this.artistEnrichmentJob.runBatch();
  }

  // Weekly on Sunday at 3:00 AM — enrich songs
  @Cron('0 3 * * 0', { timeZone: 'Europe/London' })
  async runSongEnrichment(): Promise<void> {
    await this.songEnrichmentJob.runBatch();
  }

  // Temporary catch-up: every 4 minutes
  // Monthly fallback: 2nd day of month at 4:00 AM
  @Cron('*/4 * * * *', { timeZone: 'Europe/London' })
  @Cron('0 4 2 * *', { timeZone: 'Europe/London' })
  async runOriginCountryEnrichment(): Promise<void> {
    await this.originCountryEnrichmentJob.runBatch();
  }

  // Temporary catch-up: every 2 minutes
  // Monthly fallback: 2nd day of month at 5:00 AM
  @Cron('*/2 * * * *', { timeZone: 'Europe/London' })
  @Cron('0 5 2 * *', { timeZone: 'Europe/London' })
  async runCertificationsBatch(): Promise<void> {
    await this.certificationsJob.runBatch();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // SNAPSHOTS
  // ───────────────────────────────────────────────────────────────────────────

  // Daily at 1:00 AM — artist snapshots
  @Cron('0 1 * * *', { timeZone: 'Europe/London' })
  async runArtistSnapshots(): Promise<void> {
    await this.artistSnapshotJob.run();
  }

  // Daily at 8:00 AM — song snapshots
  @Cron('0 8 * * *', { timeZone: 'Europe/London' })
  async runSongSnapshots(): Promise<void> {
    await this.songSnapshotJob.run();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // INGESTION
  // ───────────────────────────────────────────────────────────────────────────

  // Daily at 10:00 PM — ingest latest daily charts
  @Cron('0 14 * * *', { timeZone: 'Europe/London' })
  async runDailyChartIngestion(): Promise<void> {
    await this.dailyChartIngestionJob.run();
  }

  // ───────────────────────────────────────────────────────────────────────────
  // READ MODEL REFRESH
  // ───────────────────────────────────────────────────────────────────────────

  // Daily at 14:00 PM — refresh materialized views
  @Cron('0 15 * * *', { timeZone: 'Europe/London' })
  async runDailyMaterializedViewRefresh(): Promise<void> {
    await this.refreshMaterializedViewsJob.run();
  }
}
