import { Module } from '@nestjs/common';
import { JobsScheduler } from './jobs.scheduler';
import { DiscoveryJob } from './discovery/discovery.job';
import { CertificationsJob } from './certifications/certifications.job';
import { DailyChartIngestionJob } from './charts/daily-chart-ingestion.job';
import { RefreshMaterializedViewsJob } from './views/refresh-materialized-views.job';
import { OriginCountryEnrichmentJob } from './origin/origin-country-enrichment.job';
import { SongEnrichmentJob } from './catalog/song-enrichment.job';
import { ArtistEnrichmentJob } from './catalog/artist-enrichment.job';
import { RepositoryModule } from 'src/repository/repository.module';
import { ServicesModule } from 'src/services/services.module';
import { ScraperModule } from 'src/scraper/scraper.module';
import { UnifiedSnapshotJob } from './snapshots/unified-snapshot.job';
import { OfficialChartsIngestionJob } from './charts/official-charts-injecton.job';
import { BillboardIngestionJob } from './charts/billboard-injection.job';

@Module({
  imports: [RepositoryModule, ServicesModule, ScraperModule],
  providers: [
    JobsScheduler,
    DiscoveryJob,
    CertificationsJob,
    UnifiedSnapshotJob,
    DailyChartIngestionJob,
    RefreshMaterializedViewsJob,
    OriginCountryEnrichmentJob,
    SongEnrichmentJob,
    ArtistEnrichmentJob,
    OfficialChartsIngestionJob,
    BillboardIngestionJob,
  ],
  exports: [
    DiscoveryJob,
    CertificationsJob,
    DailyChartIngestionJob,
    RefreshMaterializedViewsJob,
  ],
})
export class JobsModule {}
