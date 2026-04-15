import { Module } from '@nestjs/common';
import { ScraperController } from './scraper.controller';
import { KworbArtistDiscoveryService } from './services/kworb-artist-discovery.service';
import { KworbTotalsService } from './services/kworb-totals.service';
import { SpotifyMetadataService } from './services/spotify-metadata.service';
import { RiaaCertificationService } from './services/riaa-certification.service';
import { BillboardBackfillService } from './billboard/billboard-backfill.service';
import { BullModule } from '@nestjs/bullmq';
import { BillboardBackfillProducer } from './billboard/billboard-backfill.producer';
import { BillboardBackfillProcessor } from './billboard/billboard-backfill.processor';
import { OfficialChartsBackfillProducer } from './uk-chart/official-charts-backfill.producer';
import { OfficialChartsBackfillProcessor } from './uk-chart/official-charts-backfill.processor';
import { OfficialChartsBackfillService } from './uk-chart/official-charts-backfill.service';
import { DailyChartIngestionService } from './chart/daily-chart-ingestion.service';
import { SpotifyDailyService } from './chart/spotify-daily.service';
import { OriginCountryEnrichmentService } from './origin/origin-country-enrichment.service';
import { EntityResolutionService } from 'src/services/entity-resolution.service';
import { ArtistsRepository } from 'src/repository/artists.repository';

@Module({
  imports: [
    BullModule.registerQueue(
      { name: 'billboardBackfillQueue' },
      { name: 'officialChartsBackfillQueue' },
    ),
  ],
  controllers: [ScraperController],
  providers: [
    KworbArtistDiscoveryService,
    KworbTotalsService,
    SpotifyMetadataService,
    RiaaCertificationService,
    BillboardBackfillService,
    BillboardBackfillProducer,
    BillboardBackfillProcessor,
    OfficialChartsBackfillProducer,
    OfficialChartsBackfillProcessor,
    OfficialChartsBackfillService,
    DailyChartIngestionService,
    SpotifyDailyService,
    EntityResolutionService,
    OriginCountryEnrichmentService,
    ArtistsRepository,
  ],
  exports: [
    KworbArtistDiscoveryService,
    KworbTotalsService,
    SpotifyMetadataService,
    RiaaCertificationService,
    DailyChartIngestionService,
    OriginCountryEnrichmentService,
    ArtistsRepository,
  ],
})
export class ScraperModule {}
