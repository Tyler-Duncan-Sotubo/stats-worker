import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { KworbArtistDiscoveryService } from './services/kworb-artist-discovery.service';
import { KworbTotalsService } from './services/kworb-totals.service';
import { SpotifyMetadataService } from './services/spotify-metadata.service';
import { BillboardBackfillService } from './billboard/billboard-backfill.service';
import { BillboardBackfillProducer } from './billboard/billboard-backfill.producer';
import { OfficialChartsBackfillProducer } from './uk-chart/official-charts-backfill.producer';
import { BackfillRangeDto } from './dto/backfill-range.dto';
import { OfficialChartsBackfillService } from './uk-chart/official-charts-backfill.service';
import { DailyChartIngestionService } from './chart/daily-chart-ingestion.service';

@Controller('scraper')
export class ScraperController {
  constructor(
    private kworbArtistDiscovery: KworbArtistDiscoveryService,
    private kworbTotals: KworbTotalsService,
    private spotifyMetadata: SpotifyMetadataService,
    private billboardBackfillService: BillboardBackfillService,
    private dailyChartIngestionService: DailyChartIngestionService,
    private billboardProducer: BillboardBackfillProducer,
    private readonly officialChartsProducer: OfficialChartsBackfillProducer,
    private readonly officialChartsBackfill: OfficialChartsBackfillService,
  ) {}

  @Get('discover-artists')
  async discoverArtists() {
    return this.kworbArtistDiscovery.discoverAll();
  }

  @Get('fetch-totals/:spotifyId')
  async fetchTotalsForSampleArtists(@Param('spotifyId') spotifyId: string) {
    return this.kworbTotals.fetchArtistTotals(spotifyId);
  }

  @Get('fetch-spotify-artist/:spotifyId')
  async fetchArtistMetadata(@Param('spotifyId') spotifyId: string) {
    return this.spotifyMetadata.fetchArtistMetadata(spotifyId);
  }

  @Get('fetch-track-meta/:trackId')
  async fetchTrackMetadata(@Param('trackId') trackId: string) {
    return this.spotifyMetadata.fetchTrackMetadata(trackId);
  }

  @Post('billboard')
  run() {
    return this.billboardBackfillService.run();
  }

  @Post('ingest')
  ingestAll() {
    return this.dailyChartIngestionService.runDailyIngestion();
  }

  @Post('billboard/produce')
  produce() {
    return this.billboardProducer.enqueueFullBackfill();
  }

  // // POST /api/backfill/official-charts/range
  // // body: { "fromDate": "2024-01-01", "toDate": "2024-12-31" }
  @Post('official-charts/range')
  enqueueOfficialChartsRange(@Body() body: BackfillRangeDto) {
    return this.officialChartsProducer.enqueueRangeBackfill(
      body.fromDate,
      body.toDate,
    );
  }

  // @Get('test/uk-chart')
  // async testUkChart() {
  //   const result =
  //     await this.officialChartsBackfill.testSingleDate('2026-03-27');
  //   return result;
  // }
}
