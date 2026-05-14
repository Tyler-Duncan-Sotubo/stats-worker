import { Controller, Get, Query, Param, Post, Body } from '@nestjs/common';
import { SpotifyOfficialChartsService } from './chart/spotify-official-charts.service';
import { AudiomackScraperService } from './audiomack/audiomack-scraper.service';
import { SpotifyAlbumScraperService } from './album/spotify-album-scraper.service';
import { SpotifyTrackBackfillService } from './album/spotify-track-backfill.service';

@Controller('scraper')
export class ScraperController {
  constructor(
    private readonly spotifyOfficialChartsService: SpotifyOfficialChartsService,
    private readonly audiomackScraperService: AudiomackScraperService,
    private readonly spotifyAlbumScraperService: SpotifyAlbumScraperService,
    private readonly spotifyTrackBackfillService: SpotifyTrackBackfillService,
  ) {}

  @Get('daily')
  async getDaily(
    @Query('country') country = 'ng',
    @Query('limit') limit = '100',
  ) {
    return this.spotifyOfficialChartsService.fetchDailyTracks(
      country,
      parseInt(limit, 10),
    );
  }

  @Get('spotify/login')
  async spotifyLogin() {
    await this.spotifyOfficialChartsService.saveLoginSessionLocal();
    return { ok: true };
  }

  @Get('audiomack/:slug')
  async getAudiomackArtist(@Param('slug') slug: string) {
    return this.audiomackScraperService.scrapeArtist(slug);
  }

  @Get('audiomack')
  async getAudiomackAll() {
    return this.audiomackScraperService.discoverAudiomackSlugs();
  }

  @Post('audiomack-scrape')
  async scrapeAudiomackArtist() {
    return this.audiomackScraperService.scrapeAll();
  }

  @Post('backfill/spotify-track-id')
  async backfillSpotifyTrackIds(
    @Query('dryRun') dryRun?: string,
    @Query('delayMs') delayMs?: string,
    @Query('batchSize') batchSize?: string,
  ) {
    return this.spotifyTrackBackfillService.backfill({
      dryRun: dryRun === 'true',
      delayMs: delayMs ? parseInt(delayMs, 10) : 1000,
      batchSize: batchSize ? parseInt(batchSize, 10) : 8247,
    });
  }

  // ── Album scraping ────────────────────────────────────────────────────

  @Get('album/:albumId')
  async scrapeAlbum(
    @Param('albumId') albumId: string,
    @Query('debug') debug?: string,
  ) {
    return this.spotifyAlbumScraperService.scrapeAlbum(
      albumId,
      debug === 'true',
    );
  }

  @Post('album/:albumId/ingest')
  async ingestAlbum(
    @Param('albumId') albumId: string,
    @Query('debug') debug?: string,
    @Query('primary') primary?: string,
    @Query('createMissing') createMissing?: string,
  ) {
    return this.spotifyAlbumScraperService.ingestAlbum(albumId, {
      debugMode: debug === 'true',
      isPrimary: primary !== 'false',
      createMissing: createMissing === 'true',
    });
  }

  @Post('albums/ingest')
  async ingestAlbums(
    @Body()
    body: {
      albumIds: string[];
      isPrimary?: boolean;
      delayMs?: number;
      createMissing?: boolean;
    },
    @Query('debug') debug?: string,
  ) {
    return this.spotifyAlbumScraperService.ingestAlbums(body.albumIds, {
      debugMode: debug === 'true',
      isPrimary: body.isPrimary ?? true,
      delayMs: body.delayMs ?? 2000,
      createMissing: body.createMissing ?? false,
    });
  }

  @Post('backfill/afrobeats-albums')
  async backfillAfrobeatsAlbums(
    @Query('createMissing') createMissing?: string,
    @Query('limitArtists') limitArtists?: string,
    @Query('includeGroups') includeGroups?: string,
  ) {
    return this.spotifyAlbumScraperService.backfillAfrobeatsAlbums({
      createMissing: createMissing === 'true',
      limitArtists: limitArtists ? parseInt(limitArtists, 10) : undefined,
      includeGroups: includeGroups?.split(',') ?? ['album'],
    });
  }

  @Post('backfill/top-artists-albums')
  async backfillTopArtistAlbums(
    @Query('createMissing') createMissing?: string,
    @Query('limitArtists') limitArtists?: string,
    @Query('minMonthlyListeners') minMonthlyListeners?: string,
  ) {
    return this.spotifyAlbumScraperService.backfillTopArtistAlbums({
      createMissing: createMissing === 'true',
      limitArtists: limitArtists ? parseInt(limitArtists, 10) : 100,
      minMonthlyListeners: minMonthlyListeners
        ? parseInt(minMonthlyListeners, 10)
        : 1_000_000,
    });
  }
}
