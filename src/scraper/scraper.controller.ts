import { Controller, Get, Query, Param, Post } from '@nestjs/common';
import { SpotifyOfficialChartsService } from './chart/spotify-official-charts.service';
import { AudiomackScraperService } from './audiomack/audiomack-scraper.service';

@Controller('scraper')
export class ScraperController {
  constructor(
    private readonly spotifyOfficialChartsService: SpotifyOfficialChartsService,
    private readonly audiomackScraperService: AudiomackScraperService,
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
}
