import { Controller, Get, Query } from '@nestjs/common';
import { SpotifyOfficialChartsService } from './chart/spotify-official-charts.service';

@Controller('scraper')
export class ScraperController {
  constructor(
    private readonly spotifyOfficialChartsService: SpotifyOfficialChartsService,
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
}
