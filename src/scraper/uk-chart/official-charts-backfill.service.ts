import { Inject, Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { chartEntries, songFeatures } from 'src/infrastructure/drizzle/schema';
import * as cheerio from 'cheerio';
import { EntityResolutionService } from 'src/services/entity-resolution.service';

type OfficialChartsEntry = {
  song: string;
  artist: string;
  this_week: number;
  peak_position: number | null;
  weeks_on_chart: number | null;
};

type OfficialChartsChart = {
  data: OfficialChartsEntry[];
};

@Injectable()
export class OfficialChartsBackfillService {
  private readonly logger = new Logger(OfficialChartsBackfillService.name);

  private readonly baseUrl = 'https://www.officialcharts.com';
  // private readonly chartPath = 'afrobeats-chart';
  // private readonly chartId = 'afrobeat';
  private readonly chartPath = 'singles-chart';
  private readonly chartId = '7501';

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly entityResolutionService: EntityResolutionService,
  ) {}

  async run(
    job?: Job,
    options?: {
      fromDate?: string;
      toDate?: string;
      chartPath?: string;
      chartName?: string;
      chartId?: string;
    },
  ): Promise<{ dates: number; entries: number }> {
    const chartPath = options?.chartPath ?? this.chartPath;
    const chartId = options?.chartId ?? this.chartId;
    const chartName = options?.chartName ?? 'uk_official_singles';

    const dates = this.generateWeeklyDates(options);
    this.logger.log(`Found ${dates.length} UK chart dates to process`);

    let totalEntries = 0;
    let totalDates = 0;

    for (const date of dates) {
      const chart = await this.fetchChart(date, chartPath, chartId);

      if (!chart) {
        this.logger.warn(`Skipping ${date} — failed to fetch/parse`);
        continue;
      }

      if (!chart.data.length) {
        this.logger.warn(
          `Skipping ${date} — empty chart (selectors may have changed)`,
        );
        continue;
      }

      for (const entry of chart.data) {
        const { primary, featured } = this.parseAllArtists(entry.artist);

        const primaryArtist = await this.entityResolutionService.resolveArtist({
          name: primary,
          source: 'official_charts',
          allowCreate: true,
          markProvisionalIfCreated: true,
        });

        if (!primaryArtist) {
          this.logger.warn(`Could not resolve artist: "${primary}" — skipping`);
          continue;
        }

        const song = await this.entityResolutionService.resolveSong({
          artistId: primaryArtist.id,
          title: entry.song,
          source: 'official_charts',
          allowCreate: true,
          markProvisionalIfCreated: true,
        });

        if (!song) {
          this.logger.warn(
            `Could not resolve song: "${entry.song}" — skipping`,
          );
          continue;
        }

        for (const featuredName of featured) {
          const featuredArtist =
            await this.entityResolutionService.resolveArtist({
              name: featuredName,
              source: 'official_charts',
              allowCreate: true,
              markProvisionalIfCreated: true,
            });

          if (!featuredArtist || featuredArtist.id === primaryArtist.id) {
            continue;
          }

          await this.db
            .insert(songFeatures)
            .values({
              songId: song.id,
              featuredArtistId: featuredArtist.id,
            })
            .onConflictDoNothing();
        }

        await this.db
          .insert(chartEntries)
          .values({
            artistId: primaryArtist.id,
            songId: song.id,
            chartName,
            chartTerritory: 'UK',
            position: entry.this_week,
            peakPosition: entry.peak_position ?? null,
            weeksOnChart: entry.weeks_on_chart ?? null,
            chartWeek: date,
            source: 'uk_afrobeats_chart',
          })
          .onConflictDoNothing();

        totalEntries++;
      }

      totalDates++;

      await job?.updateProgress({
        totalDates,
        totalCharts: dates.length,
        totalEntries,
        currentDate: date,
        percent: Math.round((totalDates / dates.length) * 100),
      });

      this.logger.log(
        `[${totalDates}/${dates.length}] Done ${date} — ${chart.data.length} entries`,
      );

      await this.sleep(250);
    }

    this.logger.log(
      `UK backfill complete — ${totalDates} dates, ${totalEntries} chart entries`,
    );

    return { dates: totalDates, entries: totalEntries };
  }

  async testSingleDate(
    date: string,
    options?: { chartPath?: string; chartId?: string },
  ): Promise<{
    url: string;
    entries: number;
    data: OfficialChartsEntry[];
    parsed: { primary: string; featured: string[] }[];
  }> {
    const chartPath = options?.chartPath ?? this.chartPath;
    const chartId = options?.chartId ?? this.chartId;
    const url = this.buildChartUrl(date, chartPath, chartId);

    this.logger.log(`Testing UK chart fetch: ${url}`);

    const res = await fetch(url, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (compatible; tooXclusiveStatsBot/1.0; +https://tooxclusive.com)',
      },
    });

    this.logger.log(`HTTP status: ${res.status}`);

    if (!res.ok) {
      this.logger.error(`Failed to fetch ${url} — status ${res.status}`);
      return { url, entries: 0, data: [], parsed: [] };
    }

    const html = await res.text();
    const chart = this.parseChartHtml(html);

    const parsed = chart.data.map((entry) => ({
      primary: this.parseAllArtists(entry.artist).primary,
      featured: this.parseAllArtists(entry.artist).featured,
    }));

    return {
      url,
      entries: chart.data.length,
      data: chart.data,
      parsed,
    };
  }

  private async fetchChart(
    date: string,
    chartPath: string,
    chartId: string,
  ): Promise<OfficialChartsChart | null> {
    const url = this.buildChartUrl(date, chartPath, chartId);

    const res = await fetch(url, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (compatible; tooXclusiveStatsBot/1.0; +https://tooxclusive.com)',
      },
    });

    if (!res.ok) return null;

    const html = await res.text();
    return this.parseChartHtml(html);
  }

  private parseChartHtml(html: string): OfficialChartsChart {
    const $ = cheerio.load(html);
    const rows: OfficialChartsEntry[] = [];
    const seen = new Set<number>();

    const candidates = [
      '[data-chart-position]',
      '.chart-item',
      '.chart-list-item',
      'article',
      'li',
    ];

    for (const selector of candidates) {
      $(selector).each((_, el) => {
        const $el = $(el);

        const position = this.extractInt(
          $el.attr('data-chart-position') ||
            $el.find('[data-chart-position]').attr('data-chart-position') ||
            $el.find('.position, .chart-key, .chart-position').first().text(),
        );

        if (!position || position < 1 || position > 100 || seen.has(position)) {
          return;
        }

        const rawTitle = this.cleanText(
          $el.find('h3, h4, .title, .chart-name, .track-title').first().text(),
        );

        const rawArtist = this.cleanText(
          $el
            .find('p, .artist, .chart-artist, .artist-name')
            .filter((_, node) => {
              const txt = $(node).text().trim();
              return !!txt && txt.length < 200;
            })
            .first()
            .text(),
        );

        if (!rawTitle || !rawArtist) return;

        const title = this.toTitleCase(this.stripChartPrefixes(rawTitle));
        const artist = this.stripChartPrefixes(rawArtist);
        const cleanArtist = this.stripTitleFromArtist(title, artist);

        if (!title || !cleanArtist) return;

        const peak = this.extractInt(
          $el.find('.peak, .chart-peak').first().text(),
        );
        const weeks = this.extractInt(
          $el.find('.weeks, .chart-weeks').first().text(),
        );

        rows.push({
          song: title,
          artist: cleanArtist,
          this_week: position,
          peak_position: peak,
          weeks_on_chart: weeks,
        });

        seen.add(position);
      });

      if (rows.length >= 100) break;
    }

    rows.sort((a, b) => a.this_week - b.this_week);
    return { data: rows };
  }

  private parseAllArtists(raw: string): {
    primary: string;
    featured: string[];
  } {
    const titleCased = raw
      .toLowerCase()
      .replace(/(^\w|\s\w)/g, (c) => c.toUpperCase());

    const parts = titleCased
      .split(/\s+(?:featuring|feat\.?|ft\.?|with)\s+|\s+x\s+|\s+&\s+|\/|,\s+/i)
      .map((s) => s.replace(/^[&,/]\s*/, '').trim())
      .filter((s) => s.length > 1);

    const primary = (parts[0] ?? titleCased.trim())
      .replace(/^(?:featuring|feat\.?|ft\.?)\s+/i, '')
      .trim();

    return {
      primary,
      featured: parts.slice(1),
    };
  }

  private stripChartPrefixes(value: string): string {
    return value.replace(/^(New|RE)(?=\s*[A-Z0-9])/i, '').trim();
  }

  private toTitleCase(value: string): string {
    return value.toLowerCase().replace(/(^\w|\s\w)/g, (c) => c.toUpperCase());
  }

  private stripTitleFromArtist(title: string, artist: string): string {
    const normTitle = title.toUpperCase().replace(/\s+/g, ' ').trim();
    const normArtist = artist.toUpperCase().replace(/\s+/g, ' ').trim();

    if (normArtist.startsWith(normTitle)) {
      return artist.slice(title.length).trim();
    }

    return artist.trim();
  }

  private generateWeeklyDates(options?: {
    fromDate?: string;
    toDate?: string;
  }): string[] {
    const to = options?.toDate ? new Date(options.toDate) : new Date();
    const from = options?.fromDate
      ? new Date(options.fromDate)
      : new Date('2000-01-01');

    const end = this.alignToFriday(to);
    const dates: string[] = [];

    for (
      let d = end;
      d >= from;
      d = new Date(d.getTime() - 7 * 24 * 60 * 60 * 1000)
    ) {
      dates.push(this.toIsoDate(d));
    }

    return dates;
  }

  private alignToFriday(date: Date): Date {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    while (d.getDay() !== 5) d.setDate(d.getDate() - 1);
    return d;
  }

  private toIsoDate(date: Date): string {
    return date.toISOString().slice(0, 10);
  }

  private buildChartUrl(
    date: string,
    chartPath: string,
    chartId: string,
  ): string {
    const yyyymmdd = date.replaceAll('-', '');
    return `${this.baseUrl}/charts/${chartPath}/${yyyymmdd}/${chartId}/`;
  }

  private cleanText(value: string): string {
    return value.replace(/\s+/g, ' ').trim();
  }

  private extractInt(value?: string | null): number | null {
    if (!value) return null;
    const match = value.replace(/,/g, '').match(/\d+/);
    return match ? Number(match[0]) : null;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
