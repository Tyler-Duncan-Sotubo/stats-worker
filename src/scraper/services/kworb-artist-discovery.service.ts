import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import * as cheerio from 'cheerio';

export interface DiscoveredArtist {
  name: string;
  spotifyId: string;
  appearedOnCharts: number;
  monthlyListeners?: number | null;
  dailyChange?: number | null;
  peakRank?: number | null;
  peakListeners?: number | null;
}

export interface DuplicateGroup {
  normalisedName: string;
  keptSpotifyId: string;
  keptName: string;
  rejectedIds: { spotifyId: string; name: string; appearedOnCharts: number }[];
}

export interface DiscoveryResult {
  artists: DiscoveredArtist[];
  duplicates: DuplicateGroup[];
}

@Injectable()
export class KworbArtistDiscoveryService {
  private readonly logger = new Logger(KworbArtistDiscoveryService.name);

  private normaliseName(name: string): string {
    return name
      .toLowerCase()
      .trim()
      .replace(/\s+/g, ' ')
      .replace(/[^a-z0-9 ]/g, '');
  }

  private parseNum(raw: string | undefined): number | null {
    if (!raw) return null;
    const n = parseInt(raw.replace(/[^0-9-]/g, ''), 10);
    return isNaN(n) ? null : n;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

  async discoverFromDailyChart(country = 'ng'): Promise<DiscoveredArtist[]> {
    const url = `https://kworb.net/spotify/country/${country.toLowerCase()}_daily.html`;

    const { data } = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent': 'tooXclusiveStatsBot/1.0 (+https://tooxclusive.com)',
        Accept: 'text/html,application/xhtml+xml',
      },
    });

    const $ = cheerio.load(data);
    const seen = new Map<string, string>();

    $('a[href*="/artist/"]').each((_, el) => {
      const href = $(el).attr('href') ?? '';
      const match = href.match(/\/artist\/([A-Za-z0-9]+)\.html$/);
      if (!match) return;

      const spotifyId = match[1];
      const name = $(el).text().trim();
      if (!name || seen.has(spotifyId)) return;

      seen.set(spotifyId, name);
    });

    return Array.from(seen.entries()).map(([spotifyId, name]) => ({
      name,
      spotifyId,
      appearedOnCharts: 1,
    }));
  }

  async discoverFromListenerPage(page = 1): Promise<DiscoveredArtist[]> {
    const suffix = page === 1 ? 'listeners.html' : `listeners${page}.html`;
    const url = `https://kworb.net/spotify/${suffix}`;

    const { data } = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent': 'tooXclusiveStatsBot/1.0 (+https://tooxclusive.com)',
        Accept: 'text/html,application/xhtml+xml',
      },
    });

    const $ = cheerio.load(data);
    const artists: DiscoveredArtist[] = [];
    const seen = new Set<string>();

    $('table tr').each((_, row) => {
      const cols = $(row).find('td');

      if (cols.length < 5) return;

      const link = $(row).find('a[href*="artist/"]').first();
      const href = link.attr('href') ?? '';
      const match = href.match(
        /(?:^|\/)artist\/([A-Za-z0-9]+)(?:_[a-z]+)?\.html$/,
      );
      if (!match) return;

      const spotifyId = match[1];
      if (seen.has(spotifyId)) return;
      seen.add(spotifyId);

      const name = link.text().trim();
      if (!name) return;

      const monthlyListeners = this.parseNum(cols.eq(2).text());
      const dailyChange = this.parseNum(cols.eq(3).text());
      const hasPeakColumn = cols.length >= 6;
      const peakRank = hasPeakColumn ? this.parseNum(cols.eq(4).text()) : null;
      const peakListeners = hasPeakColumn
        ? this.parseNum(cols.eq(5).text())
        : this.parseNum(cols.eq(4).text());

      artists.push({
        name,
        spotifyId,
        appearedOnCharts: 0,
        monthlyListeners,
        dailyChange,
        peakRank,
        peakListeners,
      });
    });

    return artists;
  }

  async discoverFromMultipleCharts(
    countries = ['ng', 'gh', 'ke', 'za', 'ug', 'us', 'gb', 'ca'],
  ): Promise<DiscoveryResult> {
    const allArtists: DiscoveredArtist[] = [];

    for (const country of countries) {
      try {
        const artists = await this.discoverFromDailyChart(country);
        allArtists.push(...artists);
        await this.sleep(1500);
      } catch (err) {
        this.logger.warn(
          `Failed chart scrape for ${country.toUpperCase()}: ${(err as Error).message}`,
        );
      }
    }

    return this.mergeDiscoveryResults(
      allArtists,
      `daily charts across ${countries.length} countries`,
    );
  }

  async discoverFromListenerPages(
    pages = [1, 2, 3, 4, 5, 6, 7, 8],
  ): Promise<DiscoveryResult> {
    const allArtists: DiscoveredArtist[] = [];

    for (const page of pages) {
      try {
        const artists = await this.discoverFromListenerPage(page);
        allArtists.push(...artists);
        await this.sleep(2000);
      } catch (err) {
        this.logger.warn(
          `Failed listener page ${page}: ${(err as Error).message}`,
        );
      }
    }

    this.logger.log(
      `Listener pages scraped — ${allArtists.length} raw artists from ${pages.length} pages`,
    );

    return this.mergeDiscoveryResults(
      allArtists,
      `listener pages ${pages.join(', ')}`,
    );
  }

  async discoverAll(): Promise<DiscoveryResult> {
    // Sequential — charts first, then listener pages, polite to Kworb
    const charts = await this.discoverFromMultipleCharts();
    await this.sleep(3000);
    const listeners = await this.discoverFromListenerPages([
      1, 2, 3, 4, 5, 6, 7, 8,
    ]);

    return this.mergeDiscoveryResults(
      [...charts.artists, ...listeners.artists],
      'combined charts + listeners discovery',
    );
  }

  private mergeDiscoveryResults(
    discovered: DiscoveredArtist[],
    sourceLabel: string,
  ): DiscoveryResult {
    const idMap = new Map<
      string,
      {
        name: string;
        count: number;
        monthlyListeners: number | null;
        dailyChange: number | null;
        peakRank: number | null;
        peakListeners: number | null;
      }
    >();

    for (const artist of discovered) {
      const existing = idMap.get(artist.spotifyId);
      if (existing) {
        existing.count += Math.max(artist.appearedOnCharts, 1);
        if (
          artist.monthlyListeners != null &&
          existing.monthlyListeners == null
        ) {
          existing.monthlyListeners = artist.monthlyListeners;
          existing.dailyChange = artist.dailyChange ?? null;
          existing.peakRank = artist.peakRank ?? null;
          existing.peakListeners = artist.peakListeners ?? null;
        }
      } else {
        idMap.set(artist.spotifyId, {
          name: artist.name,
          count: Math.max(artist.appearedOnCharts, 1),
          monthlyListeners: artist.monthlyListeners ?? null,
          dailyChange: artist.dailyChange ?? null,
          peakRank: artist.peakRank ?? null,
          peakListeners: artist.peakListeners ?? null,
        });
      }
    }

    const byNormName = new Map<
      string,
      { spotifyId: string; name: string; count: number }[]
    >();

    for (const [spotifyId, { name, count }] of idMap.entries()) {
      const key = this.normaliseName(name);
      const group = byNormName.get(key) ?? [];
      group.push({ spotifyId, name, count });
      byNormName.set(key, group);
    }

    const artists: DiscoveredArtist[] = [];
    const duplicates: DuplicateGroup[] = [];

    for (const [normName, group] of byNormName.entries()) {
      const winner =
        group.length === 1
          ? group[0]
          : group.sort((a, b) => b.count - a.count)[0];

      const richData = idMap.get(winner.spotifyId)!;

      if (group.length > 1) {
        const losers = group.filter((g) => g.spotifyId !== winner.spotifyId);
        duplicates.push({
          normalisedName: normName,
          keptSpotifyId: winner.spotifyId,
          keptName: winner.name,
          rejectedIds: losers.map((l) => ({
            spotifyId: l.spotifyId,
            name: l.name,
            appearedOnCharts: l.count,
          })),
        });
      }

      artists.push({
        name: winner.name,
        spotifyId: winner.spotifyId,
        appearedOnCharts: winner.count,
        monthlyListeners: richData.monthlyListeners,
        dailyChange: richData.dailyChange,
        peakRank: richData.peakRank,
        peakListeners: richData.peakListeners,
      });
    }

    this.logger.log(
      `Discovery merged — ${artists.length} unique artists` +
        (duplicates.length
          ? `, ${duplicates.length} duplicates collapsed`
          : ''),
    );

    return { artists, duplicates };
  }
}
