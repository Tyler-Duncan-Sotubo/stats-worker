// src/modules/scraper/services/kworb-artist-discovery.service.ts

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

  // ── Parse a number like "135,361,606" or "170,891" ──────────────────────
  private parseNum(raw: string | undefined): number | null {
    if (!raw) return null;
    const n = parseInt(raw.replace(/[^0-9-]/g, ''), 10);
    return isNaN(n) ? null : n;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Daily chart — extracts artist name + Spotify ID from anchor hrefs
  // ─────────────────────────────────────────────────────────────────────────
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

    const artists: DiscoveredArtist[] = Array.from(seen.entries()).map(
      ([spotifyId, name]) => ({ name, spotifyId, appearedOnCharts: 1 }),
    );

    this.logger.log(
      `Discovered ${artists.length} artists from ${country.toUpperCase()} daily chart`,
    );

    return artists;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Listeners page — richer data: monthly listeners, daily change, peak
  // URL: kworb.net/spotify/listeners.html
  //      kworb.net/spotify/listeners2.html  etc.
  //
  // Table columns: Rank | Artist | Listeners | Daily +/- | Peak | PkListeners
  // ─────────────────────────────────────────────────────────────────────────
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

      // Page 1 has 6 cols, pages 2+ have 5
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

      // Page 1: [0 rank, 1 artist, 2 listeners, 3 daily, 4 peak, 5 pkListeners]
      // Pages 2+: [0 rank, 1 artist, 2 listeners, 3 daily, 4 pkListeners]
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

    this.logger.log(
      `Discovered ${artists.length} artists from listeners page ${page}`,
    );

    return artists;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Multi-country daily charts
  // ─────────────────────────────────────────────────────────────────────────
  async discoverFromMultipleCharts(
    countries = ['ng', 'gh', 'ke', 'za', 'ug', 'us', 'gb', 'ca'],
  ): Promise<DiscoveryResult> {
    const results = await Promise.allSettled(
      countries.map((c) => this.discoverFromDailyChart(c)),
    );

    return this.mergeDiscoveryResults(
      results
        .filter(
          (r): r is PromiseFulfilledResult<DiscoveredArtist[]> =>
            r.status === 'fulfilled',
        )
        .flatMap((r) => r.value),
      `daily charts across ${countries.length} countries`,
    );
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Multiple listener pages
  // ─────────────────────────────────────────────────────────────────────────
  async discoverFromListenerPages(
    pages = [1, 2, 3, 4],
  ): Promise<DiscoveryResult> {
    const results = await Promise.allSettled(
      pages.map((page) => this.discoverFromListenerPage(page)),
    );

    return this.mergeDiscoveryResults(
      results
        .filter(
          (r): r is PromiseFulfilledResult<DiscoveredArtist[]> =>
            r.status === 'fulfilled',
        )
        .flatMap((r) => r.value),
      `listener pages ${pages.join(', ')}`,
    );
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Full discovery — charts + listeners combined
  // This is the main entry point for the daily cron
  // ─────────────────────────────────────────────────────────────────────────
  async discoverAll(): Promise<DiscoveryResult> {
    const [charts, listeners] = await Promise.all([
      this.discoverFromMultipleCharts(),
      this.discoverFromListenerPages([1, 2, 3, 4]),
    ]);

    // Merge both — listeners data enriches artists already found on charts
    return this.mergeDiscoveryResults(
      [...charts.artists, ...listeners.artists],
      'combined charts + listeners discovery',
    );
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Shared merge + dedup logic
  // ─────────────────────────────────────────────────────────────────────────
  private mergeDiscoveryResults(
    discovered: DiscoveredArtist[],
    sourceLabel: string,
  ): DiscoveryResult {
    // Accumulate by Spotify ID — keep the richest listener data when merging
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
        // Keep the listener data if this entry has it and existing doesn't
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

    // Group by normalised name to catch duplicate Spotify profiles
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

        this.logger.warn(
          `Duplicate Spotify profiles for "${normName}": ` +
            group.map((g) => `${g.spotifyId}(${g.count})`).join(', ') +
            ` → keeping ${winner.spotifyId}`,
        );

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
      `Discovery complete — ${artists.length} unique artists from ${idMap.size} raw IDs via ${sourceLabel}` +
        (duplicates.length
          ? ` (${duplicates.length} duplicate groups collapsed)`
          : ''),
    );

    return { artists, duplicates };
  }
}
