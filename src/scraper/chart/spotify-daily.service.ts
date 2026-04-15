/* eslint-disable @typescript-eslint/no-unsafe-return */
// src/scraper/services/spotify-daily.service.ts
import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import * as cheerio from 'cheerio';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import {
  KworbSpotifyDailyPayload,
  KworbSpotifyDailyRow,
} from '../dto/kworb.dto';

dayjs.extend(utc);

export interface CombinedTopItem {
  rank: number;
  artist: string;
  title: string;
  featuredArtists: string[];
  score: number;
  spotifyRank?: number;
  appleRank?: number;
}

export interface CombinedTopPayload {
  country: string;
  fetchedAtISO: string;
  label: string;
  weights: { spotify: number; apple: number };
  sourceMaxRank: number;
  items: CombinedTopItem[];
}

@Injectable()
export class SpotifyDailyService {
  private readonly logger = new Logger(SpotifyDailyService.name);

  // ─────────────────────────────────────────────────────────────────────────────
  // Shared normalization helpers
  // ─────────────────────────────────────────────────────────────────────────────

  private normalizeAll(s: string): string {
    return (s || '')
      .replace(/[\p{Cf}\uFEFF\u00A0]/gu, '')
      .replace(/[［【〔｛『«]/g, '(')
      .replace(/[］】〕｝』»]/g, ')')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private toAsciiQuotes(s: string): string {
    return s.replace(/[''‚‛]/g, "'").replace(/[""„‟«»‹›]/g, '"');
  }

  private normalizeBasic(s: string): string {
    return this.toAsciiQuotes(s ?? '')
      .normalize('NFKC')
      .trim()
      .replace(/\s+/g, ' ');
  }

  private normKey(s: string): string {
    return this.normalizeBasic(s).toLowerCase();
  }

  private trackKey(artist: string, title: string): string {
    return `${this.normKey(artist)}::${this.normKey(title)}`;
  }

  private dedupeOrder(arr: string[]): string[] {
    const out: string[] = [];
    const seen = new Set<string>();
    for (const s of arr) {
      const k = s.toLowerCase();
      if (!seen.has(k)) {
        seen.add(k);
        out.push(s);
      }
    }
    return out;
  }

  private splitArtistTitle(s: string): { artist: string; title: string } {
    const dash = s.lastIndexOf(' - ');
    if (dash > 0)
      return {
        artist: s.slice(0, dash).trim(),
        title: s.slice(dash + 3).trim(),
      };
    const endash = s.lastIndexOf(' – ');
    if (endash > 0)
      return {
        artist: s.slice(0, endash).trim(),
        title: s.slice(endash + 3).trim(),
      };
    const k = s.indexOf('-');
    if (k > 0)
      return { artist: s.slice(0, k).trim(), title: s.slice(k + 1).trim() };
    return { artist: '', title: s.trim() };
  }

  private extractFeaturesAndCleanTitle(title: string): {
    cleanTitle: string;
    featured: string[];
  } {
    let norm = this.normalizeAll(title);
    norm = norm.replace(/\[/g, '(').replace(/\]/g, ')');

    const featureBlockRe =
      /\(([^)]*?(?:\bfeat\.?\b|\bft\.?\b|\bfeaturing\b|\bwith\b|w\/|w\.)[^)]*)\)/gi;

    let m: RegExpExecArray | null;
    const blocks: string[] = [];
    while ((m = featureBlockRe.exec(norm)) !== null) {
      blocks.push(m[1]);
    }

    let clean = norm;
    if (blocks.length) {
      clean = clean
        .replace(featureBlockRe, '')
        .replace(/\s{2,}/g, ' ')
        .trim();
    }

    const buckets: string[] = [];
    for (const b of blocks) {
      const stripped = b
        .replace(/\bfeat\.?\s*/i, '')
        .replace(/\bft\.?\s*/i, '')
        .replace(/\bfeaturing\s*/i, '')
        .replace(/\bwith\s*/i, '')
        .replace(/\bw\/\s*/i, '')
        .replace(/\bw\.\s*/i, '')
        .trim();

      const parts = stripped
        .split(/\s*(?:,|&| and |\bx\b|×|\+)\s*/i)
        .map((s) => s.trim())
        .filter(Boolean);

      buckets.push(...parts);
    }

    const seen = new Set<string>();
    const featured: string[] = [];
    for (const name of buckets) {
      const key = name.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        featured.push(name);
      }
    }

    return { cleanTitle: clean, featured };
  }

  private splitArtistsField(artistStr: string): {
    lead: string;
    others: string[];
  } {
    const s = this.normalizeAll(artistStr || '');
    const cleaned = s
      .replace(/\b(?:feat\.?|ft\.?|featuring|with|w\/|w\.)\b.*$/i, '')
      .trim();

    const parts = cleaned
      .split(/\s*(?:,|&| and |\bx\b|×|\+)\s*/i)
      .map((p) => p.trim())
      .filter(Boolean);

    return { lead: parts[0] ?? cleaned, others: parts.slice(1) };
  }

  private canonicalizeRow(r: KworbSpotifyDailyRow): KworbSpotifyDailyRow {
    const { lead, others } = this.splitArtistsField(r.artist);
    return {
      ...r,
      artist: lead,
      featuredArtists: this.dedupeOrder([
        ...(r.featuredArtists ?? []),
        ...others,
      ]),
      title: r.title,
    };
  }

  private findHeaderIdx(
    $: cheerio.CheerioAPI,
    $trs: cheerio.Cheerio<any>,
  ): number {
    for (let i = 0; i < Math.min(6, $trs.length); i++) {
      const cells = $trs
        .eq(i)
        .find('th,td')
        .map((_, el) => $(el).text().trim().toLowerCase())
        .get();
      if (
        cells.some((c) => /^(pos|position|rank|#)$/.test(c)) &&
        cells.some((c) => c.includes('artist') || c.includes('title'))
      ) {
        return i;
      }
    }
    return 0;
  }

  private findLargestTable($: cheerio.CheerioAPI): cheerio.Cheerio<any> {
    let $table: cheerio.Cheerio<any> | null = null;
    $('table').each((_, el) => {
      const rows = $(el).find('tr').length;
      if (rows > 50 && !$table) $table = $(el);
    });
    return $table ?? $('table').first();
  }

  private findIdx(list: string[], keys: string[]): number {
    return list.findIndex((h) => keys.some((k) => h.includes(k)));
  }

  private timeToISO(hhmm: string, tz: string): string | undefined {
    const tzOffset: Record<string, number> = {
      EDT: -4,
      EST: -5,
      UTC: 0,
      GMT: 0,
      BST: 1,
      CET: 1,
      CEST: 2,
    };
    const offset = tzOffset[tz];
    if (offset === undefined) return undefined;
    const [h, m] = hhmm.split(':').map((v) => parseInt(v, 10));
    if (!Number.isFinite(h) || !Number.isFinite(m)) return undefined;
    return dayjs
      .utc()
      .hour(h)
      .minute(m)
      .second(0)
      .millisecond(0)
      .add(offset, 'hour')
      .toISOString();
  }

  private parseHeaderMeta(heading: string): {
    pageTimeISO?: string;
    chartDateISO?: string;
  } {
    const timeMatch = heading.match(/\|\s*([0-9]{2}:[0-9]{2})\s+([A-Z]{3})/);
    const pageTimeISO = timeMatch
      ? this.timeToISO(timeMatch[1], timeMatch[2])
      : undefined;

    const dateMatch = heading.match(/(\d{4}-\d{2}-\d{2})/);
    const chartDateISO = dateMatch
      ? `${dateMatch[1]}T00:00:00.000Z`
      : undefined;

    return { pageTimeISO, chartDateISO };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Spotify daily fetcher
  // ─────────────────────────────────────────────────────────────────────────────

  async fetchDailyTracks(
    country = 'ng',
    limit = 20,
  ): Promise<KworbSpotifyDailyPayload> {
    const url = `https://kworb.net/spotify/country/${country.toLowerCase()}_daily.html`;
    const res = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent':
          'tooXclusiveChartsBot/1.0 (+https://tooxclusive.com/charts)',
        Accept: 'text/html,application/xhtml+xml',
      },
      validateStatus: (s) => s >= 200 && s < 400,
    });

    const $ = cheerio.load(res.data);
    const heading = $('h1, h2').first().text().trim();
    const label = heading || `${country.toUpperCase()} Spotify Daily (Kworb)`;
    const { pageTimeISO, chartDateISO } = this.parseHeaderMeta(heading);

    const $table = this.findLargestTable($);
    const $trs = $table.find('tr');
    const headerIdx = this.findHeaderIdx($, $trs);

    const headers = $trs
      .eq(headerIdx)
      .find('th,td')
      .map((_, el) => $(el).text().trim().toLowerCase())
      .get();

    const idx = {
      rank: this.findIdx(headers, ['pos', 'rank', '#', 'position']),
      artistTitle: this.findIdx(headers, [
        'artist and title',
        'artist - title',
        'artist – title',
        'artist',
        'title',
      ]),
    };
    if (idx.rank < 0) idx.rank = 0;
    if (idx.artistTitle < 0) idx.artistTitle = Math.max(headers.length - 1, 1);

    const rows: KworbSpotifyDailyRow[] = [];

    for (let r = headerIdx + 1; r < $trs.length; r++) {
      const $tds = $trs.eq(r).find('td');
      if ($tds.length <= Math.max(idx.rank, idx.artistTitle)) continue;

      const rank = parseInt(
        $tds.eq(idx.rank).text().trim().replace(/[^\d]/g, ''),
        10,
      );
      if (!Number.isFinite(rank) || rank <= 0) continue;

      const atCell = $tds.eq(idx.artistTitle);
      const merged = this.normalizeAll(atCell.text());
      let { artist, title: rawTitle } = this.splitArtistTitle(merged);

      if (!artist || !rawTitle) {
        const anchors = atCell.find('a');
        if (anchors.length >= 2) {
          artist = this.normalizeAll(anchors.eq(0).text());
          rawTitle = this.normalizeAll(anchors.eq(1).text());
        }
      }
      if (!artist || !rawTitle) continue;

      const { cleanTitle, featured } =
        this.extractFeaturesAndCleanTitle(rawTitle);
      rows.push({ rank, artist, title: cleanTitle, featuredArtists: featured });

      if (rows.length >= limit) break;
    }

    return {
      country: country.toLowerCase(),
      label,
      fetchedAtISO: dayjs.utc().toISOString(),
      pageTimeISO,
      chartDateISO,
      rows: rows.sort((a, b) => a.rank - b.rank),
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Apple Music daily fetcher
  // ─────────────────────────────────────────────────────────────────────────────

  async fetchAppleDailyTracks(
    country = 'ng',
    limit = 0,
  ): Promise<KworbSpotifyDailyPayload> {
    const url = `https://kworb.net/charts/apple_s/${country.toLowerCase()}.html`;
    const res = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent':
          'tooXclusiveChartsBot/1.0 (+https://tooxclusive.com/charts)',
        Accept: 'text/html,application/xhtml+xml',
      },
      validateStatus: (s) => s >= 200 && s < 400,
    });

    const $ = cheerio.load(res.data);
    const heading = $('h1, h2').first().text().trim();
    const label =
      heading || `${country.toUpperCase()} Apple Music Daily (Kworb)`;
    const { pageTimeISO, chartDateISO } = this.parseHeaderMeta(heading);

    const $table = this.findLargestTable($);
    const $trs = $table.find('tr');
    const headerIdx = this.findHeaderIdx($, $trs);

    const headers = $trs
      .eq(headerIdx)
      .find('th,td')
      .map((_, el) => $(el).text().trim().toLowerCase())
      .get();

    const idx = {
      rank: this.findIdx(headers, ['pos', 'rank', '#', 'position']),
      artistTitle: this.findIdx(headers, [
        'artist and title',
        'artist - title',
        'artist – title',
        'artist',
        'title',
      ]),
    };
    if (idx.rank < 0) idx.rank = 0;
    if (idx.artistTitle < 0) idx.artistTitle = Math.max(headers.length - 1, 1);

    const rows: KworbSpotifyDailyRow[] = [];
    const maxRows = limit > 0 ? limit : 0;

    for (let r = headerIdx + 1; r < $trs.length; r++) {
      const $tds = $trs.eq(r).find('td');
      if ($tds.length <= Math.max(idx.rank, idx.artistTitle)) continue;

      const rank = parseInt(
        $tds.eq(idx.rank).text().trim().replace(/[^\d]/g, ''),
        10,
      );
      if (!Number.isFinite(rank) || rank <= 0) continue;

      const atCell = $tds.eq(idx.artistTitle);
      const merged = this.normalizeAll(atCell.text());
      let { artist, title: rawTitle } = this.splitArtistTitle(merged);

      if (!artist || !rawTitle) {
        const anchors = atCell.find('a');
        if (anchors.length >= 2) {
          artist = this.normalizeAll(anchors.eq(0).text());
          rawTitle = this.normalizeAll(anchors.eq(1).text());
        }
      }
      if (!artist || !rawTitle) continue;

      const { cleanTitle, featured } =
        this.extractFeaturesAndCleanTitle(rawTitle);
      rows.push({ rank, artist, title: cleanTitle, featuredArtists: featured });

      if (maxRows && rows.length >= maxRows) break;
    }

    const sorted = rows
      .sort((a, b) => a.rank - b.rank)
      .filter((r) => r.rank <= 200);

    this.logger.log(
      `Fetched ${sorted.length} Apple Music tracks for ${country.toUpperCase()}`,
    );

    return {
      country: country.toLowerCase(),
      label,
      fetchedAtISO: dayjs.utc().toISOString(),
      pageTimeISO,
      chartDateISO,
      rows: sorted,
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Combined Spotify + Apple Top 100
  // ─────────────────────────────────────────────────────────────────────────────

  async combineTop100(
    country = 'ng',
    opts?: {
      spotifyWeight?: number;
      appleWeight?: number;
      sourceMaxRank?: number;
      cap?: number;
    },
  ): Promise<CombinedTopPayload> {
    const [spotify, apple] = await Promise.all([
      this.fetchDailyTracks(country),
      this.fetchAppleDailyTracks(country),
    ]);

    const wS = opts?.spotifyWeight ?? 1.0;
    const wA = opts?.appleWeight ?? 1.0;
    const maxRank = opts?.sourceMaxRank ?? 200;
    const cap = opts?.cap ?? 100;

    const points = (rank?: number) =>
      rank && rank >= 1 && rank <= maxRank ? maxRank + 1 - rank : 0;

    const buildMap = (rows: KworbSpotifyDailyRow[]) => {
      const map = new Map<string, KworbSpotifyDailyRow>();
      for (const raw of rows) {
        if (raw.rank > maxRank) continue;
        const r = this.canonicalizeRow(raw);
        const key = this.trackKey(r.artist, r.title);
        const prev = map.get(key);
        if (!prev || r.rank < prev.rank) map.set(key, r);
      }
      return map;
    };

    const sMap = buildMap(spotify.rows);
    const aMap = buildMap(apple.rows);
    const keys = new Set<string>([...sMap.keys(), ...aMap.keys()]);

    const combined: CombinedTopItem[] = [];
    for (const key of keys) {
      const s = sMap.get(key);
      const a = aMap.get(key);
      combined.push({
        rank: 0,
        artist: s?.artist ?? a?.artist ?? '',
        title: s?.title ?? a?.title ?? '',
        featuredArtists: this.dedupeOrder([
          ...(s?.featuredArtists ?? []),
          ...(a?.featuredArtists ?? []),
        ]),
        score: wS * points(s?.rank) + wA * points(a?.rank),
        spotifyRank: s?.rank,
        appleRank: a?.rank,
      });
    }

    combined.sort((x, y) => {
      if (y.score !== x.score) return y.score - x.score;
      const xBest = Math.min(
        x.spotifyRank ?? Infinity,
        x.appleRank ?? Infinity,
      );
      const yBest = Math.min(
        y.spotifyRank ?? Infinity,
        y.appleRank ?? Infinity,
      );
      if (xBest !== yBest) return xBest - yBest;
      const t = x.title.localeCompare(y.title, undefined, {
        sensitivity: 'accent',
      });
      if (t !== 0) return t;
      return x.artist.localeCompare(y.artist, undefined, {
        sensitivity: 'accent',
      });
    });

    combined.forEach((item, i) => (item.rank = i + 1));

    return {
      country: country.toLowerCase(),
      fetchedAtISO: dayjs.utc().toISOString(),
      label: `${country.toUpperCase()} Combined Top ${cap} (Spotify + Apple via Kworb)`,
      weights: { spotify: wS, apple: wA },
      sourceMaxRank: maxRank,
      items: combined.slice(0, cap),
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Apple ZA Top 50 (flat)
  // ─────────────────────────────────────────────────────────────────────────────

  async buildAppleTop50ZAFlat(opts?: {
    sourceMaxRank?: number;
    cap?: number;
  }): Promise<KworbSpotifyDailyPayload> {
    const maxRank = opts?.sourceMaxRank ?? 200;
    const cap = opts?.cap ?? 50;

    const apple = await this.fetchAppleDailyTracks('za');
    const byKey = new Map<string, KworbSpotifyDailyRow>();

    for (const raw of apple.rows) {
      if (raw.rank > maxRank) continue;
      const r = this.canonicalizeRow(raw);
      const key = this.trackKey(r.artist, r.title);
      const prev = byKey.get(key);
      if (!prev || r.rank < prev.rank) byKey.set(key, r);
    }

    const top = Array.from(byKey.values())
      .sort((a, b) => a.rank - b.rank)
      .slice(0, cap);

    return {
      country: 'za',
      label: 'ZA Apple Music Daily',
      fetchedAtISO: dayjs.utc().toISOString(),
      rows: top.map((r) => ({
        rank: r.rank,
        artist: r.artist,
        title: r.title,
        featuredArtists: r.featuredArtists ?? [],
      })),
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Apple Global Top 200 (flat)
  // ─────────────────────────────────────────────────────────────────────────────

  async buildAppleGlobal200Flat(opts?: {
    cap?: number;
    sourceMaxRank?: number;
  }): Promise<KworbSpotifyDailyPayload> {
    const cap = opts?.cap ?? 200;
    const maxRank = opts?.sourceMaxRank ?? 200;

    const res = await axios.get<string>(
      'https://kworb.net/apple_songs/index.html',
      {
        timeout: 15_000,
        headers: {
          'User-Agent':
            'tooXclusiveChartsBot/1.0 (+https://tooxclusive.com/charts)',
          Accept: 'text/html,application/xhtml+xml',
        },
        validateStatus: (s) => s >= 200 && s < 400,
      },
    );

    const $ = cheerio.load(res.data);
    const $table = this.findLargestTable($);
    const $trs = $table.find('tr');
    const headerIdx = this.findHeaderIdx($, $trs);

    const headers = $trs
      .eq(headerIdx)
      .find('th,td')
      .map((_, el) => $(el).text().trim().toLowerCase())
      .get();

    const idx = {
      rank: this.findIdx(headers, ['pos', 'rank', '#', 'position']),
      artistTitle: this.findIdx(headers, [
        'artist and title',
        'artist - title',
        'artist – title',
        'artist',
        'title',
      ]),
    };
    if (idx.rank < 0) idx.rank = 0;
    if (idx.artistTitle < 0) idx.artistTitle = Math.max(headers.length - 1, 1);

    const rows: KworbSpotifyDailyRow[] = [];

    for (let r = headerIdx + 1; r < $trs.length; r++) {
      const $tds = $trs.eq(r).find('td');
      if ($tds.length <= Math.max(idx.rank, idx.artistTitle)) continue;

      const rank = parseInt(
        $tds.eq(idx.rank).text().trim().replace(/[^\d]/g, ''),
        10,
      );
      if (!Number.isFinite(rank) || rank <= 0 || rank > maxRank) continue;

      const atCell = $tds.eq(idx.artistTitle);
      const merged = this.normalizeAll(atCell.text());
      let { artist, title: rawTitle } = this.splitArtistTitle(merged);

      if (!artist || !rawTitle) {
        const anchors = atCell.find('a');
        if (anchors.length >= 2) {
          artist = this.normalizeAll(anchors.eq(0).text());
          rawTitle = this.normalizeAll(anchors.eq(1).text());
        }
      }
      if (!artist || !rawTitle) continue;

      const { cleanTitle, featured } =
        this.extractFeaturesAndCleanTitle(rawTitle);
      rows.push({ rank, artist, title: cleanTitle, featuredArtists: featured });

      if (rows.length >= maxRank) break;
    }

    const byKey = new Map<string, KworbSpotifyDailyRow>();
    for (const raw of rows) {
      const r = this.canonicalizeRow(raw);
      const key = this.trackKey(r.artist, r.title);
      const prev = byKey.get(key);
      if (!prev || r.rank < prev.rank) byKey.set(key, r);
    }

    const top = Array.from(byKey.values())
      .sort((a, b) => a.rank - b.rank)
      .slice(0, cap);

    return {
      country: 'global',
      label: `Global Apple Music Top ${cap} (via Kworb)`,
      fetchedAtISO: dayjs.utc().toISOString(),
      rows: top.map((r) => ({
        rank: r.rank,
        artist: r.artist,
        title: r.title,
        featuredArtists: r.featuredArtists ?? [],
      })),
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // East Africa Combined Top 50 (flat)
  // ─────────────────────────────────────────────────────────────────────────────

  async buildEastAfricaTop50Flat(opts?: {
    countries?: string[];
    cap?: number;
    sourceMaxRank?: number;
    platformWeights?: { spotify: number; apple: number };
    countryWeights?: Record<string, number>;
  }): Promise<KworbSpotifyDailyPayload> {
    const countries = (
      opts?.countries?.length ? opts.countries : ['ke', 'tz', 'ug', 'rw', 'et']
    ).map((c) => c.toLowerCase());

    const cap = opts?.cap ?? 50;
    const maxRank = opts?.sourceMaxRank ?? 200;
    const wP = {
      spotify: opts?.platformWeights?.spotify ?? 1.0,
      apple: opts?.platformWeights?.apple ?? 1.0,
    };

    const wC: Record<string, number> = {};
    for (const c of countries) wC[c] = opts?.countryWeights?.[c] ?? 1.0;

    const results = await Promise.all(
      countries.map(async (c) => {
        const [sp, ap] = await Promise.all([
          this.fetchDailyTracks(c).catch(() => undefined),
          this.fetchAppleDailyTracks(c).catch(() => undefined),
        ]);
        return { c, sp, ap };
      }),
    );

    const points = (rank?: number) =>
      rank && rank >= 1 && rank <= maxRank ? maxRank + 1 - rank : 0;

    type Acc = {
      artist: string;
      title: string;
      featured: string[];
      score: number;
      bestRank?: number;
    };
    const acc = new Map<string, Acc>();

    for (const { c, sp, ap } of results) {
      const cw = wC[c] ?? 1.0;

      const ingest = (rows?: KworbSpotifyDailyRow[], weight = 1) => {
        if (!rows?.length) return;
        for (const raw of rows) {
          if (raw.rank > maxRank) continue;
          const r = this.canonicalizeRow(raw);
          const key = this.trackKey(r.artist, r.title);
          const p = points(r.rank) * weight * cw;
          const cur = acc.get(key);
          if (!cur) {
            acc.set(key, {
              artist: r.artist,
              title: r.title,
              featured: this.dedupeOrder(r.featuredArtists ?? []),
              score: p,
              bestRank: r.rank,
            });
          } else {
            cur.score += p;
            cur.featured = this.dedupeOrder([
              ...(cur.featured ?? []),
              ...(r.featuredArtists ?? []),
            ]);
            if (r.rank && (cur.bestRank == null || r.rank < cur.bestRank))
              cur.bestRank = r.rank;
          }
        }
      };

      ingest(sp?.rows, wP.spotify);
      ingest(ap?.rows, wP.apple);
    }

    const combined = Array.from(acc.values());
    combined.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const ar = a.bestRank ?? Infinity;
      const br = b.bestRank ?? Infinity;
      if (ar !== br) return ar - br;
      const t = a.title.localeCompare(b.title, undefined, {
        sensitivity: 'accent',
      });
      if (t !== 0) return t;
      return a.artist.localeCompare(b.artist, undefined, {
        sensitivity: 'accent',
      });
    });

    return {
      country: 'east-africa',
      label: `East Africa Combined Top ${cap}`,
      fetchedAtISO: dayjs.utc().toISOString(),
      rows: combined.slice(0, cap).map((it, i) => ({
        rank: i + 1,
        artist: it.artist,
        title: it.title,
        featuredArtists: it.featured ?? [],
      })),
    };
  }
}
