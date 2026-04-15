import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import * as cheerio from 'cheerio';

export interface ArtistTotals {
  spotifyId: string;
  totalStreams: number;
  totalStreamsAsLead: number;
  totalStreamsSolo: number;
  totalStreamsAsFeature: number;
  dailyStreams: number;
  dailyStreamsAsLead: number;
  dailyStreamsAsFeature: number;
  trackCount: number;
  lastUpdated: string; // raw date string from page e.g. "2026/04/09"
}

export interface SongTotal {
  spotifyTrackId: string;
  title: string;
  streams: number;
  dailyStreams: number;
  isFeature: boolean; // true when Kworb prefixes with *
}

export interface ArtistTotalsPayload {
  artistName: string;
  spotifyId: string;
  totals: ArtistTotals;
  songs: SongTotal[];
  fetchedAtISO: string;
}

@Injectable()
export class KworbTotalsService {
  private readonly logger = new Logger(KworbTotalsService.name);

  private parseNum(raw: string): number {
    const cleaned = (raw ?? '').replace(/[^0-9]/g, '');
    const n = parseInt(cleaned, 10);
    return Number.isFinite(n) ? n : 0;
  }

  async fetchArtistTotals(spotifyId: string): Promise<ArtistTotalsPayload> {
    const url = `https://kworb.net/spotify/artist/${spotifyId}_songs.html`;

    const { data } = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent': 'tooXclusiveStatsBot/1.0 (+https://tooxclusive.com)',
        Accept: 'text/html,application/xhtml+xml',
      },
    });

    const $ = cheerio.load(data);

    // ── Artist name ──────────────────────────────────────────────────
    const artistName = $('h1, h2')
      .first()
      .text()
      .replace('- Spotify Top Songs', '')
      .trim();

    // ── Last updated date ────────────────────────────────────────────
    const bodyText = $('body').text();
    const dateMatch = bodyText.match(/Last updated:\s*(\d{4}\/\d{2}\/\d{2})/);
    const lastUpdated = dateMatch?.[1] ?? '';

    // ── Artist summary table (Streams / Daily / Tracks rows) ─────────
    // The first table has 5 columns: blank, Total, As lead, Solo, As feature
    let totalStreams = 0;
    let totalStreamsAsLead = 0;
    let totalStreamsSolo = 0;
    let totalStreamsAsFeature = 0;
    let dailyStreams = 0;
    let dailyStreamsAsLead = 0;
    let dailyStreamsAsFeature = 0;
    let trackCount = 0;

    $('table')
      .first()
      .find('tr')
      .each((_, row) => {
        const cells = $(row).find('td');
        if (cells.length < 5) return;

        const label = cells.eq(0).text().trim().toLowerCase();

        if (label === 'streams') {
          totalStreams = this.parseNum(cells.eq(1).text());
          totalStreamsAsLead = this.parseNum(cells.eq(2).text());
          totalStreamsSolo = this.parseNum(cells.eq(3).text());
          totalStreamsAsFeature = this.parseNum(cells.eq(4).text());
        }

        if (label === 'daily') {
          dailyStreams = this.parseNum(cells.eq(1).text());
          dailyStreamsAsLead = this.parseNum(cells.eq(2).text());
          dailyStreamsAsFeature = this.parseNum(cells.eq(4).text());
        }

        if (label === 'tracks') {
          trackCount = this.parseNum(cells.eq(1).text());
        }
      });

    // ── Song table ───────────────────────────────────────────────────
    // Second table: Song Title | Streams | Daily
    // * prefix in cell text = feature track
    // Song link goes to open.spotify.com/track/{TRACK_ID}
    const songs: SongTotal[] = [];

    $('table')
      .eq(1)
      .find('tr')
      .each((_, row) => {
        const cells = $(row).find('td');
        if (cells.length < 3) return;

        const titleCell = cells.eq(0);
        const rawText = titleCell.text().trim();

        // Feature flag — Kworb renders as "* Song Title" in text
        const isFeature = rawText.startsWith('*');
        const title = rawText.replace(/^\*\s*/, '').trim();
        if (!title) return;

        // Extract Spotify track ID from the anchor href
        const trackHref = titleCell.find('a').attr('href') ?? '';
        const trackMatch = trackHref.match(
          /open\.spotify\.com\/track\/([A-Za-z0-9]+)/,
        );
        const spotifyTrackId = trackMatch?.[1] ?? '';

        const streams = this.parseNum(cells.eq(1).text());
        const dailyStreams = this.parseNum(cells.eq(2).text());

        songs.push({ spotifyTrackId, title, streams, dailyStreams, isFeature });
      });

    this.logger.log(
      `Fetched ${songs.length} songs for ${artistName} (${spotifyId}) — total streams: ${totalStreams.toLocaleString()}`,
    );

    return {
      artistName,
      spotifyId,
      totals: {
        spotifyId,
        totalStreams,
        totalStreamsAsLead,
        totalStreamsSolo,
        totalStreamsAsFeature,
        dailyStreams,
        dailyStreamsAsLead,
        dailyStreamsAsFeature,
        trackCount,
        lastUpdated,
      },
      songs,
      fetchedAtISO: new Date().toISOString(),
    };
  }

  // Fetch totals for multiple artists — used by the nightly cron
  async fetchMultipleArtists(
    spotifyIds: string[],
    delayMs = 1200, // be polite to Kworb — don't hammer them
  ): Promise<ArtistTotalsPayload[]> {
    const results: ArtistTotalsPayload[] = [];

    for (const id of spotifyIds) {
      try {
        const payload = await this.fetchArtistTotals(id);
        results.push(payload);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        this.logger.error(`Failed to fetch totals for ${id}: ${errorMessage}`);
      }

      // Polite delay between requests
      if (delayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    return results;
  }
}
