// src/scraper/services/spotify-official-charts.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { chromium } from 'playwright';
import * as fs from 'fs';
import * as path from 'path';

export interface SpotifyOfficialChartRow {
  rank: number;
  artist: string;
  title: string;
  imageUrl?: string;
  spotifyUrl?: string;
  streams?: number | null;
}

export interface SpotifyOfficialChartPayload {
  country: string;
  label: string;
  fetchedAtISO: string;
  source: 'spotify_charts';
  rows: SpotifyOfficialChartRow[];
}

const AUTH_STATE_PATH = path.resolve(process.cwd(), 'spotify-auth.json');

@Injectable()
export class SpotifyOfficialChartsService {
  private readonly logger = new Logger(SpotifyOfficialChartsService.name);

  constructor(private readonly config: ConfigService) {}

  // ─── 1. LOCAL ONLY: Manual login, saves session to disk ──────────────────
  // Run this on your laptop when you need to refresh the session manually.
  // Then base64-encode the result and upload to Railway:
  //   base64 spotify-auth.json | pbcopy
  // Paste into Railway env var: SPOTIFY_AUTH_JSON_B64
  async saveLoginSessionLocal(): Promise<void> {
    this.logger.log('Launching headed browser for manual login...');

    const browser = await chromium.launch({ headless: false });
    const context = await browser.newContext({
      viewport: { width: 1440, height: 900 },
    });

    const page = await context.newPage();
    await page.goto('https://charts.spotify.com', { waitUntil: 'networkidle' });

    this.logger.log(
      'Please log in manually in the browser. Waiting 90 seconds...',
    );
    await page.waitForTimeout(90_000);

    await context.storageState({ path: AUTH_STATE_PATH });
    this.logger.log(`Session saved to ${AUTH_STATE_PATH}`);
    this.logger.log(
      'Now run: base64 spotify-auth.json | pbcopy  (mac) or base64 spotify-auth.json (linux)',
    );
    this.logger.log(
      'Then paste the output into Railway env var: SPOTIFY_AUTH_JSON_B64',
    );

    await browser.close();
  }

  // ─── 2. RAILWAY: Restore auth state from env var ─────────────────────────
  // Called automatically on startup. Reads SPOTIFY_AUTH_JSON_B64 from env,
  // decodes it, and writes spotify-auth.json to disk so the scraper can use it.
  ensureAuthStateFile(): void {
    if (fs.existsSync(AUTH_STATE_PATH)) {
      this.logger.log(
        'spotify-auth.json already exists on disk, skipping restore.',
      );
      return;
    }

    const b64 = this.config.get<string>('SPOTIFY_AUTH_JSON_B64');

    if (!b64) {
      this.logger.warn(
        'SPOTIFY_AUTH_JSON_B64 is not set and spotify-auth.json does not exist. ' +
          'Scraper will fail until you set the env var or run saveLoginSessionLocal().',
      );
      return;
    }

    const json = Buffer.from(b64, 'base64').toString('utf-8');
    fs.writeFileSync(AUTH_STATE_PATH, json, 'utf-8');
    this.logger.log(
      'spotify-auth.json restored from SPOTIFY_AUTH_JSON_B64 env var ✓',
    );
  }

  // ─── 3. HEADLESS AUTO-REFRESH: Attempts automated login ──────────────────
  // Requires SPOTIFY_EMAIL and SPOTIFY_PASSWORD env vars.
  // Call this from a cron job or after a 401 to refresh the session.
  // WARNING: May fail if Spotify shows CAPTCHA or 2FA — monitor logs.
  async refreshLoginSessionHeadless(): Promise<void> {
    const email = this.config.get<string>('SPOTIFY_EMAIL');
    const password = this.config.get<string>('SPOTIFY_PASSWORD');

    if (!email || !password) {
      throw new Error(
        'SPOTIFY_EMAIL and SPOTIFY_PASSWORD must be set for headless refresh.',
      );
    }

    this.logger.log('Attempting headless Spotify login...');

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      viewport: { width: 1440, height: 900 },
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    });

    const page = await context.newPage();

    try {
      this.logger.log('Navigating to Spotify login page...');
      await page.goto('https://accounts.spotify.com/en/login', {
        waitUntil: 'networkidle',
        timeout: 30_000,
      });

      await page.fill('input[data-testid="login-username"]', email);
      await page.fill('input[data-testid="login-password"]', password);
      await page.click('button[data-testid="login-button"]');

      // Wait for redirect away from login page
      await page.waitForURL(
        (url) => !url.toString().includes('accounts.spotify.com/en/login'),
        {
          timeout: 30_000,
        },
      );

      const currentUrl = page.url();
      this.logger.log(`Redirected to: ${currentUrl}`);

      if (currentUrl.includes('accounts.spotify.com')) {
        // Still on accounts — likely CAPTCHA or 2FA
        const pageContent = await page.content();
        if (
          pageContent.includes('captcha') ||
          pageContent.includes('CAPTCHA')
        ) {
          throw new Error(
            'Spotify showed a CAPTCHA during headless login. ' +
              'Run saveLoginSessionLocal() manually and update SPOTIFY_AUTH_JSON_B64.',
          );
        }
        throw new Error(
          `Login did not complete. Still on: ${currentUrl}. ` +
            'May require 2FA or manual intervention.',
        );
      }

      // Navigate to charts to fully establish session
      await page.goto('https://charts.spotify.com', {
        waitUntil: 'networkidle',
        timeout: 30_000,
      });

      await context.storageState({ path: AUTH_STATE_PATH });

      // Also update the in-memory env var representation (for logging purposes)
      const json = fs.readFileSync(AUTH_STATE_PATH, 'utf-8');
      const newB64 = Buffer.from(json).toString('base64');
      this.logger.log('Headless login successful ✓');
      this.logger.log(
        'IMPORTANT: Update your SPOTIFY_AUTH_JSON_B64 Railway env var with this new value:',
      );
      this.logger.log(`SPOTIFY_AUTH_JSON_B64=${newB64}`);
    } finally {
      await page.close().catch(() => undefined);
      await context.close().catch(() => undefined);
      await browser.close().catch(() => undefined);
    }
  }

  // ─── Token extraction ─────────────────────────────────────────────────────
  private async extractToken(country: string): Promise<string> {
    if (!fs.existsSync(AUTH_STATE_PATH)) {
      // Last-ditch: try restoring from env
      this.ensureAuthStateFile();
    }

    if (!fs.existsSync(AUTH_STATE_PATH)) {
      throw new Error(
        `No Spotify session found. Set SPOTIFY_AUTH_JSON_B64 env var or run saveLoginSessionLocal().`,
      );
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      storageState: AUTH_STATE_PATH,
      viewport: { width: 1440, height: 900 },
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    });

    const page = await context.newPage();
    let chartsToken: string | null = null;

    page.on('request', (req) => {
      if (req.url().includes('charts-spotify-com-service.spotify.com')) {
        const auth = req.headers()['authorization'];
        if (auth?.startsWith('Bearer ')) {
          chartsToken = auth.replace('Bearer ', '').trim();
          this.logger.log('Bearer token intercepted ✓');
        }
      }
    });

    page.on('requestfailed', (req) => {
      this.logger.warn(
        `[request failed] ${req.method()} ${req.url()} => ${req.failure()?.errorText}`,
      );
    });

    try {
      const chartPageUrl = `https://charts.spotify.com/charts/view/regional-${country}-daily/latest`;
      this.logger.log(`Loading chart page to intercept token: ${chartPageUrl}`);

      await page.goto(chartPageUrl, {
        waitUntil: 'networkidle',
        timeout: 45_000,
      });

      if (!chartsToken) {
        this.logger.log('Token not yet found, waiting an extra 5s...');
        await page.waitForTimeout(5_000);
      }

      if (!chartsToken) {
        const currentUrl = page.url();

        if (
          currentUrl.includes('login') ||
          currentUrl.includes('accounts.spotify.com')
        ) {
          throw new Error(
            'SESSION_EXPIRED: Spotify session has expired. ' +
              'Either call refreshLoginSessionHeadless() or run saveLoginSessionLocal() ' +
              'and update SPOTIFY_AUTH_JSON_B64.',
          );
        }

        throw new Error(
          'Could not intercept Bearer token. The page may not have made any chart API calls.',
        );
      }

      return chartsToken;
    } finally {
      await page.close().catch(() => undefined);
      await context.close().catch(() => undefined);
      await browser.close().catch(() => undefined);
    }
  }

  // ─── Resolve entries array from any known response shape ─────────────────
  private resolveEntries(data: any): any[] {
    if (Array.isArray(data?.entries)) {
      this.logger.log(
        `Response shape: { entries[] } — ${data.entries.length} entries`,
      );
      return data.entries as any[];
    }

    if (Array.isArray(data?.chartEntryViewResponses?.[0]?.entries)) {
      const entries = data.chartEntryViewResponses[0].entries;
      this.logger.log(
        `Response shape: { chartEntryViewResponses[0].entries[] } — ${entries.length} entries`,
      );
      return entries as any[];
    }

    if (
      Array.isArray(data?.displayChart?.chartEntryViewResponses?.[0]?.entries)
    ) {
      const entries = data.displayChart.chartEntryViewResponses[0].entries;
      this.logger.log(
        `Response shape: { displayChart.chartEntryViewResponses[0].entries[] } — ${entries.length} entries`,
      );
      return entries as any[];
    }

    this.logger.error(
      `Unknown API response shape. Top-level keys: ${Object.keys(data ?? {}).join(', ')}`,
    );
    this.logger.error(
      `Raw response preview: ${JSON.stringify(data).slice(0, 500)}`,
    );

    throw new Error(
      `Unexpected Spotify Charts API response shape. Keys: ${Object.keys(data ?? {}).join(', ')}`,
    );
  }

  // ─── Map raw entry to SpotifyOfficialChartRow ─────────────────────────────
  private mapEntry(entry: any): SpotifyOfficialChartRow {
    const trackId = entry.trackMetadata?.trackUri?.split(':')?.[2];

    return {
      rank: entry.chartEntryData?.currentRank,
      title: entry.trackMetadata?.trackName,
      artist: (entry.trackMetadata?.artists ?? [])
        .map((a: any) => a.name as string)
        .join(', '),
      streams: entry.chartEntryData?.rankingMetric?.value ?? null,
      spotifyUrl: trackId
        ? `https://open.spotify.com/track/${trackId}`
        : undefined,
      imageUrl: entry.trackMetadata?.displayImageUri || undefined,
    };
  }

  // ─── Fetch chart JSON using intercepted token ─────────────────────────────
  private async fetchChartJson(
    country: string,
    token: string,
    date = 'latest',
    limit = 200,
  ): Promise<SpotifyOfficialChartRow[]> {
    const apiUrl = `https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-${country}-daily/${date}`;

    this.logger.log(`Fetching chart JSON: ${apiUrl}`);

    const res = await fetch(apiUrl, {
      headers: {
        Authorization: `Bearer ${token}`,
        'App-Platform': 'WebPlayer',
        'Content-Type': 'application/json',
      },
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(
        `Spotify Charts API returned ${res.status}: ${body.slice(0, 300)}`,
      );
    }

    const data = await res.json();
    const entries = this.resolveEntries(data);

    this.logger.log(`Total entries from API: ${entries.length}`);

    return entries
      .map((entry: any) => this.mapEntry(entry))
      .filter((row) => row.rank && row.title && row.artist)
      .sort((a, b) => a.rank - b.rank)
      .slice(0, limit);
  }

  // ─── Public: fetch daily chart ────────────────────────────────────────────
  async fetchDailyTracks(
    country = 'ng',
    limit = 200,
    date = 'latest',
  ): Promise<SpotifyOfficialChartPayload> {
    const normalizedCountry = country.toLowerCase();

    this.logger.log(
      `Fetching Spotify daily chart for ${normalizedCountry.toUpperCase()} (${date})`,
    );

    let token: string;

    try {
      token = await this.extractToken(normalizedCountry);
    } catch (err: any) {
      // If session expired, attempt headless refresh once and retry
      if (
        err?.message?.includes('SESSION_EXPIRED') &&
        this.config.get('SPOTIFY_EMAIL') &&
        this.config.get('SPOTIFY_PASSWORD')
      ) {
        this.logger.warn('Session expired — attempting headless refresh...');
        await this.refreshLoginSessionHeadless();
        token = await this.extractToken(normalizedCountry);
      } else {
        throw err;
      }
    }

    const rows = await this.fetchChartJson(
      normalizedCountry,
      token,
      date,
      limit,
    );

    this.logger.log(`Returning ${rows.length} rows for ${normalizedCountry}`);

    return {
      country: normalizedCountry,
      label: `${normalizedCountry.toUpperCase()} Spotify Daily Chart`,
      fetchedAtISO: new Date().toISOString(),
      source: 'spotify_charts',
      rows,
    };
  }
}
