// audiomack-scraper.service.ts

import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { sql } from 'drizzle-orm';
import { AUDIOMACK_ARTISTS } from './audiomack-slug-overrides';
import { artistAudiomackSnapshots } from 'src/infrastructure/drizzle/schema';

export interface AudiomackArtistStats {
  audiomackSlug: string;
  totalPlays: number | null;
  monthlyListeners: number | null;
  followers: number | null;
  scrapedAt: Date;
}

@Injectable()
export class AudiomackScraperService {
  private readonly logger = new Logger(AudiomackScraperService.name);

  constructor(@Inject(DRIZZLE) private readonly db: DrizzleDB) {}

  // ── Parse helpers ──────────────────────────────────────────────────────────

  private parseCount(raw: string): number | null {
    if (!raw || !raw.trim()) return null; // ← guard empty string
    const cleaned = raw.trim().toUpperCase().replace(/,/g, '');
    const match = cleaned.match(/^([\d.]+)(K|M|B)?$/);
    if (!match) return null;

    const num = parseFloat(match[1]);
    const suffix = match[2];

    if (suffix === 'B') return Math.round(num * 1_000_000_000);
    if (suffix === 'M') return Math.round(num * 1_000_000);
    if (suffix === 'K') return Math.round(num * 1_000);
    return Math.round(num);
  }

  private extractAllStats(html: string): {
    followers: number | null;
    totalPlays: number | null;
    monthlyListeners: number | null;
  } {
    const results: Record<string, number | null> = {};

    // Match every SidebarStats block
    // <span class="SidebarStats-value">6.32M</span><span class="SidebarStats-title">Followers</span>
    const blockRegex =
      /<span class="SidebarStats-value">([^<]+)<\/span>\s*<span class="SidebarStats-title">([^<]+)<\/span>/g;

    let match: RegExpExecArray | null;
    while ((match = blockRegex.exec(html)) !== null) {
      const value = match[1].trim();
      const label = match[2].trim();
      results[label] = this.parseCount(value);
    }

    return {
      followers: results['Followers'] ?? 0,
      totalPlays: results['Total Account Plays'] ?? 0,
      monthlyListeners: results['Monthly Listeners'] ?? 0,
    };
  }

  // ── Fetch single artist ────────────────────────────────────────────────────

  async scrapeArtist(audiomackSlug: string): Promise<AudiomackArtistStats> {
    console.log(`[Audiomack] Starting scrape for ${audiomackSlug}`);
    const url = `https://audiomack.com/${audiomackSlug}`;
    this.logger.log(`[Audiomack] Scraping ${url}`);

    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; TooXclusiveBot/1.0)',
        Accept: 'text/html',
      },
    });

    if (!res.ok) {
      throw new Error(`[Audiomack] HTTP ${res.status} for ${url}`);
    }

    const html = await res.text();
    const { followers, totalPlays, monthlyListeners } =
      this.extractAllStats(html);

    this.logger.log(
      `[Audiomack] ${audiomackSlug} — plays=${totalPlays} monthly=${monthlyListeners} followers=${followers}`,
    );

    return {
      audiomackSlug,
      totalPlays,
      monthlyListeners,
      followers,
      scrapedAt: new Date(),
    };
  }

  async getScraperInfo(): Promise<{
    totalArtists: number;
    lastScrape: Date | null;
  }> {
    const result = await this.db.execute(sql`
      SELECT
        COUNT(DISTINCT artist_id) AS total_artists,
        MAX(snapshot_date) AS last_scrape
      FROM artist_audiomack_snapshots
    `);

    const row = result.rows[0] as any;
    return {
      totalArtists: parseInt(row.total_artists, 10),
      lastScrape: row.last_scrape ? new Date(row.last_scrape) : null,
    };
  }

  // ── Persist snapshot ───────────────────────────────────────────────────────

  async persistSnapshot(artistId: string, stats: AudiomackArtistStats) {
    const today = new Date().toISOString().slice(0, 10);

    console.log(
      `[Audiomack] Persisting snapshot for artist_id=${artistId} slug=${stats.audiomackSlug} date=${today} plays=${stats.totalPlays} monthly=${stats.monthlyListeners} followers=${stats.followers}`,
    );

    await this.db
      .insert(artistAudiomackSnapshots)
      .values({
        artistId,
        audiomackSlug: stats.audiomackSlug,
        snapshotDate: today,
        totalPlays: stats.totalPlays ?? 0,
        monthlyPlays: stats.monthlyListeners ?? 0,
        followers: stats.followers ?? 0,
      })
      .onConflictDoUpdate({
        target: [
          artistAudiomackSnapshots.artistId,
          artistAudiomackSnapshots.snapshotDate,
        ],
        set: {
          totalPlays: sql`EXCLUDED.total_plays`,
          monthlyPlays: sql`EXCLUDED.monthly_plays`,
          followers: sql`EXCLUDED.followers`,
          audiomackSlug: sql`EXCLUDED.audiomack_slug`,
          updatedAt: sql`now()`,
        },
      });
  }

  // ── Scrape and persist ─────────────────────────────────────────────────────

  async scrapeAndPersist(
    artistId: string,
    audiomackSlug: string,
  ): Promise<AudiomackArtistStats> {
    const stats = await this.scrapeArtist(audiomackSlug);
    await this.persistSnapshot(artistId, stats);
    return stats;
  }

  // ── Scrape all mapped artists ──────────────────────────────────────────────

  async scrapeAll(): Promise<void> {
    // First run — seed from the known list
    // Subsequent runs — use what's already in snapshots table
    const snapshotResult = await this.db.execute(sql`
    SELECT DISTINCT artist_id, audiomack_slug
    FROM artist_audiomack_snapshots
    WHERE audiomack_slug IS NOT NULL
  `);

    const alreadyScraped = new Set(
      (snapshotResult.rows as any[]).map((r) => r.artist_id as string),
    );

    // If snapshots exist use them, otherwise seed from the list
    let artists: { artist_id: string; audiomack_slug: string }[];

    if (alreadyScraped.size > 0) {
      artists = snapshotResult.rows as any[];
    } else {
      // First run — resolve artist IDs from DB slugs
      const slugList = AUDIOMACK_ARTISTS.map((a) => a.slug);
      const dbResult = await this.db.execute(sql`
  SELECT id, slug FROM artists
  WHERE slug = ANY(ARRAY[${sql.join(
    slugList.map((s) => sql`${s}`),
    sql`, `,
  )}])
    AND entity_status = 'canonical'
`);

      const dbMap = new Map(
        (dbResult.rows as any[]).map((r) => [r.slug, r.id]),
      );

      artists = AUDIOMACK_ARTISTS.filter((a) => dbMap.has(a.slug)).map((a) => ({
        artist_id: dbMap.get(a.slug)!,
        audiomack_slug: a.audiomackSlug,
      }));
    }

    this.logger.log(`[Audiomack] Scraping ${artists.length} artists`);

    for (const { artist_id, audiomack_slug } of artists) {
      try {
        await this.scrapeAndPersist(artist_id, audiomack_slug);
        await new Promise((r) => setTimeout(r, 2000));
      } catch (err) {
        this.logger.error(
          `[Audiomack] Failed for ${audiomack_slug}: ${(err as Error).message}`,
        );
      }
    }

    this.logger.log(`[Audiomack] Done scraping ${artists.length} artists`);
  }

  async discoverAudiomackSlugs() {
    for (const artist of AUDIOMACK_ARTISTS) {
      try {
        const res = await fetch(
          `https://audiomack.com/${artist.audiomackSlug}`,
          {
            headers: { 'User-Agent': 'Mozilla/5.0' },
          },
        );

        if (res.ok) {
          const html = await res.text();

          const blockRegex =
            /<span class="SidebarStats-value">([^<]+)<\/span>\s*<span class="SidebarStats-title">([^<]+)<\/span>/g;

          const stats: Record<string, string> = {};
          let match: RegExpExecArray | null;
          while ((match = blockRegex.exec(html)) !== null) {
            stats[match[2].trim()] = match[1].trim();
          }

          const plays = stats['Total Account Plays'] ?? null;
          const monthly = stats['Monthly Listeners'] ?? null;
          const followers = stats['Followers'] ?? null;

          if (plays || monthly || followers) {
            console.log(
              `✅ ${artist.slug} → /${artist.audiomackSlug} | plays=${plays} monthly=${monthly} followers=${followers}`,
            );
          } else {
            console.log(
              `⚠️  ${artist.slug} → /${artist.audiomackSlug} (no stats — slug may be wrong)`,
            );
          }
        } else {
          console.log(
            `❌ ${artist.slug} → /${artist.audiomackSlug} (${res.status})`,
          );
        }
      } catch {
        console.log(`❌ ${artist.slug} → /${artist.audiomackSlug} (failed)`);
      }

      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}
