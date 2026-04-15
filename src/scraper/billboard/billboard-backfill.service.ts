import { Inject, Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import { chartEntries, songFeatures } from 'src/infrastructure/drizzle/schema';
import { EntityResolutionService } from 'src/services/entity-resolution.service';

type BillboardEntry = {
  song: string;
  artist: string;
  this_week: number;
  peak_position: number | null;
  weeks_on_chart: number | null;
};

type BillboardChart = { data: BillboardEntry[] };

@Injectable()
export class BillboardBackfillService {
  private readonly logger = new Logger(BillboardBackfillService.name);

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly entityResolutionService: EntityResolutionService,
  ) {}

  async run(
    job?: Job,
    options?: { fromDate?: string; toDate?: string },
  ): Promise<{ dates: number; entries: number }> {
    // ── Warm cache once — loads all artists + songs into memory ──────────
    // Subsequent resolveArtist/resolveSong calls hit memory, not the DB
    await this.entityResolutionService.warmCache();

    const dates = await this.fetchDates(options);
    this.logger.log(`Found ${dates.length} Billboard chart dates to process`);

    let totalEntries = 0;
    let totalDates = 0;

    for (const date of dates) {
      const chart = await this.fetchChart(date);

      if (!chart) {
        this.logger.warn(`Skipping ${date} — failed to fetch`);
        continue;
      }

      for (const entry of chart.data) {
        try {
          const { primary, featured } = this.parseAllArtists(entry.artist);

          // ── Resolve primary artist ──────────────────────────────────
          const primaryArtist =
            await this.entityResolutionService.resolveArtist(
              {
                name: primary,
                source: 'billboard',
                allowCreate: true,
                markProvisionalIfCreated: true,
              },
              this.db,
            );

          if (!primaryArtist) {
            this.logger.warn(
              `Could not resolve primary artist: "${primary}" — skipping`,
            );
            continue;
          }

          // ── Resolve song — pass artistSlug to avoid re-query ───────
          const song = await this.entityResolutionService.resolveSong(
            {
              artistId: primaryArtist.id,
              artistSlug: primaryArtist.slug, // already have it
              title: entry.song,
              source: 'billboard',
              allowCreate: true,
              markProvisionalIfCreated: true,
            },
            this.db,
          );

          if (!song) {
            this.logger.warn(
              `Could not resolve song: "${entry.song}" — skipping`,
            );
            continue;
          }

          // ── Resolve featured artists ────────────────────────────────
          for (const featuredName of featured) {
            const featuredArtist =
              await this.entityResolutionService.resolveArtist(
                {
                  name: featuredName,
                  source: 'billboard',
                  allowCreate: true,
                },
                this.db,
              );

            if (!featuredArtist || featuredArtist.id === primaryArtist.id)
              continue;

            await this.db
              .insert(songFeatures)
              .values({ songId: song.id, featuredArtistId: featuredArtist.id })
              .onConflictDoNothing();
          }

          // ── Write chart entry ───────────────────────────────────────
          const rows = await this.db
            .insert(chartEntries)
            .values({
              artistId: primaryArtist.id,
              songId: song.id,
              chartName: 'billboard_hot_100',
              chartTerritory: 'US',
              position: entry.this_week,
              peakPosition: entry.peak_position ?? null,
              weeksOnChart: entry.weeks_on_chart ?? null,
              chartWeek: date,
            })
            .onConflictDoNothing()
            .returning({ id: chartEntries.id });

          if (rows.length) {
            totalEntries++;
          }
        } catch (err) {
          this.logger.error(
            `Failed ${date} #${entry.this_week} "${entry.song}" by "${entry.artist}": ${(err as Error).message}`,
          );
        }
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
        `[${totalDates}/${dates.length}] Done ${date} — ${chart.data.length} rows processed`,
      );

      await this.sleep(150);
    }

    // ── Clear cache after run — free memory ──────────────────────────────
    this.entityResolutionService.clearCache();

    this.logger.log(
      `Backfill complete — ${totalDates} dates, ${totalEntries} chart entries inserted`,
    );

    return { dates: totalDates, entries: totalEntries };
  }

  // ── Artist string parsing ─────────────────────────────────────────────

  private parseAllArtists(raw: string): {
    primary: string;
    featured: string[];
  } {
    const parts = raw
      .split(/\s+(?:featuring|feat\.?|ft\.?|with)\s+|\s+x\s+|\s+&\s+|,\s+/i)
      .map((s) =>
        s
          .trim()
          .replace(/^[&,]\s*/, '')
          .trim(),
      )
      .filter(Boolean)
      .filter((s) => s.length > 1);

    return {
      primary: parts[0] ?? raw.trim(),
      featured: parts.slice(1),
    };
  }

  // ── Data fetching ─────────────────────────────────────────────────────

  private async fetchDates(options?: {
    fromDate?: string;
    toDate?: string;
  }): Promise<string[]> {
    const res = await fetch(
      'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/valid_dates.json',
    );
    const dates: string[] = await res.json();

    return dates
      .filter((date) => {
        if (options?.fromDate && date < options.fromDate) return false;
        if (options?.toDate && date > options.toDate) return false;
        return true;
      })
      .sort((a, b) => b.localeCompare(a)); // newest → oldest
  }

  private async fetchChart(date: string): Promise<BillboardChart | null> {
    const res = await fetch(
      `https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/${date}.json`,
    );
    if (!res.ok) return null;
    return res.json() as Promise<BillboardChart>;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
