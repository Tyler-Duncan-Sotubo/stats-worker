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
    await this.entityResolutionService.warmCache();

    const dates = await this.fetchDates(options);
    this.logger.log(`Billboard backfill — ${dates.length} dates to process`);

    let totalEntries = 0;
    let totalDates = 0;

    for (const date of dates) {
      const chart = await this.fetchChart(date);

      if (!chart) continue;

      for (const entry of chart.data) {
        try {
          const { primary, featured } = this.parseAllArtists(entry.artist);

          // Artists — never create from Billboard
          const primaryArtist =
            await this.entityResolutionService.resolveArtist(
              {
                name: primary,
                source: 'billboard',
                allowCreate: false,
                markProvisionalIfCreated: false,
              },
              this.db,
            );

          if (!primaryArtist) continue;

          // Songs — allow creation only if artist already exists
          const song = await this.entityResolutionService.resolveSong(
            {
              artistId: primaryArtist.id,
              artistSlug: primaryArtist.slug,
              title: entry.song,
              source: 'billboard',
              allowCreate: true,
              markProvisionalIfCreated: true,
            },
            this.db,
          );

          if (!song) continue;

          // Featured artists — never create from Billboard
          for (const featuredName of featured) {
            const featuredArtist =
              await this.entityResolutionService.resolveArtist(
                {
                  name: featuredName,
                  source: 'billboard',
                  allowCreate: false,
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

          if (rows.length) totalEntries++;
        } catch (err) {
          this.logger.error(
            `Failed ${date} #${entry.this_week} "${entry.song}": ${(err as Error).message}`,
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

      if (totalDates % 50 === 0) {
        this.logger.log(
          `Progress — ${totalDates}/${dates.length} dates, ${totalEntries} entries`,
        );
      }

      await this.sleep(150);
    }

    this.entityResolutionService.clearCache();

    this.logger.log(
      `Backfill complete — ${totalDates} dates, ${totalEntries} entries`,
    );

    return { dates: totalDates, entries: totalEntries };
  }

  async runLatest(): Promise<{ entries: number }> {
    const today = new Date().toISOString().split('T')[0];

    // Fetch valid dates and get the most recent one
    const allDates = await this.fetchDates({ toDate: today });
    const latestDate = allDates[0]; // already sorted descending

    if (!latestDate) {
      this.logger.warn('No valid Billboard dates found');
      return { entries: 0 };
    }

    this.logger.log(`Billboard latest — fetching Hot 100 for ${latestDate}`);

    const chart = await this.fetchChart(latestDate);

    if (!chart || !chart.data.length) {
      this.logger.warn(`No data returned for ${latestDate}`);
      return { entries: 0 };
    }

    let totalEntries = 0;

    for (const entry of chart.data) {
      try {
        const { primary, featured } = this.parseAllArtists(entry.artist);

        const primaryArtist = await this.entityResolutionService.resolveArtist(
          {
            name: primary,
            source: 'billboard',
            allowCreate: false,
            markProvisionalIfCreated: false,
          },
          this.db,
        );

        if (!primaryArtist) continue;

        const song = await this.entityResolutionService.resolveSong(
          {
            artistId: primaryArtist.id,
            artistSlug: primaryArtist.slug,
            title: entry.song,
            source: 'billboard',
            allowCreate: true,
            markProvisionalIfCreated: true,
          },
          this.db,
        );

        if (!song) continue;

        for (const featuredName of featured) {
          const featuredArtist =
            await this.entityResolutionService.resolveArtist(
              {
                name: featuredName,
                source: 'billboard',
                allowCreate: false,
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
            chartWeek: latestDate,
          })
          .onConflictDoNothing()
          .returning({ id: chartEntries.id });

        if (rows.length) totalEntries++;
      } catch (err) {
        this.logger.error(
          `Failed #${entry.this_week} "${entry.song}": ${(err as Error).message}`,
        );
      }
    }

    this.logger.log(
      `Billboard latest complete — ${totalEntries} entries for ${latestDate}`,
    );

    return { entries: totalEntries };
  }

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
      .sort((a, b) => b.localeCompare(a));
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
