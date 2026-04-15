import { and, desc, eq, lt } from 'drizzle-orm';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { DRIZZLE } from 'src/infrastructure/drizzle/drizzle.module';
import type { DrizzleDB } from 'src/infrastructure/drizzle/drizzle.module';
import {
  chartEntries,
  chartEntrySnapshots,
  songFeatures,
} from 'src/infrastructure/drizzle/schema';
import { SpotifyDailyService } from './spotify-daily.service';
import { KworbSpotifyDailyRow } from '../dto/kworb.dto';
import { EntityResolutionService } from 'src/services/entity-resolution.service';

const SPOTIFY_DAILY_COUNTRIES = ['ng', 'za'] as const;
const APPLE_DAILY_COUNTRIES = ['ng', 'gh', 'ke', 'za', 'ug'] as const;
const EAST_AFRICA_COUNTRIES = ['ke', 'tz', 'ug', 'rw', 'et'] as const;

type SpotifyCountry = (typeof SPOTIFY_DAILY_COUNTRIES)[number];
type AppleCountry = (typeof APPLE_DAILY_COUNTRIES)[number];
type ChartTrend = 'NEW' | 'UP' | 'DOWN' | 'SAME';

@Injectable()
export class DailyChartIngestionService {
  private readonly logger = new Logger(DailyChartIngestionService.name);

  constructor(
    @Inject(DRIZZLE) private readonly db: DrizzleDB,
    private readonly spotifyDailyService: SpotifyDailyService,
    private readonly entityResolutionService: EntityResolutionService,
  ) {}

  async runDailyIngestion(): Promise<void> {
    const today = new Date().toISOString().split('T')[0];
    this.logger.log(`Starting daily chart ingestion for ${today}`);

    await Promise.allSettled([
      this.ingestAfricanCountryCharts(today),
      this.ingestCombinedCharts(today),
    ]);

    this.logger.log(`Daily chart ingestion complete for ${today}`);
  }

  private async ingestAfricanCountryCharts(date: string): Promise<void> {
    await Promise.allSettled([
      this.ingestSpotifyCountries(date),
      this.ingestAppleCountries(date),
    ]);
  }

  private async ingestSpotifyCountries(date: string): Promise<void> {
    for (const country of SPOTIFY_DAILY_COUNTRIES) {
      await this.ingestSpotifyDaily(country, date);
    }
  }

  private async ingestAppleCountries(date: string): Promise<void> {
    for (const country of APPLE_DAILY_COUNTRIES) {
      await this.ingestAppleDaily(country, date);
    }
  }

  private async ingestCombinedCharts(date: string): Promise<void> {
    await Promise.allSettled([
      this.ingestTooXclusiveTop100(date),
      this.ingestEastAfricaTop50(date),
    ]);
  }

  private async ingestSpotifyDaily(
    country: SpotifyCountry,
    date: string,
  ): Promise<void> {
    try {
      const payload = await this.spotifyDailyService.fetchDailyTracks(
        country,
        100,
      );

      await this.persistRows(
        payload.rows,
        `spotify_daily_${country}`,
        country.toUpperCase(),
        date,
        'kworb',
      );

      this.logger.log(
        `[Spotify/${country.toUpperCase()}] Ingested ${payload.rows.length} entries`,
      );
    } catch (err) {
      this.logger.error(
        `[Spotify/${country.toUpperCase()}] Failed: ${(err as Error).message}`,
      );
    }
  }

  private async ingestAppleDaily(
    country: AppleCountry,
    date: string,
  ): Promise<void> {
    try {
      const payload = await this.spotifyDailyService.fetchAppleDailyTracks(
        country,
        100,
      );

      await this.persistRows(
        payload.rows,
        `apple_daily_${country}`,
        country.toUpperCase(),
        date,
        'apple_music',
      );

      this.logger.log(
        `[Apple/${country.toUpperCase()}] Ingested ${payload.rows.length} entries`,
      );
    } catch (err) {
      this.logger.error(
        `[Apple/${country.toUpperCase()}] Failed: ${(err as Error).message}`,
      );
    }
  }

  private async ingestTooXclusiveTop100(date: string): Promise<void> {
    try {
      const payload = await this.spotifyDailyService.combineTop100('ng', {
        spotifyWeight: 1.0,
        appleWeight: 1.0,
        sourceMaxRank: 100,
        cap: 100,
      });

      const rows: KworbSpotifyDailyRow[] = payload.items.map((item) => ({
        rank: item.rank,
        artist: item.artist,
        title: item.title,
        featuredArtists: item.featuredArtists,
      }));

      await this.persistRows(rows, 'tooxclusive_top_100', 'NG', date, 'manual');

      this.logger.log(`[TooXclusive Top 100] Ingested ${rows.length} entries`);
    } catch (err) {
      this.logger.error(
        `[TooXclusive Top 100] Failed: ${(err as Error).message}`,
      );
    }
  }

  private async ingestEastAfricaTop50(date: string): Promise<void> {
    try {
      const payload = await this.spotifyDailyService.buildEastAfricaTop50Flat({
        countries: [...EAST_AFRICA_COUNTRIES],
        cap: 50,
        sourceMaxRank: 100,
      });

      await this.persistRows(
        payload.rows,
        'tooxclusive_east_africa_top_50',
        'EAST_AFRICA',
        date,
        'manual',
      );

      this.logger.log(
        `[East Africa Top 50] Ingested ${payload.rows.length} entries`,
      );
    } catch (err) {
      this.logger.error(
        `[East Africa Top 50] Failed: ${(err as Error).message}`,
      );
    }
  }

  private async persistRows(
    rows: KworbSpotifyDailyRow[],
    chartName: string,
    chartTerritory: string,
    chartWeek: string,
    source: 'kworb' | 'apple_music' | 'manual',
  ): Promise<void> {
    for (const row of rows) {
      const primaryArtist = await this.entityResolutionService.resolveArtist({
        name: row.artist,
        source,
        allowCreate: true,
        markProvisionalIfCreated: source !== 'kworb',
      });

      if (!primaryArtist) continue;

      const song = await this.entityResolutionService.resolveSong({
        artistId: primaryArtist.id,
        title: row.title,
        source,
        allowCreate: true,
        markProvisionalIfCreated: source !== 'kworb',
      });

      if (!song) continue;

      for (const featuredName of row.featuredArtists ?? []) {
        const featuredArtist = await this.entityResolutionService.resolveArtist(
          {
            name: featuredName,
            source,
            allowCreate: true,
            markProvisionalIfCreated: true,
          },
        );

        if (!featuredArtist || featuredArtist.id === primaryArtist.id) continue;

        await this.db
          .insert(songFeatures)
          .values({
            songId: song.id,
            featuredArtistId: featuredArtist.id,
          })
          .onConflictDoNothing();
      }

      const previousEntry = await this.findPreviousChartEntry({
        songId: song.id,
        chartName,
        chartTerritory,
        chartWeek,
      });

      const inserted = await this.db
        .insert(chartEntries)
        .values({
          artistId: primaryArtist.id,
          songId: song.id,
          chartName,
          chartTerritory,
          position: row.rank,
          peakPosition: null,
          weeksOnChart: null,
          chartWeek,
          source,
        })
        .onConflictDoNothing()
        .returning({ id: chartEntries.id });

      if (!inserted.length) {
        continue;
      }

      const entryId = inserted[0].id;
      const snapshot = this.buildSnapshot(
        row.rank,
        previousEntry?.position ?? null,
      );

      await this.db
        .insert(chartEntrySnapshots)
        .values({
          entryId,
          prevRank: snapshot.prevRank,
          delta: snapshot.delta,
          trend: snapshot.trend,
        })
        .onConflictDoUpdate({
          target: chartEntrySnapshots.entryId,
          set: {
            prevRank: snapshot.prevRank,
            delta: snapshot.delta,
            trend: snapshot.trend,
          },
        });
    }
  }

  private async findPreviousChartEntry(params: {
    songId: string;
    chartName: string;
    chartTerritory: string;
    chartWeek: string;
  }): Promise<{ id: string; position: number | null } | undefined> {
    const rows = await this.db
      .select({
        id: chartEntries.id,
        position: chartEntries.position,
      })
      .from(chartEntries)
      .where(
        and(
          eq(chartEntries.songId, params.songId),
          eq(chartEntries.chartName, params.chartName),
          eq(chartEntries.chartTerritory, params.chartTerritory),
          lt(chartEntries.chartWeek, params.chartWeek),
        ),
      )
      .orderBy(desc(chartEntries.chartWeek))
      .limit(1);

    return rows[0];
  }

  private buildSnapshot(
    currentRank: number,
    previousRank: number | null,
  ): {
    prevRank: number | null;
    delta: number | null;
    trend: ChartTrend;
  } {
    if (previousRank == null) {
      return {
        prevRank: null,
        delta: null,
        trend: 'NEW',
      };
    }

    if (currentRank < previousRank) {
      return {
        prevRank: previousRank,
        delta: previousRank - currentRank,
        trend: 'UP',
      };
    }

    if (currentRank > previousRank) {
      return {
        prevRank: previousRank,
        delta: currentRank - previousRank,
        trend: 'DOWN',
      };
    }

    return {
      prevRank: previousRank,
      delta: 0,
      trend: 'SAME',
    };
  }
}
