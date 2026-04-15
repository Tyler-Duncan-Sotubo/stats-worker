import {
  pgTable,
  uuid,
  text,
  boolean,
  integer,
  bigint,
  date,
  timestamp,
  index,
  uniqueIndex,
  smallint,
  varchar,
} from 'drizzle-orm/pg-core';
import { relations } from 'drizzle-orm';
import { defaultId } from '../default-id';

/* ============================================================================
   ARTISTS
============================================================================ */

export const artists = pgTable(
  'artists',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    name: text('name').notNull(),
    normalizedName: text('normalized_name').notNull(),
    canonicalName: text('canonical_name'),

    spotifyId: text('spotify_id'),
    slug: text('slug').notNull(),

    originCountry: text('origin_country'),
    debutYear: integer('debut_year'),
    imageUrl: text('image_url'),
    popularity: integer('popularity'),

    isAfrobeats: boolean('is_afrobeats').notNull().default(false),
    isAfrobeatsOverride: boolean('is_afrobeats_override')
      .notNull()
      .default(false),

    bio: text('bio'),

    // identity / lifecycle
    entityStatus: text('entity_status').notNull().default('canonical'), // canonical | provisional | merged | rejected
    sourceOfTruth: text('source_of_truth'), // spotify | kworb | billboard | official_charts | manual
    needsReview: boolean('needs_review').notNull().default(false),
    mergedIntoArtistId: uuid('merged_into_artist_id'),

    kworbStatus: text('kworb_status'),
    kworbLastCheckedAt: timestamp('kworb_last_checked_at'),

    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('artists_spotify_id_idx').on(t.spotifyId),
    uniqueIndex('artists_slug_idx').on(t.slug),
    index('artists_normalized_name_idx').on(t.normalizedName),
    index('artists_origin_country_idx').on(t.originCountry),
    index('artists_is_afrobeats_idx').on(t.isAfrobeats),
    index('artists_popularity_idx').on(t.popularity),
    index('artists_entity_status_idx').on(t.entityStatus),
    index('artists_needs_review_idx').on(t.needsReview),
  ],
);

/* ============================================================================
   ARTIST MONTHLY LISTENER SNAPSHOTS
============================================================================ */

export const artistMonthlyListenerSnapshots = pgTable(
  'artist_monthly_listener_snapshots',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),
    spotifyId: text('spotify_id').notNull(),
    snapshotDate: date('snapshot_date', { mode: 'string' }).notNull(),
    monthlyListeners: bigint('monthly_listeners', { mode: 'number' }).notNull(),
    dailyChange: bigint('daily_change', { mode: 'number' }),
    peakRank: integer('peak_rank'),
    peakListeners: bigint('peak_listeners', { mode: 'number' }),
    source: text('source').notNull().default('kworb'),
    createdAt: timestamp('created_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    uniqueIndex('uq_artist_monthly_listener_snapshot').on(
      table.artistId,
      table.snapshotDate,
    ),
    index('idx_amls_snapshot_date').on(table.snapshotDate),
    index('idx_amls_monthly_listeners').on(
      table.snapshotDate,
      table.monthlyListeners,
    ),
    index('idx_amls_daily_change').on(table.snapshotDate, table.dailyChange),
    index('idx_amls_artist').on(table.artistId, table.snapshotDate),
    index('idx_amls_spotify_id').on(table.spotifyId),
  ],
);

/* ============================================================================
   ARTIST ALIASES
============================================================================ */

export const artistAliases = pgTable(
  'artist_aliases',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    alias: text('alias').notNull(),
    normalizedAlias: text('normalized_alias').notNull(),

    source: text('source'), // billboard | official_charts | riaa | kworb | manual
    isPrimary: boolean('is_primary').notNull().default(false),

    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('artist_aliases_artist_alias_idx').on(
      t.artistId,
      t.normalizedAlias,
    ),
    index('artist_aliases_normalized_alias_idx').on(t.normalizedAlias),
    index('artist_aliases_source_idx').on(t.source),
  ],
);

/* ============================================================================
   ARTIST EXTERNAL IDS
============================================================================ */

export const artistExternalIds = pgTable(
  'artist_external_ids',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    source: text('source').notNull(), // spotify | kworb | musicbrainz | apple_music
    externalId: text('external_id').notNull(),
    externalUrl: text('external_url'),

    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('artist_external_ids_source_id_idx').on(t.source, t.externalId),
    index('artist_external_ids_artist_idx').on(t.artistId),
  ],
);

/* ============================================================================
   ARTIST GENRES
============================================================================ */

export const artistGenres = pgTable(
  'artist_genres',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    genre: text('genre').notNull(),
    isPrimary: boolean('is_primary').notNull().default(false),
  },
  (t) => [
    uniqueIndex('artist_genres_artist_genre_idx').on(t.artistId, t.genre),
    index('artist_genres_genre_idx').on(t.genre),
  ],
);

/* ============================================================================
   ALBUMS
============================================================================ */

export const albums = pgTable(
  'albums',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    title: text('title').notNull(),
    slug: text('slug').notNull(),
    spotifyAlbumId: text('spotify_album_id').notNull(),

    albumType: text('album_type').notNull().default('album'),
    releaseDate: date('release_date'),
    imageUrl: text('image_url'),
    totalTracks: integer('total_tracks'),

    isAfrobeats: boolean('is_afrobeats').notNull().default(false),
    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('albums_spotify_album_id_idx').on(t.spotifyAlbumId),
    index('albums_artist_id_idx').on(t.artistId),
    index('albums_release_date_idx').on(t.releaseDate),
    index('albums_is_afrobeats_idx').on(t.isAfrobeats),
  ],
);

/* ============================================================================
   SONGS
============================================================================ */

export const songs = pgTable(
  'songs',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    albumId: uuid('album_id').references(() => albums.id, {
      onDelete: 'set null',
    }),

    title: text('title').notNull(),
    normalizedTitle: text('normalized_title').notNull(),
    canonicalTitle: text('canonical_title'),
    slug: text('slug').notNull(),

    spotifyTrackId: text('spotify_track_id'),
    releaseDate: date('release_date'),
    durationMs: integer('duration_ms'),

    explicit: boolean('explicit').notNull().default(false),
    isAfrobeats: boolean('is_afrobeats').notNull().default(false),
    imageUrl: text('image_url'),

    // identity / lifecycle
    entityStatus: text('entity_status').notNull().default('canonical'),
    sourceOfTruth: text('source_of_truth'),
    needsReview: boolean('needs_review').notNull().default(false),
    mergedIntoSongId: uuid('merged_into_song_id'),

    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('songs_spotify_track_id_idx').on(t.spotifyTrackId),
    uniqueIndex('songs_slug_idx').on(t.slug),
    uniqueIndex('songs_artist_normalized_title_idx').on(
      t.artistId,
      t.normalizedTitle,
    ),
    index('songs_artist_id_idx').on(t.artistId),
    index('songs_album_id_idx').on(t.albumId),
    index('songs_release_date_idx').on(t.releaseDate),
    index('songs_is_afrobeats_idx').on(t.isAfrobeats),
    index('songs_entity_status_idx').on(t.entityStatus),
    index('songs_needs_review_idx').on(t.needsReview),
  ],
);

/* ============================================================================
   SONG ALIASES
============================================================================ */

export const songAliases = pgTable(
  'song_aliases',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    songId: uuid('song_id')
      .notNull()
      .references(() => songs.id, { onDelete: 'cascade' }),

    alias: text('alias').notNull(),
    normalizedAlias: text('normalized_alias').notNull(),

    source: text('source'),
    isPrimary: boolean('is_primary').notNull().default(false),

    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('song_aliases_song_alias_idx').on(t.songId, t.normalizedAlias),
    index('song_aliases_normalized_alias_idx').on(t.normalizedAlias),
    index('song_aliases_source_idx').on(t.source),
  ],
);

/* ============================================================================
   SONG EXTERNAL IDS
============================================================================ */

export const songExternalIds = pgTable(
  'song_external_ids',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    songId: uuid('song_id')
      .notNull()
      .references(() => songs.id, { onDelete: 'cascade' }),

    source: text('source').notNull(), // spotify | isrc | kworb | apple_music
    externalId: text('external_id').notNull(),
    externalUrl: text('external_url'),

    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('song_external_ids_source_id_idx').on(t.source, t.externalId),
    index('song_external_ids_song_idx').on(t.songId),
  ],
);

/* ============================================================================
   SONG FEATURES
============================================================================ */

export const songFeatures = pgTable(
  'song_features',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    songId: uuid('song_id')
      .notNull()
      .references(() => songs.id, { onDelete: 'cascade' }),

    featuredArtistId: uuid('featured_artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),
  },
  (t) => [
    uniqueIndex('song_features_song_artist_idx').on(
      t.songId,
      t.featuredArtistId,
    ),
    index('song_features_featured_artist_idx').on(t.featuredArtistId),
  ],
);

/* ============================================================================
   ARTIST STATS SNAPSHOTS
============================================================================ */

export const artistStatsSnapshots = pgTable(
  'artist_stats_snapshots',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    totalStreams: bigint('total_streams', { mode: 'number' }),
    totalStreamsAsLead: bigint('total_streams_as_lead', { mode: 'number' }),
    totalStreamsSolo: bigint('total_streams_solo', { mode: 'number' }),
    totalStreamsAsFeature: bigint('total_streams_as_feature', {
      mode: 'number',
    }),

    dailyStreams: bigint('daily_streams', { mode: 'number' }),
    dailyStreamsAsLead: bigint('daily_streams_as_lead', { mode: 'number' }),
    dailyStreamsAsFeature: bigint('daily_streams_as_feature', {
      mode: 'number',
    }),

    trackCount: integer('track_count'),
    sourceUpdatedAt: date('source_updated_at'),
    snapshotDate: date('snapshot_date').notNull(),

    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('artist_stats_artist_date_idx').on(t.artistId, t.snapshotDate),
    index('artist_stats_snapshot_date_idx').on(t.snapshotDate),
    index('artist_stats_total_streams_idx').on(t.totalStreams),
    index('artist_stats_daily_streams_idx').on(t.dailyStreams),
  ],
);

/* ============================================================================
   SONG STATS SNAPSHOTS
============================================================================ */

export const songStatsSnapshots = pgTable(
  'song_stats_snapshots',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    songId: uuid('song_id')
      .notNull()
      .references(() => songs.id, { onDelete: 'cascade' }),

    spotifyStreams: bigint('spotify_streams', { mode: 'number' }),
    dailyStreams: bigint('daily_streams', { mode: 'number' }),
    snapshotDate: date('snapshot_date').notNull(),
  },
  (t) => [
    uniqueIndex('song_stats_song_date_idx').on(t.songId, t.snapshotDate),
    index('song_stats_snapshot_date_idx').on(t.snapshotDate),
    index('song_stats_streams_idx').on(t.spotifyStreams),
    index('song_stats_daily_streams_idx').on(t.dailyStreams),
  ],
);

/* ============================================================================
   CERTIFICATIONS
============================================================================ */

export const certifications = pgTable(
  'certifications',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id').references(() => artists.id, {
      onDelete: 'cascade',
    }),
    songId: uuid('song_id').references(() => songs.id, { onDelete: 'cascade' }),
    albumId: uuid('album_id').references(() => albums.id, {
      onDelete: 'cascade',
    }),

    territory: text('territory').notNull(),
    body: text('body').notNull(),
    title: text('title').notNull().default(''),
    level: text('level').notNull(),
    units: bigint('units', { mode: 'number' }),
    certifiedAt: date('certified_at'),
    sourceUrl: text('source_url'),

    // safer ingestion / resolution
    rawArtistName: text('raw_artist_name'),
    rawTitle: text('raw_title'),
    resolutionStatus: text('resolution_status').notNull().default('matched'), // matched | artist_only | unresolved

    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
  },
  (t) => [
    index('certs_artist_territory_idx').on(t.artistId, t.territory),
    index('certs_song_territory_idx').on(t.songId, t.territory),
    index('certs_territory_level_idx').on(t.territory, t.level),
    index('certs_certified_at_idx').on(t.certifiedAt),
    index('certs_resolution_status_idx').on(t.resolutionStatus),
    uniqueIndex('certs_unique_idx').on(
      t.artistId,
      t.territory,
      t.body,
      t.title,
    ),
  ],
);

/* ============================================================================
   CHART ENTRIES
============================================================================ */

export const chartEntries = pgTable(
  'chart_entries',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id').references(() => artists.id, {
      onDelete: 'cascade',
    }),
    songId: uuid('song_id').references(() => songs.id, { onDelete: 'cascade' }),
    albumId: uuid('album_id').references(() => albums.id, {
      onDelete: 'cascade',
    }),

    chartName: text('chart_name').notNull(),
    chartTerritory: text('chart_territory'),
    position: integer('position').notNull(),
    peakPosition: integer('peak_position'),
    weeksOnChart: integer('weeks_on_chart'),
    chartWeek: date('chart_week').notNull(),

    source: text('source'),
    sourceRowHash: text('source_row_hash'),
    ingestedAt: timestamp('ingested_at').notNull().defaultNow(),
  },
  (t) => [
    // one song can appear only once in a chart/territory/week
    uniqueIndex('chart_entries_song_chart_territory_week_idx').on(
      t.songId,
      t.chartName,
      t.chartTerritory,
      t.chartWeek,
    ),

    // one position can belong to only one row in a chart/territory/week
    uniqueIndex('chart_entries_chart_position_week_idx').on(
      t.chartName,
      t.chartTerritory,
      t.chartWeek,
      t.position,
    ),

    index('chart_entries_chart_week_idx').on(t.chartName, t.chartWeek),
    index('chart_entries_artist_chart_idx').on(t.artistId, t.chartName),
    index('chart_entries_position_idx').on(t.chartName, t.position),
    index('chart_entries_peak_idx').on(t.chartName, t.peakPosition),
    index('chart_entries_source_idx').on(t.source),
  ],
);

/* ============================================================================
   CHART ENTRY SNAPSHOTS
============================================================================ */

export const chartEntrySnapshots = pgTable(
  'chart_entry_snapshots',
  {
    entryId: uuid('entry_id')
      .primaryKey()
      .references(() => chartEntries.id, { onDelete: 'cascade' }),

    prevRank: smallint('prev_rank'),
    delta: smallint('delta'),
    trend: varchar('trend').notNull().default('NEW'),
  },
  (t) => [uniqueIndex('ux_snapshot_entry').on(t.entryId)],
);

/* ============================================================================
   AWARD RECORDS
============================================================================ */

export const awardRecords = pgTable(
  'award_records',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id').references(() => artists.id, {
      onDelete: 'cascade',
    }),
    songId: uuid('song_id').references(() => songs.id, {
      onDelete: 'set null',
    }),
    albumId: uuid('album_id').references(() => albums.id, {
      onDelete: 'set null',
    }),

    awardBody: text('award_body').notNull(),
    awardName: text('award_name').notNull(),
    category: text('category').notNull(),
    result: text('result').notNull(),
    year: integer('year').notNull(),
    ceremony: text('ceremony'),
    territory: text('territory'),
    sourceUrl: text('source_url'),
    notes: text('notes'),

    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
  },
  (t) => [
    index('awards_artist_idx').on(t.artistId),
    index('awards_year_idx').on(t.year),
    index('awards_body_idx').on(t.awardBody),
    index('awards_result_idx').on(t.result),
    uniqueIndex('awards_unique_idx').on(
      t.artistId,
      t.awardBody,
      t.awardName,
      t.year,
    ),
  ],
);

/* ============================================================================
   RECORDS
============================================================================ */

export const records = pgTable(
  'records',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id').references(() => artists.id, {
      onDelete: 'cascade',
    }),
    songId: uuid('song_id').references(() => songs.id, { onDelete: 'cascade' }),
    albumId: uuid('album_id').references(() => albums.id, {
      onDelete: 'cascade',
    }),

    recordType: text('record_type').notNull(),
    recordValue: text('record_value').notNull(),
    numericValue: bigint('numeric_value', { mode: 'number' }),
    scope: text('scope').notNull(),
    isActive: boolean('is_active').notNull().default(true),
    setOn: date('set_on'),
    brokenOn: date('broken_on'),
    notes: text('notes'),
  },
  (t) => [
    index('records_scope_type_idx').on(t.scope, t.recordType),
    index('records_artist_idx').on(t.artistId),
    index('records_song_idx').on(t.songId),
    index('records_is_active_idx').on(t.isActive),
    index('records_numeric_value_idx').on(t.recordType, t.numericValue),
  ],
);

/* ============================================================================
   RELATIONS
============================================================================ */

export const artistsRelations = relations(artists, ({ one, many }) => ({
  mergedInto: one(artists, {
    fields: [artists.mergedIntoArtistId],
    references: [artists.id],
  }),
  aliases: many(artistAliases),
  externalIds: many(artistExternalIds),
  genres: many(artistGenres),
  albums: many(albums),
  songs: many(songs),
  features: many(songFeatures),
  statsSnapshots: many(artistStatsSnapshots),
  certifications: many(certifications),
  chartEntries: many(chartEntries),
  awardRecords: many(awardRecords),
  records: many(records),
}));

export const artistAliasesRelations = relations(artistAliases, ({ one }) => ({
  artist: one(artists, {
    fields: [artistAliases.artistId],
    references: [artists.id],
  }),
}));

export const artistExternalIdsRelations = relations(
  artistExternalIds,
  ({ one }) => ({
    artist: one(artists, {
      fields: [artistExternalIds.artistId],
      references: [artists.id],
    }),
  }),
);

export const albumsRelations = relations(albums, ({ one, many }) => ({
  artist: one(artists, { fields: [albums.artistId], references: [artists.id] }),
  songs: many(songs),
  certifications: many(certifications),
  chartEntries: many(chartEntries),
}));

export const songsRelations = relations(songs, ({ one, many }) => ({
  artist: one(artists, { fields: [songs.artistId], references: [artists.id] }),
  album: one(albums, { fields: [songs.albumId], references: [albums.id] }),
  mergedInto: one(songs, {
    fields: [songs.mergedIntoSongId],
    references: [songs.id],
  }),
  aliases: many(songAliases),
  externalIds: many(songExternalIds),
  features: many(songFeatures),
  statsSnapshots: many(songStatsSnapshots),
  certifications: many(certifications),
  chartEntries: many(chartEntries),
  records: many(records),
}));

export const songAliasesRelations = relations(songAliases, ({ one }) => ({
  song: one(songs, {
    fields: [songAliases.songId],
    references: [songs.id],
  }),
}));

export const songExternalIdsRelations = relations(
  songExternalIds,
  ({ one }) => ({
    song: one(songs, {
      fields: [songExternalIds.songId],
      references: [songs.id],
    }),
  }),
);

export const songFeaturesRelations = relations(songFeatures, ({ one }) => ({
  song: one(songs, { fields: [songFeatures.songId], references: [songs.id] }),
  featuredArtist: one(artists, {
    fields: [songFeatures.featuredArtistId],
    references: [artists.id],
  }),
}));

export const artistStatsSnapshotsRelations = relations(
  artistStatsSnapshots,
  ({ one }) => ({
    artist: one(artists, {
      fields: [artistStatsSnapshots.artistId],
      references: [artists.id],
    }),
  }),
);

export const songStatsSnapshotsRelations = relations(
  songStatsSnapshots,
  ({ one }) => ({
    song: one(songs, {
      fields: [songStatsSnapshots.songId],
      references: [songs.id],
    }),
  }),
);

export const certificationsRelations = relations(certifications, ({ one }) => ({
  artist: one(artists, {
    fields: [certifications.artistId],
    references: [artists.id],
  }),
  song: one(songs, { fields: [certifications.songId], references: [songs.id] }),
  album: one(albums, {
    fields: [certifications.albumId],
    references: [albums.id],
  }),
}));

export const chartEntriesRelations = relations(chartEntries, ({ one }) => ({
  artist: one(artists, {
    fields: [chartEntries.artistId],
    references: [artists.id],
  }),
  song: one(songs, { fields: [chartEntries.songId], references: [songs.id] }),
  album: one(albums, {
    fields: [chartEntries.albumId],
    references: [albums.id],
  }),
  snapshot: one(chartEntrySnapshots, {
    fields: [chartEntries.id],
    references: [chartEntrySnapshots.entryId],
  }),
}));

export const chartEntrySnapshotsRelations = relations(
  chartEntrySnapshots,
  ({ one }) => ({
    entry: one(chartEntries, {
      fields: [chartEntrySnapshots.entryId],
      references: [chartEntries.id],
    }),
  }),
);

export const awardRecordsRelations = relations(awardRecords, ({ one }) => ({
  artist: one(artists, {
    fields: [awardRecords.artistId],
    references: [artists.id],
  }),
  song: one(songs, { fields: [awardRecords.songId], references: [songs.id] }),
  album: one(albums, {
    fields: [awardRecords.albumId],
    references: [albums.id],
  }),
}));

export const recordsRelations = relations(records, ({ one }) => ({
  artist: one(artists, {
    fields: [records.artistId],
    references: [artists.id],
  }),
  song: one(songs, { fields: [records.songId], references: [songs.id] }),
  album: one(albums, { fields: [records.albumId], references: [albums.id] }),
}));
