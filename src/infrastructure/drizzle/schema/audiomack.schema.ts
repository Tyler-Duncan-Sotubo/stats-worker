import {
  pgTable,
  uuid,
  text,
  bigint,
  date,
  timestamp,
  index,
  uniqueIndex,
} from 'drizzle-orm/pg-core';
import { relations } from 'drizzle-orm';
import { defaultId } from '../default-id';
import { artists, songs } from './schema';

/* ============================================================================
   ARTIST AUDIOMACK SNAPSHOTS
============================================================================ */

export const artistAudiomackSnapshots = pgTable(
  'artist_audiomack_snapshots',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    artistId: uuid('artist_id')
      .notNull()
      .references(() => artists.id, { onDelete: 'cascade' }),

    audiomackId: text('audiomack_id'),
    audiomackSlug: text('audiomack_slug'),

    snapshotDate: date('snapshot_date', { mode: 'string' }).notNull(),

    totalPlays: bigint('total_plays', { mode: 'number' }),
    monthlyPlays: bigint('monthly_plays', { mode: 'number' }),
    dailyPlays: bigint('daily_plays', { mode: 'number' }),
    followers: bigint('followers', { mode: 'number' }),
    favorites: bigint('favorites', { mode: 'number' }),

    source: text('source').notNull().default('audiomack'),

    createdAt: timestamp('created_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    uniqueIndex('uq_artist_audiomack_snapshot').on(t.artistId, t.snapshotDate),
    index('idx_aams_snapshot_date').on(t.snapshotDate),
    index('idx_aams_total_plays').on(t.snapshotDate, t.totalPlays),
    index('idx_aams_daily_plays').on(t.snapshotDate, t.dailyPlays),
    index('idx_aams_artist').on(t.artistId, t.snapshotDate),
    index('idx_aams_audiomack_id').on(t.audiomackId),
    index('idx_aams_audiomack_slug').on(t.audiomackSlug),
  ],
);

/* ============================================================================
   SONG AUDIOMACK SNAPSHOTS
============================================================================ */

export const songAudiomackSnapshots = pgTable(
  'song_audiomack_snapshots',
  {
    id: uuid('id').primaryKey().$defaultFn(defaultId),

    songId: uuid('song_id')
      .notNull()
      .references(() => songs.id, { onDelete: 'cascade' }),

    audiomackId: text('audiomack_id'),
    audiomackUrl: text('audiomack_url'),

    snapshotDate: date('snapshot_date', { mode: 'string' }).notNull(),

    totalPlays: bigint('total_plays', { mode: 'number' }),
    dailyPlays: bigint('daily_plays', { mode: 'number' }),
    downloads: bigint('downloads', { mode: 'number' }),
    favorites: bigint('favorites', { mode: 'number' }),
    reposts: bigint('reposts', { mode: 'number' }),

    source: text('source').notNull().default('audiomack'),

    createdAt: timestamp('created_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    uniqueIndex('uq_song_audiomack_snapshot').on(t.songId, t.snapshotDate),
    index('idx_sams_snapshot_date').on(t.snapshotDate),
    index('idx_sams_total_plays').on(t.snapshotDate, t.totalPlays),
    index('idx_sams_daily_plays').on(t.snapshotDate, t.dailyPlays),
    index('idx_sams_song').on(t.songId, t.snapshotDate),
    index('idx_sams_audiomack_id').on(t.audiomackId),
  ],
);

// new relation exports
export const artistAudiomackSnapshotsRelations = relations(
  artistAudiomackSnapshots,
  ({ one }) => ({
    artist: one(artists, {
      fields: [artistAudiomackSnapshots.artistId],
      references: [artists.id],
    }),
  }),
);

export const songAudiomackSnapshotsRelations = relations(
  songAudiomackSnapshots,
  ({ one }) => ({
    song: one(songs, {
      fields: [songAudiomackSnapshots.songId],
      references: [songs.id],
    }),
  }),
);
