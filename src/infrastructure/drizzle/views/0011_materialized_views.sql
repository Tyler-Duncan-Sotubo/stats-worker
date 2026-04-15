-- ─────────────────────────────────────────────────────────────────────────────
-- 1. song_chart_summary
-- Aggregated chart performance per song per chart per territory
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW song_chart_summary AS
SELECT
  s.id                                          AS song_id,
  s.title                                       AS song_title,
  s.slug                                        AS song_slug,
  s.spotify_track_id,
  s.is_afrobeats                                AS song_is_afrobeats,
  s.release_date,
  a.id                                          AS artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats                                AS artist_is_afrobeats,
  a.origin_country,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL')        AS chart_territory,
  MIN(ce.position)                              AS peak_position,
  MAX(ce.weeks_on_chart)                        AS max_weeks_on_chart,
  COUNT(*)                                      AS total_chart_weeks,
  COUNT(*) FILTER (WHERE ce.position = 1)       AS weeks_at_number_1,
  COUNT(*) FILTER (WHERE ce.position <= 10)     AS weeks_in_top_10,
  COUNT(*) FILTER (WHERE ce.position <= 40)     AS weeks_in_top_40,
  MIN(ce.chart_week)                            AS first_charted,
  MAX(ce.chart_week)                            AS last_charted
FROM chart_entries ce
JOIN songs   s ON ce.song_id   = s.id
JOIN artists a ON ce.artist_id = a.id
WHERE ce.song_id   IS NOT NULL
  AND ce.artist_id IS NOT NULL
GROUP BY
  s.id, s.title, s.slug, s.spotify_track_id,
  s.is_afrobeats, s.release_date,
  a.id, a.name, a.slug, a.image_url,
  a.is_afrobeats, a.origin_country,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL');

CREATE UNIQUE INDEX idx_scs_unique
  ON song_chart_summary (song_id, chart_name, chart_territory);
CREATE INDEX idx_scs_territory_chart
  ON song_chart_summary (chart_territory, chart_name);
CREATE INDEX idx_scs_peak
  ON song_chart_summary (chart_territory, chart_name, peak_position ASC);
CREATE INDEX idx_scs_weeks
  ON song_chart_summary (chart_territory, chart_name, max_weeks_on_chart DESC);
CREATE INDEX idx_scs_weeks_at_1
  ON song_chart_summary (chart_territory, chart_name, weeks_at_number_1 DESC);
CREATE INDEX idx_scs_artist
  ON song_chart_summary (artist_id);
CREATE INDEX idx_scs_afrobeats
  ON song_chart_summary (artist_is_afrobeats, chart_territory, chart_name);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. chart_latest_leaderboard
-- Current week position per song per chart — newest chart_week wins
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW chart_latest_leaderboard AS
SELECT DISTINCT ON (
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL'),
  ce.song_id
)
  ce.id                                         AS entry_id,
  ce.song_id,
  ce.artist_id,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL')        AS chart_territory,
  ce.position,
  ce.peak_position,
  ce.weeks_on_chart,
  ce.chart_week,
  -- snapshot delta from chart_entry_snapshots
  ces.prev_rank,
  ces.delta,
  ces.trend,
  s.title                                       AS song_title,
  s.slug                                        AS song_slug,
  s.spotify_track_id,
  s.image_url                                   AS song_image_url,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats
FROM chart_entries ce
JOIN songs   s   ON ce.song_id   = s.id
JOIN artists a   ON ce.artist_id = a.id
LEFT JOIN chart_entry_snapshots ces ON ces.entry_id = ce.id
WHERE ce.song_id   IS NOT NULL
  AND ce.artist_id IS NOT NULL
ORDER BY
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL'),
  ce.song_id,
  ce.chart_week DESC;

CREATE UNIQUE INDEX idx_cll_unique
  ON chart_latest_leaderboard (chart_name, chart_territory, song_id);
CREATE INDEX idx_cll_territory_chart_position
  ON chart_latest_leaderboard (chart_territory, chart_name, position ASC);
CREATE INDEX idx_cll_afrobeats
  ON chart_latest_leaderboard (is_afrobeats, chart_territory, chart_name, position ASC);
CREATE INDEX idx_cll_artist
  ON chart_latest_leaderboard (artist_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. artist_stream_summary
-- Latest snapshot totals per artist
-- Note: artist_stats_snapshots does have artist_id directly
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW artist_stream_summary AS
SELECT DISTINCT ON (ass.artist_id)
  ass.artist_id,
  ass.total_streams,
  ass.total_streams_as_lead,
  ass.total_streams_as_feature,
  ass.daily_streams,
  ass.track_count,
  ass.snapshot_date,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats,
  a.origin_country,
  a.spotify_id,
  a.popularity
FROM artist_stats_snapshots ass
JOIN artists a ON ass.artist_id = a.id
ORDER BY ass.artist_id, ass.snapshot_date DESC;

CREATE UNIQUE INDEX idx_asm_unique
  ON artist_stream_summary (artist_id);
CREATE INDEX idx_asm_streams
  ON artist_stream_summary (total_streams DESC NULLS LAST);
CREATE INDEX idx_asm_afrobeats_streams
  ON artist_stream_summary (is_afrobeats, total_streams DESC NULLS LAST);
CREATE INDEX idx_asm_daily
  ON artist_stream_summary (daily_streams DESC NULLS LAST);
CREATE INDEX idx_asm_country
  ON artist_stream_summary (origin_country, total_streams DESC NULLS LAST);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. song_stream_summary
-- Latest snapshot per song
-- song_stats_snapshots only has song_id — join through songs for artist
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW song_stream_summary AS
SELECT DISTINCT ON (sss.song_id)
  sss.song_id,
  sss.spotify_streams                           AS total_spotify_streams,
  sss.daily_streams,
  sss.snapshot_date,
  s.artist_id,
  s.title                                       AS song_title,
  s.slug                                        AS song_slug,
  s.spotify_track_id,
  s.release_date,
  s.is_afrobeats                                AS song_is_afrobeats,
  s.image_url                                   AS song_image_url,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats                                AS artist_is_afrobeats,
  a.origin_country
FROM song_stats_snapshots sss
JOIN songs   s ON sss.song_id  = s.id
JOIN artists a ON s.artist_id  = a.id
ORDER BY sss.song_id, sss.snapshot_date DESC;

CREATE UNIQUE INDEX idx_ssm_unique
  ON song_stream_summary (song_id);
CREATE INDEX idx_ssm_streams
  ON song_stream_summary (total_spotify_streams DESC NULLS LAST);
CREATE INDEX idx_ssm_afrobeats_streams
  ON song_stream_summary (artist_is_afrobeats, total_spotify_streams DESC NULLS LAST);
CREATE INDEX idx_ssm_daily
  ON song_stream_summary (daily_streams DESC NULLS LAST);
CREATE INDEX idx_ssm_artist
  ON song_stream_summary (artist_id, total_spotify_streams DESC NULLS LAST);
CREATE INDEX idx_ssm_release
  ON song_stream_summary (release_date DESC NULLS LAST);


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. artist_certification_summary
-- Aggregated RIAA/BPI counts and units per artist per territory per body
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW artist_certification_summary AS
SELECT
  c.artist_id,
  c.territory,
  c.body,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats,
  a.origin_country,
  COUNT(*)                                      AS total_certifications,
  COUNT(*) FILTER (WHERE c.level = 'diamond')   AS diamond_count,
  COUNT(*) FILTER (WHERE c.level = 'platinum')  AS platinum_count,
  COUNT(*) FILTER (WHERE c.level = 'gold')      AS gold_count,
  COUNT(*) FILTER (WHERE c.level = 'silver')    AS silver_count,
  COALESCE(SUM(c.units), 0)                     AS total_platinum_units,
  MAX(c.certified_at)                           AS latest_certification,
  MIN(c.certified_at)                           AS earliest_certification
FROM certifications c
JOIN artists a ON c.artist_id = a.id
WHERE c.artist_id IS NOT NULL
GROUP BY
  c.artist_id, c.territory, c.body,
  a.name, a.slug, a.image_url,
  a.is_afrobeats, a.origin_country;

CREATE UNIQUE INDEX idx_acm_unique
  ON artist_certification_summary (artist_id, territory, body);
CREATE INDEX idx_acm_territory_body
  ON artist_certification_summary (territory, body);
CREATE INDEX idx_acm_afrobeats
  ON artist_certification_summary (is_afrobeats, territory, body);
CREATE INDEX idx_acm_units
  ON artist_certification_summary (territory, body, total_platinum_units DESC);
CREATE INDEX idx_acm_total
  ON artist_certification_summary (territory, body, total_certifications DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. artist_chart_summary
-- Aggregated chart performance at artist level across all songs
-- Unique to your schema — song_features means a featured artist
-- can be credited separately from the primary artist
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW artist_chart_summary AS

-- Primary artist appearances
SELECT
  a.id                                          AS artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats,
  a.origin_country,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL')        AS chart_territory,
  COUNT(DISTINCT ce.chart_week)                 AS total_chart_weeks,
  COUNT(DISTINCT ce.song_id)                    AS distinct_songs_charted,
  MIN(ce.position)                              AS best_peak_position,
  COUNT(*) FILTER (WHERE ce.position = 1)       AS weeks_at_number_1,
  COUNT(*) FILTER (WHERE ce.position <= 10)     AS weeks_in_top_10,
  MIN(ce.chart_week)                            AS first_chart_appearance,
  MAX(ce.chart_week)                            AS latest_chart_appearance,
  'primary'                                     AS role
FROM chart_entries ce
JOIN artists a ON ce.artist_id = a.id
WHERE ce.artist_id IS NOT NULL
  AND ce.song_id   IS NOT NULL
GROUP BY
  a.id, a.name, a.slug, a.image_url,
  a.is_afrobeats, a.origin_country,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL')

UNION ALL

-- Feature appearances via song_features
SELECT
  a.id                                          AS artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats,
  a.origin_country,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL')        AS chart_territory,
  COUNT(DISTINCT ce.chart_week)                 AS total_chart_weeks,
  COUNT(DISTINCT ce.song_id)                    AS distinct_songs_charted,
  MIN(ce.position)                              AS best_peak_position,
  COUNT(*) FILTER (WHERE ce.position = 1)       AS weeks_at_number_1,
  COUNT(*) FILTER (WHERE ce.position <= 10)     AS weeks_in_top_10,
  MIN(ce.chart_week)                            AS first_chart_appearance,
  MAX(ce.chart_week)                            AS latest_chart_appearance,
  'feature'                                     AS role
FROM song_features sf
JOIN artists     a  ON sf.featured_artist_id = a.id
JOIN chart_entries ce ON ce.song_id          = sf.song_id
WHERE ce.song_id IS NOT NULL
GROUP BY
  a.id, a.name, a.slug, a.image_url,
  a.is_afrobeats, a.origin_country,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL');

CREATE UNIQUE INDEX idx_achs_unique
  ON artist_chart_summary (artist_id, chart_name, chart_territory, role);
CREATE INDEX idx_achs_territory_chart
  ON artist_chart_summary (chart_territory, chart_name, total_chart_weeks DESC);
CREATE INDEX idx_achs_afrobeats
  ON artist_chart_summary (is_afrobeats, chart_territory, chart_name);
CREATE INDEX idx_achs_number_1
  ON artist_chart_summary (chart_territory, chart_name, weeks_at_number_1 DESC);
CREATE INDEX idx_achs_peak
  ON artist_chart_summary (chart_territory, chart_name, best_peak_position ASC);