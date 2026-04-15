-- =========================================================
-- ARTIST GROWTH SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS artist_growth_summary;

CREATE MATERIALIZED VIEW artist_growth_summary AS
SELECT
  s1.artist_id,
  a.name        AS artist_name,
  a.slug        AS artist_slug,
  a.image_url,
  a.is_afrobeats,
  a.origin_country,
  s1.snapshot_date,
  s1.daily_streams,
  s1.total_streams,

  (s1.daily_streams - s0.daily_streams) AS daily_growth,
  (s1.total_streams - s7.total_streams) AS growth_7d

FROM artist_stats_snapshots s1
JOIN artists a ON a.id = s1.artist_id

LEFT JOIN artist_stats_snapshots s0
  ON s1.artist_id = s0.artist_id
  AND s0.snapshot_date = s1.snapshot_date - INTERVAL '1 day'

LEFT JOIN artist_stats_snapshots s7
  ON s1.artist_id = s7.artist_id
  AND s7.snapshot_date = s1.snapshot_date - INTERVAL '7 days';

CREATE UNIQUE INDEX idx_ags_unique
  ON artist_growth_summary (artist_id, snapshot_date);

CREATE INDEX idx_ags_daily_growth
  ON artist_growth_summary (snapshot_date, daily_growth DESC);

CREATE INDEX idx_ags_7d_growth
  ON artist_growth_summary (snapshot_date, growth_7d DESC);

CREATE INDEX idx_ags_afrobeats
  ON artist_growth_summary (is_afrobeats, snapshot_date, growth_7d DESC);


-- =========================================================
-- ARTIST TRENDING SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS artist_trending_summary;

CREATE MATERIALIZED VIEW artist_trending_summary AS
SELECT
  ag.*,
  (
    COALESCE(ag.daily_growth, 0) * 0.6 +
    COALESCE(ag.growth_7d, 0) * 0.4
  ) AS momentum_score
FROM artist_growth_summary ag;

CREATE UNIQUE INDEX idx_ats_unique
  ON artist_trending_summary (artist_id, snapshot_date);

CREATE INDEX idx_ats_momentum
  ON artist_trending_summary (snapshot_date, momentum_score DESC);

CREATE INDEX idx_ats_afrobeats
  ON artist_trending_summary (is_afrobeats, snapshot_date, momentum_score DESC);


-- =========================================================
-- SONG GROWTH SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS song_growth_summary;

CREATE MATERIALIZED VIEW song_growth_summary AS
SELECT
  s1.song_id,
  s.title AS song_title,
  s.slug  AS song_slug,
  s.artist_id,
  a.name  AS artist_name,
  a.slug  AS artist_slug,
  s1.snapshot_date,
  s1.daily_streams,
  s1.spotify_streams AS total_streams,

  (s1.daily_streams - s0.daily_streams) AS daily_growth,
  (s1.spotify_streams - s7.spotify_streams) AS growth_7d

FROM song_stats_snapshots s1
JOIN songs s   ON s.id = s1.song_id
JOIN artists a ON a.id = s.artist_id

LEFT JOIN song_stats_snapshots s0
  ON s1.song_id = s0.song_id
  AND s0.snapshot_date = s1.snapshot_date - INTERVAL '1 day'

LEFT JOIN song_stats_snapshots s7
  ON s1.song_id = s7.song_id
  AND s7.snapshot_date = s1.snapshot_date - INTERVAL '7 days';

CREATE UNIQUE INDEX idx_sgs_unique
  ON song_growth_summary (song_id, snapshot_date);

CREATE INDEX idx_sgs_daily_growth
  ON song_growth_summary (snapshot_date, daily_growth DESC);

CREATE INDEX idx_sgs_7d_growth
  ON song_growth_summary (snapshot_date, growth_7d DESC);


-- =========================================================
-- SONG TRENDING SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS song_trending_summary;

CREATE MATERIALIZED VIEW song_trending_summary AS
SELECT
  sg.*,
  (
    COALESCE(sg.daily_growth, 0) * 0.7 +
    COALESCE(sg.growth_7d, 0) * 0.3
  ) AS momentum_score
FROM song_growth_summary sg;

CREATE UNIQUE INDEX idx_sts_unique
  ON song_trending_summary (song_id, snapshot_date);

CREATE INDEX idx_sts_momentum
  ON song_trending_summary (snapshot_date, momentum_score DESC);


-- =========================================================
-- ARTIST COUNTRY SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS artist_country_summary;

CREATE MATERIALIZED VIEW artist_country_summary AS
SELECT
  a.origin_country,
  a.id AS artist_id,
  a.name AS artist_name,
  a.slug AS artist_slug,
  a.image_url,
  a.is_afrobeats,
  ass.total_streams,
  ass.daily_streams
FROM artist_stream_summary ass
JOIN artists a ON a.id = ass.artist_id;

CREATE UNIQUE INDEX idx_acs_unique
  ON artist_country_summary (origin_country, artist_id);

CREATE INDEX idx_acs_streams
  ON artist_country_summary (origin_country, total_streams DESC);

CREATE INDEX idx_acs_daily
  ON artist_country_summary (origin_country, daily_streams DESC);


-- =========================================================
-- ARTIST RECENT CHART SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS artist_recent_chart_summary;

CREATE MATERIALIZED VIEW artist_recent_chart_summary AS
SELECT
  a.id AS artist_id,
  a.name AS artist_name,
  a.slug AS artist_slug,
  a.image_url,
  a.is_afrobeats,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL') AS chart_territory,

  COUNT(*) AS chart_entries,
  COUNT(*) FILTER (WHERE ce.position <= 10) AS top_10s,
  COUNT(*) FILTER (WHERE ce.position = 1) AS number_1s,

  MIN(ce.position) AS best_peak,
  MIN(ce.chart_week) AS first_recent_entry,
  MAX(ce.chart_week) AS latest_recent_entry

FROM chart_entries ce
JOIN artists a ON a.id = ce.artist_id

WHERE ce.chart_week >= CURRENT_DATE - INTERVAL '90 days'

GROUP BY
  a.id, a.name, a.slug, a.image_url,
  a.is_afrobeats,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL');

CREATE UNIQUE INDEX idx_arcs_unique
  ON artist_recent_chart_summary (artist_id, chart_name, chart_territory);

CREATE INDEX idx_arcs_top10
  ON artist_recent_chart_summary (chart_territory, chart_name, top_10s DESC);

CREATE INDEX idx_arcs_peak
  ON artist_recent_chart_summary (chart_territory, chart_name, best_peak ASC);


-- =========================================================
--  ARTIST MONTHLY LISTENER SUMMARY
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS artist_monthly_listener_summary;

CREATE MATERIALIZED VIEW artist_monthly_listener_summary AS
SELECT DISTINCT ON (amls.artist_id)
  amls.artist_id,
  amls.spotify_id,
  amls.snapshot_date,
  amls.monthly_listeners,
  amls.daily_change,
  amls.peak_rank,
  amls.peak_listeners,
  a.name AS artist_name,
  a.slug AS artist_slug,
  a.image_url AS artist_image_url,
  a.is_afrobeats,
  a.origin_country
FROM artist_monthly_listener_snapshots amls
JOIN artists a ON a.id = amls.artist_id
ORDER BY amls.artist_id, amls.snapshot_date DESC;

CREATE UNIQUE INDEX idx_amlsum_unique
  ON artist_monthly_listener_summary (artist_id);

CREATE INDEX idx_amlsum_listeners
  ON artist_monthly_listener_summary (monthly_listeners DESC);

CREATE INDEX idx_amlsum_daily_change
  ON artist_monthly_listener_summary (daily_change DESC);

CREATE INDEX idx_amlsum_afrobeats
  ON artist_monthly_listener_summary (is_afrobeats, monthly_listeners DESC);

CREATE INDEX idx_amlsum_country
  ON artist_monthly_listener_summary (origin_country, monthly_listeners DESC);