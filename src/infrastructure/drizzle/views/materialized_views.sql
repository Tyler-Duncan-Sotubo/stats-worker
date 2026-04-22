-- =============================================================
-- STATS ENGINE — ALL MATERIALIZED VIEWS
-- Drop order matters: dependent views first
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- DROP ALL (reverse dependency order)
-- ─────────────────────────────────────────────────────────────
DROP MATERIALIZED VIEW IF EXISTS artist_country_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_trending_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_growth_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_trending_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_growth_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_recent_chart_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_monthly_listener_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_stream_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_stream_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_certification_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_chart_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_records_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_awards_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS chart_latest_leaderboard CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_chart_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_certifications_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_search_summary CASCADE;

-- =============================================================
-- 1. SONG CHART SUMMARY
-- Aggregated chart performance per song per chart per territory
-- =============================================================
CREATE MATERIALIZED VIEW song_chart_summary AS
WITH base AS (
  SELECT
    ce.song_id,
    ce.chart_name,
    COALESCE(ce.chart_territory, 'GLOBAL') AS chart_territory,
    ce.position,
    ce.weeks_on_chart,
    ce.chart_week
  FROM chart_entries ce
  WHERE ce.song_id IS NOT NULL
    AND ce.artist_id IS NOT NULL
),
aggregated AS (
  SELECT
    song_id,
    chart_name,
    chart_territory,
    MIN(position)                                               AS peak_position,
    MAX(weeks_on_chart)                                         AS max_weeks_on_chart,
    COUNT(DISTINCT chart_week)                                  AS total_chart_weeks,
    COUNT(DISTINCT chart_week) FILTER (WHERE position = 1)     AS weeks_at_number_1,
    COUNT(DISTINCT chart_week) FILTER (WHERE position <= 10)   AS weeks_in_top_10,
    COUNT(DISTINCT chart_week) FILTER (WHERE position <= 40)   AS weeks_in_top_40,
    MIN(chart_week)                                             AS first_charted,
    MAX(chart_week)                                             AS last_charted
  FROM base
  GROUP BY song_id, chart_name, chart_territory
)
SELECT
  agg.song_id,
  s.title                   AS song_title,
  s.slug                    AS song_slug,
  s.spotify_track_id,
  s.is_afrobeats            AS song_is_afrobeats,
  s.release_date,
  a.id                      AS artist_id,
  a.name                    AS artist_name,
  a.slug                    AS artist_slug,
  a.image_url               AS artist_image_url,
  a.is_afrobeats            AS artist_is_afrobeats,
  a.origin_country,
  agg.chart_name,
  agg.chart_territory,
  agg.peak_position,
  agg.max_weeks_on_chart,
  agg.total_chart_weeks,
  agg.weeks_at_number_1,
  agg.weeks_in_top_10,
  agg.weeks_in_top_40,
  agg.first_charted,
  agg.last_charted
FROM aggregated agg
JOIN songs   s ON s.id = agg.song_id
JOIN artists a ON a.id = s.artist_id; -- ← via songs not chart_entries

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

-- =============================================================
-- 2. CHART LATEST LEADERBOARD
-- Current week position per song per chart — newest chart_week wins
-- =============================================================
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


-- =============================================================
-- 3. ARTIST STREAM SUMMARY
-- Latest snapshot totals per artist
-- =============================================================
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


-- =============================================================
-- 4. SONG STREAM SUMMARY
-- Latest snapshot per song
-- =============================================================
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


-- =============================================================
-- 5. ARTIST CERTIFICATION SUMMARY
-- Aggregated RIAA/BPI counts and units per artist per territory per body
-- =============================================================
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


-- =============================================================
-- 6. ARTIST CHART SUMMARY
-- Aggregated chart performance at artist level — primary + feature
-- =============================================================
CREATE MATERIALIZED VIEW artist_chart_summary AS

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
JOIN artists      a  ON sf.featured_artist_id = a.id
JOIN chart_entries ce ON ce.song_id           = sf.song_id
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


-- =============================================================
-- 7. ARTIST AWARDS SUMMARY
-- =============================================================
CREATE MATERIALIZED VIEW artist_awards_summary AS
SELECT
  ar.id,
  ar.artist_id,
  ar.award_body,
  ar.award_name,
  ar.category,
  ar.result,
  ar.year,
  ar.ceremony,
  ar.territory,
  ar.source_url,
  ar.notes,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats,
  a.origin_country
FROM award_records ar
JOIN artists a ON a.id = ar.artist_id;

CREATE UNIQUE INDEX idx_aas_unique
  ON artist_awards_summary (id);
CREATE INDEX idx_aas_artist
  ON artist_awards_summary (artist_id, year DESC);
CREATE INDEX idx_aas_result
  ON artist_awards_summary (artist_id, result);
CREATE INDEX idx_aas_body
  ON artist_awards_summary (award_body, year DESC);
CREATE INDEX idx_aas_afrobeats
  ON artist_awards_summary (is_afrobeats, award_body, year DESC);


-- =============================================================
-- 8. ARTIST RECORDS SUMMARY
-- =============================================================
CREATE MATERIALIZED VIEW artist_records_summary AS
SELECT
  r.id,
  r.artist_id,
  r.record_type,
  r.record_value,
  r.numeric_value,
  r.scope,
  r.is_active,
  r.set_on,
  r.broken_on,
  r.notes,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.is_afrobeats,
  a.origin_country
FROM records r
JOIN artists a ON a.id = r.artist_id;

CREATE UNIQUE INDEX idx_ars_unique
  ON artist_records_summary (id);
CREATE INDEX idx_ars_artist
  ON artist_records_summary (artist_id, is_active, set_on DESC);
CREATE INDEX idx_ars_active
  ON artist_records_summary (artist_id, is_active);
CREATE INDEX idx_ars_scope
  ON artist_records_summary (scope, is_active);
CREATE INDEX idx_ars_afrobeats
  ON artist_records_summary (is_afrobeats, is_active);


-- =============================================================
-- 9. ARTIST GROWTH SUMMARY
-- Daily and 7-day stream growth per artist per snapshot
-- =============================================================
CREATE MATERIALIZED VIEW artist_growth_summary AS
SELECT
  s1.artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url,
  a.is_afrobeats,
  a.origin_country,
  s1.snapshot_date,
  s1.daily_streams,
  s1.total_streams,
  (s1.daily_streams - s0.daily_streams)         AS daily_growth,
  (s1.total_streams - s7.total_streams)         AS growth_7d
FROM artist_stats_snapshots s1
JOIN artists a ON a.id = s1.artist_id
LEFT JOIN artist_stats_snapshots s0
  ON s1.artist_id    = s0.artist_id
  AND s0.snapshot_date = s1.snapshot_date - INTERVAL '1 day'
LEFT JOIN artist_stats_snapshots s7
  ON s1.artist_id    = s7.artist_id
  AND s7.snapshot_date = s1.snapshot_date - INTERVAL '7 days';

CREATE UNIQUE INDEX idx_ags_unique
  ON artist_growth_summary (artist_id, snapshot_date);
CREATE INDEX idx_ags_daily_growth
  ON artist_growth_summary (snapshot_date, daily_growth DESC);
CREATE INDEX idx_ags_7d_growth
  ON artist_growth_summary (snapshot_date, growth_7d DESC);
CREATE INDEX idx_ags_afrobeats
  ON artist_growth_summary (is_afrobeats, snapshot_date, growth_7d DESC);


-- =============================================================
-- 10. ARTIST TRENDING SUMMARY
-- Momentum score = weighted growth (depends on artist_growth_summary)
-- =============================================================
CREATE MATERIALIZED VIEW artist_trending_summary AS
SELECT
  ag.*,
  (
    COALESCE(ag.daily_growth, 0) * 0.6 +
    COALESCE(ag.growth_7d,    0) * 0.4
  ) AS momentum_score
FROM artist_growth_summary ag;

CREATE UNIQUE INDEX idx_ats_unique
  ON artist_trending_summary (artist_id, snapshot_date);
CREATE INDEX idx_ats_momentum
  ON artist_trending_summary (snapshot_date, momentum_score DESC);
CREATE INDEX idx_ats_afrobeats
  ON artist_trending_summary (is_afrobeats, snapshot_date, momentum_score DESC);


-- =============================================================
-- 11. SONG GROWTH SUMMARY
-- Daily and 7-day stream growth per song per snapshot
-- =============================================================
CREATE MATERIALIZED VIEW song_growth_summary AS
SELECT
  s1.song_id,
  s.title                                       AS song_title,
  s.slug                                        AS song_slug,
  s.artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  s1.snapshot_date,
  s1.daily_streams,
  s1.spotify_streams                            AS total_streams,
  (s1.daily_streams    - s0.daily_streams)      AS daily_growth,
  (s1.spotify_streams  - s7.spotify_streams)    AS growth_7d
FROM song_stats_snapshots s1
JOIN songs   s ON s.id = s1.song_id
JOIN artists a ON a.id = s.artist_id
LEFT JOIN song_stats_snapshots s0
  ON s1.song_id      = s0.song_id
  AND s0.snapshot_date = s1.snapshot_date - INTERVAL '1 day'
LEFT JOIN song_stats_snapshots s7
  ON s1.song_id      = s7.song_id
  AND s7.snapshot_date = s1.snapshot_date - INTERVAL '7 days';

CREATE UNIQUE INDEX idx_sgs_unique
  ON song_growth_summary (song_id, snapshot_date);
CREATE INDEX idx_sgs_daily_growth
  ON song_growth_summary (snapshot_date, daily_growth DESC);
CREATE INDEX idx_sgs_7d_growth
  ON song_growth_summary (snapshot_date, growth_7d DESC);


-- =============================================================
-- 12. SONG TRENDING SUMMARY
-- Momentum score per song (depends on song_growth_summary)
-- =============================================================
CREATE MATERIALIZED VIEW song_trending_summary AS
SELECT
  sg.*,
  (
    COALESCE(sg.daily_growth, 0) * 0.7 +
    COALESCE(sg.growth_7d,    0) * 0.3
  ) AS momentum_score
FROM song_growth_summary sg;

CREATE UNIQUE INDEX idx_sts_unique
  ON song_trending_summary (song_id, snapshot_date);
CREATE INDEX idx_sts_momentum
  ON song_trending_summary (snapshot_date, momentum_score DESC);


-- =============================================================
-- 13. ARTIST MONTHLY LISTENER SUMMARY
-- Latest listener snapshot per artist
-- =============================================================
CREATE MATERIALIZED VIEW artist_monthly_listener_summary AS
SELECT
  *,
  ROW_NUMBER() OVER (ORDER BY monthly_listeners DESC) AS global_rank
FROM (
  SELECT DISTINCT ON (amls.artist_id)
    amls.artist_id,
    amls.spotify_id,
    amls.snapshot_date,
    amls.monthly_listeners,
    amls.daily_change,
    amls.peak_rank,
    amls.peak_listeners,
    a.name          AS artist_name,
    a.slug          AS artist_slug,
    a.image_url     AS artist_image_url,
    a.is_afrobeats,
    a.origin_country
  FROM artist_monthly_listener_snapshots amls
  JOIN artists a ON a.id = amls.artist_id
  ORDER BY amls.artist_id, amls.snapshot_date DESC
) latest
ORDER BY monthly_listeners DESC;

CREATE UNIQUE INDEX idx_amlsum_unique
  ON artist_monthly_listener_summary (artist_id);
CREATE INDEX idx_amlsum_daily_change
  ON artist_monthly_listener_summary (daily_change DESC);
CREATE INDEX idx_amlsum_afrobeats
  ON artist_monthly_listener_summary (is_afrobeats, monthly_listeners DESC);
CREATE INDEX idx_amlsum_country
  ON artist_monthly_listener_summary (origin_country, monthly_listeners DESC);

-- keep this too — still useful for filtered queries
CREATE INDEX idx_amlsum_listeners
  ON artist_monthly_listener_summary (monthly_listeners DESC);

-- add this new one
CREATE INDEX idx_amlsum_global_rank
  ON artist_monthly_listener_summary (global_rank);


-- =============================================================
-- 14. ARTIST RECENT CHART SUMMARY
-- Chart activity in the last 90 days
-- =============================================================
CREATE MATERIALIZED VIEW artist_recent_chart_summary AS
SELECT
  a.id                                          AS artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url,
  a.is_afrobeats,
  ce.chart_name,
  COALESCE(ce.chart_territory, 'GLOBAL')        AS chart_territory,
  COUNT(*)                                      AS chart_entries,
  COUNT(*) FILTER (WHERE ce.position <= 10)     AS top_10s,
  COUNT(*) FILTER (WHERE ce.position = 1)       AS number_1s,
  MIN(ce.position)                              AS best_peak,
  MIN(ce.chart_week)                            AS first_recent_entry,
  MAX(ce.chart_week)                            AS latest_recent_entry
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


-- =============================================================
-- 15. ARTIST COUNTRY SUMMARY
-- Per-country stream leaderboard (depends on artist_stream_summary)
-- =============================================================
CREATE MATERIALIZED VIEW artist_country_summary AS
SELECT
  a.origin_country,
  a.id                                          AS artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
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


-- =============================================================
-- 16. SONG SEARCH SUMMARY
-- Full-text searchable song index for ask module song resolution
-- =============================================================
CREATE MATERIALIZED VIEW song_search_summary AS
SELECT
  s.id,
  s.title,
  s.slug,
  s.spotify_track_id,
  s.is_afrobeats,
  s.image_url                                   AS song_image_url,
  a.id                                          AS artist_id,
  a.name                                        AS artist_name,
  a.slug                                        AS artist_slug,
  a.image_url                                   AS artist_image_url,
  a.origin_country,
  a.is_afrobeats                                AS artist_is_afrobeats,
  ss.total_spotify_streams                      AS total_streams,
  ss.daily_streams,
  to_tsvector('english', s.title || ' ' || a.name) AS search_vector
FROM songs s
JOIN artists a ON a.id = s.artist_id
LEFT JOIN song_stream_summary ss ON ss.song_id = s.id
WHERE s.entity_status = 'canonical'
  AND s.merged_into_song_id IS NULL;

CREATE UNIQUE INDEX idx_sss_unique
  ON song_search_summary (id);
CREATE INDEX idx_sss_search
  ON song_search_summary USING gin(search_vector);
CREATE INDEX idx_sss_streams
  ON song_search_summary (total_streams DESC NULLS LAST);
CREATE INDEX idx_sss_artist
  ON song_search_summary (artist_id);
CREATE INDEX idx_sss_afrobeats
  ON song_search_summary (is_afrobeats, total_streams DESC NULLS LAST);

