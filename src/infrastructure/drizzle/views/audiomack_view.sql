-- =============================================================
-- AUDIOMACK MATERIALIZED VIEWS
-- =============================================================

-- ─────────────────────────────────────────────────────────────
-- DROP (reverse dependency order)
-- ─────────────────────────────────────────────────────────────
DROP MATERIALIZED VIEW IF EXISTS artist_audiomack_growth_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_audiomack_trending_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS song_audiomack_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS artist_audiomack_summary CASCADE;


-- =============================================================
-- 1. ARTIST AUDIOMACK SUMMARY
-- Latest snapshot per artist
-- =============================================================
CREATE MATERIALIZED VIEW artist_audiomack_summary AS
SELECT DISTINCT ON (aas.artist_id)
  aas.artist_id,
  aas.audiomack_id,
  aas.audiomack_slug,
  aas.snapshot_date,
  aas.total_plays,
  aas.monthly_plays,
  aas.daily_plays,
  aas.followers,
  aas.favorites,
  a.name           AS artist_name,
  a.slug           AS artist_slug,
  a.image_url      AS artist_image_url,
  a.is_afrobeats,
  a.origin_country,
  ROW_NUMBER() OVER (ORDER BY aas.total_plays DESC NULLS LAST) AS audiomack_rank
FROM artist_audiomack_snapshots aas
JOIN artists a ON a.id = aas.artist_id
ORDER BY aas.artist_id, aas.snapshot_date DESC;

CREATE UNIQUE INDEX idx_aasm_unique
  ON artist_audiomack_summary (artist_id);
CREATE INDEX idx_aasm_total_plays
  ON artist_audiomack_summary (total_plays DESC NULLS LAST);
CREATE INDEX idx_aasm_monthly_plays
  ON artist_audiomack_summary (monthly_plays DESC NULLS LAST);
CREATE INDEX idx_aasm_daily_plays
  ON artist_audiomack_summary (daily_plays DESC NULLS LAST);
CREATE INDEX idx_aasm_afrobeats
  ON artist_audiomack_summary (is_afrobeats, total_plays DESC NULLS LAST);
CREATE INDEX idx_aasm_country
  ON artist_audiomack_summary (origin_country, total_plays DESC NULLS LAST);
CREATE INDEX idx_aasm_rank
  ON artist_audiomack_summary (audiomack_rank);
CREATE INDEX idx_aasm_followers
  ON artist_audiomack_summary (followers DESC NULLS LAST);


-- =============================================================
-- 2. SONG AUDIOMACK SUMMARY
-- Latest snapshot per song
-- =============================================================
CREATE MATERIALIZED VIEW song_audiomack_summary AS
SELECT DISTINCT ON (sas.song_id)
  sas.song_id,
  sas.audiomack_id,
  sas.audiomack_url,
  sas.snapshot_date,
  sas.total_plays,
  sas.daily_plays,
  sas.downloads,
  sas.favorites,
  sas.reposts,
  s.title          AS song_title,
  s.slug           AS song_slug,
  s.release_date,
  s.is_afrobeats   AS song_is_afrobeats,
  s.image_url      AS song_image_url,
  a.id             AS artist_id,
  a.name           AS artist_name,
  a.slug           AS artist_slug,
  a.image_url      AS artist_image_url,
  a.is_afrobeats   AS artist_is_afrobeats,
  a.origin_country
FROM song_audiomack_snapshots sas
JOIN songs   s ON s.id = sas.song_id
JOIN artists a ON a.id = s.artist_id
ORDER BY sas.song_id, sas.snapshot_date DESC;

CREATE UNIQUE INDEX idx_sasm_unique
  ON song_audiomack_summary (song_id);
CREATE INDEX idx_sasm_total_plays
  ON song_audiomack_summary (total_plays DESC NULLS LAST);
CREATE INDEX idx_sasm_daily_plays
  ON song_audiomack_summary (daily_plays DESC NULLS LAST);
CREATE INDEX idx_sasm_artist
  ON song_audiomack_summary (artist_id, total_plays DESC NULLS LAST);
CREATE INDEX idx_sasm_afrobeats
  ON song_audiomack_summary (artist_is_afrobeats, total_plays DESC NULLS LAST);
CREATE INDEX idx_sasm_downloads
  ON song_audiomack_summary (downloads DESC NULLS LAST);
CREATE INDEX idx_sasm_release
  ON song_audiomack_summary (release_date DESC NULLS LAST);


-- =============================================================
-- 3. ARTIST AUDIOMACK GROWTH SUMMARY
-- Daily and 7-day play growth per artist per snapshot
-- mirrors artist_growth_summary pattern
-- =============================================================
CREATE MATERIALIZED VIEW artist_audiomack_growth_summary AS
SELECT
  s1.artist_id,
  a.name           AS artist_name,
  a.slug           AS artist_slug,
  a.image_url,
  a.is_afrobeats,
  a.origin_country,
  s1.snapshot_date,
  s1.daily_plays,
  s1.total_plays,
  (s1.daily_plays  - s0.daily_plays)   AS daily_growth,
  (s1.total_plays  - s7.total_plays)   AS growth_7d
FROM artist_audiomack_snapshots s1
JOIN artists a ON a.id = s1.artist_id
LEFT JOIN artist_audiomack_snapshots s0
  ON s1.artist_id    = s0.artist_id
 AND s0.snapshot_date = s1.snapshot_date - INTERVAL '1 day'
LEFT JOIN artist_audiomack_snapshots s7
  ON s1.artist_id    = s7.artist_id
 AND s7.snapshot_date = s1.snapshot_date - INTERVAL '7 days';

CREATE UNIQUE INDEX idx_aagm_unique
  ON artist_audiomack_growth_summary (artist_id, snapshot_date);
CREATE INDEX idx_aagm_daily_growth
  ON artist_audiomack_growth_summary (snapshot_date, daily_growth DESC);
CREATE INDEX idx_aagm_7d_growth
  ON artist_audiomack_growth_summary (snapshot_date, growth_7d DESC);
CREATE INDEX idx_aagm_afrobeats
  ON artist_audiomack_growth_summary (is_afrobeats, snapshot_date, growth_7d DESC);
CREATE INDEX idx_aagm_country
  ON artist_audiomack_growth_summary (origin_country, snapshot_date, growth_7d DESC);


-- =============================================================
-- 4. ARTIST AUDIOMACK TRENDING SUMMARY
-- Momentum score — weighted growth
-- mirrors artist_trending_summary pattern
-- =============================================================
CREATE MATERIALIZED VIEW artist_audiomack_trending_summary AS
SELECT
  ag.*,
  (
    COALESCE(ag.daily_growth, 0) * 0.6 +
    COALESCE(ag.growth_7d,    0) * 0.4
  ) AS momentum_score
FROM artist_audiomack_growth_summary ag;

CREATE UNIQUE INDEX idx_aatm_unique
  ON artist_audiomack_trending_summary (artist_id, snapshot_date);
CREATE INDEX idx_aatm_momentum
  ON artist_audiomack_trending_summary (snapshot_date, momentum_score DESC);
CREATE INDEX idx_aatm_afrobeats
  ON artist_audiomack_trending_summary (is_afrobeats, snapshot_date, momentum_score DESC);
CREATE INDEX idx_aatm_country
  ON artist_audiomack_trending_summary (origin_country, snapshot_date, momentum_score DESC);