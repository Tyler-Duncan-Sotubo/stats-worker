# TooXclusive Stats

Music intelligence engine powering [tooxclusive.com/stats](https://tooxclusive.com/stats) — a fan-facing stats and records platform for Afrobeats and global music data.

---

## What is this?

A StatMuse-style answers engine for music. Fans ask questions like:

- Who has the most streams on Spotify in history?
- Most streamed Afrobeats song of all time?
- Burna Boy career stats
- Fastest song to reach 1 billion streams?

The engine answers with structured, shareable stat cards backed by real data.

---

## Data Sources

| Source                      | Data                                                      |
| --------------------------- | --------------------------------------------------------- |
| Spotify Web API             | Artist metadata, followers, monthly listeners, track info |
| Kworb                       | African daily chart discovery (NG, GH, KE, ZA, UG)        |
| RIAA / BPI / IFPI           | Certifications (gold, platinum, diamond)                  |
| Billboard / Official Charts | Chart history and peak positions                          |
| Grammy / BET / MOBO         | Award records                                             |

---

## Tech Stack

| Layer      | Technology               |
| ---------- | ------------------------ |
| Backend    | NestJS + TypeScript      |
| Database   | PostgreSQL + Drizzle ORM |
| Scraping   | Axios + Cheerio          |
| Scheduling | NestJS Cron              |
| Hosting    | Railway                  |

---

## Project Structure

```bash
src/
  modules/
    discovery/        # Kworb chart scraper — seeds Spotify IDs
    spotify/          # Spotify API metadata + enrichment
    artists/
    songs/
    albums/
    charts/
    awards/
    records/          # Milestone tracking (most streamed, fastest to 1B etc.)
  infrastructure/
    drizzle/          # Schema + migrations
```

---

## Setup

```bash
npm install
```

```env
SPOTIFY_CLIENT_ID=
SPOTIFY_CLIENT_SECRET=
DATABASE_URL=
```

```bash
# development
npm run dev

# production
npm run start:prod
```

---

## Schema Overview

Core tables: `artists`, `songs`, `albums`, `song_features`, `artist_genres`,
`artist_stats_snapshots`, `song_stats_snapshots`, `certifications`,
`chart_entries`, `award_records`, `records`

Stats are captured as daily snapshots to power historical trend queries.
The `records` table drives milestone answers — each row is a queryable record
with a numeric value for ranking and an active flag for tracking when records are broken.

---

## Roadmap

### Phase 1 — Foundation

- [x] Kworb artist discovery (African charts)
- [x] Spotify metadata enrichment
- [x] Drizzle schema

### Phase 2 — Data pipeline

- [ ] Nightly snapshot cron jobs
- [ ] Certification scraper (RIAA, BPI)
- [ ] Chart history ingestion
- [ ] Award records seeding

### Phase 3 — Stats engine

- [ ] Question parser
- [ ] Stat card API
- [ ] tooxclusive.com/stats frontend

---

## Part of TooXclusive

This engine runs as a standalone NestJS service and feeds data to
[tooxclusive.com](https://tooxclusive.com) — the leading Afrobeats editorial platform.
# stats-worker
