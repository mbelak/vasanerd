# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Scrape results (async, resumable — saves to progress/{race}/)
python3 scripts/scraper.py --race vasaloppet --year 2026
python3 scripts/scraper.py --race tjejvasan
python3 scripts/scraper.py --race ultravasan
python3 scripts/scraper.py --race oppet_spar_mandag
python3 scripts/scraper.py --race oppet_spar_sondag

# Export progress data to site/data/{race}/ JSON files
python3 scripts/build_site_data.py --race vasaloppet

# Generate XML sitemaps
python3 scripts/generate_sitemap.py

# Fetch weather data from Open-Meteo
python3 scripts/fetch_weather.py

# Deploy Cloudflare Workers
cd workers/og-image && npx wrangler deploy
cd workers/og-page && npx wrangler deploy
```

Dependencies: `pip install -r requirements.txt` (aiohttp, beautifulsoup4).

## Architecture

**Data pipeline:** `scripts/scraper.py → progress/{race}/ → scripts/build_site_data.py → site/data/{race}/`

Five races share one codebase, selected via `--race`: vasaloppet (90km, 10 checkpoints), tjejvasan (30km, 5 checkpoints), ultravasan (90km, 9 checkpoints), oppet_spar_mandag (90km, 10 checkpoints), oppet_spar_sondag (90km, 10 checkpoints). Race configs live in `RACE_CONFIGS` dict in `scripts/scraper.py` and are imported by other scripts.

### Scraper (scripts/scraper.py)
4-phase async pipeline: (1) paginate participant lists → (2) extract idpe + history → (3) fetch detail pages → (4) compile results. Progress is saved per-phase to `progress/{race}/` for resumability. Uses semaphore-based concurrency with exponential backoff.

### Build (scripts/build_site_data.py)
Reads `progress/{race}/details_{year}.json`, flattens via `build_csv_row()`, builds compressed per-year JSON with a keymap (short aliases), persons.json (search index), person shards (256 hex-bucketed files for fast lookup), and year_stats.json (aggregated statistics).

### Frontend (site/index.html)
Single-file SPA (~400KB) with inline CSS and JS. Uses Chart.js 4 via CDN. Data loaded lazily per year via `ensureYear()`. Multi-race support via `RACE_CONFIGS`, `currentRace`, and `switchRace()`. 13 tabs rendered on-demand with `_viewRendered` tracking.

### Edge Functions
- `workers/og-image/` — Cloudflare Worker at `vasanerd.se/og/*`, generates OG images on-demand
- `workers/og-page/` — Cloudflare Worker at `vasanerd.se/p/*`, serves OG meta tags for social crawlers, redirects browsers to SPA `/#person-{idpe}`
- `site/functions/p/[id].js` — Cloudflare Pages Function, same person-page routing with persons.json lookup

## Key Patterns

- All scripts use `ROOT = Path(__file__).resolve().parent.parent` for paths relative to repo root
- `from scraper import RACE_CONFIGS, build_csv_row` — scripts import shared config from scraper.py
- Data files use key compression: `build_keymap()` maps long column names to short aliases (a, b, c...), stored in `_keymap.json`
- Person identification: `idpe` is persistent across years, `idp` is year-specific
- Checkpoint data columns follow pattern: `{cp_prefix}_{tid,klocktid,stracktid,km_per_h,placering}`

## Data Integrity Rules (MANDATORY)

Every entry in `details_{year}.json` **MUST** have:
1. **`idpe`** — persistent person ID. Without this, the person won't appear in search or person views. The scraper sets this in phase 2 via `extract_idpe()` from the detail page HTML. If you add entries manually or via scripts, you MUST fetch and set the idpe.
2. **`idp`** — year-specific participant ID (used as the dict key in details files).
3. **`ar`** — the year as an integer.

**Never modify `details_{year}.json` without preserving all existing entries.** If you need to re-scrape or update placements, update fields in-place — do not delete and recreate the file (the scraper's rate limiting makes full re-scrapes unreliable).

**After adding new entries to details:** always run `scripts/update_placements.py` (if available) and then `scripts/build_site_data.py --race {race}` to rebuild the site data. The build reads details + idpe_map to generate persons.json, person shards, and per-year JSON. Missing idpe = missing from search.

## Adding a New Year for a Race

Step-by-step checklist for importing a year's data (e.g. 2024 for oppet_spar_sondag):

### 1. Find the correct event code
The event code determines which data the results API returns. Verify it BEFORE scraping:
```bash
# Test the list page with ajax — check that the title shows the correct year
curl -s "https://results.vasaloppet.se/2026/?pid=search&event=EVENT_CODE&num_results=1&ajax=2&onlycontent=1" | head -5
```
For historical years, the event code may differ from the current prefix pattern. If so, add it to `old_event_codes` in RACE_CONFIGS. The `event_prefixes` pattern generates codes like `PREFIX{YY}00`, but old years may use completely different codes (e.g. `ÖSS_` vs `ÖSS9_`).

### 2. Update `scripts/scraper.py`
- Add the year to `RACE_CONFIGS["{race}"]["years"]`
- If the event code differs from the prefix pattern, add `old_event_codes: {YEAR: "CODE"}`

### 3. Clean stale progress data (if re-scraping)
If a previous scrape attempt left bad data:
```bash
rm progress/{race}/list_{year}.json progress/{race}/details_{year}.json
```
Also clean `idpe_map.json` if it has wrong year_idps/year_events (see scraper.py phase 2 — bogus entries come from list pages scraped with wrong event codes).

### 4. Run the scraper
```bash
python3 scripts/scraper.py --race {race} --year {year}
```
Verify `details_{year}.json` has the expected number of entries (~finisher count).

### 5. Update `site/index.html`
- Add the year to `RACE_CONFIGS.{race}.years`
- Add seeding time cutoffs to `STARTLED_TIME_CUTOFFS.{race}`. These are the checkpoint times that define seeding group boundaries (Led 1–10 for 90km races). Find the official seeding cutoff times from Vasaloppet's website or results for that year and convert them to elapsed-time strings (`HH:MM:SS`). The array has one entry per checkpoint. Use `null` for checkpoints that don't have a cutoff. If official times are unavailable, copy from an adjacent year as a temporary placeholder and note it needs updating.

### 6. Update `scripts/fetch_weather.py`
- Add `{year}: "{YYYY-MM-DD}"` with the race date
- Run `python3 scripts/fetch_weather.py`

### 7. Build site data
```bash
python3 scripts/build_site_data.py --race {race}
```

### 8. Generate sitemaps
```bash
python3 scripts/generate_sitemap.py
```

### 9. Verify
- `details_{year}.json` has expected entry count
- `site/data/{race}/{year}.json` has reasonable size
- Top finishers look correct in the summary output

### Common pitfall: wrong event codes
The results API silently returns data for the CURRENT year if an event code is invalid or maps to a different year. This means list pages can return 15K+ results that look correct but are actually from the wrong year. Always verify the year in the returned data by checking the ajax response title or detail pages.

## Git

Do not add `Co-Authored-By` lines to commit messages.

## Language

Communicate with the user in Swedish. Code, documentation, and **all UI text** are in English. Never use Swedish in the UI — labels, buttons, descriptions, tooltips, error messages, etc. must all be in English.
