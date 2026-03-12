# Vasanerd

Scrapes, processes, and visualizes race results from Vasaloppet, Tjejvasan, Ultravasan, and Öppet Spår. The frontend is a single-page app hosted on Cloudflare Pages with OG image generation via Cloudflare Workers.

## Races

- **Vasaloppet** — 90 km, 10 checkpoints (2015–2026)
- **Tjejvasan** — 30 km, 5 checkpoints (2017–2026)
- **Ultravasan 90** — 90 km, 9 checkpoints (2014–2025)
- **Öppet Spår måndag 90** — 90 km, 10 checkpoints (2026)
- **Öppet Spår söndag 90** — 90 km, 10 checkpoints (2026)

## Architecture

```
scripts/scraper.py → progress/{race}/ → scripts/build_site_data.py → site/data/{race}/
```

### Data pipeline

1. **Scraper** (`scripts/scraper.py`) — Async 4-phase pipeline: paginate participant lists → extract IDs + history → fetch detail pages → compile results. Progress saved per-phase for resumability.
2. **Build** (`scripts/build_site_data.py`) — Flattens scraped data into compressed per-year JSON, person shards (256 hex-bucketed files), search index, and aggregated year statistics.
3. **Weather** (`scripts/fetch_weather.py`) — Fetches historical race-day weather from Open-Meteo.
4. **Sitemap** (`scripts/generate_sitemap.py`) — Generates XML sitemaps for SEO.

### Frontend

Single-file SPA (`site/index.html`) with inline CSS/JS. Uses Chart.js for visualizations. Data loaded lazily per year. 13 tabs covering overview, individual results, comparisons, pacing analysis, and more.

### Edge functions

- `workers/og-image/` — Generates OG images on-demand at `vasanerd.se/og/*`
- `workers/og-page/` — Serves OG meta tags for social crawlers at `vasanerd.se/p/*`
- `site/functions/p/[id].js` — Cloudflare Pages Function for person-page routing

## Directory structure

```
vasanerd/
├── scripts/                — Python data pipeline
│   ├── scraper.py          — Main scraper (async, resumable)
│   ├── build_site_data.py  — Export progress/ → site/data/
│   ├── fetch_weather.py    — Weather data from Open-Meteo
│   └── generate_sitemap.py — SEO sitemaps
├── progress/               — Scraper cache (gitignored)
├── site/                   — Frontend (Cloudflare Pages)
│   ├── index.html          — Single-page app
│   ├── data/               — JSON data per race and year
│   └── functions/          — CF Pages Functions
├── workers/                — Cloudflare Workers
│   ├── og-image/           — OG image generation
│   └── og-page/            — OG meta tag serving
├── .github/workflows/      — CI/CD (deploys on push to main)
└── requirements.txt
```

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Scrape results (async, resumable)
python scripts/scraper.py --race vasaloppet --year 2026
python scripts/scraper.py --race tjejvasan
python scripts/scraper.py --race ultravasan
python scripts/scraper.py --race oppet_spar_mandag
python scripts/scraper.py --race oppet_spar_sondag

# Export to site/data/
python scripts/build_site_data.py --race vasaloppet

# Fetch weather data
python scripts/fetch_weather.py

# Generate sitemap
python scripts/generate_sitemap.py
```

## Deployment

Pushing to `main` triggers a GitHub Actions workflow that deploys:
- **Cloudflare Pages** — `site/` directory
- **Cloudflare Workers** — `workers/og-image/` and `workers/og-page/`

Requires `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` as repository secrets.
