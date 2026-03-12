# Vasaloppet Scraper

Scrapes and visualizes race results from Vasaloppet, Tjejvasan, and Ultravasan.

## Directory Structure

```
vasaloppet-scraper/
├── scripts/              — Python scripts
│   ├── scraper.py        — Main scraper (async, resumable)
│   ├── build_site_data.py — progress/ → site/data/ (JSON export)
│   ├── fetch_weather.py  — Weather data from Open-Meteo
│   ├── generate_sitemap.py — SEO sitemaps
│   ├── fix_cross_year_dupes.py — One-off fix: duplicates
│   ├── rescrape_broken.py — One-off fix: broken splits
│   └── rescrape_history.py — One-off fix: history
├── progress/             — Scraper cache (gitignored)
├── site/                 — Frontend (Cloudflare Pages)
│   ├── index.html        — Single-page app
│   ├── data/             — JSON data per race and year
│   └── functions/        — CF Pages Functions
├── workers/              — Cloudflare Workers (OG image/page)
├── requirements.txt
└── .env
```

## Data Flow

```
scripts/scraper.py → progress/{race}/ → scripts/build_site_data.py → site/data/{race}/
```

## Usage

```bash
# Scrape results
python scripts/scraper.py --race vasaloppet --year 2026

# Export to site/data/
python scripts/build_site_data.py --race vasaloppet

# Generate sitemap
python scripts/generate_sitemap.py

# Fetch weather data
python scripts/fetch_weather.py
```

## Races

- **Vasaloppet** — 90 km, 10 checkpoints
- **Tjejvasan** — 30 km, 5 checkpoints
- **Ultravasan 90** — 90 km, 9 checkpoints
