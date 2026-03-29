#!/usr/bin/env python3
"""
Generate sitemap index + sub-sitemaps for vasanerd.se.

Includes all skier profile pages from both races.
Splits into multiple sitemaps to stay under the 50,000 URL limit.

Usage: python generate_sitemap.py
"""

import json
import os
import re
import unicodedata
from datetime import date
from xml.sax.saxutils import escape

SITE_URL = "https://vasanerd.se"
SITE_DIR = os.path.join(os.path.dirname(__file__), "..", "site")
DATA_DIR = os.path.join(SITE_DIR, "data")
MAX_URLS_PER_SITEMAP = 40000

today = date.today().isoformat()


def name_slug(namn):
    """Generate URL slug from name, matching the frontend nameSlug() function."""
    # "Last, First (NAT)" -> "First Last"
    clean = re.sub(r"\s*\(\w+\)", "", namn).strip()
    parts = clean.split(",", 1)
    full = (parts[1].strip() + " " + parts[0].strip()) if len(parts) == 2 else clean
    # Normalize accents and slugify
    slug = unicodedata.normalize("NFKD", full)
    slug = "".join(c for c in slug if not unicodedata.combining(c))
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug.lower()).strip("_")
    return slug


def load_person_urls():
    """Load global_persons.json and generate URL paths for each person."""
    path = os.path.join(DATA_DIR, "global_persons.json")
    if not os.path.exists(path):
        print(f"  Error: {path} not found")
        return []
    with open(path, encoding="utf-8") as f:
        persons = json.load(f)
    print(f"  global_persons.json: {len(persons)} persons")
    urls = []
    for p in persons:
        # Use the idpe from the race with most years
        best = max(p["r"], key=lambda r: len(r["y"]))
        slug = name_slug(p["n"])
        if slug:
            urls.append(f"/{slug}/{best['i']}")
    return sorted(set(urls))


def make_url(loc, changefreq, priority):
    return (
        f"  <url><loc>{escape(loc)}</loc><lastmod>{today}</lastmod>"
        f"<changefreq>{changefreq}</changefreq><priority>{priority}</priority></url>"
    )


def write_sitemap(filename, url_lines):
    path = os.path.join(SITE_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        f.write("\n".join(url_lines))
        f.write("\n</urlset>\n")
    print(f"  {filename}: {len(url_lines)} URLs")
    return filename


def main():
    print("Loading person URLs...")
    person_urls = load_person_urls()
    print(f"  Total unique URLs: {len(person_urls)}")

    sitemap_files = []

    # Static pages sitemap
    static = [make_url(f"{SITE_URL}/", "weekly", "1.0")]
    sitemap_files.append(write_sitemap("sitemap-static.xml", static))

    # Person sitemaps (chunked)
    for i in range(0, len(person_urls), MAX_URLS_PER_SITEMAP):
        chunk = person_urls[i : i + MAX_URLS_PER_SITEMAP]
        idx = i // MAX_URLS_PER_SITEMAP + 1
        lines = [make_url(f"{SITE_URL}{url}", "yearly", "0.5") for url in chunk]
        sitemap_files.append(write_sitemap(f"sitemap-persons-{idx}.xml", lines))

    # Sitemap index
    index_path = os.path.join(SITE_DIR, "sitemap.xml")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for sf in sitemap_files:
            f.write(f"  <sitemap><loc>{SITE_URL}/{sf}</loc><lastmod>{today}</lastmod></sitemap>\n")
        f.write("</sitemapindex>\n")

    print(f"sitemap.xml: index with {len(sitemap_files)} sub-sitemaps")


if __name__ == "__main__":
    main()
