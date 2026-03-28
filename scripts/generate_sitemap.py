#!/usr/bin/env python3
"""
Generate sitemap index + sub-sitemaps for vasanerd.se.

Includes all skier profile pages from both races.
Splits into multiple sitemaps to stay under the 50,000 URL limit.

Usage: python generate_sitemap.py
"""

import json
import os
from datetime import date
from xml.sax.saxutils import escape

SITE_URL = "https://vasanerd.se"
RACES = ["vasaloppet", "tjejvasan", "ultravasan", "oppet_spar_mandag", "oppet_spar_sondag", "birken", "nsl"]
SITE_DIR = os.path.join(os.path.dirname(__file__), "..", "site")
DATA_DIR = os.path.join(SITE_DIR, "data")
MAX_URLS_PER_SITEMAP = 40000

today = date.today().isoformat()


def load_person_ids():
    """Collect unique idpe values from all race persons.json files."""
    ids = set()
    for race in RACES:
        path = os.path.join(DATA_DIR, race, "persons.json")
        if not os.path.exists(path):
            print(f"  Warning: {path} not found, skipping")
            continue
        with open(path, encoding="utf-8") as f:
            persons = json.load(f)
        print(f"  {race}: {len(persons)} persons")
        ids.update(persons.keys())
    return sorted(ids)


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
    print("Loading person IDs...")
    person_ids = load_person_ids()
    print(f"  Total unique: {len(person_ids)}")

    sitemap_files = []

    # Static pages sitemap
    static = [make_url(f"{SITE_URL}/", "weekly", "1.0")]
    sitemap_files.append(write_sitemap("sitemap-static.xml", static))

    # Person sitemaps (chunked)
    for i in range(0, len(person_ids), MAX_URLS_PER_SITEMAP):
        chunk = person_ids[i : i + MAX_URLS_PER_SITEMAP]
        idx = i // MAX_URLS_PER_SITEMAP + 1
        lines = [make_url(f"{SITE_URL}/p/{idpe}", "yearly", "0.5") for idpe in chunk]
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
