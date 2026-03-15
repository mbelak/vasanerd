#!/usr/bin/env python3
"""Fetch official placements from results list pages and update details cache.

This is faster than re-scraping all detail pages — it fetches ~200 paginated
list pages (men + women + overall) instead of ~14000 individual detail pages.
"""

import asyncio
import json
import logging
import re
import sys
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from scraper import fetch

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S", level=logging.INFO)
log = logging.getLogger(__name__)

BASE_URL = "https://results.vasaloppet.se/2026/"
EVENT = "VL_9VL2600"
YEAR = 2026
CONCURRENCY = 5
DETAILS_PATH = ROOT / "progress" / "vasaloppet" / f"details_{YEAR}.json"


def list_url(page: int, sex: str = "") -> str:
    """Build results list URL with optional sex filter (M/W)."""
    url = (
        f"{BASE_URL}?pid=search&event={EVENT}"
        f"&num_results=100&search_event={EVENT}"
        f"&ranking=time_finish_brutto"
        f"&page={page}&ajax=2&onlycontent=1"
    )
    if sex:
        url += f"&search%5Bsex%5D={sex}"
    return url


def extract_placements(html: str) -> list[tuple[str, str]]:
    """Extract (idp, placement) from a results list page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for li in soup.find_all("li", class_="list-group-item"):
        if "list-group-header" in " ".join(li.get("class", [])):
            continue

        # Extract placement from type-place div
        place_div = li.find("div", class_=re.compile(r"type-place"))
        if not place_div:
            continue
        place = place_div.get_text(strip=True)
        if not place or not place.isdigit():
            continue

        # Extract idp from link
        link = li.find("a", href=True)
        if not link:
            continue
        m = re.search(r"idp=([^&\"]+)", link["href"])
        if not m:
            continue
        idp = m.group(1)

        results.append((idp, place))
    return results


def find_max_page(html: str) -> int:
    """Find the highest page number from pagination links."""
    soup = BeautifulSoup(html, "html.parser")
    max_page = 1
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if a:
            m = re.search(r"page=(\d+)", a["href"])
            if m:
                p = int(m.group(1))
                if p > max_page:
                    max_page = p
    return max_page


async def fetch_all_placements(session: aiohttp.ClientSession, sem: asyncio.Semaphore, sex: str) -> dict[str, str]:
    """Fetch all placements for a gender from paginated list pages."""
    label = sex or "ALL"

    # First page to get max page number
    url = list_url(1, sex)
    html = await fetch(session, url, sem)
    max_page = find_max_page(html)
    results = extract_placements(html)
    placements = {idp: place for idp, place in results}
    log.info(f"  {label}: max page={max_page}, first page has {len(results)} results")

    if max_page <= 1:
        return placements

    # Fetch remaining pages in batches
    for batch_start in range(2, max_page + 1, CONCURRENCY):
        batch_end = min(batch_start + CONCURRENCY, max_page + 1)
        pages = list(range(batch_start, batch_end))
        coros = [fetch(session, list_url(p, sex), sem) for p in pages]
        htmls = await asyncio.gather(*coros, return_exceptions=True)
        for page_html in htmls:
            if isinstance(page_html, Exception):
                log.warning(f"  {label}: fetch error: {page_html}")
                continue
            for idp, place in extract_placements(page_html):
                placements[idp] = place

        if (batch_start - 2) % 20 == 0:
            log.info(f"    {label}: pages {batch_start}-{batch_end - 1} done, {len(placements)} placements")

    return placements


async def main():
    log.info(f"Loading details from {DETAILS_PATH}")
    with open(DETAILS_PATH, "r", encoding="utf-8") as f:
        details = json.load(f)
    log.info(f"  {len(details)} entries")

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        log.info("Fetching men's placements...")
        men_placements = await fetch_all_placements(session, sem, "M")
        log.info(f"  Men: {len(men_placements)} placements")

        log.info("Fetching women's placements...")
        women_placements = await fetch_all_placements(session, sem, "W")
        log.info(f"  Women: {len(women_placements)} placements")

        log.info("Fetching overall placements (both genders)...")
        overall_placements = await fetch_all_placements(session, sem, "")
        log.info(f"  Overall: {len(overall_placements)} placements")

    # Update details with correct placements
    updated_mal = 0
    updated_plac = 0
    updated_total = 0

    for idp, data in details.items():
        # Update placering (men's/women's placement)
        new_place = men_placements.get(idp) or women_placements.get(idp)
        if new_place:
            # Update Mål checkpoint placement
            for mt in data.get("mellantider", []):
                if mt.get("kontrollpunkt") == "Mål":
                    if mt.get("placering") != new_place:
                        mt["placering"] = new_place
                        updated_mal += 1
                    break

            # Update placering field
            if data.get("placering") != new_place:
                data["placering"] = new_place
                updated_plac += 1

        # Update placering_totalt
        new_total = overall_placements.get(idp)
        if new_total and data.get("placering_totalt") != new_total:
            data["placering_totalt"] = new_total
            updated_total += 1

    log.info(f"Updated: {updated_mal} Mål placements, {updated_plac} placering, {updated_total} placering_totalt")

    # Save
    with open(DETAILS_PATH, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False)
    log.info(f"Saved {DETAILS_PATH}")

    # Verify
    for name_prefix in ["Dalbye, Aksel", "Öman, Daniel", "Kardin, Oskar"]:
        for idp, data in details.items():
            if data.get("namn", "").startswith(name_prefix):
                mt = data.get("mellantider", [])
                mal = [m for m in mt if m.get("kontrollpunkt") == "Mål"]
                mal_p = mal[0]["placering"] if mal else "?"
                log.info(f"  {data['namn']}: placering={data.get('placering')}, "
                         f"placering_totalt={data.get('placering_totalt')}, mal_plac={mal_p}")
                break


if __name__ == "__main__":
    asyncio.run(main())
