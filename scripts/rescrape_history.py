#!/usr/bin/env python3
"""
Re-fetch history for all known idpe values to get correct year_idps
(especially 2021/2022 which were previously corrupted by backfill bug).
Then fetch detail data for newly discovered (year, idp) pairs.
"""

import asyncio
import json
import logging
from pathlib import Path

import aiohttp

from urllib.parse import quote

from scraper import (
    HEADERS, REQUEST_DELAY, MAX_RETRIES, BACKOFF_BASE, CONCURRENCY,
    YEARS, BASE_URL,
    history_url, parse_history_page, parse_detail_page, is_valid_result,
    event_code_primary,
    details_progress_path, save_json, load_json, idpe_map_path,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("rescrape_history")


async def fetch(session, url, sem):
    async with sem:
        for attempt in range(MAX_RETRIES + 1):
            try:
                await asyncio.sleep(REQUEST_DELAY)
                async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 429:
                        delay = BACKOFF_BASE * (2 ** attempt)
                        log.warning(f"429 — waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    resp.raise_for_status()
                    return await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < MAX_RETRIES:
                    delay = BACKOFF_BASE * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    raise
    return ""


async def main():
    map_file = idpe_map_path()
    idpe_map = load_json(map_file)
    log.info(f"Loaded {len(idpe_map)} entries from idpe_map")

    # Collect idpes that need history refresh (missing year_events or missing years)
    to_refresh = []
    for key, entry in idpe_map.items():
        idpe = entry.get("idpe")
        if not idpe or len(idpe) < 10:
            continue
        # Refresh if no year_events (old format) or if we only have recent years
        if "year_events" not in entry:
            to_refresh.append((key, idpe))

    log.info(f"Need to refresh history for {len(to_refresh)} idpe values")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        # Phase 1: Re-fetch all history pages
        batch_size = CONCURRENCY * 2
        updated = 0
        for i in range(0, len(to_refresh), batch_size):
            batch = to_refresh[i:i + batch_size]

            async def fetch_hist(key, idpe):
                url = history_url(idpe)
                html = await fetch(session, url, sem)
                return key, idpe, parse_history_page(html)

            coros = [fetch_hist(k, idpe) for k, idpe in batch]
            results = await asyncio.gather(*coros, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    continue
                key, idpe, history = result
                entry = idpe_map[key]
                year_idps = entry.get("year_idps", {})
                year_events = entry.get("year_events", {})

                new_years = 0
                for h in history:
                    yr_str = str(h["year"])
                    if yr_str not in year_idps:
                        year_idps[yr_str] = h["idp"]
                        new_years += 1
                    year_events[yr_str] = h["event"]

                # Add event codes for years that already had idps (from YEARS list)
                for yr_str in year_idps:
                    yr = int(yr_str)
                    if yr_str not in year_events and yr in YEARS:
                        year_events[yr_str] = event_code_primary(yr)

                entry["year_idps"] = year_idps
                entry["year_events"] = year_events
                if new_years > 0:
                    updated += 1

            if (i // batch_size) % 20 == 0:
                log.info(f"  History: {i + len(batch)}/{len(to_refresh)} processed, {updated} updated")
                save_json(map_file, idpe_map)

        save_json(map_file, idpe_map)
        log.info(f"Phase 1 done: {updated} entries got new year mappings")

        # Phase 2: Fetch detail data for newly discovered (year, idp) pairs
        details_by_year = {}
        for year in YEARS:
            path = details_progress_path(year)
            existing = load_json(path)
            details_by_year[year] = existing if isinstance(existing, dict) else {}

        tasks = []
        for key, entry in idpe_map.items():
            year_idps = entry.get("year_idps", {})
            year_events = entry.get("year_events", {})
            idpe_key = entry.get("idpe") or key
            for yr_str, idp in year_idps.items():
                year = int(yr_str)
                if year not in YEARS:
                    continue
                if idp in details_by_year.get(year, {}):
                    continue
                evt = year_events.get(yr_str, "")
                tasks.append((year, idp, idpe_key, evt))

        log.info(f"Phase 2: {len(tasks)} detail pages to fetch")

        if not tasks:
            log.info("No new details to fetch!")
            return

        completed = 0
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]

            async def fetch_det(idp, year, idpe_key, evt):
                if not evt:
                    evt = event_code_primary(year)
                url = (
                    f"{BASE_URL}?content=detail&fpid=list&pid=list"
                    f"&idp={quote(idp, safe='')}&lang=SE&event={quote(evt, safe='')}"
                    f"&ajax=2&onlycontent=1"
                )
                html = await fetch(session, url, sem)
                data = parse_detail_page(html)
                if not is_valid_result(data):
                    return None
                data["idp"] = idp
                data["ar"] = year
                data["idpe"] = idpe_key
                return data

            coros = [fetch_det(idp, yr, idpe, evt) for yr, idp, idpe, evt in batch]
            results = await asyncio.gather(*coros, return_exceptions=True)

            for (year, idp, idpe_key, _), result in zip(batch, results):
                if isinstance(result, Exception):
                    log.warning(f"Detail error {year} {idp[:20]}: {result}")
                    continue
                if result is not None:
                    details_by_year.setdefault(year, {})[idp] = result
                    completed += 1

            if (i // batch_size) % 10 == 0 or i + batch_size >= len(tasks):
                for year in YEARS:
                    if details_by_year.get(year):
                        save_json(details_progress_path(year), details_by_year[year])
                log.info(f"  Details: {i + len(batch)}/{len(tasks)} processed, {completed} fetched")

        log.info(f"Phase 2 done: {completed} new detail pages fetched")

        log.info("Done! Run build_site_data.py to regenerate site JSON.")


if __name__ == "__main__":
    asyncio.run(main())
