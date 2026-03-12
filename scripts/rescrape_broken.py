#!/usr/bin/env python3
"""Rescrape entries with broken splits data (status table parsed instead of real splits)."""

import asyncio
import json
import logging
from pathlib import Path

import aiohttp

from scraper import (
    HEADERS, REQUEST_DELAY, MAX_RETRIES, BACKOFF_BASE, CONCURRENCY,
    OUTPUT_FILE, YEARS, PRIMARY_YEAR,
    detail_ajax_url, parse_detail_page, is_valid_result,
    details_progress_path, save_json, load_json,
    compile_and_save,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("rescrape")

ROOT = Path(__file__).resolve().parent.parent
RESCRAPE_FILE = ROOT / "progress" / "rescrape_ids.json"


async def fetch(session, url, sem):
    async with sem:
        for attempt in range(MAX_RETRIES + 1):
            try:
                await asyncio.sleep(REQUEST_DELAY)
                async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status in (429, 500, 503):
                        delay = BACKOFF_BASE * (2 ** attempt)
                        log.warning(f"HTTP {resp.status} — retry {attempt+1}, wait {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    resp.raise_for_status()
                    return await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(BACKOFF_BASE * (2 ** attempt))
                else:
                    raise
    return ""


async def main():
    broken_ids = json.loads(RESCRAPE_FILE.read_text())
    log.info(f"Rescraping {len(broken_ids)} entries with broken splits...")

    details = load_json(details_progress_path(PRIMARY_YEAR))
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        done = 0
        batch_size = CONCURRENCY * 2
        for i in range(0, len(broken_ids), batch_size):
            batch = broken_ids[i:i + batch_size]

            async def fetch_one(idp):
                url = detail_ajax_url(idp, PRIMARY_YEAR)
                html = await fetch(session, url, sem)
                data = parse_detail_page(html)
                if not is_valid_result(data):
                    return idp, None
                old = details.get(idp, {})
                data["idp"] = idp
                data["ar"] = old.get("ar", PRIMARY_YEAR)
                data["idpe"] = old.get("idpe", idp)
                return idp, data

            results = await asyncio.gather(*[fetch_one(idp) for idp in batch], return_exceptions=True)

            for r in results:
                if isinstance(r, Exception):
                    log.warning(f"Error: {r}")
                    continue
                idp, data = r
                if data:
                    details[idp] = data
                    done += 1

            if (i // batch_size) % 10 == 0:
                log.info(f"  {done}/{len(broken_ids)} rescraped...")
                save_json(details_progress_path(PRIMARY_YEAR), details)

        save_json(details_progress_path(PRIMARY_YEAR), details)
        log.info(f"Done — {done} entries rescraped")

    # Rebuild CSV
    all_results = list(details.values())
    log.info(f"Rebuilding CSV with {len(all_results)} entries...")
    compile_and_save(all_results)


if __name__ == "__main__":
    asyncio.run(main())
