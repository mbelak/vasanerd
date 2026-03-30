#!/usr/bin/env python3
"""
Vasaloppet Multi-year Results Scraper (Robust & Scalable)
Scrapes results from results.vasaloppet.se for Vasaloppet 90km.

Features:
- Pagination over all participants (~14 000/year)
- Async with aiohttp + semaphore-based parallelism
- Resumable: saves progress to the progress/ directory
- Exponential backoff on HTTP 429/500/503
- Cross-year matching via idpe (persistent person ID)
"""

import asyncio
import csv
import json
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path
from typing import Optional, Union
from urllib.parse import parse_qs, quote, urlparse

import aiohttp
from bs4 import BeautifulSoup


# --- Race configs ---
RACE_CONFIGS = {
    "vasaloppet": {
        "display_name": "Vasaloppet",
        "distance_km": 90,
        "event_prefixes": ["VL_HCH8NDMR"],
        "years": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026],
        "checkpoints": [
            "Högsta punkten", "Smågan", "Mångsbodarna", "Risberg",
            "Evertsberg", "Oxberg", "Hökberg", "Eldris",
            "Mora Förvarning", "Mål",
        ],
        "old_event_codes": {
            2008: ["VL_9999991678885900000000F2"],
            2009: ["VL_9999991678885900000000F1"],
            2010: ["VL_9999991678885900000000C9"],
            2011: ["VL_999999167888590000000015"],
            2012: ["VL_000017167888590000000007"],
            2013: ["VL_999999167888590000000065"],
            2014: ["VL_999999167888590000000301"],
            2015: ["VL_9999991678885900000003C8"],
            2016: ["VL_9999991678885A000000048A"],
            2017: ["VL_9999991678885A0000000551"],
            2018: ["VL_9999991678885B00000006B0"],
            2019: ["VL_9999991678885C0000000700"],
            2020: ["VL_999999167888680000000764"],
            2021: ["VLE_00001716788876000000089Z"],
        },
        "history_filter": "Vasaloppet",
        "history_event_pattern": r"VLE?_",  # Matches VL_ and VLE_ (Vasaloppet Elite)
    },
    "vasaloppet_30": {
        "display_name": "Vasaloppet 30",
        "distance_km": 30,
        "event_prefixes": ["VL30_HCH8NDMR"],
        "years": [2023, 2024, 2025, 2026],
        "checkpoints": [
            "Oxberg", "Hökberg", "Eldris",
            "Mora Förvarning", "Mål",
        ],
        "old_event_codes": {},
        "history_filter": "Vasaloppet 30",
        "history_event_pattern": r"VL30[TM]?_",
    },
    "vasaloppet_45": {
        "display_name": "Vasaloppet 45",
        "distance_km": 45,
        "event_prefixes": ["VL45_HCH8NDMR"],
        "years": [2023, 2024, 2025, 2026],
        "checkpoints": [
            "Lillsjön", "Oxberg", "Hökberg", "Eldris",
            "Mora Förvarning", "Mål",
        ],
        "old_event_codes": {},
        "history_filter": "Vasaloppet 45",
        "history_event_pattern": r"VL45[TM]?_",
    },
    "tjejvasan": {
        "display_name": "Tjejvasan",
        "distance_km": 30,
        "event_prefixes": [
            "TVT_HCH8NDMR",  # Competition
            "TVM_HCH8NDMR",  # Recreational
            "TVJ_HCH8NDMR",  # Junior 17-20
        ],
        "years": [2017, 2018, 2019, 2020, 2022, 2023, 2024, 2025, 2026],
        "checkpoints": [
            "Oxberg", "Hökberg", "Eldris",
            "Mora Förvarning", "Mål",
        ],
        "old_event_codes": {
            2017: ["TVT_9999991678885A0000000579", "TVM_9999991678885A0000000579"],
            2018: ["TVT_9999991678885B00000006A7", "TVM_9999991678885B00000006A7"],
            2019: ["TVT_9999991678885C00000006F7", "TVM_9999991678885C00000006F7"],
            2020: ["TVT_99999916788868000000075A", "TVM_99999916788868000000075A"],
            2022: ["TVT_9999991678887600000008EZ", "TVM_9999991678887600000008EZ"],
        },
        "history_filter": "Tjejvasan",
        "history_event_pattern": r"TV[TMJ]?_",  # Matches TVT_, TVM_, TVJ_, TV_
    },
    "ultravasan": {
        "display_name": "Ultravasan 90",
        "distance_km": 90,
        "event_prefixes": ["UL90_HCH8NDMR"],
        "years": [2014, 2015, 2016, 2017, 2018, 2019, 2022, 2023, 2024, 2025],
        "checkpoints": [
            "Högsta punkten", "Smågan", "Mångsbodarna", "Risberg",
            "Evertsberg", "Oxberg", "Hökberg", "Eldris", "Mål",
        ],
        "old_event_codes": {
            2014: ["UL90_000017167888590000000399"],
            2015: ["UL90_9999991678885A00000003FE"],
            2016: ["UL90_9999991678885A00000004CC"],
            2017: ["UL90_9999991678885A0000000621"],
            2018: ["UL90_9999991678885B00000006D4"],
            2019: ["UL90_9999991678885C000000070B"],
            2022: ["UL90_HCH8NDMR2201"],
            2023: ["UL90_HCH8NDMR2301"],
            2024: ["UL90_HCH8NDMR2401"],
            2025: ["UL90_HCH8NDMR2501"],
        },
        "history_filter": "Ultravasan",
        "history_event_pattern": r"UL90_",
    },
    "oppet_spar_mandag": {
        "display_name": "Öppet Spår måndag 90",
        "distance_km": 90,
        "event_prefixes": ["ÖSM9_HCH8NDMR"],
        "years": [2023, 2024, 2025, 2026],
        "checkpoints": [
            "Högsta punkten", "Smågan", "Mångsbodarna", "Risberg",
            "Evertsberg", "Oxberg", "Hökberg", "Eldris",
            "Mora Förvarning", "Mål",
        ],
        "old_event_codes": {2023: "ÖSM_HCH8NDMR2300"},
        "history_filter": "Öppet Spår",
        "history_event_pattern": r"ÖSM9?_",
    },
    "oppet_spar_sondag": {
        "display_name": "Öppet Spår söndag 90",
        "distance_km": 90,
        "event_prefixes": ["ÖSS9_HCH8NDMR"],
        "years": [2024, 2025, 2026],
        "checkpoints": [
            "Högsta punkten", "Smågan", "Mångsbodarna", "Risberg",
            "Evertsberg", "Oxberg", "Hökberg", "Eldris",
            "Mora Förvarning", "Mål",
        ],
        "old_event_codes": {2024: "ÖSS_HCH8NDMR2400"},
        "history_filter": "Öppet Spår",
        "history_event_pattern": r"ÖSS9?_",
    },
    "birken": {
        "display_name": "Birkebeinerrennet",
        "distance_km": 54,
        "base_url": "https://birkebeiner.r.mikatiming.com/",
        "event_prefixes": ["RENN_2EFCHTCB1"],
        "years": [2026],
        "checkpoints": [
            "Skramstadsetra", "Raudfjellet", "Kvarstad", "Midtfjellet",
            "Sjusjøn 1", "Sjusjøn 2", "Sjusjøn 3",
            "Kubruveita", "Forvarsel", "Finish",
        ],
        "history_filter": "Birkebeinerrennet",
        "history_event_pattern": r"RENN_",
        "race_count_field": "antal_lopp",
        "has_merke": True,
    },
    "lofsdalen_epic": {
        "display_name": "Lofsdalen Epic",
        "distance_km": 55,
        "event_prefixes": [],  # EQ Timing, not mikatiming
        "years": [2022, 2023, 2024, 2025, 2026],
        "checkpoints": ["Finish"],
        "history_filter": "",
        "history_event_pattern": "",
    },
    "nsl": {
        "display_name": "Nordenskiöldsloppet",
        "distance_km": 220,
        "event_prefixes": [],  # Neptron API, not mikatiming
        "years": [2017, 2018, 2019, 2021, 2022, 2024, 2025, 2026],
        "checkpoints": [
            "15km", "28km", "41km", "50km", "57km", "70km", "86km",
            "98km", "105km", "113km", "121km", "130km", "141km",
            "155km", "168km", "182km", "195km", "200km", "Finish",
        ],
        "history_filter": "",
        "history_event_pattern": "",
    },
    "engelbrektsloppet": {
        "display_name": "Engelbrektsloppet",
        "distance_km": 60,
        "event_prefixes": [],  # Neptron API, not mikatiming
        "years": [2024, 2025, 2026],
        "checkpoints": [
            "9km", "14km", "21km", "29km", "30km",
            "40km", "44km", "51km", "58km", "Finish",
        ],
        "history_filter": "",
        "history_event_pattern": "",
    },
}

# --- Parse --race argument ---
import argparse
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--race", default="vasaloppet", choices=list(RACE_CONFIGS.keys()))
_args, _ = _parser.parse_known_args()
ACTIVE_RACE = _args.race
_RC = RACE_CONFIGS[ACTIVE_RACE]

# --- Configuration (from race config) ---
YEARS = _RC["years"]
BASE_URL = _RC.get("base_url", "https://results.vasaloppet.se/2026/")
EVENT_PREFIXES = _RC["event_prefixes"]
PRIMARY_YEAR = YEARS[-1]
MAX_PARTICIPANTS = 0  # 0 = all
CONCURRENCY = 20
REQUEST_DELAY = 0.05
BATCH_SAVE_SIZE = 10
MAX_RETRIES = 5
BACKOFF_BASE = 2  # seconds

ROOT = Path(__file__).resolve().parent.parent
PROGRESS_DIR = ROOT / "progress" / ACTIVE_RACE
OUTPUT_FILE = f"{ACTIVE_RACE}_resultat.csv"

HEADERS = {
    "User-Agent": "Vasaloppet-Scraper/2.0 (educational project)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "sv-SE,sv;q=0.9",
}

CHECKPOINTS = _RC["checkpoints"]

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("vasaloppet")

# --- Graceful shutdown ---
_shutdown = False


def _handle_signal(sig, frame):
    global _shutdown
    if _shutdown:
        log.warning("Forced exit")
        sys.exit(1)
    _shutdown = True
    log.warning("Interrupt received — saving progress and exiting...")


signal.signal(signal.SIGINT, _handle_signal)


# --- URL generation ---
def event_codes(year: int) -> list[str]:
    """Return all event codes for a given year (may be multiple, e.g. for Tjejvasan)."""
    old_codes = _RC.get("old_event_codes", {})
    if year in old_codes:
        codes = old_codes[year]
        return codes if isinstance(codes, list) else [codes]
    return [f"{prefix}{year % 100}00" for prefix in EVENT_PREFIXES]


def event_code_primary(year: int) -> str:
    """Return the first (primary) event code for a year."""
    return event_codes(year)[0]


def list_url(year: int, page: int, evt: str) -> str:
    return (
        f"{BASE_URL}?pid=search&event={evt}"
        f"&num_results=100&search_event={evt}"
        f"&page={page}&ajax=2&onlycontent=1"
    )


def detail_url(idp: str, year: int, evt: str = "") -> str:
    ev = evt or event_code_primary(year)
    return (
        f"{BASE_URL}?content=detail&fpid=list&pid=list"
        f"&idp={quote(idp, safe='')}&lang=SE&event={ev}"
    )


def detail_ajax_url(idp: str, year: int, evt: str = "") -> str:
    ev = evt or event_code_primary(year)
    return (
        f"{BASE_URL}?content=detail&fpid=list&pid=list"
        f"&idp={quote(idp, safe='')}&lang=SE&event={ev}"
        f"&ajax=2&onlycontent=1"
    )


def history_url(idpe: str) -> str:
    return (
        f"{BASE_URL}?pid=historic&ajax=2&onlycontent=1"
        f"&idpe={quote(idpe, safe='')}&lang=SE&fpid=list&history=1"
    )


# --- Progress management ---
def load_json(path: Path) -> Union[dict, list]:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {} if path.suffix == ".json" else []


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def list_progress_path(year: int) -> Path:
    return PROGRESS_DIR / f"list_{year}.json"


def idpe_map_path() -> Path:
    return PROGRESS_DIR / "idpe_map.json"


def details_progress_path(year: int) -> Path:
    return PROGRESS_DIR / f"details_{year}.json"


# --- HTTP with backoff ---
async def fetch(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> str:
    """Fetch a URL with exponential backoff and semaphore."""
    async with sem:
        for attempt in range(MAX_RETRIES + 1):
            if _shutdown:
                raise KeyboardInterrupt("Shutdown requested")
            try:
                await asyncio.sleep(REQUEST_DELAY)
                async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status in (429, 500, 503):
                        delay = BACKOFF_BASE * (2 ** attempt)
                        log.warning(f"HTTP {resp.status} — retry {attempt+1}/{MAX_RETRIES}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    if resp.status in (400, 401, 403, 404):
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history,
                            status=resp.status, message=f"HTTP {resp.status} (not retryable)")
                    resp.raise_for_status()
                    return await resp.text()
            except aiohttp.ClientResponseError as e:
                if e.status and e.status < 500:
                    raise  # Not retryable (4xx)
                if attempt < MAX_RETRIES:
                    delay = BACKOFF_BASE * (2 ** attempt)
                    log.warning(f"Request error: {e} — retry {attempt+1}/{MAX_RETRIES}, waiting {delay}s")
                    await asyncio.sleep(delay)
                else:
                    log.error(f"Request failed after {MAX_RETRIES} retries: {url}")
                    raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < MAX_RETRIES:
                    delay = BACKOFF_BASE * (2 ** attempt)
                    log.warning(f"Request error: {e} — retry {attempt+1}/{MAX_RETRIES}, waiting {delay}s")
                    await asyncio.sleep(delay)
                else:
                    log.error(f"Request failed after {MAX_RETRIES} retries: {url}")
                    raise
    raise RuntimeError(f"All {MAX_RETRIES} retries exhausted for: {url}")


# --- Parsing ---
def extract_total_pages(html: str) -> Optional[int]:
    """Parse the highest page number from pagination links."""
    max_page = None
    for m in re.finditer(r"[?&]page=(\d+)", html):
        p = int(m.group(1))
        if max_page is None or p > max_page:
            max_page = p
    return max_page


def extract_participant_ids(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    ids = []
    seen = set()
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "idp=" not in href:
            continue
        parsed = parse_qs(urlparse(href).query)
        if "idp" in parsed:
            idp = parsed["idp"][0]
            if idp not in seen:
                seen.add(idp)
                ids.append(idp)
    return ids


def extract_idpe(html: str) -> Optional[str]:
    m = re.search(r"idpe=([A-Za-z0-9]{8,64})", html)
    return m.group(1) if m else None


def parse_history_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    # Build regex from race config: VL_ for vasaloppet, TV[TMJ]?_ for tjejvasan
    event_pattern = _RC.get("history_event_pattern", r"VL_")
    for li in soup.find_all("li", class_=True):
        classes = " ".join(li.get("class", []))
        event_match = re.search(r"event-(" + event_pattern + r"[A-Za-z0-9]+)", classes)
        if not event_match:
            continue
        evt = event_match.group(1)

        # Extract year from text (more reliable than event code)
        text = li.get_text(separator=" ", strip=True)
        year_match = re.search(r"\b(20\d{2})\b", text)
        if not year_match:
            # Fallback: try to decode from new-style event code
            year_suffix = re.search(r"(\d{4})$", evt)
            if not year_suffix:
                continue
            year_code = int(year_suffix.group(1))
            year = 2000 + year_code // 100
        else:
            year = int(year_match.group(1))

        detail_link = li.find("a", href=re.compile(r"idp="))
        if not detail_link:
            continue
        href = detail_link.get("href", "")
        parsed = parse_qs(urlparse(href).query)
        idp = parsed.get("idp", [None])[0]
        if not idp:
            continue
        place_match = re.search(r"Place\s*(\d+)|Plac\.\s*(\d+)|(\d+)\.\s*plats", text)
        place = ""
        if place_match:
            place = next(g for g in place_match.groups() if g)
        time_match = re.search(r"(\d{2}:\d{2}:\d{2})", text)
        result_time = time_match.group(1) if time_match else ""
        results.append({
            "year": year,
            "idp": idp,
            "event": evt,
            "place": place,
            "time": result_time,
        })
    return results


def parse_detail_page(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    field_mappings = {
        "Namn": "namn", "Name": "namn",
        "Startnummer": "startnummer", "Bib": "startnummer", "Bib Number": "startnummer",
        "Klass": "klass", "Class": "klass", "Age Group": "klass",
        "Startgrupp": "startgrupp", "Start group": "startgrupp", "Start Group": "startgrupp",
        "Lag": "lag", "Team": "lag",
        "Klubb/Stad": "klubb", "Club/City": "klubb",
    }
    result_mappings = {
        "Placering (Klass)": "placering_klass",  # Must come before "Placering"
        "Plac. (Klass)": "placering_klass",
        "Place (Age Group)": "placering_klass",
        "Plac. (Totalt)": "placering_totalt",
        "Place (Total)": "placering_totalt",
        "Placering": "placering", "Place": "placering",
        "Totaltid (Brutto)": "bruttotid", "Totaltid": "bruttotid", "Gross time": "bruttotid",
        "Time Total": "bruttotid",
        "Snitthastighet (km/h)": "snitthastighet",
        "Status": "status", "Race Status": "status",
        "Starttid": "starttid", "Starttime": "starttid",
    }
    all_mappings = {**field_mappings, **result_mappings}

    for el in soup.find_all(class_=re.compile(r"^f-")):
        cn = " ".join(el.get("class", []))
        text = el.get_text(strip=True)
        if "f-__fullname" in cn or "f-name" in cn:
            data["namn"] = text
        elif "f-bib" in cn or "f-start_no" in cn:
            data["startnummer"] = text

    for td in soup.find_all(["td", "th", "dt", "div", "span", "label"]):
        lt = td.get_text(strip=True)
        for label, key in all_mappings.items():
            if lt == label or lt.startswith(label):
                ve = td.find_next_sibling(["td", "dd", "div", "span"])
                if ve:
                    val = ve.get_text(strip=True)
                    if val and val not in ("–", "-"):
                        data[key] = val
                break  # Stop after first match to avoid overwriting

    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            lt = dt.get_text(strip=True)
            for label, key in all_mappings.items():
                if label in lt:
                    val = dd.get_text(strip=True)
                    if val and val not in ("–", "-"):
                        data[key] = val
                    break  # Stop after first match

    history_filter = _RC.get("history_filter", "Vasaloppet")
    race_count_field = _RC.get("race_count_field", "antal_vasalopp")
    for h3 in soup.find_all("h3"):
        if "Historiska" in h3.get_text() or "Historic" in h3.get_text():
            ht = h3.find_next("table")
            if ht:
                for row in ht.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        evt = cells[0].get_text(strip=True)
                        cnt = cells[1].get_text(strip=True)
                        if evt == history_filter and cnt.isdigit():
                            data[race_count_field] = cnt
                        elif evt == "Medaljår" and cnt.isdigit():
                            data["medaljar"] = cnt
            break

    data["mellantider"] = parse_splits(soup)

    # Parse Birkebeiner merke tid (per-class cutoff time)
    if _RC.get("has_merke"):
        for table in soup.find_all("table"):
            text = table.get_text()
            if "Merke tid" not in text:
                continue
            for row in table.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2 and cells[0] == "Merke tid Finish":
                    data["merke_tid"] = cells[1]
            break

    return data


def parse_splits(soup: BeautifulSoup) -> list[dict]:
    splits = []
    split_table = None
    fallback_table = None
    for table in soup.find_all("table"):
        text = table.get_text()
        # Prefer tables with known split headers (most reliable)
        has_header = "Mellantid" in text or "Klockan" in text or "Split Time" in text or "Time Of Day" in text
        cp_count = sum(1 for cp in CHECKPOINTS if cp in text)
        if has_header:
            split_table = table
            break
        elif cp_count >= 2 and fallback_table is None:
            fallback_table = table
    if not split_table:
        split_table = fallback_table
    if not split_table:
        return splits

    headers = []
    thead = split_table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all(["th", "td"])]
    else:
        first = split_table.find("tr")
        if first:
            headers = [th.get_text(strip=True) for th in first.find_all(["th", "td"])]

    tbody = split_table.find("tbody")
    rows = tbody.find_all("tr") if tbody else split_table.find_all("tr")[1:]

    header_map = {
        "Klockan": "klocktid", "Clock": "klocktid", "Time Of Day": "klocktid",
        "Tid": "tid", "Time": "tid",
        "Sträcktid": "stracktid", "Leg time": "stracktid", "Split Time": "stracktid", "Diff": "stracktid",
        "min/km": "min_per_km", "min/ km": "min_per_km", "km/h": "km_per_h",
        "Plac.": "placering", "Place": "placering", "Rank": "placering",
    }

    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        sd = {"kontrollpunkt": cells[0]}
        for i, ct in enumerate(cells[1:], 1):
            if i < len(headers):
                for hk, fn in header_map.items():
                    if hk in headers[i]:
                        sd[fn] = ct
                        break
            else:
                fallback = ["klocktid", "tid", "stracktid", "min_per_km", "km_per_h", "placering"]
                idx = i - 1
                if idx < len(fallback):
                    sd[fallback[idx]] = ct
        if sd.get("kontrollpunkt"):
            splits.append(sd)
    return splits


def _extract_detail_year(html: str) -> Optional[int]:
    """Extract the actual year shown on a detail page.
    Looks for 'År  2026' or 'Year  2026' in the page content."""
    m = re.search(r"(?:År|Year)\s*</\w+>\s*<\w+[^>]*>\s*(20\d{2})", html)
    if m:
        return int(m.group(1))
    # Fallback: look for event heading like 'Vasaloppet 2026' or 'Birkebeinerrennet 2026'
    m2 = re.search(r"(?:Vasaloppet|Tjejvasan|Birkebeinerrennet)\s+(20\d{2})", html)
    if m2:
        return int(m2.group(1))
    return None


def is_valid_result(data: dict) -> bool:
    namn = data.get("namn", "")
    if not namn or namn.endswith("–") or namn == "–":
        return False
    return True


# --- CSV generation ---
def build_csv_row(data: dict) -> dict:
    row = {
        "ar": data.get("ar", ""),
        "idpe": data.get("idpe", ""),
        "idp": data.get("idp", ""),
        "namn": data.get("namn", ""),
        "startnummer": data.get("startnummer", ""),
        "klass": data.get("klass", ""),
        "startgrupp": data.get("startgrupp", ""),
        "lag": data.get("lag", ""),
        "klubb": data.get("klubb", ""),
        "placering": data.get("placering", ""),
        "placering_klass": data.get("placering_klass", ""),
        "placering_totalt": data.get("placering_totalt", ""),
        "bruttotid": data.get("bruttotid", ""),
        "snitthastighet": data.get("snitthastighet", ""),
        "status": data.get("status", ""),
        "starttid": data.get("starttid", ""),
        "antal_vasalopp": data.get("antal_vasalopp", "") or data.get("antal_lopp", ""),
        "medaljar": data.get("medaljar", ""),
        "merke_tid": data.get("merke_tid", ""),
    }
    splits = data.get("mellantider", [])
    split_by_name = {s.get("kontrollpunkt", ""): s for s in splits}
    for cp in CHECKPOINTS:
        s = split_by_name.get(cp, {})
        prefix = (cp.lower().replace(" ", "_")
                  .replace("å", "a").replace("ä", "a").replace("ö", "o")
                  .replace("ø", "o"))
        row[f"{prefix}_klocktid"] = s.get("klocktid", "")
        row[f"{prefix}_tid"] = s.get("tid", "")
        row[f"{prefix}_stracktid"] = s.get("stracktid", "")
        row[f"{prefix}_min_per_km"] = s.get("min_per_km", "")
        row[f"{prefix}_km_per_h"] = s.get("km_per_h", "")
        row[f"{prefix}_placering"] = s.get("placering", "")
    return row


def save_csv(all_results: list[dict], filename: str = OUTPUT_FILE):
    if not all_results:
        log.warning("No results to save!")
        return
    rows = [build_csv_row(r) for r in all_results]
    fieldnames = list(rows[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    years = set(r["ar"] for r in rows)
    log.info(f"Saved {len(rows)} rows to {filename}")
    log.info(f"  Years: {sorted(years)}, Columns: {len(fieldnames)}")


# --- Phase 1: List pages (pagination) ---
async def scrape_all_lists(session: aiohttp.ClientSession, sem: asyncio.Semaphore):
    """Fetch all idps for each year via pagination."""
    all_idps = {}  # year -> [idp, ...]
    all_idp_events = {}  # (year, idp) -> event_code

    for year in YEARS:
        if _shutdown:
            break

        # Load existing progress
        progress_file = list_progress_path(year)
        existing = load_json(progress_file)

        # Progress format: {"ids": [...], "last_page": N, "idp_events": {...}} or legacy format (plain list)
        if isinstance(existing, dict) and "ids" in existing:
            year_ids = list(existing["ids"])
            last_page = existing.get("last_page", len(year_ids) // 100)
            # Restore cached idp_events
            for idp, evt in existing.get("idp_events", {}).items():
                all_idp_events[(year, idp)] = evt
        elif isinstance(existing, list):
            year_ids = list(existing)
            last_page = len(year_ids) // 100
        else:
            year_ids = []
            last_page = 0
        seen = set(year_ids)

        # If we already have enough, skip
        if year_ids and 0 < MAX_PARTICIPANTS <= len(year_ids):
            all_idps[year] = year_ids[:MAX_PARTICIPANTS]
            log.info(f"Phase 1: Loading {len(all_idps[year])} idps from cache for {year} (already >= {MAX_PARTICIPANTS})")
            continue

        # Migration: mark old caches as complete if they look done
        if year_ids and isinstance(existing, dict) and not existing.get("complete"):
            if len(year_ids) >= 100 and existing.get("last_page") == 0:
                existing["complete"] = True
                save_json(progress_file, existing)

        # Skip re-pagination if cache is marked complete
        if year_ids and isinstance(existing, dict) and existing.get("complete"):
            all_idps[year] = year_ids
            log.info(f"Phase 1: Loading {len(year_ids)} idps from cache for {year} (complete)")
            continue

        if year_ids:
            log.info(f"Phase 1: {len(year_ids)} idps in cache for {year}, resuming...")
        else:
            log.info(f"Phase 1: Paginating list pages for {year}...")

        # Iterate over all event codes for this year (e.g. TVT, TVM, TVJ for Tjejvasan)
        evts = event_codes(year)
        for evt in evts:
            if _shutdown:
                break

            page = 1
            total_pages = None
            evt_label = evt.split("_")[0]  # E.g. "TVT", "TVM", "VL"

            while not _shutdown:
                url = list_url(year, page, evt)
                try:
                    html = await fetch(session, url, sem)
                except Exception as e:
                    log.error(f"Could not fetch page {page} for {year}/{evt_label}: {e}")
                    break

                if total_pages is None:
                    total_pages = extract_total_pages(html)
                    if total_pages:
                        log.info(f"  {year}/{evt_label}: {total_pages} pages total")

                ids = extract_participant_ids(html)
                new_ids = [idp for idp in ids if idp not in seen]

                for idp in new_ids:
                    seen.add(idp)
                    year_ids.append(idp)
                    all_idp_events[(year, idp)] = evt

                if total_pages and page >= total_pages:
                    log.info(f"  {year}/{evt_label} page {page}/{total_pages}: done ({len(new_ids)} new, {len(year_ids)} total)")
                    break
                if not total_pages and not ids:
                    log.info(f"  {year}/{evt_label} page {page}: empty → done")
                    break

                if page % 20 == 0:
                    log.info(f"  {year}/{evt_label} page {page}/{total_pages or '?'}: {len(year_ids)} so far...")

                if page % BATCH_SAVE_SIZE == 0:
                    save_json(progress_file, {"ids": year_ids, "last_page": page})

                if 0 < MAX_PARTICIPANTS <= len(year_ids):
                    year_ids = year_ids[:MAX_PARTICIPANTS]
                    log.info(f"  {year}: limited to {MAX_PARTICIPANTS} participants")
                    break

                page += 1

        # Save with idp_events for this year
        year_idp_events = {idp: all_idp_events.get((year, idp), "") for idp in year_ids if all_idp_events.get((year, idp))}
        save_json(progress_file, {"ids": year_ids, "last_page": 0, "idp_events": year_idp_events, "complete": True})
        all_idps[year] = year_ids
        log.info(f"  {year}: {len(year_ids)} participants fetched")

    return all_idps, all_idp_events


# --- Phase 2: idpe extraction ---
async def scrape_idpe_mapping(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    all_idps: dict[int, list[str]],
    all_idp_events: dict[tuple, str] = None,
):
    """Fetch idpe and historical year->idp mappings for each person."""
    map_file = idpe_map_path()
    idpe_map: dict = load_json(map_file)

    # Find which idps we already have
    known_idps = set()
    null_idpe_idps = set()  # idps that need re-scraping (missing idpe)
    # Build reverse map: idp -> idpe_map key
    idp_to_entry_key = {}
    for key, entry in idpe_map.items():
        has_valid_idpe = entry.get("idpe") is not None
        for yr_str, idp in entry.get("year_idps", {}).items():
            if isinstance(idp, str):
                known_idps.add(idp)
                idp_to_entry_key[idp] = key
                if not has_valid_idpe:
                    null_idpe_idps.add(idp)

    # Backfill year_events for existing entries that lack them
    if all_idp_events:
        backfilled = 0
        for key, entry in idpe_map.items():
            if not entry.get("year_events"):
                year_idps = entry.get("year_idps", {})
                year_events = {}
                for yr_str, idp in year_idps.items():
                    evt = all_idp_events.get((int(yr_str), idp))
                    if evt:
                        year_events[yr_str] = evt
                if year_events:
                    entry["year_events"] = year_events
                    backfilled += 1
        if backfilled:
            log.info(f"Phase 2: Backfilled year_events for {backfilled} entries")
            save_json(map_file, idpe_map)

    # Build list of (idp, year) for all that are missing or have null idpe
    todo: list[tuple[str, int]] = []
    seen_idps = set()
    for year in YEARS:
        for idp in all_idps.get(year, []):
            if idp in seen_idps:
                continue
            if idp not in known_idps or idp in null_idpe_idps:
                todo.append((idp, year))
                seen_idps.add(idp)
    if null_idpe_idps:
        log.info(f"Phase 2: {len(null_idpe_idps)} idps with null idpe will be re-scraped")

    total_all = sum(len(v) for v in all_idps.values())
    if not todo:
        log.info(f"Phase 2: All {total_all} idpes already in cache")
        return idpe_map

    log.info(f"Phase 2: Fetching idpe for {len(todo)} participants ({total_all - len(todo)} in cache)...")

    async def _map_one_idpe(idp, source_year):
        """Fetch idpe + history for a participant."""
        try:
            url = detail_url(idp, source_year)
            html = await fetch(session, url, sem)
            idpe = extract_idpe(html)

            if not idpe:
                evt_fallback = (all_idp_events or {}).get((source_year, idp), event_code_primary(source_year))
                return idp, {
                    "idpe": None,
                    "year_idps": {str(source_year): idp},
                    "year_events": {str(source_year): evt_fallback},
                }

            hist_url = history_url(idpe)
            hist_html = await fetch(session, hist_url, sem)
            history = parse_history_page(hist_html)

            year_idps = {str(source_year): idp}
            evt_from_list = (all_idp_events or {}).get((source_year, idp), event_code_primary(source_year))
            year_events = {str(source_year): evt_from_list}
            for h in history:
                year_idps[str(h["year"])] = h["idp"]
                year_events[str(h["year"])] = h["event"]

            return idpe, {
                "idpe": idpe,
                "year_idps": year_idps,
                "year_events": year_events,
            }
        except Exception as e:
            log.warning(f"  ERROR {idp[:20]}: {e}")
            evt_fallback = (all_idp_events or {}).get((source_year, idp), event_code_primary(source_year))
            return idp, {
                "idpe": None,
                "year_idps": {str(source_year): idp},
                "year_events": {str(source_year): evt_fallback},
            }

    processed = 0
    batch_size = CONCURRENCY * 2
    for i in range(0, len(todo), batch_size):
        if _shutdown:
            break
        batch = todo[i:i + batch_size]
        coros = [_map_one_idpe(idp, yr) for idp, yr in batch]
        results = await asyncio.gather(*coros, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                log.warning(f"  Batch error: {result}")
                continue
            key, entry = result

            # If re-scrape yielded a valid idpe, remove old idp-keyed entries
            if entry.get("idpe"):
                for yr_str, idp in list(entry.get("year_idps", {}).items()):
                    old_key = idp_to_entry_key.get(idp)
                    if old_key and old_key != key and old_key in idpe_map:
                        old_entry = idpe_map[old_key]
                        if old_entry.get("idpe") is None:
                            # Merge old data before deleting
                            for oyr, oidp in old_entry.get("year_idps", {}).items():
                                entry["year_idps"].setdefault(oyr, oidp)
                            for oyr, oevt in old_entry.get("year_events", {}).items():
                                entry["year_events"].setdefault(oyr, oevt)
                            del idpe_map[old_key]

            if key in idpe_map and entry.get("idpe"):
                existing = idpe_map[key]
                existing["year_idps"].update(entry["year_idps"])
                existing["year_events"].update(entry.get("year_events", {}))
            else:
                idpe_map[key] = entry

        processed += len(batch)
        if processed % 100 < batch_size or processed <= batch_size:
            log.info(f"  [{processed}/{len(todo)}] idpe mapping...")

        if processed % (BATCH_SAVE_SIZE * batch_size) < batch_size:
            save_json(map_file, idpe_map)

    save_json(map_file, idpe_map)
    log.info(f"Phase 2: Done — {len(idpe_map)} unique persons mapped")
    return idpe_map


# --- Phase 3: Detail data ---
async def fetch_detail(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    idp: str,
    year: int,
    idpe_key: str,
    event_override: str = "",
) -> Optional[dict]:
    """Fetch detail data for a person and a year."""
    if event_override:
        url = (
            f"{BASE_URL}?content=detail&fpid=list&pid=list"
            f"&idp={quote(idp, safe='')}&lang=SE&event={quote(event_override, safe='')}"
            f"&ajax=2&onlycontent=1"
        )
    else:
        url = detail_ajax_url(idp, year)
    try:
        html = await fetch(session, url, sem)
        data = parse_detail_page(html)
        if not is_valid_result(data):
            return None
        # Validate that the returned data actually matches the requested year.
        # results.vasaloppet.se sometimes returns the latest result instead of
        # the requested year, leading to duplicate/misattributed data.
        page_year = _extract_detail_year(html)
        if page_year and page_year != year:
            log.warning(f"Year mismatch {year} {idp[:20]}: page returned year {page_year}, skipping")
            return None
        data["idp"] = idp
        data["ar"] = year
        data["idpe"] = idpe_key
        return data
    except Exception as e:
        log.warning(f"Detail error {year} {idp[:20]}: {e}")
        return None


async def scrape_all_details(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    idpe_map: dict,
):
    """Fetch detail data for all (year, idp) pairs."""
    # Load existing details
    details_by_year: dict[int, dict] = {}
    for year in YEARS:
        path = details_progress_path(year)
        existing = load_json(path)
        # existing is a dict with idp as key
        details_by_year[year] = existing if isinstance(existing, dict) else {}

    # Update idpe in cached details where idpe has changed (null -> valid)
    updated_idpe_count = 0
    for key, entry in idpe_map.items():
        idpe = entry.get("idpe")
        if not idpe:
            continue
        for yr_str, idp in entry.get("year_idps", {}).items():
            year = int(yr_str)
            if year not in YEARS:
                continue
            cached = details_by_year.get(year, {}).get(idp)
            if cached and cached.get("idpe") != idpe:
                cached["idpe"] = idpe
                updated_idpe_count += 1
    if updated_idpe_count:
        log.info(f"Phase 3: Updated idpe for {updated_idpe_count} cached details")

    # Build list of all (year, idp, idpe_key, event) that are missing
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
            valid = event_codes(year)
            if evt not in valid:
                evt = valid[0]
            tasks.append((year, idp, idpe_key, evt))

    if not tasks:
        log.info("Phase 3: All details already in cache")
    else:
        log.info(f"Phase 3: Fetching {len(tasks)} detail pages ({sum(len(d) for d in details_by_year.values())} in cache)...")

        completed = 0
        # Run in batches to allow periodic saving
        batch_size = CONCURRENCY * 2
        for i in range(0, len(tasks), batch_size):
            if _shutdown:
                break

            batch = tasks[i:i + batch_size]
            coros = [
                fetch_detail(session, sem, idp, year, idpe_key, evt)
                for year, idp, idpe_key, evt in batch
            ]
            results = await asyncio.gather(*coros, return_exceptions=True)

            for (year, idp, idpe_key, _evt), result in zip(batch, results):
                if isinstance(result, Exception):
                    log.warning(f"Batch error {year} {idp[:20]}: {result}")
                    continue
                if result is not None:
                    details_by_year.setdefault(year, {})[idp] = result
                    completed += 1

            # Save periodically
            if (i // batch_size) % 5 == 0 or i + batch_size >= len(tasks):
                for year in YEARS:
                    save_json(details_progress_path(year), details_by_year.get(year, {}))

            total_done = completed + sum(
                1 for yr, idp, _, _e in tasks[:i] if idp in details_by_year.get(yr, {})
            )
            if (i // batch_size) % 10 == 0:
                log.info(f"  {completed} new details fetched so far...")

    # Save final results
    for year in YEARS:
        save_json(details_progress_path(year), details_by_year.get(year, {}))

    # Collect all results
    all_results = []
    for year in YEARS:
        for idp, data in details_by_year.get(year, {}).items():
            all_results.append(data)

    log.info(f"Phase 3: Done — {len(all_results)} total result rows")
    return all_results


# --- Phase 4: Compilation ---
def compile_and_save(all_results: list[dict]):
    """Compile results."""
    if not all_results:
        log.warning("No results to compile!")
        return

    # Summary
    by_idpe = {}
    for r in all_results:
        by_idpe.setdefault(r.get("idpe", r.get("idp", "")), []).append(r)
    multi_year = {k: v for k, v in by_idpe.items() if len(v) > 1}

    log.info(f"Summary: {len(all_results)} rows, "
             f"{len(by_idpe)} unique skiers, "
             f"{len(multi_year)} with multiple years")

    # Print top-20
    print(f"\n{'=' * 70}")
    print(f"  CROSS-YEAR SUMMARY — {len(by_idpe)} unique skiers")
    print(f"{'=' * 70}")
    shown = 0
    for idpe, results in sorted(
        by_idpe.items(),
        key=lambda x: min((int(r.get("placering_totalt")) if str(r.get("placering_totalt","")).isdigit() else 9999) for r in x[1]),
    ):
        if shown >= 20:
            print(f"  ... and {len(by_idpe) - 20} more")
            break
        namn = results[0].get("namn", "Unknown")
        years_str = ", ".join(
            f"{r['ar']}: place {r.get('placering_totalt', '?')} ({r.get('bruttotid', '?')})"
            for r in sorted(results, key=lambda x: x["ar"])
        )
        multi = " ***" if len(results) > 1 else ""
        print(f"  {namn:<35} {years_str}{multi}")
        shown += 1


# --- Main ---
async def async_main():
    print("=" * 70)
    print(f"  {_RC['display_name']} Results Scraper v2.0 — {YEARS[0]}–{YEARS[-1]}")
    print(f"  Max participants: {MAX_PARTICIPANTS or 'all'}, "
          f"Parallelism: {CONCURRENCY}, Delay: {REQUEST_DELAY}s")
    print("=" * 70)

    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        # Phase 1: List pages
        all_idps, all_idp_events = await scrape_all_lists(session, sem)
        if _shutdown:
            log.info("Aborted after phase 1 — progress saved")
            return

        total_idps = sum(len(v) for v in all_idps.values())
        if not total_idps:
            log.error(f"No participants found for {YEARS}!")
            return

        # Phase 2: idpe mapping (all years)
        idpe_map = await scrape_idpe_mapping(session, sem, all_idps, all_idp_events)
        if _shutdown:
            log.info("Aborted after phase 2 — progress saved")
            return

        # Phase 3: Details
        all_results = await scrape_all_details(session, sem, idpe_map)
        if _shutdown:
            log.info("Aborted after phase 3 — progress saved")
            return

        # Phase 4: Compilation
        compile_and_save(all_results)


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
