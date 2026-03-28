#!/usr/bin/env python3
"""
Neptron Timing Scraper for Nordenskiöldsloppet (NSL).

Fetches race results from the Neptron API and produces output compatible
with build_site_data.py (details_{year}.json + idpe_map.json).

Usage:
    python3 scripts/scrape_neptron.py --race nsl
    python3 scripts/scrape_neptron.py --race nsl --year 2026
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time as time_mod
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# --- Neptron event codes per year ---
NEPTRON_EVENTS = {
    2026: {"code": "nsl2026", "base": "https://results.neptron.se"},
    2025: {"code": "nsl2025", "base": "https://archive.neptrontiming.se"},
    2024: {"code": "nsl2024", "base": "https://archive.neptrontiming.se"},
    2022: {"code": "rbnsl2022", "base": "https://archive.neptrontiming.se"},
    2021: {"code": "rbnsl2021", "base": "https://archive.neptrontiming.se"},
    2019: {"code": "redbullnordenskioldsloppet2019", "base": "https://archive.neptrontiming.se"},
    2018: {"code": "nordenskioldsloppet2018", "base": "https://archive.neptrontiming.se"},
    2017: {"code": "redbullnordenskioldsloppet2017", "base": "https://archive.neptrontiming.se"},
    2016: {"code": "nordenskioldsloppet2016", "base": "https://archive.neptrontiming.se"},
}

ALL_YEARS = sorted(NEPTRON_EVENTS.keys())

# --- Canonical checkpoint zone mapping ---
# Maps each year's actual split names to canonical zone names.
# None = drop that checkpoint.
CANONICAL_CP_MAP = {
    2026: {
        "16km": "15km", "29km": "28km", "42km": "41km", "55km": "50km",
        "70km": "70km", "86km": "86km", "98km": "98km", "105km": "105km",
        "113km": "113km", "125km": "130km", "141km": "141km", "155km": "155km",
        "168km": "168km", "182km": "182km", "195km": "195km", "200km": "200km",
    },
    2025: {
        "16km": "15km", "29km": "28km", "42km": "41km", "55km": "50km",
        "70km": "70km", "86km": "86km", "98km": "98km", "105km": "105km",
        "113km": "113km", "125km": "130km", "141km": "141km", "155km": "155km",
        "168km": "168km", "182km": "182km", "195km": "195km", "200km": "200km",
    },
    2024: {
        "15km": "15km", "29km": "28km", "43km": "41km", "56km": "50km",
        "71km": "70km", "86km": "86km", "98km": "98km", "105km": "105km",
        "108km": None, "111km": "113km", "119km": "121km", "131km": "130km",
        "145km": "141km", "159km": "155km", "173km": "168km", "188km": "182km",
        "201km": "200km", "206km": None,
    },
    2022: {
        "14km": "15km", "28km": "28km", "41km": "41km", "47km": "50km",
        "57km": "57km", "76km": "70km", "89km": "86km", "102km": "105km",
        "121km": "121km", "131km": "130km", "137km": "141km", "150km": "155km",
        "164km": "168km", "179km": "182km", "184km": "195km",
    },
    2021: {
        "15km": "15km", "24km": "28km", "38km": "41km", "51km": "50km",
        "57km": "57km", "67km": "70km", "78km": "86km", "92km": "98km",
        "99km": "105km", "110km": "113km", "121km": "121km", "128km": "130km",
        "142km": "141km", "153km": "155km", "163km": "168km", "169km": None,
        "182km": "182km", "196km": "195km", "205km": "200km",
    },
    2019: {
        "14km": "15km", "28km": "28km", "41km": "41km", "48km": "50km",
        "57km": "57km", "75km": "70km", "86km": "86km", "94km": "98km",
        "97km": None, "100km": "105km", "109km": "113km", "121km": "121km",
        "140km": "141km", "150km": "155km", "158km": "168km", "172km": None,
        "186km": "182km", "200km": "195km", "213km": "200km",
    },
    2018: {
        "14km": "15km", "28km": "28km", "41km": "41km", "47km": "50km",
        "61km": "57km", "72km": "70km", "79km": "86km", "87km": None,
        "99km": "98km", "111km": "113km", "119km": "121km", "131km": "130km",
        "137km": "141km", "151km": "155km", "157km": None, "170km": "168km",
        "187km": "182km", "198km": "195km", "212km": "200km",
    },
    2017: {
        "14km": "15km", "28km": "28km", "41km": "41km", "47km": "50km",
        "55km": "57km", "67km": "70km", "79km": "86km", "87km": None,
        "99km": "98km", "111km": "113km", "119km": "121km", "131km": "130km",
        "143km": "141km", "151km": "155km", "157km": None, "170km": "168km",
        "184km": "182km", "198km": "195km", "212km": "200km",
    },
    2016: {
        "22km": "15km", "35km": "28km", "50km": "41km", "65km": "57km",
        "82km": "86km", "99km": "98km", "114km": "113km", "129km": "130km",
        "142km": "141km", "164km": "168km", "176km": "182km", "184km": "195km",
    },
}


def generate_idpe(last_name: str, first_name: str, yob: int) -> str:
    """Generate a deterministic persistent person ID from name + year of birth."""
    key = f"{last_name}_{first_name}_{yob}".lower().strip()
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:16].upper()


def normalize_time(t: str) -> str:
    """Normalize Neptron time strings to HH:MM:SS format.

    Handles formats like:
    - "12:15:25.3" -> "12:15:25"
    - "1.02:30:00" -> "26:30:00" (day prefix)
    - "57:06.6" -> "00:57:06" (MM:SS.d)
    - "" or None -> ""
    """
    if not t:
        return ""

    # Strip fractional seconds
    t = re.sub(r"\.\d+$", "", t.strip())

    # Handle day prefix: "D.HH:MM:SS"
    if "." in t:
        parts = t.split(".", 1)
        try:
            days = int(parts[0])
            rest = parts[1]
            hms = rest.split(":")
            if len(hms) == 3:
                h = int(hms[0]) + days * 24
                return f"{h:02d}:{hms[1]}:{hms[2]}"
        except (ValueError, IndexError):
            pass

    parts = t.split(":")
    if len(parts) == 2:
        # MM:SS format -> HH:MM:SS
        return f"00:{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    if len(parts) == 3:
        return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:{parts[2].zfill(2)}"

    return t


def transform_result(result: dict, year: int, cp_map: dict) -> tuple[str, dict]:
    """Transform a Neptron API result into the Vasanerd details format.

    Returns (idp, data_dict).
    """
    start_no = str(result.get("startNo", ""))
    idp = f"NSL{year}_{start_no}"

    first_name = (result.get("firstName") or "").strip()
    last_name = (result.get("lastName") or "").strip()
    yob = result.get("yoB") or 0

    # Country code from flag or country field
    country = (result.get("flag") or result.get("country") or "").strip().upper()
    if not country or country == "0":
        country = "UNK"

    # Format name as "Last, First (COUNTRY)" to match existing convention
    namn = f"{last_name}, {first_name} ({country})"

    # Gender → klass: D for female, H for male (is_female checks startswith("D"))
    gender = (result.get("gender") or "").upper()
    category = result.get("category") or ""
    if gender == "F" or category == "Women":
        klass = "D"
    else:
        klass = "H"

    # Status mapping
    status_code = result.get("statusCode") or ""
    status_map = {
        "FIN": "Finished",
        "DNF": "DNF",
        "DNS": "Did not start",
        "DSQ": "Disqualified",
        "OTL": "DNF",  # Over time limit
    }
    status = status_map.get(status_code, status_code)

    # Time fields — only set bruttotid for finishers (DNF runners have partial times)
    bruttotid = ""
    snitthastighet = ""
    if status_code == "FIN":
        bruttotid = normalize_time(result.get("time") or result.get("totalTime") or "")
        speed = result.get("speed")
        snitthastighet = f"{speed:.2f}" if isinstance(speed, (int, float)) and speed > 0 else ""

    # Build mellantider from splits using canonical checkpoint mapping
    mellantider = []
    for split in result.get("splits") or []:
        split_name = split.get("splitName", "")
        if split_name == "Finish":
            canonical = "Finish"
        else:
            canonical = cp_map.get(split_name)
            if canonical is None:
                continue  # Drop this checkpoint

        leg_speed = split.get("legSpeed")
        km_per_h = f"{leg_speed:.2f}" if isinstance(leg_speed, (int, float)) and leg_speed > 0 else ""

        mellantider.append({
            "kontrollpunkt": canonical,
            "klocktid": normalize_time(split.get("wallTime") or ""),
            "tid": normalize_time(split.get("time") or ""),
            "stracktid": normalize_time(split.get("legSplit") or split.get("split") or ""),
            "km_per_h": km_per_h,
            "placering": str(split.get("placeByGender") or split.get("placeByRace") or ""),
        })

    idpe = generate_idpe(last_name, first_name, yob)

    data = {
        "namn": namn,
        "startnummer": start_no,
        "klubb": result.get("club") or "",
        "klass": klass,
        "startgrupp": "",
        "lag": result.get("team") or "",
        "placering": str(result.get("placeByGender") or ""),
        "placering_totalt": str(result.get("placeByRace") or ""),
        "bruttotid": bruttotid,
        "snitthastighet": snitthastighet,
        "status": status,
        "starttid": result.get("startTime") or "",
        "mellantider": mellantider,
        "idp": idp,
        "idpe": idpe,
        "ar": str(year),
    }

    return idp, data


def fetch_year(year: int, progress_dir: Path) -> dict:
    """Fetch all results for a given year from the Neptron API."""
    event = NEPTRON_EVENTS[year]
    url = f"{event['base']}/webapi/{event['code']}/results"

    details_file = progress_dir / f"details_{year}.json"
    raw_file = progress_dir / f"raw_{year}.json"

    # Check for existing data
    if details_file.exists():
        with open(details_file) as f:
            existing = json.load(f)
        log.info(f"  {year}: already have {len(existing)} entries, skipping")
        return existing

    log.info(f"  {year}: fetching from {url}")

    # Fetch with retry
    for attempt in range(3):
        try:
            resp = requests.get(url, params={"pageSize": 1000}, timeout=60)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == 2:
                log.error(f"  {year}: failed after 3 attempts: {e}")
                return {}
            wait = 2 ** (attempt + 1)
            log.warning(f"  {year}: attempt {attempt + 1} failed ({e}), retrying in {wait}s")
            time_mod.sleep(wait)

    api_data = resp.json()
    results = api_data.get("results", [])
    log.info(f"  {year}: got {len(results)} results (numResults={api_data.get('numResults')})")

    # Save raw response as backup
    with open(raw_file, "w") as f:
        json.dump(api_data, f)

    # Transform to Vasanerd format
    cp_map = CANONICAL_CP_MAP.get(year, {})
    details = {}
    for result in results:
        idp, data = transform_result(result, year, cp_map)
        details[idp] = data

    # Save details
    with open(details_file, "w") as f:
        json.dump(details, f, ensure_ascii=False, indent=1)
    log.info(f"  {year}: saved {len(details)} entries to {details_file.name}")

    return details


def build_idpe_map(all_details: dict[int, dict], progress_dir: Path) -> dict:
    """Build cross-year person mapping from all years' details."""
    idpe_map = {}

    for year, details in sorted(all_details.items()):
        for idp, data in details.items():
            idpe = data.get("idpe", "")
            if not idpe:
                continue

            if idpe not in idpe_map:
                idpe_map[idpe] = {
                    "idpe": idpe,
                    "namn": data.get("namn", ""),
                    "year_idps": {},
                    "year_events": {},
                }

            idpe_map[idpe]["year_idps"][str(year)] = idp
            event = NEPTRON_EVENTS.get(year, {})
            idpe_map[idpe]["year_events"][str(year)] = event.get("code", "")

    # Save
    map_file = progress_dir / "idpe_map.json"
    with open(map_file, "w") as f:
        json.dump(idpe_map, f, ensure_ascii=False, indent=1)
    log.info(f"Saved idpe_map with {len(idpe_map)} unique persons")

    return idpe_map


def main():
    parser = argparse.ArgumentParser(description="Scrape Nordenskiöldsloppet from Neptron API")
    parser.add_argument("--race", default="nsl", help="Race key (default: nsl)")
    parser.add_argument("--year", type=int, help="Scrape only this year")
    args = parser.parse_args()

    progress_dir = ROOT / "progress" / args.race
    progress_dir.mkdir(parents=True, exist_ok=True)

    years = [args.year] if args.year else ALL_YEARS

    log.info(f"Scraping {args.race} for years: {years}")

    all_details = {}
    for year in years:
        if year not in NEPTRON_EVENTS:
            log.warning(f"  {year}: no Neptron event configured, skipping")
            continue
        details = fetch_year(year, progress_dir)
        all_details[year] = details

    # Build idpe map from all years (load existing years not in current scrape)
    for year in ALL_YEARS:
        if year not in all_details:
            details_file = progress_dir / f"details_{year}.json"
            if details_file.exists():
                with open(details_file) as f:
                    all_details[year] = json.load(f)

    build_idpe_map(all_details, progress_dir)

    # Summary
    total = sum(len(d) for d in all_details.values())
    log.info(f"Done! Total entries across {len(all_details)} years: {total}")


if __name__ == "__main__":
    main()
