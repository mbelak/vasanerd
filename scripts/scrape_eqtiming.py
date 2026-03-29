#!/usr/bin/env python3
"""
EQ Timing Scraper for Lofsdalen Epic.

Fetches race results from the EQ Timing CSV API and produces output compatible
with build_site_data.py (details_{year}.json + idpe_map.json).

Usage:
    python3 scripts/scrape_eqtiming.py --race lofsdalen_epic
    python3 scripts/scrape_eqtiming.py --race lofsdalen_epic --year 2026
"""

import argparse
import csv
import hashlib
import io
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

# --- EQ Timing event IDs per year ---
EQTIMING_EVENTS = {
    2026: {"event_id": 79840, "stage_55": "Epic 55"},
    2025: {"event_id": 73158, "stage_55": "Epic 55"},
    2024: {"event_id": 68820, "stage_55": "Epic 55"},
    2023: {"event_id": 65263, "stage_55": "Epic 55"},
    2022: {"event_id": 59450, "stage_55": "Pilgrimsloppet Epic"},
}

ALL_YEARS = sorted(EQTIMING_EVENTS.keys())

API_BASE = "https://live.eqtiming.com/api"


def generate_idpe(surname: str, firstname: str, club: str) -> str:
    """Generate a deterministic persistent person ID from name + club."""
    key = f"{surname}_{firstname}_{club}".lower().strip()
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:16].upper()


def normalize_time(t: str) -> str:
    """Normalize EQ Timing time format to HH:MM:SS.

    Handles: "2:32:42.9" -> "02:32:42", "02:32:42.770" -> "02:32:42"
    """
    if not t:
        return ""
    # Strip fractional seconds
    t = re.sub(r"\.\d+$", "", t.strip())
    parts = t.split(":")
    if len(parts) == 3:
        return f"{int(parts[0]):02d}:{parts[1].zfill(2)}:{parts[2].zfill(2)}"
    if len(parts) == 2:
        return f"00:{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    return t


def parse_csv_results(csv_text: str, year: int, stage_filter: str) -> dict:
    """Parse EQ Timing CSV and return details dict."""
    # Strip BOM
    csv_text = csv_text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";", quotechar='"')

    details = {}
    for row in reader:
        stage = (row.get("Stage") or "").strip()
        if stage != stage_filter:
            continue

        start_no = (row.get("Startnumber") or "").strip()
        if not start_no:
            continue

        firstname = (row.get("Firstname") or "").strip()
        surname = (row.get("Surname") or "").strip()
        gender = (row.get("Gender") or "").strip().upper()
        nat = (row.get("Nat") or "").strip().upper()
        club = (row.get("Club") or "").strip()
        klass_raw = (row.get("Class") or "").strip()

        # Format name as "Surname, Firstname (NAT)"
        if not nat or nat == "0":
            nat = "UNK"
        namn = f"{surname}, {firstname} ({nat})"

        # Gender -> klass: D for female, H for male
        klass = "D" if gender == "F" else "H"

        # Time
        time_str = (row.get("Total Time") or "").strip()
        bruttotid = normalize_time(time_str) if time_str and time_str != "0:00.0" else ""

        # Status: if no time, check if DNS/DNF
        if bruttotid:
            status = "Finished"
        else:
            status = "DNF"
            bruttotid = ""

        # Placements
        rank_gender = (row.get("Rank Gender") or "").strip()
        rank_total = (row.get("Rank Total") or "").strip()

        # Speed (approximate from time and 55km distance)
        snitthastighet = ""
        if bruttotid:
            parts = bruttotid.split(":")
            if len(parts) == 3:
                total_h = int(parts[0]) + int(parts[1]) / 60 + int(parts[2]) / 3600
                if total_h > 0:
                    snitthastighet = f"{55 / total_h:.2f}"

        idp = f"LE{year}_{start_no}"
        idpe = generate_idpe(surname, firstname, club)

        details[idp] = {
            "namn": namn,
            "startnummer": start_no,
            "klubb": club,
            "klass": klass,
            "startgrupp": klass_raw,
            "lag": "",
            "placering": rank_gender,
            "placering_totalt": rank_total,
            "bruttotid": bruttotid,
            "snitthastighet": snitthastighet,
            "status": status,
            "starttid": "",
            "mellantider": [],
            "idp": idp,
            "idpe": idpe,
            "ar": str(year),
        }

    return details


def fetch_year(year: int, progress_dir: Path) -> dict:
    """Fetch all results for a given year from EQ Timing."""
    event = EQTIMING_EVENTS[year]
    event_id = event["event_id"]
    stage_filter = event["stage_55"]

    details_file = progress_dir / f"details_{year}.json"

    if details_file.exists():
        with open(details_file) as f:
            existing = json.load(f)
        log.info(f"  {year}: already have {len(existing)} entries, skipping")
        return existing

    url = f"{API_BASE}/Report/220?eventId={event_id}"
    log.info(f"  {year}: fetching from {url}")

    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == 2:
                log.error(f"  {year}: failed after 3 attempts: {e}")
                return {}
            wait = 2 ** (attempt + 1)
            log.warning(f"  {year}: attempt {attempt + 1} failed ({e}), retrying in {wait}s")
            time_mod.sleep(wait)

    csv_text = resp.text
    details = parse_csv_results(csv_text, year, stage_filter)
    log.info(f"  {year}: parsed {len(details)} '{stage_filter}' entries")

    # Save
    with open(details_file, "w") as f:
        json.dump(details, f, ensure_ascii=False, indent=1)
    log.info(f"  {year}: saved to {details_file.name}")

    return details


def build_idpe_map(all_details: dict[int, dict], progress_dir: Path) -> dict:
    """Build cross-year person mapping."""
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
            idpe_map[idpe]["year_events"][str(year)] = str(EQTIMING_EVENTS.get(year, {}).get("event_id", ""))

    map_file = progress_dir / "idpe_map.json"
    with open(map_file, "w") as f:
        json.dump(idpe_map, f, ensure_ascii=False, indent=1)
    log.info(f"Saved idpe_map with {len(idpe_map)} unique persons")
    return idpe_map


def main():
    parser = argparse.ArgumentParser(description="Scrape Lofsdalen Epic from EQ Timing")
    parser.add_argument("--race", default="lofsdalen_epic", help="Race key")
    parser.add_argument("--year", type=int, help="Scrape only this year")
    args = parser.parse_args()

    progress_dir = ROOT / "progress" / args.race
    progress_dir.mkdir(parents=True, exist_ok=True)

    years = [args.year] if args.year else ALL_YEARS

    log.info(f"Scraping {args.race} for years: {years}")

    all_details = {}
    for year in years:
        if year not in EQTIMING_EVENTS:
            log.warning(f"  {year}: no event configured, skipping")
            continue
        details = fetch_year(year, progress_dir)
        all_details[year] = details

    # Load existing years not in current scrape
    for year in ALL_YEARS:
        if year not in all_details:
            details_file = progress_dir / f"details_{year}.json"
            if details_file.exists():
                with open(details_file) as f:
                    all_details[year] = json.load(f)

    build_idpe_map(all_details, progress_dir)

    total = sum(len(d) for d in all_details.values())
    log.info(f"Done! Total entries across {len(all_details)} years: {total}")


if __name__ == "__main__":
    main()
