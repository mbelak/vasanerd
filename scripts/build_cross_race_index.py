#!/usr/bin/env python3
"""
Build cross-race person index for global search.

Runs after all individual build_site_data.py runs. Loads each race's persons.json,
matches persons across races by name + nationality, and produces a single
global_persons.json for the frontend.

Usage:
    python3 scripts/build_cross_race_index.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "site" / "data"

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

ALL_RACES = [
    "vasaloppet", "vasaloppet_45", "vasaloppet_30", "tjejvasan", "ultravasan",
    "oppet_spar_mandag", "oppet_spar_sondag",
    "birken", "nsl", "lofsdalen_epic",
]

# IOC → ISO-2 nationality normalization (superset of the frontend's normNat)
NORM_NAT = {
    "SWE": "SE", "NOR": "NO", "FIN": "FI", "DEN": "DK", "GER": "DE",
    "FRA": "FR", "GBR": "GB", "USA": "US", "CAN": "CA", "AUT": "AT",
    "SUI": "CH", "NED": "NL", "ITA": "IT", "CZE": "CZ", "POL": "PL",
    "EST": "EE", "ISL": "IS", "RUS": "RU", "AUS": "AU", "BEL": "BE",
    "ESP": "ES", "GRE": "GR", "IND": "IN", "LAT": "LV", "MEX": "MX",
    "NZL": "NZ", "POR": "PT", "SLO": "SI", "SVK": "SK", "UKR": "UA",
    "LTU": "LT", "HUN": "HU", "ROU": "RO", "BRA": "BR", "KOR": "KR",
    "JPN": "JP", "IRL": "IE", "LUX": "LU", "CHN": "CN", "RSA": "ZA",
    "ARG": "AR", "CHI": "CL", "CRO": "HR", "SRB": "RS", "BUL": "BG",
    "TUR": "TR", "ISR": "IL", "KAZ": "KZ", "BLR": "BY", "MDA": "MD",
    "GEO": "GE", "ARM": "AM", "AND": "AD", "MNE": "ME", "ALB": "AL",
    "MKD": "MK", "BIH": "BA", "CYP": "CY", "MLT": "MT", "LIE": "LI",
    "MON": "MC", "SMR": "SM", "FAR": "FO",
}


def strip_nat(name: str) -> str:
    return re.sub(r"\s*\(\w+\)\s*$", "", name).strip()


def extract_nat(name: str) -> str:
    m = re.search(r"\((\w+)\)\s*$", name)
    return m.group(1).upper() if m else ""


def norm_nat(nat: str) -> str:
    return NORM_NAT.get(nat, nat)


def norm_name(name: str) -> str:
    """Normalize name: strip accents, lowercase."""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    return name.lower().strip()


def load_all_shard_data(race: str) -> dict[str, dict]:
    """Load club and performance data from ALL person shards for a race.

    Returns {idpe: {"clubs": {year: club}, "times": {year: bruttotid}, "classes": {year: klass}}}.
    """
    shard_dir = DATA_DIR / race / "p"
    keymap_file = DATA_DIR / race / "_keymap.json"

    if not shard_dir.exists() or not keymap_file.exists():
        return {}

    with open(keymap_file) as f:
        reverse_keymap = json.load(f)
    km = {v: k for k, v in reverse_keymap.items()}

    result = {}
    for shard_file in shard_dir.glob("*.json"):
        with open(shard_file) as f:
            shard = json.load(f)
        for idpe, rows in shard.items():
            clubs = {}
            for row in rows:
                year = row.get(km.get("ar", "ar"), "")
                club = row.get(km.get("klubb", "klubb"), "")
                if year and club:
                    yr = int(year) if str(year).isdigit() else year
                    clubs[yr] = club
            result[idpe] = {"clubs": clubs}

    log.info(f"  {race}: loaded shard data for {len(result)} persons")
    return result


def score_match(entry_a: dict, entry_b: dict) -> int:
    """Score how likely two entries (from different races) refer to the same person.

    Each entry has: clubs, times, classes (from shard data), years (from persons.json).
    Returns a compatibility score. Higher = more likely same person.
    Note: raw finish times are NOT compared since races have different distances.
    """
    score = 0
    years_a = set(entry_a.get("years", []))
    years_b = set(entry_b.get("years", []))
    clubs_a = entry_a.get("clubs", {})
    clubs_b = entry_b.get("clubs", {})

    overlap_years = years_a & years_b

    # Same club in overlapping years (+20)
    for yr in overlap_years:
        ca = clubs_a.get(yr, "").lower()
        cb = clubs_b.get(yr, "").lower()
        if ca and cb and ca == cb:
            score += 20
            break

    # Active in same era (+5)
    if overlap_years:
        score += 5

    return score


def build_global_index():
    """Build global_persons.json from all race persons.json files."""

    # 1. Load all persons.json
    all_entries = []  # (race, idpe, name, nat, years)
    for race in ALL_RACES:
        persons_file = DATA_DIR / race / "persons.json"
        if not persons_file.exists():
            log.warning(f"  {race}: no persons.json, skipping")
            continue
        with open(persons_file) as f:
            persons = json.load(f)
        for idpe, info in persons.items():
            name = info["namn"]
            raw_nat = extract_nat(name)
            nat = norm_nat(raw_nat)
            all_entries.append((race, idpe, name, nat, info["years"]))
        log.info(f"  {race}: {len(persons)} persons loaded")

    log.info(f"Total entries: {len(all_entries)}")

    # 2. Group by (normalized_name, normalized_nat)
    groups = defaultdict(list)
    for entry in all_entries:
        race, idpe, name, nat, years = entry
        nn = norm_name(strip_nat(name))
        groups[(nn, nat)].append(entry)

    log.info(f"Name groups: {len(groups)}")

    # 3. Preload all shard data for scoring (once per race)
    log.info("Loading shard data for disambiguation...")
    all_shard_data = {}  # race -> {idpe: {clubs, times, classes}}
    for race in ALL_RACES:
        all_shard_data[race] = load_all_shard_data(race)

    # 4. Process each group
    global_persons = []
    ambiguous_count = 0

    for (nn, nat), group in groups.items():
        # Check if disambiguation is needed
        races_in_group = defaultdict(list)
        for entry in group:
            races_in_group[entry[0]].append(entry)

        has_multi_in_same_race = any(len(v) > 1 for v in races_in_group.values())

        if not has_multi_in_same_race:
            # Simple case: at most 1 entry per race → merge all into one person
            best_entry = max(group, key=lambda e: len(e[4]))
            display_name = best_entry[2]
            race_list = [{"k": e[0], "i": e[1], "y": sorted(e[4])} for e in group]
            global_persons.append({"n": display_name, "r": race_list})
        else:
            # Complex case: need scoring to match entries across races
            ambiguous_count += 1

            # Build person clusters using greedy matching with scoring
            # Sort entries by year count descending so the most prolific identities
            # form clusters first and attract correct cross-race matches
            sorted_group = sorted(group, key=lambda e: len(e[4]), reverse=True)
            clusters = []  # list of lists of (race, idpe, name, nat, years)

            for entry in sorted_group:
                race, idpe, name, nat_e, years = entry
                entry_data = all_shard_data.get(race, {}).get(idpe, {})
                entry_data["years"] = years

                best_cluster = None
                best_score = -1

                for ci, cluster in enumerate(clusters):
                    existing_same_race = [e for e in cluster if e[0] == race]
                    if existing_same_race:
                        if any(e[1] == idpe for e in existing_same_race):
                            best_cluster = ci
                            best_score = 999
                            break
                        continue

                    total_score = 0
                    for ce in cluster:
                        ce_data = all_shard_data.get(ce[0], {}).get(ce[1], {})
                        ce_data["years"] = ce[4]
                        total_score += score_match(entry_data, ce_data)

                    if total_score > best_score:
                        best_score = total_score
                        best_cluster = ci

                if best_cluster is not None and best_score >= 0:
                    clusters[best_cluster].append(entry)
                else:
                    clusters.append([entry])

            for cluster in clusters:
                best_entry = max(cluster, key=lambda e: len(e[4]))
                display_name = best_entry[2]
                race_map = {}
                for e in cluster:
                    if e[0] not in race_map:
                        race_map[e[0]] = {"i": e[1], "y": list(e[4])}
                    else:
                        race_map[e[0]]["y"].extend(e[4])
                race_list = [{"k": r, "i": d["i"], "y": sorted(set(d["y"]))} for r, d in race_map.items()]
                global_persons.append({"n": display_name, "r": race_list})

    log.info(f"Disambiguated {ambiguous_count} name groups")
    log.info(f"Global persons: {len(global_persons)}")

    # 5. Write output
    out_path = DATA_DIR / "global_persons.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(global_persons, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    log.info(f"Written {out_path.name} ({size_mb:.1f} MB, {len(global_persons)} persons)")


if __name__ == "__main__":
    build_global_index()
