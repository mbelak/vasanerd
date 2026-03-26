#!/usr/bin/env python3
"""
Build static JSON files for the frontend directly from progress files.

Run: python build_site_data.py --race vasaloppet|tjejvasan

Creates:
  site/data/{race}/{year}.json  — all flattened result rows per year
  site/data/{race}/year_stats.json — precomputed yearly aggregates
  site/data/{race}/persons.json    — search index {idpe: {namn, years}}
"""

from __future__ import annotations

import argparse
import json
import logging
import bisect
import math
import os
import re
import signal
from pathlib import Path

from scraper import RACE_CONFIGS, build_csv_row

# Category mapping for races with multiple event prefixes (e.g. Tjejvasan)
EVENT_CATEGORY_MAP = {
    "TVT": "Competitive",
    "TVM": "Recreational",
    "TVJ": "Junior",
}

# Restore signal handler (scraper.py registers its own on import)
signal.signal(signal.SIGINT, signal.default_int_handler)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

BASE_OUT_DIR = Path(__file__).resolve().parent.parent / "site" / "data"
PROGRESS_DIR = Path(__file__).resolve().parent.parent / "progress"
MAX_PART_MB = 24


def parse_args():
    parser = argparse.ArgumentParser(description="Build site JSON from progress files")
    parser.add_argument("--race", default="vasaloppet", choices=list(RACE_CONFIGS.keys()))
    return parser.parse_args()


# --- Helpers ---

def parse_time_minutes(t: str) -> float | None:
    """'08:19:46' -> 499.767"""
    if not t or not re.match(r"^\d+:\d+:\d+$", t):
        return None
    parts = t.split(":")
    return float(parts[0]) * 60 + float(parts[1]) + float(parts[2]) / 60.0


def safe_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def is_finisher(row: dict) -> bool:
    t = row.get("bruttotid", "")
    return bool(t and re.match(r"^\d+:\d+:\d+$", t))


def is_female(row: dict) -> bool:
    klass = row.get("klass") or ""
    if klass.startswith("D") or klass.startswith("W") or klass.startswith("Kvinner") or klass.startswith("Elite Kvinner"):
        return True
    # Fallback for VLE 2021 etc where klass is missing but startgrupp exists
    return (row.get("startgrupp") or "").lower() == "women"


def extract_nation(namn: str) -> str:
    m = re.search(r"\((\w+)\)", namn or "")
    return m.group(1) if m else "UNK"


def percentile_index(n: int, p: float) -> int:
    """Match SQL: v_times[greatest(1, (p * n)::int)]  (1-indexed -> 0-indexed)
    PostgreSQL numeric::int rounds to nearest (half away from zero), not truncates."""
    idx = round(p * n)
    return max(0, idx - 1)


def cp_prefix(name: str) -> str:
    return (name.lower().replace(" ", "_")
            .replace("å", "a").replace("ä", "a").replace("ö", "o")
            .replace("ø", "o"))


def parse_time_seconds(t: str) -> float | None:
    """'08:19:46' -> 29986.0 seconds."""
    if not t or not re.match(r"^\d+:\d+:\d+$", t):
        return None
    parts = t.split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


def compute_missing_placements(rows: list[dict], checkpoints: list[str], force: bool = False):
    """Compute checkpoint placements from elapsed times for races missing them.

    Placements are computed per gender group (matching how other races report them).
    If force=True, recompute even if placement data already exists.
    """
    for cp_name in checkpoints:
        prefix = cp_prefix(cp_name)
        plac_key = f"{prefix}_placering"
        tid_key = f"{prefix}_tid"

        # Skip if any row already has placement data for this checkpoint
        if not force and any(r.get(plac_key) for r in rows):
            continue

        # Group by gender and rank within each group
        for gender_filter in (True, False):  # True = female, False = male
            timed = []
            for r in rows:
                if is_female(r) != gender_filter:
                    continue
                secs = parse_time_seconds(r.get(tid_key, ""))
                if secs is not None:
                    timed.append((secs, r))

            if not timed:
                continue

            # Sort by time ascending
            timed.sort(key=lambda x: x[0])

            # Assign ranks with ties (same time = same rank)
            rank = 1
            for i, (secs, r) in enumerate(timed):
                if i > 0 and secs > timed[i - 1][0]:
                    rank = i + 1
                r[plac_key] = str(rank)


def compute_dnf_gained(rows: list[dict], checkpoints: list[str]):
    """For each finisher, compute how many DNF runners were ahead at their last checkpoint.

    Adds two types of fields to each finisher's row:
      - dnf_gained: total count of DNF runners who were ahead
      - dnf_gained_cp_{prefix}: count per checkpoint where DNF runners stopped
    """
    cp_names_no_finish = [cp for cp in checkpoints if cp not in ("Mål", "Finish")]
    cp_prefixes_nf = [cp_prefix(cp) for cp in cp_names_no_finish]

    # Identify finishers and DNF runners
    finisher_rows = [r for r in rows if is_finisher(r)]
    dnf_rows = [r for r in rows if not is_finisher(r)
                and r.get("status") not in ("Startade inte", "Did not start", "DNS", "Not Started")]

    # For each DNF runner, find their last checkpoint and placement there
    # Group by checkpoint: {cp_prefix: [sorted list of placements]}
    dnf_by_cp: dict[str, list[int]] = {}
    for r in dnf_rows:
        last_prefix = None
        for prefix in reversed(cp_prefixes_nf):
            if r.get(f"{prefix}_tid"):
                last_prefix = prefix
                break
        if last_prefix is None:
            continue
        plac_str = r.get(f"{last_prefix}_placering", "")
        if not plac_str or not str(plac_str).isdigit():
            continue
        plac = int(plac_str)
        if last_prefix not in dnf_by_cp:
            dnf_by_cp[last_prefix] = []
        dnf_by_cp[last_prefix].append(plac)

    # Sort each checkpoint's DNF placements for efficient counting
    for prefix in dnf_by_cp:
        dnf_by_cp[prefix].sort()

    # For each finisher, count DNF runners ahead at each checkpoint
    for r in finisher_rows:
        total_gained = 0
        for prefix in cp_prefixes_nf:
            if prefix not in dnf_by_cp:
                continue
            plac_str = r.get(f"{prefix}_placering", "")
            if not plac_str or not str(plac_str).isdigit():
                continue
            finisher_plac = int(plac_str)
            # Count DNF runners with placement < finisher's placement (i.e. ahead)
            # Since sorted, use bisect for O(log n)
            count = bisect.bisect_left(dnf_by_cp[prefix], finisher_plac)
            if count > 0:
                r[f"dnf_gained_cp_{prefix}"] = str(count)
                total_gained += count
        if total_gained > 0:
            r["dnf_gained"] = str(total_gained)


# --- Load progress files ---

def load_progress(race: str, years: list[int]) -> dict[int, list[dict]]:
    """Load details files and flatten with build_csv_row."""
    rc = RACE_CONFIGS[race]
    has_categories = len(rc.get("event_prefixes", [])) > 1
    all_rows = {}
    for year in years:
        # Try race subdirectory first, then root progress (legacy for vasaloppet)
        path = PROGRESS_DIR / race / f"details_{year}.json"
        if not path.exists() and race == "vasaloppet":
            path = PROGRESS_DIR / f"details_{year}.json"
        if not path.exists():
            log.warning(f"No progress file for {year}: {path}")
            all_rows[year] = []
            continue

        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Load idp_events mapping if race has multiple categories
        idp_events = {}
        if has_categories:
            list_path = PROGRESS_DIR / race / f"list_{year}.json"
            if list_path.exists():
                with open(list_path, "r", encoding="utf-8") as f:
                    list_data = json.load(f)
                idp_events = list_data.get("idp_events", {})

        rows = []
        for idp, data in raw.items():
            data["ar"] = str(year)
            if not data.get("idp"):
                data["idp"] = idp
            row = build_csv_row(data)
            # Remove empty values and em-dash ("–"/"-" = no data in scraped HTML)
            row = {k: v for k, v in row.items()
                   if v is not None and v != "" and v not in ("–", "-")}

            # Set category based on event prefix
            if has_categories and idp in idp_events:
                event_code = idp_events[idp]
                prefix = event_code.split("_")[0]  # e.g. "TVT"
                row["kategori"] = EVENT_CATEGORY_MAP.get(prefix, prefix)

            rows.append(row)

        # Calculate overall placement based on bruttotid
        if has_categories:
            finishers = [r for r in rows if is_finisher(r)]
            finishers.sort(key=lambda r: r.get("bruttotid", "99:99:99"))
            for i, r in enumerate(finishers):
                r["placering_total_overall"] = str(i + 1)

        log.info(f"  {year}: {len(rows)} rows loaded")
        all_rows[year] = rows

    return all_rows


# --- Write year JSON ---

def build_keymap(all_rows: dict) -> dict:
    """Build a stable key→short-alias mapping from all rows."""
    all_keys = sorted(set(k for rows in all_rows.values() for r in rows for k in r))
    # Korta alias: a, b, ..., z, A, B, ..., Z, aa, ab, ...
    def alias(i):
        chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if i < len(chars):
            return chars[i]
        return chars[i // len(chars) - 1] + chars[i % len(chars)]
    return {k: alias(i) for i, k in enumerate(all_keys)}


def compact_rows(rows: list[dict], keymap: dict) -> list[dict]:
    """Replace long keys with short aliases."""
    return [{keymap[k]: v for k, v in r.items()} for r in rows]


def write_year_json(rows: list[dict], year: int, out_dir: Path, keymap: dict | None = None) -> dict:
    """Write {year}.json, split if > MAX_PART_MB. Returns year_parts info."""
    # Remove old part files first
    for old_part in out_dir.glob(f"{year}_*.json"):
        old_part.unlink()

    write_rows = compact_rows(rows, keymap) if keymap else rows
    full_json = json.dumps(write_rows, ensure_ascii=False, separators=(",", ":"))
    size_mb = len(full_json.encode("utf-8")) / 1024 / 1024

    if size_mb > MAX_PART_MB:
        num_parts = int(size_mb // MAX_PART_MB) + 1
        chunk_size = len(write_rows) // num_parts + 1
        for i in range(num_parts):
            chunk = write_rows[i * chunk_size:(i + 1) * chunk_size]
            part_path = out_dir / f"{year}_{i}.json"
            with open(part_path, "w", encoding="utf-8") as f:
                json.dump(chunk, f, ensure_ascii=False, separators=(",", ":"))
            part_mb = os.path.getsize(part_path) / 1024 / 1024
            log.info(f"  -> {part_path.name} ({part_mb:.1f} MB)")
        # Remove old full file (redundant with split files)
        full_path = out_dir / f"{year}.json"
        if full_path.exists():
            full_path.unlink()
            log.info(f"  -> Removed {year}.json (replaced by {num_parts} parts)")
        log.info(f"  -> {year}: {size_mb:.1f} MB total, split into {num_parts} parts")
        return {str(year): num_parts}
    else:
        full_path = out_dir / f"{year}.json"
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(full_json)
        log.info(f"  -> {year}.json ({size_mb:.1f} MB)")
        return {}


# --- Build persons.json ---

def build_persons(all_rows: dict[int, list[dict]]) -> dict:
    persons = {}
    for year, rows in all_rows.items():
        if year < 2000:
            continue  # Skip historical years (e.g. 1922) — unreliable idpe mappings
        for r in rows:
            idpe = r.get("idpe", "")
            if not idpe:
                continue
            if idpe not in persons:
                persons[idpe] = {"namn": r.get("namn", ""), "years": []}
            persons[idpe]["years"].append(year)
    return persons


# --- Build person shards ---

def build_person_shards(all_rows: dict[int, list[dict]], keymap: dict, out_dir: Path):
    """Generate 256 shard files with all rows per person, grouped by idpe[-2:]."""
    shards: dict[str, dict[str, list]] = {}  # shard_key -> {idpe: [rows]}
    for year, rows in all_rows.items():
        if year < 2000:
            continue  # Skip historical years
        compact = compact_rows(rows, keymap)
        for r in compact:
            idpe = r.get(keymap.get("idpe", "idpe"), "")
            if not idpe:
                continue
            shard_key = idpe[-2:].lower()
            if shard_key not in shards:
                shards[shard_key] = {}
            if idpe not in shards[shard_key]:
                shards[shard_key][idpe] = []
            shards[shard_key][idpe].append(r)

    shard_dir = out_dir / "p"
    # Remove old shards
    if shard_dir.exists():
        for old in shard_dir.glob("*.json"):
            old.unlink()
    os.makedirs(shard_dir, exist_ok=True)

    total_size = 0
    for key, persons in shards.items():
        path = shard_dir / f"{key}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(persons, f, ensure_ascii=False, separators=(",", ":"))
        total_size += os.path.getsize(path)

    avg_kb = total_size / len(shards) / 1024 if shards else 0
    log.info(f"  -> {len(shards)} shards in p/ (avg {avg_kb:.0f} KB, total {total_size/1024/1024:.1f} MB)")


# --- Build year_stats ---

def build_year_stats(all_rows: dict[int, list[dict]], checkpoints: list[str], race: str) -> list[dict]:
    stats = []
    cp_prefixes = [cp_prefix(cp) for cp in checkpoints]

    for year in sorted(all_rows.keys()):
        rows = all_rows[year]
        if not rows:
            continue

        total = len(rows)
        finishers = [r for r in rows if is_finisher(r)]
        finisher_m = [r for r in finishers if not is_female(r)]
        finisher_f = [r for r in finishers if is_female(r)]
        dns = [r for r in rows if r.get("status") in ("Startade inte", "Did not start", "DNS")]
        dsq = [r for r in rows if not is_finisher(r) and r.get("status") != "Startade inte"
               and r.get("bruttotid", "") not in ("", None)]
        dns_count = len(dns)
        dsq_count = len(dsq)
        starter_count = total - dns_count
        finisher_count = len(finishers)
        dnf_count = starter_count - finisher_count - dsq_count

        # Avg speed (fallback: distance / hours if snitthastighet is missing)
        distance_km = RACE_CONFIGS[race]["distance_km"]
        speeds = []
        for r in finishers:
            s = safe_float(r.get("snitthastighet"))
            if s is not None:
                speeds.append(s)
            else:
                mins = parse_time_minutes(r.get("bruttotid", ""))
                if mins and mins > 0:
                    speeds.append(distance_km / (mins / 60.0))
        avg_speed = sum(speeds) / len(speeds) if speeds else None

        # Percentiles
        finish_minutes = sorted([m for r in finishers if (m := parse_time_minutes(r.get("bruttotid", ""))) is not None])
        n = len(finish_minutes)
        if n > 0:
            p10 = finish_minutes[percentile_index(n, 0.10)]
            p25 = finish_minutes[percentile_index(n, 0.25)]
            median = finish_minutes[percentile_index(n, 0.50)]
            p75 = finish_minutes[percentile_index(n, 0.75)]
            p90 = finish_minutes[percentile_index(n, 0.90)]
        else:
            p10 = p25 = median = p75 = p90 = 0

        # Fastest
        def fastest(subset):
            if not subset:
                return None, None
            def sort_key(r):
                t = r.get("bruttotid", "99:99:99")
                p = r.get("placering_totalt", "")
                return (t, int(p) if p.isdigit() else 999999)
            best = min(subset, key=sort_key)
            return best.get("bruttotid"), best.get("namn")

        fastest_time_m, fastest_name_m = fastest(finisher_m)
        fastest_time_f, fastest_name_f = fastest(finisher_f)

        # Checkpoint speeds
        def checkpoint_avg_speeds(subset):
            result = {}
            for prefix in cp_prefixes:
                vals = [v for r in subset if (v := safe_float(r.get(f"{prefix}_km_per_h"))) is not None]
                result[prefix] = sum(vals) / len(vals) if vals else None
            return result

        cp_m = checkpoint_avg_speeds(finisher_m)
        cp_f = checkpoint_avg_speeds(finisher_f)
        cp_avg = checkpoint_avg_speeds(finishers)

        # Median placement
        placements = sorted([int(r["placering_totalt"]) for r in finishers
                           if r.get("placering_totalt", "").isdigit()])
        median_placement = placements[len(placements) // 2] if placements else None

        # Nationality distribution
        nat_dist = {}
        for r in rows:
            nat = extract_nation(r.get("namn", ""))
            nat_dist[nat] = nat_dist.get(nat, 0) + 1

        # DNF by checkpoint — find last passed checkpoint
        # Skip "Mål" — if they reached the finish they are finishers (matches SQL logic)
        dnf_rows = [r for r in rows
                    if not is_finisher(r) and r.get("status") != "Startade inte"]
        dnf_by_cp = {}
        dnf_cps = [(p, n) for p, n in zip(cp_prefixes, checkpoints) if n not in ("Mål", "Finish")]
        for r in dnf_rows:
            last_cp = "Unknown"
            for prefix, name in reversed(dnf_cps):
                tid = r.get(f"{prefix}_tid")
                if tid:
                    last_cp = "Förvarning" if name == "Mora Förvarning" else name
                    break
            dnf_by_cp[last_cp] = dnf_by_cp.get(last_cp, 0) + 1

        # Start group matrix
        RESULT_GROUP_CUTOFFS = [
            (150, "Elite"), (500, "Group 1"), (1000, "Group 2"),
            (2000, "Group 3"), (3200, "Group 4"), (4400, "Group 5"),
            (5600, "Group 6"), (6800, "Group 7"), (8400, "Group 8"),
            (10000, "Group 9"),
        ]

        def result_group(r):
            pt = r.get("placering_totalt", "")
            if not pt or not pt.isdigit():
                return "?"
            p = int(pt)
            for cutoff, name in RESULT_GROUP_CUTOFFS:
                if p <= cutoff:
                    return name
            return "Group 10+"

        sg_matrix = {}
        for r in finishers:
            sg = r.get("startgrupp") or "?"
            rg = result_group(r)
            if sg not in sg_matrix:
                sg_matrix[sg] = {}
            sg_matrix[sg][rg] = sg_matrix[sg].get(rg, 0) + 1

        # Histogram bins (15-minute buckets)
        histogram = {}
        for mins in finish_minutes:
            bucket = int(math.floor(mins / 15) * 15)
            histogram[str(bucket)] = histogram.get(str(bucket), 0) + 1

        # Startgroup checkpoint splits: median/mean/p25/p75 elapsed time per checkpoint per startgroup
        sg_splits = {}
        sg_groups = {}
        for r in finishers:
            sg = r.get("startgrupp") or "?"
            if sg == "?":
                continue
            if sg not in sg_groups:
                sg_groups[sg] = []
            sg_groups[sg].append(r)
        for sg, sg_rows in sg_groups.items():
            cp_stats = {}
            for prefix in cp_prefixes:
                elapsed = [m for r in sg_rows if (m := parse_time_minutes(r.get(f"{prefix}_tid", ""))) is not None and m > 0]
                if not elapsed:
                    continue
                elapsed.sort()
                en = len(elapsed)
                cp_stats[prefix] = {
                    "median": elapsed[percentile_index(en, 0.50)],
                    "mean": round(sum(elapsed) / en, 2),
                    "p25": elapsed[percentile_index(en, 0.25)],
                    "p75": elapsed[percentile_index(en, 0.75)],
                    "count": en,
                }
            if cp_stats:
                sg_splits[sg] = cp_stats

        # Performance medal: finish within winner + 50% (only vasaloppet)
        if race == "vasaloppet":
            pm_cutoff_m = parse_time_minutes(fastest_time_m) * 1.5 if fastest_time_m else None
            pm_cutoff_f = parse_time_minutes(fastest_time_f) * 1.5 if fastest_time_f else None
            pm_count_m = sum(1 for r in finisher_m if pm_cutoff_m and (m := parse_time_minutes(r.get("bruttotid", ""))) and m <= pm_cutoff_m)
            pm_count_f = sum(1 for r in finisher_f if pm_cutoff_f and (m := parse_time_minutes(r.get("bruttotid", ""))) and m <= pm_cutoff_f)
        else:
            pm_cutoff_m = pm_cutoff_f = pm_count_m = pm_count_f = None

        # Birkebeiner merke: per-class cutoff from merke_tid field
        merke_count = merke_count_m = merke_count_f = None
        merke_cutoffs = None
        if RACE_CONFIGS[race].get("has_merke"):
            merke_count = 0
            merke_count_m = 0
            merke_count_f = 0
            cutoff_map = {}
            for r in finishers:
                mt = r.get("merke_tid", "")
                bt = r.get("bruttotid", "")
                klass = r.get("klass", "")
                if mt and klass:
                    cutoff_map[klass] = mt
                if mt and bt:
                    mt_s = parse_time_seconds(mt)
                    bt_s = parse_time_seconds(bt)
                    if mt_s is not None and bt_s is not None and bt_s <= mt_s:
                        merke_count += 1
                        if is_female(r):
                            merke_count_f += 1
                        else:
                            merke_count_m += 1
            merke_cutoffs = cutoff_map if cutoff_map else None

        stat = {
            "year": year,
            "finisher_count": finisher_count,
            "finisher_count_m": len(finisher_m),
            "finisher_count_f": len(finisher_f),
            "starter_count": starter_count,
            "dns_count": dns_count,
            "dnf_count": dnf_count,
            "dsq_count": dsq_count,
            "total_entries": total,
            "fastest_time_m": fastest_time_m,
            "fastest_name_m": fastest_name_m,
            "fastest_time_f": fastest_time_f,
            "fastest_name_f": fastest_name_f,
            "avg_speed": avg_speed,
            "p10_min": p10,
            "p25_min": p25,
            "median_min": median,
            "p75_min": p75,
            "p90_min": p90,
            "checkpoint_speeds_m": cp_m,
            "checkpoint_speeds_f": cp_f,
            "checkpoint_avg_speeds": cp_avg,
            "median_placement": median_placement,
            "nationality_distribution": nat_dist,
            "dnf_by_checkpoint": dnf_by_cp,
            "startgroup_matrix": sg_matrix,
            "startgroup_splits": sg_splits,
            "histogram_bins": histogram,
            "pm_count_m": pm_count_m,
            "pm_count_f": pm_count_f,
            "pm_cutoff_m_min": pm_cutoff_m,
            "pm_cutoff_f_min": pm_cutoff_f,
            "merke_count": merke_count,
            "merke_count_m": merke_count_m,
            "merke_count_f": merke_count_f,
            "merke_cutoffs": merke_cutoffs,
        }
        stats.append(stat)

    return stats


# --- Main ---

def main():
    args = parse_args()
    race = args.race
    rc = RACE_CONFIGS[race]
    years = rc["years"]
    checkpoints = rc["checkpoints"]
    out_dir = BASE_OUT_DIR / race

    log.info(f"Building site data for {rc['display_name']} ({years[0]}-{years[-1]})")
    os.makedirs(out_dir, exist_ok=True)

    # 1. Load progress files
    log.info("Loading progress files...")
    all_rows = load_progress(race, years)

    total_rows = sum(len(r) for r in all_rows.values())
    if not total_rows:
        log.error("No rows loaded!")
        return

    log.info(f"Total: {total_rows} rows")

    # 1b. Compute missing checkpoint placements from elapsed times
    for year, rows in all_rows.items():
        if rows:
            compute_missing_placements(rows, checkpoints, force=(race == "birken"))

    # 1c. Compute DNF gained positions for each finisher
    for year, rows in all_rows.items():
        if rows:
            compute_dnf_gained(rows, checkpoints)

    # 2. Build keymap and write per-year JSON
    log.info("Building keymap...")
    keymap = build_keymap(all_rows)
    # Write keymap (short→long, for frontend expansion)
    reverse_keymap = {v: k for k, v in keymap.items()}
    keymap_path = out_dir / "_keymap.json"
    with open(keymap_path, "w", encoding="utf-8") as f:
        json.dump(reverse_keymap, f, ensure_ascii=False, separators=(",", ":"))
    log.info(f"  -> _keymap.json ({len(keymap)} keys)")

    log.info("Writing per-year JSON (compressed keys)...")
    year_parts = {}
    for year in years:
        rows = all_rows.get(year, [])
        if rows:
            parts = write_year_json(rows, year, out_dir, keymap)
            year_parts.update(parts)

    if year_parts:
        log.info(f"NOTE: Update year_parts in index.html: {year_parts}")

    # 3. Build persons.json
    log.info("Building persons.json...")
    persons = build_persons(all_rows)
    persons_path = out_dir / "persons.json"
    with open(persons_path, "w", encoding="utf-8") as f:
        json.dump(persons, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = os.path.getsize(persons_path) / 1024
    log.info(f"  -> persons.json ({size_kb:.0f} KB, {len(persons)} persons)")

    # 3b. Build person shards (for fast person page loading)
    log.info("Building person shards...")
    build_person_shards(all_rows, keymap, out_dir)

    # 4. Build year_stats.json
    log.info("Building year_stats.json...")
    stats = build_year_stats(all_rows, checkpoints, race)
    stats_path = out_dir / "year_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, separators=(",", ":"))
    log.info(f"  -> year_stats.json ({len(stats)} years)")

    log.info("Done!")


if __name__ == "__main__":
    main()
