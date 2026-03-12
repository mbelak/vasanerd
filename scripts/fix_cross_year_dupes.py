#!/usr/bin/env python3
"""Remove rows from year JSON files that are duplicates of 2026 data.

These rows were created by a scraper bug where results.vasaloppet.se
returned 2026 data for historical idp lookups. This script detects them
by matching (idpe, bruttotid, startnummer) signatures against 2026.

Only removes rows where ALL of idpe, bruttotid, and startnummer match 2026
AND the year file is not 2026 itself. Single-match years (likely genuine
identical times) are skipped with a threshold.
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = str(ROOT / "site" / "data" / "vasaloppet")
KEYMAP_FILE = os.path.join(DATA_DIR, "_keymap.json")
DUPE_THRESHOLD = 5  # Only clean years with more than this many dupes


def load_keymap():
    try:
        with open(KEYMAP_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def expand(row, km):
    return {km.get(k, k): v for k, v in row.items()} if km else row


def compact(row, km):
    """Reverse keymap: convert expanded keys back to short keys."""
    if not km:
        return row
    rev = {v: k for k, v in km.items()}
    return {rev.get(k, k): v for k, v in row.items()}


def main():
    km = load_keymap()

    # Load 2026 signatures
    with open(os.path.join(DATA_DIR, "2026.json")) as f:
        data_2026 = json.load(f)

    sig_2026 = set()
    for row in data_2026:
        r = expand(row, km)
        idpe = r.get("idpe", "")
        bt = r.get("bruttotid", "")
        sn = r.get("startnummer", "")
        if idpe and bt:
            sig_2026.add((idpe, bt, sn))

    print(f"Loaded {len(sig_2026)} signatures from 2026")

    # Process each year
    data_path = Path(DATA_DIR).resolve()
    years = sorted(
        f.replace(".json", "")
        for f in os.listdir(DATA_DIR)
        if f.endswith(".json") and f[0].isdigit() and "_" not in f
        and Path(os.path.join(DATA_DIR, f)).resolve().parent == data_path
    )

    total_removed = 0
    for y in years:
        if y == "2026":
            continue

        filepath = os.path.join(DATA_DIR, f"{y}.json")
        with open(filepath) as f:
            data = json.load(f)

        # Find duplicates
        clean = []
        dupes = []
        for row in data:
            r = expand(row, km)
            sig = (r.get("idpe", ""), r.get("bruttotid", ""), r.get("startnummer", ""))
            if sig in sig_2026 and sig[1]:  # has bruttotid
                dupes.append(r.get("namn", "?"))
            else:
                clean.append(row)

        if len(dupes) > DUPE_THRESHOLD:
            print(f"  {y}: removing {len(dupes)} duplicate rows (keeping {len(clean)} of {len(data)})")
            tmp = filepath + ".tmp"
            with open(tmp, "w") as f:
                json.dump(clean, f, separators=(",", ":"), ensure_ascii=False)
            os.replace(tmp, filepath)
            total_removed += len(dupes)
        elif dupes:
            print(f"  {y}: {len(dupes)} potential dupes (below threshold {DUPE_THRESHOLD}, skipping)")
        else:
            print(f"  {y}: clean")

    print(f"\nDone. Removed {total_removed} duplicate rows total.")

    # Also clean progress files
    if total_removed > 0:
        print("\nCleaning progress files...")
        clean_progress_files(sig_2026, km)
        print("Done. Now re-run: python3 build_site_data.py --race vasaloppet")


def clean_progress_files(sig_2026, km):
    """Remove duplicate entries from progress/details_*.json files."""
    progress_dirs = [str(ROOT / "progress" / "vasaloppet"), str(ROOT / "progress")]

    # Build 2026 signatures from progress file
    progress_2026_sigs = set()
    for d in progress_dirs:
        path = os.path.join(d, "details_2026.json")
        if os.path.exists(path):
            with open(path) as f:
                raw = json.load(f)
            for idp, data in raw.items():
                bt = data.get("bruttotid", "")
                sn = data.get("startnummer", "")
                if bt:
                    progress_2026_sigs.add((idp, bt, sn))
            break

    if not progress_2026_sigs:
        print("  Could not load 2026 progress signatures, skipping")
        return

    for d in progress_dirs:
        if not os.path.isdir(d):
            continue
        for fname in sorted(os.listdir(d)):
            if not fname.startswith("details_") or not fname.endswith(".json"):
                continue
            year = fname.replace("details_", "").replace(".json", "")
            if year == "2026":
                continue
            path = os.path.join(d, fname)
            with open(path) as f:
                raw = json.load(f)

            removed = 0
            clean = {}
            for idp, data in raw.items():
                sig = (idp, data.get("bruttotid", ""), data.get("startnummer", ""))
                if sig in progress_2026_sigs and sig[1]:
                    removed += 1
                else:
                    clean[idp] = data

            if removed > DUPE_THRESHOLD:
                print(f"  {path}: removed {removed} entries (keeping {len(clean)})")
                tmp = path + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(clean, f, separators=(",", ":"), ensure_ascii=False)
                os.replace(tmp, path)
            elif removed:
                print(f"  {path}: {removed} potential dupes (below threshold, skipping)")


if __name__ == "__main__":
    main()
