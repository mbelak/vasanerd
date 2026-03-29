# Global Search Design

## Problem

Each race has its own `persons.json` with race-specific `idpe` values. The same person competing in multiple races (e.g. Vasaloppet and Tjejvasan) has different `idpe` values and cannot be found across races from a single search. Users want to search once and see all races a person has competed in.

## Solution Overview

1. **Build-time matching script** generates a `global_persons.json` that links the same person across races using fuzzy matching.
2. **Frontend toggle** ("Search all races") on the existing search field switches between race-specific and global search.
3. **Minimal profile view** lists all races/years with links to race-specific results.

## Build-Time Matching

### Script

New script `scripts/build_global_persons.py` that:

1. Loads `persons.json` from every race in `site/data/`.
2. For each person entry, extracts matching signals from the name field and supplementary data.
3. Groups persons across races into global identities.
4. Outputs `site/data/global_persons.json`.

### Matching Algorithm

**Primary key:** Normalized name — lowercase, stripped of diacritics, trimmed whitespace.

**Disambiguation signals** (used when multiple candidates share the same normalized name):
- **Nationality** — extracted from the `(XXX)` suffix in the `namn` field.
- **Club/team** — available in person shard data per race; loaded during build.
- **Age class** — available in person shard data; helps distinguish parent/child with same name.

**Matching strategy:**

1. Build a lookup table keyed by `(normalized_name, nationality)`.
2. Persons from different races with the same key are considered the same person.
3. If multiple candidates exist for a key (e.g. same name + nationality in same race across years), use club and age class overlap as tiebreakers.
4. Unresolved ambiguities: keep entries separate (prefer false negatives over false positives).

### Output Format

`site/data/global_persons.json`:

```json
{
  "g_abc123": {
    "namn": "Belak, Martin (SVE)",
    "races": {
      "vasaloppet": {
        "idpe": "9999991678885900000C4080",
        "years": [2024, 2025, 2026]
      },
      "tjejvasan": {
        "idpe": "8888881234567800000A1234",
        "years": [2025]
      }
    }
  }
}
```

- `g_abc123` — global ID, generated as the first 12 hex chars of SHA-256 of `(normalized_name, nationality)`. Collisions at this length are negligible for the dataset size (~100K persons).
- `namn` — display name, taken from the most recent entry.
- `races` — map of race key to `{idpe, years}`.

Persons who only appear in a single race are still included (the global index is a superset).

### File Size Estimate

Current `persons.json` sizes (approximate):
- vasaloppet: ~3 MB (largest, ~100K+ persons)
- Other races: 100KB–1MB each

`global_persons.json` will be roughly the sum minus deduplication savings. Expected: 3–5 MB. Acceptable for a lazy-loaded file that's only fetched when global search is toggled on.

## Frontend Changes

### Search Toggle

- Add a checkbox/switch labeled "Search all races" below or beside the existing search input.
- Default: off (current behavior, race-specific search).
- When toggled on:
  - Fetch and cache `global_persons.json` (lazy load on first toggle).
  - Search filters against the global index instead of `persons[currentRace]`.

### Search Results (Global Mode)

Each result shows:
- Person name (with nationality flag)
- Underneath: compact list of race badges with year counts, e.g. `Vasaloppet (3) · Tjejvasan (1)`

### Click Behavior

Clicking a global search result opens a **global profile view** (new lightweight panel/section):

- Header: person name + nationality
- Body: list of races, each showing:
  - Race name
  - Years participated (as clickable links)
  - Click on a year → switches to that race and opens the person tab with the race-specific `idpe`

### No Aggregated Statistics in v1

The global profile is a navigation hub only. No cross-race stats, trends, or comparisons.

## Data Pipeline Integration

Add `build_global_persons.py` to the build pipeline, to be run after all race-specific builds:

```bash
# After per-race builds
python3 scripts/build_global_persons.py
```

The script reads from `site/data/{race}/persons.json` (already-built output), so it has no dependency on progress files.

## Scope & Limitations

- **False positives:** Two different people with the same name and nationality will be merged. Acceptable in v1; can be improved later with birth year or manual overrides.
- **False negatives:** Name variations across races (e.g. "Eriksson" vs "Ericsson") won't match. Acceptable in v1.
- **Birken / NSL:** Different data sources but same `namn` format convention. Matching works the same way.
- **No manual override system** in v1. Could be added later as a JSON allow/deny list.
- **No incremental rebuild.** Script re-generates the full file each time. Fast enough given file sizes.

## Future Extensions (Not in Scope)

- Cross-race statistics (total distance, best placements, trends)
- Fuzzy name matching (Levenshtein / phonetic)
- Manual identity linking/splitting UI
- Pre-loading global index for instant toggle
