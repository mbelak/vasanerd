# Positions Gained from DNF Runners

**Date:** 2026-03-26
**Status:** Approved

## Summary

Show each finisher how many placement positions they gained because runners ahead of them dropped out (DNF). Displayed in the person detail view per race year, with a total count and a breakdown by checkpoint showing where the DNF runners stopped.

## Definitions

- **"Ahead"**: A DNF runner is considered "ahead" of a finisher if, at the DNF runner's last completed checkpoint, the DNF runner had a better (lower numerical) placement than the finisher at that same checkpoint.
- **Last checkpoint**: Determined by iterating backwards through the race's checkpoint list and finding the last one with a non-empty `{cp}_tid` value.
- **Gained position**: Each DNF runner who was ahead at their last checkpoint counts as exactly 1 gained position.

## Data Pipeline Changes

### build_site_data.py

After all rows are flattened via `build_csv_row()`, compute two new fields per finisher:

```python
# New fields added to each finisher's row:
"dnf_gained": 47,          # total positions gained from DNF runners
"dnf_gained_by_cp": {       # breakdown: checkpoint_prefix -> count
    "risberg": 12,
    "evertsberg": 8,
    "oxberg": 15,
    "hokberg": 7,
    "eldris": 3,
    "mora_forvarning": 2
}
```

### Algorithm

```
For each year's data:
  1. Separate rows into finishers and DNF runners
     - Finisher: has non-empty bruttotid and status != "Startade inte"
     - DNF: status == "Brutit" (or no bruttotid and not DNS)

  2. For each DNF runner, determine their last checkpoint:
     - Iterate backwards through race checkpoints (excluding Mal/Finish)
     - Find last checkpoint where {cp}_tid is non-empty
     - Record: (dnf_person, last_checkpoint, placement_at_that_checkpoint)
     - Skip if placement is missing/empty at that checkpoint
     - Parse placement as integer for comparison (stored as string in data)

  3. Group DNF runners by their last checkpoint:
     - dnf_at_checkpoint[cp] = [(dnf_person, placement), ...]

  4. For each finisher:
     - dnf_gained = 0
     - dnf_gained_by_cp = {}
     - For each checkpoint that has DNF runners:
       - Get finisher's placement at that checkpoint
       - Skip if finisher has no placement at that checkpoint
       - Count DNF runners at that checkpoint with better (lower) placement
       - Add count to dnf_gained and dnf_gained_by_cp[cp]
     - Store both fields in the finisher's row
```

### Complexity

- Per year: O(finishers * checkpoints) after pre-grouping DNF runners
- DNF runners grouped by checkpoint once, then for each finisher we compare against each checkpoint's DNF list
- With ~13k finishers, ~3k DNF, ~10 checkpoints: manageable in build step

### Keymap

No manual registration needed. The `build_keymap()` function auto-detects all keys from rows. `dnf_gained` gets a short alias automatically. `dnf_gained_by_cp` is a nested object — it will need to either:
- Be stored as a JSON string in the row, or
- Be flattened to individual fields like `dnf_gained_cp_risberg`, `dnf_gained_cp_evertsberg`, etc.

**Decision: Flatten to individual fields.** This is consistent with how all other checkpoint data is stored (e.g., `risberg_placering`). Pattern: `dnf_gained_cp_{checkpoint_prefix}`. This keeps keymap compression working automatically.

Final field names per finisher:
```
dnf_gained                      # total count (integer)
dnf_gained_cp_hogsta_punkten    # count at Hogsta punkten (integer or absent)
dnf_gained_cp_smagan            # count at Smagan
dnf_gained_cp_mangsbodarna      # count at Mangsbodarna
dnf_gained_cp_risberg           # ...etc for each checkpoint except Mal
```

## Frontend Changes

### Location

Person detail view (`_renderPersonNew`), displayed per year result below the existing splits table.

### Component: DNF Gained Insight Box

Rendered only when:
- The person is a finisher (has `bruttotid`)
- `dnf_gained` > 0

#### Layout

```
┌─────────────────────────────────────────────┐
│  Gained 47 positions from DNF runners       │
│                                             │
│  Evertsberg  ████████████████  15           │
│  Risberg     ██████████████    12           │
│  Oxberg      ████████         8             │
│  Hokberg     ██████           7             │
│  Eldris      ███              3             │
│  Forvarning  ██               2             │
└─────────────────────────────────────────────┘
```

- Title: "Gained {N} positions from DNF runners"
- Horizontal bar chart using plain HTML/CSS (no Chart.js dependency for this)
- Bars sorted by count descending
- Checkpoint display names from `CHECKPOINTS` array (human-readable labels)
- Bar width proportional to the max count in the breakdown
- Compact styling consistent with existing stat cards in person view

### Styling

- Same card styling as existing person view components (white background, subtle border, rounded corners)
- Bar color: a muted accent color, consistent with the site's existing palette
- Responsive: bars stack naturally on narrow screens

## Edge Cases

| Case | Behavior |
|------|----------|
| DNF runner has no placement at their last checkpoint | Skip — cannot compare |
| Finisher has no placement at a checkpoint | Skip that checkpoint's DNF comparison |
| Mal/Finish checkpoint | Excluded — no one DNFs at the finish line |
| DNS (Startade inte) | Excluded entirely — never in the race |
| dnf_gained is 0 | Do not render the component |
| Person is DNF themselves | Do not render the component |
| Race has no DNF runners | No finisher gets the component |

## Files to Modify

1. **`scripts/build_site_data.py`** — Add DNF gained calculation after row flattening
2. **`site/index.html`** — Add rendering function and call it from person year detail view

## Out of Scope

- Career aggregation across years
- Showing this in the main results table
- Calculating how many positions a DNF runner "gave away"
