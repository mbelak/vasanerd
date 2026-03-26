# DNF Gained Positions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show each finisher how many placement positions they gained because runners ahead of them dropped out (DNF), displayed in the person detail view with a per-checkpoint breakdown.

**Architecture:** Pre-compute `dnf_gained` and `dnf_gained_cp_{checkpoint}` fields in `build_site_data.py` after row flattening. Display in the frontend person view as a compact insight box with horizontal bars below the splits table.

**Tech Stack:** Python (build pipeline), vanilla JS/HTML/CSS (frontend)

---

### Task 1: Add DNF gained computation to build_site_data.py

**Files:**
- Modify: `scripts/build_site_data.py:554-581` (in `main()`, after `compute_missing_placements` and before keymap building)

**Spec reference:** See `docs/superpowers/specs/2026-03-26-dnf-gained-positions-design.md` — Algorithm section.

- [ ] **Step 1: Add the `compute_dnf_gained` function**

Add this function after `compute_missing_placements` (after line 145) in `build_site_data.py`:

```python
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
                and r.get("status") not in ("Startade inte", "Did not start", "DNS")]

    # For each DNF runner, find their last checkpoint and placement there
    # Group by checkpoint: {cp_prefix: [(placement_int, dnf_row), ...]}
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
            import bisect
            count = bisect.bisect_left(dnf_by_cp[prefix], finisher_plac)
            if count > 0:
                r[f"dnf_gained_cp_{prefix}"] = str(count)
                total_gained += count
        if total_gained > 0:
            r["dnf_gained"] = str(total_gained)
```

- [ ] **Step 2: Move the `import bisect` to the top of the file**

Add `import bisect` at the top of `build_site_data.py` (after `import math` on line 18), and remove the inline `import bisect` from inside the function:

```python
import math
import bisect
```

In the function, remove the line `import bisect` from inside the loop.

- [ ] **Step 3: Call `compute_dnf_gained` in `main()`**

In `main()`, after the `compute_missing_placements` loop (after line 581) and before the keymap building (line 584), add:

```python
    # 1c. Compute DNF gained positions for each finisher
    for year, rows in all_rows.items():
        if rows:
            compute_dnf_gained(rows, checkpoints)
```

- [ ] **Step 4: Run the build to verify**

Run for vasaloppet to check output:

```bash
cd /Users/martinbelak/dev/vasanerd && python3 scripts/build_site_data.py --race vasaloppet
```

Expected: builds successfully, no errors. The new fields will be auto-included in keymap and person shards.

- [ ] **Step 5: Verify the data looks correct**

Quick check that the fields exist and have reasonable values:

```bash
cd /Users/martinbelak/dev/vasanerd && python3 -c "
import json
with open('site/data/vasaloppet/_keymap.json') as f:
    km = json.load(f)
reverse = {v:k for k,v in km.items()}
print('dnf keys in keymap:', [v for k,v in km.items() if 'dnf_gained' in v])
# Check a person shard for dnf_gained
with open('site/data/vasaloppet/p/00.json') as f:
    shard = json.load(f)
for idpe, rows in list(shard.items())[:3]:
    for r in rows:
        expanded = {reverse.get(k,k): v for k,v in r.items()}
        if expanded.get('dnf_gained'):
            print(f'{idpe} ({expanded.get(\"ar\")}): dnf_gained={expanded[\"dnf_gained\"]}')
            for ek,ev in expanded.items():
                if ek.startswith('dnf_gained_cp_'):
                    print(f'  {ek}={ev}')
            break
"
```

Expected: at least some persons have `dnf_gained` values, and `dnf_gained_cp_*` fields exist.

- [ ] **Step 6: Commit**

```bash
git add scripts/build_site_data.py
git commit -m "feat: compute DNF gained positions per finisher in build pipeline"
```

---

### Task 2: Render DNF gained insight box in person view

**Files:**
- Modify: `site/index.html:4862-4890` (person year detail rendering area)

**Spec reference:** See design doc — Frontend Changes section.

- [ ] **Step 1: Add the `_renderDnfGainedBox` function**

Add this function after `_renderPersonSplitsTable` (after line 5044) in `site/index.html`:

```javascript
function _renderDnfGainedBox(r) {
  const total = parseInt(r.dnf_gained);
  if (!total || isNaN(total)) return '';
  // Collect per-checkpoint breakdown
  const entries = [];
  for (const cp of CHECKPOINTS) {
    if (cp.label === 'Finish' || cp.label === 'Mål') continue;
    const val = parseInt(r['dnf_gained_cp_' + cp.key]);
    if (val > 0) entries.push({ label: cp.label, count: val });
  }
  entries.sort((a, b) => b.count - a.count);
  const maxCount = entries.length ? entries[0].count : 1;
  const barsHtml = entries.map(e => {
    const pct = Math.round((e.count / maxCount) * 100);
    return `<div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.25rem">
      <span style="min-width:100px;font-size:0.8rem;text-align:right;color:var(--text-dim)">${escapeHtml(e.label)}</span>
      <div style="flex:1;height:18px;background:var(--bg-subtle);border-radius:3px;overflow:hidden">
        <div style="height:100%;width:${pct}%;background:var(--accent,#3b82f6);border-radius:3px;min-width:2px"></div>
      </div>
      <span style="min-width:28px;font-size:0.8rem;font-weight:600">${e.count}</span>
    </div>`;
  }).join('');
  return `<div class="card" style="margin-bottom:0.75rem">
    <h3 style="margin-bottom:0.5rem">Gained ${total} position${total !== 1 ? 's' : ''} from DNF runners</h3>
    ${barsHtml}
  </div>`;
}
```

- [ ] **Step 2: Insert the DNF gained box into the person year detail**

In the `container.innerHTML` template (around line 4877), add `${dnfGainedHtml}` after `${splitsHtml}`. First, generate it before the template:

Find this line (around line 4862):
```javascript
  const splitsHtml = `<div class="card" id="person-splits-card" style="margin-bottom:0.75rem">
```

Add before it:
```javascript
  const dnfGainedHtml = hasFinish ? _renderDnfGainedBox(r) : '';
```

Then in the `container.innerHTML` template, after `${splitsHtml}` (line 4886), add:
```javascript
    ${dnfGainedHtml}
```

So the template becomes:
```javascript
  container.innerHTML = `
    ${pmInfo}
    ${hasFinish ? `<div class="card" style="margin-bottom:0.75rem"><h3>Finish Time Distribution (${yr})</h3><div class="chart-container-tall"><canvas id="chart-person-histogram"></canvas></div></div>` : ''}
    ${segHtml}
    ${pacHtml}
    <div class="grid grid-2" style="margin-bottom:0.75rem">
      <div class="card"><h3>Speed per Segment (${yr})</h3><div class="chart-container-tall"><canvas id="chart-person-speed-single"></canvas></div></div>
      <div class="card"><h3>Placement per Checkpoint (${yr})</h3><div class="chart-container-tall"><canvas id="chart-person-place-single"></canvas></div></div>
    </div>
    ${splitsHtml}
    ${dnfGainedHtml}
  `;
```

- [ ] **Step 3: Verify in browser**

Open the site locally and navigate to a person who finished a race. The DNF gained box should appear below the splits table with horizontal bars showing the breakdown.

Things to check:
- Box appears for finishers with DNF gained > 0
- Box does NOT appear for DNF runners
- Bars are sorted by count descending
- Checkpoint labels are human-readable
- Styling matches existing card components

- [ ] **Step 4: Commit**

```bash
git add site/index.html
git commit -m "feat: show positions gained from DNF runners in person view"
```

---

### Task 3: Rebuild all race data

**Files:** No code changes — just running the build pipeline.

- [ ] **Step 1: Rebuild all races that have data**

```bash
cd /Users/martinbelak/dev/vasanerd
python3 scripts/build_site_data.py --race vasaloppet
python3 scripts/build_site_data.py --race tjejvasan
python3 scripts/build_site_data.py --race ultravasan
python3 scripts/build_site_data.py --race oppet_spar_mandag
python3 scripts/build_site_data.py --race oppet_spar_sondag
```

Only run for races that have progress data files. Skip races that error with "No rows loaded".

- [ ] **Step 2: Commit rebuilt data**

```bash
git add site/data/
git commit -m "chore: rebuild site data with DNF gained positions"
```
