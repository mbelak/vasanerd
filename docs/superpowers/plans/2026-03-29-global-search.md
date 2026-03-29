# Global Search Implementation Plan (v2 — Client-Side Only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users toggle "Search all races" to find a person across all races and see a minimal global profile with links to race-specific results.

**Architecture:** No build script needed. The frontend loads all 7 race-specific `persons.json` files (~6 MB total) on demand when the user toggles global search. Results are merged client-side by name + nationality matching. The toggle lives on the existing overview search input.

**Tech Stack:** Vanilla JS (frontend only), existing project patterns.

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `site/index.html` | Toggle UI, global data loading, merged search, global profile view |

---

### Task 1: Toggle UI and Global Data Loading

**Files:**
- Modify: `site/index.html`

- [ ] **Step 1: Add toggle checkbox HTML**

Find line 621 (`<div class="search-results" id="overview-person-search-results"></div>`) and add after it, before the closing `</div>` of `.search-input-wrap`:

```html
<label id="global-search-label" style="display:flex;align-items:center;gap:6px;margin-top:8px;font-size:0.85rem;color:var(--text-secondary);cursor:pointer;user-select:none">
  <input type="checkbox" id="global-search-toggle"> Search all races
</label>
```

- [ ] **Step 2: Add global persons loading function**

Find line 1289 (after the closing `}` of `_loadPersonsIndex`) and add:

```javascript
// Global search: load all races' persons.json and merge by name+nationality
let _globalSearchEntries = null;
let _globalSearchLoading = false;

async function _loadAllRacePersons() {
  if (_globalSearchEntries) return _globalSearchEntries;
  if (_globalSearchLoading) return null;
  _globalSearchLoading = true;
  const races = Object.keys(RACE_CONFIGS);
  const allPersons = {};
  await Promise.all(races.map(async race => {
    try {
      const resp = await fetch(DATA_BASE + '/' + race + '/persons.json?' + _cacheBust);
      if (resp.ok) allPersons[race] = await resp.json();
    } catch (e) { /* skip unavailable races */ }
  }));
  // Merge by name (exact match on namn field)
  const byName = {};
  for (const [race, persons] of Object.entries(allPersons)) {
    for (const [idpe, info] of Object.entries(persons)) {
      const key = info.namn;
      if (!key) continue;
      if (!byName[key]) byName[key] = { namn: key, races: {} };
      byName[key].races[race] = { idpe, years: info.years };
    }
  }
  // Build searchable entries array
  _globalSearchEntries = Object.values(byName).map(entry => {
    const raw = entry.namn;
    const surname = raw.split(',')[0].trim().toLowerCase();
    const full = fullName(raw).toLowerCase();
    const first = full.split(' ')[0];
    const raceList = Object.entries(entry.races).map(([r, d]) => ({
      race: r, idpe: d.idpe, years: d.years
    }));
    return { namn: raw, surname, full, first, races: raceList };
  });
  _globalSearchLoading = false;
  return _globalSearchEntries;
}
```

- [ ] **Step 3: Verify data loads in browser console**

Open site, run in console:
```javascript
fetch('data/vasaloppet/persons.json').then(r=>r.json()).then(d=>console.log('vasaloppet:', Object.keys(d).length))
```
Expected: Logs person count.

- [ ] **Step 4: Commit**

```bash
git add site/index.html
git commit -m "feat: add global search toggle and all-race data loader"
```

---

### Task 2: Global Search Logic in _wirePersonSearch

**Files:**
- Modify: `site/index.html`

- [ ] **Step 1: Modify renderOverviewPersonSearch to wire global toggle**

Replace the `renderOverviewPersonSearch` function (line 4680-4688) with:

```javascript
// Overview page search — navigates to person tab, supports global search toggle
function renderOverviewPersonSearch() {
  const input = document.getElementById('overview-person-search');
  const results = document.getElementById('overview-person-search-results');
  const globalToggle = document.getElementById('global-search-toggle');
  if (!input) return;

  // Wire the standard per-race search
  _wirePersonSearch(input, results, (idpe) => navigateToPerson(idpe), '#overview .search-input-wrap');

  // Wire global toggle
  if (globalToggle) {
    globalToggle.onchange = async () => {
      if (globalToggle.checked) {
        input.placeholder = 'Loading all races...';
        await _loadAllRacePersons();
        input.placeholder = 'Search all races...';
      } else {
        input.placeholder = 'Find your result...';
      }
      // Re-trigger search if there's a query
      const q = input.value.trim();
      if (q.length >= 2 && input.oninput) input.oninput();
    };
  }
}
```

- [ ] **Step 2: Modify _wirePersonSearch to accept global mode**

The function `_wirePersonSearch` (line 4600) needs to check global toggle state and use global entries when active. Modify the `showResults` inner function.

At the top of `_wirePersonSearch`, after building `entries` (line 4602-4608), modify `showResults` to switch data source:

Replace the existing `showResults` function (lines 4627-4654) with:

```javascript
  function showResults(query) {
    const words = query.toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return;
    const globalToggle = document.getElementById('global-search-toggle');
    const isGlobal = globalToggle && globalToggle.checked && _globalSearchEntries;
    const searchEntries = isGlobal ? _globalSearchEntries : entries;
    const scored = [];
    for (const e of searchEntries) {
      const s = scoreMatch(e, words);
      if (s >= 0) scored.push({ e, s });
      if (scored.length > 200) break;
    }
    scored.sort((a, b) => a.s - b.s || a.e.full.length - b.e.full.length);
    const matches = scored.slice(0, 15);
    if (!matches.length) { results.innerHTML = '<div class="search-result-item" style="color:var(--text-dim)">No results</div>'; results.classList.add('open'); return; }

    if (isGlobal) {
      results.innerHTML = matches.map(({ e }) => {
        const nat = getNat(e.namn);
        const raceSummary = e.races.map(r => {
          const rc = RACE_CONFIGS[r.race];
          const name = rc ? rc.name : r.race;
          return name + '\u00A0(' + r.years.length + ')';
        }).join(' · ');
        return '<div class="search-result-item search-result-global" style="cursor:pointer">'
          + flagHtml(nat) + escapeHtml(fullName(e.namn))
          + ' <span style="color:var(--text-dim);font-size:0.7rem;margin-left:auto;white-space:nowrap">'
          + escapeHtml(raceSummary) + '</span></div>';
      }).join('');
      results.classList.add('open');
      results.querySelectorAll('.search-result-global').forEach((el, i) => {
        el.onclick = () => {
          input.value = '';
          results.classList.remove('open');
          _showGlobalProfile(matches[i].e);
        };
      });
    } else {
      results.innerHTML = matches.map(({ e }) => {
        const nat = getNat(e.raw);
        const sorted = e.info.years.sort((a,b) => a - b);
        const yrs = sorted.length > 3 ? `${sorted.length}× (${sorted[0]}–${sorted[sorted.length-1]})` : sorted.join(', ');
        return `<div class="search-result-item" data-idpe="${escapeHtml(e.idpe)}">${flagHtml(nat)}${escapeHtml(fullName(e.raw))} <span style="color:var(--text-dim);font-size:0.75rem;margin-left:auto;white-space:nowrap">${escapeHtml(yrs)}</span></div>`;
      }).join('');
      results.classList.add('open');
      results.querySelectorAll('.search-result-item[data-idpe]').forEach(el => {
        el.onclick = () => {
          const idpe = el.dataset.idpe;
          input.value = '';
          results.classList.remove('open');
          onSelect(idpe);
        };
      });
    }
  }
```

- [ ] **Step 3: Test in browser**

1. Open site, type a name → standard per-race results.
2. Check "Search all races" → loading indicator → search shows race badges.
3. Uncheck → back to normal.

- [ ] **Step 4: Commit**

```bash
git add site/index.html
git commit -m "feat: global search mode with race badges in results"
```

---

### Task 3: Global Profile View

**Files:**
- Modify: `site/index.html`

- [ ] **Step 1: Add _showGlobalProfile function**

Add this function right after the `_loadAllRacePersons` function (added in Task 1):

```javascript
function _showGlobalProfile(entry) {
  // Switch to person tab
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  const personTab = document.querySelector('.tab[data-view="person"]');
  if (personTab) personTab.classList.add('active');
  const personView = document.getElementById('person');
  if (personView) personView.classList.add('active');

  const container = document.getElementById('person-content');
  if (!container) return;

  const nat = getNat(entry.namn);
  let html = '<div style="max-width:600px;margin:0 auto;padding:1.5rem 0">';
  html += '<h2 style="margin:0 0 0.25rem 0;font-size:1.4rem">' + flagHtml(nat) + ' ' + escapeHtml(fullName(entry.namn)) + '</h2>';
  html += '<p style="color:var(--text-dim);font-size:0.85rem;margin:0 0 1.5rem 0">'
    + entry.races.length + (entry.races.length === 1 ? ' race' : ' races') + '</p>';

  // Sort races by total years descending
  const sortedRaces = [...entry.races].sort((a, b) => b.years.length - a.years.length);

  for (const r of sortedRaces) {
    const rc = RACE_CONFIGS[r.race];
    const raceName = rc ? rc.name : r.race;
    const sortedYears = [...r.years].sort((a, b) => b - a);
    html += '<div style="margin-bottom:1.2rem">';
    html += '<div style="font-weight:600;margin-bottom:6px;font-size:0.95rem">' + escapeHtml(raceName)
      + ' <span style="color:var(--text-dim);font-weight:400">(' + sortedYears.length + ')</span></div>';
    html += '<div style="display:flex;flex-wrap:wrap;gap:6px">';
    for (const y of sortedYears) {
      html += '<a href="#" class="global-profile-year" data-race="' + r.race
        + '" data-idpe="' + r.idpe + '"'
        + ' style="padding:4px 12px;border-radius:4px;background:var(--bg-secondary);'
        + 'font-size:0.85rem;text-decoration:none;color:var(--text-primary);'
        + 'transition:background 0.15s"'
        + ' onmouseover="this.style.background=\'var(--accent)\';this.style.color=\'#fff\'"'
        + ' onmouseout="this.style.background=\'var(--bg-secondary)\';this.style.color=\'var(--text-primary)\'"'
        + '>' + y + '</a>';
    }
    html += '</div></div>';
  }
  html += '</div>';

  container.innerHTML = html;

  // Wire year links
  container.querySelectorAll('.global-profile-year').forEach(a => {
    a.onclick = async (ev) => {
      ev.preventDefault();
      const race = a.dataset.race;
      const idpe = a.dataset.idpe;
      if (race !== currentRace) {
        switchRace(race);
        // Re-load persons index for the new race before rendering person
        await _loadPersonsIndex();
      }
      renderPerson(idpe);
    };
  });
}
```

- [ ] **Step 2: Test the global profile**

1. Toggle "Search all races" on.
2. Search a name → click a result → global profile appears in person tab.
3. Profile shows races sorted by number of years, each with year badges.
4. Click a year badge → switches to that race, loads person detail view.

- [ ] **Step 3: Commit**

```bash
git add site/index.html
git commit -m "feat: add global profile view with race/year navigation"
```

---

### Task 4: End-to-End Testing

**Files:** No file changes — verification only.

- [ ] **Step 1: Test normal search is unaffected**

1. Open site with Vasaloppet selected.
2. Search for a name → results show per-race as before (no race badges).
3. Click result → person detail view loads normally.

- [ ] **Step 2: Test global search flow**

1. Check "Search all races" toggle.
2. Search "Eriksson" → results show with race badges.
3. Find a person with multiple races → click → global profile.
4. Profile lists races with year counts.
5. Click a year in a different race → race switches, person loads.

- [ ] **Step 3: Test edge cases**

1. Toggle on → search → toggle off → results revert to per-race.
2. Switch race via nav while toggle is on → search still works.
3. Toggle on with empty search → no error.
4. Search a name that only exists in one race → global result still works, profile shows single race.

- [ ] **Step 4: Commit any final fixes**

```bash
git add site/index.html
git commit -m "fix: polish global search integration"
```
(Only if fixes were needed.)
