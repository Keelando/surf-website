# Maintainability & Readability Audit — 2026-07-14

**Status:** Findings documented; work not started
**Plan:** Address in decreasing priority order, starting next session
**Context:** Site is feature-complete apart from a few planned additions (pressure/fronts
page, webcam ML dataset). Priority is shifting from features to maintainability.

The Python backend is in good shape (`lib/config.py` well-adopted, crontab has a single
validated source, pipeline scripts reasonably sized). The bottlenecks are concentrated
in the frontend JS.

---

## Priority 1 — Frontend JS: duplication with no sharing mechanism

The site mixes two module systems: the tides page uses ES modules
(`tides-refactored.js` + `tides-modules/`), while every other page uses classic global
scripts — `index.html` loads **19 script tags in a hand-maintained order**. Because the
non-tides files can't import anything, helpers get copy-pasted instead of shared:

- `createDirectionalMarker` exists **three times** (`stations-map.js:332`,
  `winds-map.js:22`, `lightstation-map.js:57`), each drifting slightly (different
  parameters, stale-handling).
- `createAngularSpreadVector` duplicated between `main.js:87` and `webcams-v4.js`.
- Time formatting reimplemented in ≥4 files (`formatTime`/`formatTimestamp` in
  `forecasts.js`, `webcams-v4.js`, `tides-modules/utils.js`, `chart-utils-v4.js`,
  `lightstation-charts.js`) with *different* formats and timezones — a subtle
  inconsistency bug waiting to happen.
- Stale-styling logic (colors, "STALE" header text) is repeated twice within
  `stations-map.js` alone (~line 526 and ~850) and again in the other map files.

**Fix:** converge on ES modules site-wide and extract a shared `utils` module
(formatting, staleness, marker/arrow SVG). Single highest-leverage refactor — it
eliminates most duplication and the fragile load-order dependency at once. The tides
modules prove the pattern already works in this codebase. Do incrementally, page by
page (tides is already done; each page conversion is independently shippable).

## Priority 2 — Oversized functions

- `loadBuoyData()` in `main.js` is **~728 lines** (lines 202–930). This is the
  already-planned buoy-card refactor (make cards metadata-driven).
- `storm_surge_page.js` has near-twin ~300-line functions `updateForecastChart` and
  `updateHindcastChart` — should be one parameterized function.
- Milder cases of fetch + transform + build-HTML all in one body:
  `webcams-v4.js` `createWebcamCard` (155 lines), `wind-stations.js`
  `renderWindChart` (159 lines).

## Priority 3 — No unit tests for frontend logic

Tests are Playwright e2e only (console errors, a11y, screenshots) — 16 tests. None of
the formatting, freshness, or direction/marker logic has unit coverage, which makes
Priorities 1–2 riskier than they need to be.

**Fix:** add unit tests *as part of* the helper extraction in Priority 1 (extracted
pure functions are trivially testable), not as a separate up-front effort.

## Priority 4 — Manual cache busting

Version strings are a mix of `?v=1`, `?v=2`, `?v=3`, and `?v=YYYYMMDD` dates across
9 HTML files, bumped by hand (recurring chore, easy to forget).

**Fix:** a ~30-line script that rewrites `?v=` from each file's content hash, run via
pre-commit hook or npm script. Pairs naturally with the ES-module migration since both
touch every `<script>` tag.

## Priority 5 — Repo hygiene (quick wins, can be done any time)

- `archive/` has **56 tracked files** of dead migration code, including full duplicates
  of live files (`validate_stations.py`, a second `config.py`). Git history preserves
  them; they pollute grep results and lint runs. Delete the directory.
- `validate_stations.py` sits at repo root while similar tooling lives in `scripts/` —
  move or delete.
- Root clutter: `lint-js.log`, `ruff.err/log/out`, `pytest.log` at root — gitignore
  and point tools at `logs/`.
- Naming drift: `-v4` suffixes with no other versions in existence;
  `storm_surge_page.js` (snake_case) vs `wind-stations.js` (kebab-case). Cheap to fix
  during the ES-module migration since imports get rewritten anyway.
- Python stragglers hardcoding `~/.local/share/*.sqlite` instead of using
  `lib/config.py` (5 files): `scripts/fetch/fetch_whiterock_weather.py`,
  `scripts/parse/wind_to_sqlite.py`, `scripts/export/export_wind_24hr_timeseries.py`,
  `scripts/export/export_hindcast_json.py`, `scripts/migrate_wind_direction_field.py`.

## Deferred / not worth it now

- `health_check.py` (836 lines) — largest Python file; split eventually, not urgent.
- Export-script SQLite boilerplate consolidation — already assessed LOW VALUE in
  NEXT_SESSION.md tech-debt list; scripts have legitimate differences.
- HTML `<head>` boilerplate duplication across 9 pages — known trade-off of the
  no-build-step architecture; nav is already JS-injected (single source). Revisit only
  if a build step arrives via Priority 4.

---

## Why this order

Priorities 1–2 are the ones that pay off when the pressure-fronts page gets built —
a new page currently means re-copying the marker/formatting/staleness code yet again.
Priority 3 rides along with 1. Priority 4 removes a recurring manual chore. Priority 5
is filler for spare minutes.
