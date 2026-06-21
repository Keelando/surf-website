# TODO List

## Upcoming Tasks

### Back-to-Top Button (Mobile-first)

Small floating "scroll to top" button. Additive only — **do not** restructure or
rework the existing nav, and don't pull in any library. (NB: the original spec
said "don't make the nav sticky / don't add a hamburger" — that's stale; we
*did* go sticky + hamburger on 2026-06-21. Those constraints no longer apply;
just don't touch the nav for THIS task.) Reuse existing theme variables and
patterns. Self-contained markup + CSS + a small scroll listener, consistent with
the rest of the site.

**Behavior:**
- Small circular button, fixed to viewport, smooth-scrolls page to top on tap —
  back to the hero/conditions area. (Full nav is always reachable via the sticky
  hamburger; this is about returning to the top of long pages, not nav access.)
- Hidden by default; fades in after scrolling past ~1 viewport height (past the
  hero/conditions area). Fades out again near the top.
- Smooth scroll, not an instant jump (except reduced-motion, see below).
- Hide when scrolled to the very bottom (the duplicate bottom nav is right there).

**Placement / sizing:**
- Bottom-right, in the thumb zone, with margin from edges so it clears the
  phone's system gesture bar.
- Touch target >= ~44px.
- Must not overlap/obscure the gale/wind warning banner, bottom nav, or any
  interactive content.

**Visual:**
- Match site palette + light/dark theme (use existing theme vars) — should read
  as part of the site, not a bolted-on widget. Subtle shadow / semi-transparent
  bg so it stays legible over both the dark hero photo and lighter content.
- Upward arrow / clean "↑", no text label. Gentle fade/slide transition,
  nothing flashy.

**Accessibility:**
- Accessible label (e.g. "Back to top"). Keyboard-focusable, activates on click
  and keyboard. Respect `prefers-reduced-motion`: skip smooth scroll, just jump.
- Honor the no-`onclick`-property ESLint rule — use `addEventListener`.

**Priority:** Low-medium. Confirmed no global back-to-top exists today (only
section-scroll helpers in main.js/stations-map.js). Mobile nav is sticky now but
does NOT replace this (nav reach vs. return-to-top on long pages).

---

### Tighten Graph Margins (Buoys page + audit others)

Real estate is at a premium — the graphs need all the space they can get to
breathe.

- Start with the **buoys page** (`site/index.html` / `site/assets/js/main.js`
  charts): trim padding/margins around the ECharts plots (grid `left/right/top/
  bottom`, container padding, surrounding card margins).
- Then **inspect the other chart pages** (tides, winds, forecasts, storm surge,
  lightstations) for the same margin-reduction opportunities — apply where the
  layout allows without clipping axis labels / legends.
- Keep it consistent: prefer a shared pattern (e.g. `chart-utils-v4.js`) over
  per-page one-offs so the tightening doesn't drift between pages.

**Priority:** Medium.

---

### Dev Branch + Preview Subdomain

**Goal:** Use a `dev` branch for work-in-progress and serve it at `dev.halibutbank.ca` for visual QA before merging to production.

**Workflow:**
1. Work on `dev` branch, push freely
2. Preview changes at `dev.halibutbank.ca`
3. Open a quick PR when ready — scan the diff for anything weird
4. Merge to `main` → production updates

**Implementation:**
- DNS: Add A record for `dev.halibutbank.ca`
- Caddy: Add a second site block (auto-TLS via Let's Encrypt)
- Git worktree: `git worktree add ~/envcan_wave-dev dev` (no second clone needed)
- Symlink live data: `ln -s ~/envcan_wave/site/data ~/envcan_wave-dev/site/data`
- Backend scripts (parsers, exporters) stay on `main` — `dev` is frontend preview only

**Benefits:**
- Catch visual regressions before they hit production
- PR history gives a changelog of "what changed and why"
- 30-second self-review checkpoint without heavy process

**Priority:** Medium — not urgent, but a good safety net since the site is live at halibutbank.ca

---

### Lighthouse Performance Reports (Next Session)

Add Lighthouse performance auditing to monitor frontend performance and accessibility.

**Implementation ideas:**
- Automated Lighthouse CI reports for key pages (index, winds, tides, forecasts)
- Performance budgets and alerts
- Track metrics over time (performance, accessibility, best practices, SEO)
- Identify optimization opportunities

**Priority:** Medium (site works well, but good to monitor)

---

### Backend Data Audit (Future Task - Rainy Day Project)

**Goal:** Audit the backend data pipeline for errors and missed opportunities

**Tasks:**
- [ ] **Environment Canada XML audit**
  - Parse sample SWOB-ML XMLs to identify all available fields
  - Compare against fields currently being captured in `buoy_to_influx_sqlite.py` and `wind_to_sqlite.py`
  - Document fields we're currently ignoring/throwing away
  - Evaluate which additional fields would be useful (e.g., humidity, dewpoint, visibility for buoys)
  - Check if EC provides wave spectral data (period bands, directional spectra) that we're missing

- [ ] **NOAA data audit**
  - Review NOAA .txt and .spec file formats for additional fields
  - Check if we're missing any useful meteorological data
  - Verify all spectral wave components are being captured correctly

- [ ] **Error logging audit**
  - Review all parser logs for recurring errors or warnings
  - Check for data validation issues (malformed XMLs, unexpected values)
  - Identify stations with frequent data gaps or stale data
  - Look for silent failures in data processing

- [ ] **Database schema optimization**
  - Check for unused columns that could be removed
  - Identify missing indexes that would improve query performance
  - Consider adding data quality flags (e.g., sensor status, QC codes)

- [ ] **Data completeness report**
  - Generate statistics on data availability per station
  - Identify time gaps in historical data
  - Check if all expected fields are being populated

**Benefits:**
- Capture more useful data from existing sources
- Improve data quality and reliability
- Better understanding of system health

**Priority:** Low (system works well, but could be optimized)

---

---

### Known Issues (Not Currently Affecting Operation)

- **ECharts "connect nulls" behavior**
  - Charts may connect lines across data gaps in some scenarios
  - Not currently observed as a problem in production
  - Would require injecting explicit null values at gap timestamps if needed

---

## Completed (2026-03-19)

✅ **Automated Test Suite + Pre-Commit Hook**
  - 209 pytest tests covering lib/ modules, data transformations, XML parsing, integration
  - Pre-commit hook runs ruff + pytest + eslint on every commit
  - `npm run test` and `npm run test:python` convenience targets
  - Test files: test_units, test_directions, test_stations, test_config, test_downsample, test_timestamps, test_xml_parsing, test_integration

✅ **Jericho Wind Station Integration**
  - Fetch script, cron job, stations.json config all implemented and operational

---

## Completed (2026-03-14)

✅ **Dark Mode**
  - Full dark theme across all 8 pages with CSS variable system
  - Toggle in nav with localStorage persistence, default light
  - Map tiles intentionally stay light; markers use fixed colours
  - Screenshot pipeline for visual QA (light + dark × 8 pages)

✅ **Monorepo Merge**
  - Frontend merged into backend under `site/` (commit `b7a44e0`)
  - Single repo at `~/envcan_wave/`, no separate `~/site/`

---

## Completed (2026-01-15)

✅ **24-Hour Time Format Standardization**
  - Updated all frontend time displays to use strict 24-hour format
  - Fixed Leaflet map popups (stations-map.js, winds-map.js)
  - Fixed tide displays (tides.js, tides-modules/display.js, sunlight.js)
  - All times now Vancouver timezone-aware with DST handling

✅ **Mud Bay Webcam Interval Update**
  - Increased snapshot interval from 15 to 20 minutes to prevent duplicates
  - Updated crontab and frontend webcams.html

---

## Completed (2026-01-12)

✅ **Tide Page Refactoring**
  - Broke down 1886-line tides.js monolith into modules
  - Created tides-modules/ directory with focused components

✅ **Wave Direction Vectors on Map**
  - Directional arrows on buoy cards and map markers
  - Blue arrows for wave direction, red for wind
  - Shows direction data is coming FROM (meteorological convention)

✅ **Surrey Geodetic Tide Simplification**
  - Now using Surrey/FlowWorks pre-calculated tidal residual
  - Removed redundant geodetic offset calculations
  - Archived previous logic to `archive/geodetic-tide-corrections-2026-01-12`

✅ **Storm Surge Page Fixes**
  - Fixed "Invalid Date" on storm surge card
  - Added Surrey stations (Crescent Beach Ocean/Channel) to hindcast plot

✅ **Station Registry Enforcement**
  - All scripts now use `lib/stations.py` as single source of truth
  - Removed hardcoded station lists

---

## Completed (2026-01-02)

✅ **Current Time Indicator on Tide Charts**
  - Added prominent marker showing current tide level on "Today" view
  - Interpolates predicted tide at current time
  - Applies tide residual (observed - predicted) for improved accuracy
  - Even with 1-hour-old observations, residual provides better estimate than prediction alone
  - Red marker when residual available, orange when prediction-only
  - Tooltip shows breakdown: current tide, predicted value, residual offset
  - File: `/home/keelando/envcan_wave/site/assets/js/tides.js:1440-1527`

---

## Completed (2025-12-18)

✅ **White Rock Station ID Rename**
  - Renamed `whiterock_pier` → `whiterock_east` to match actual location
  - Updated backend: stations.json, export_wind_json.py, export_wind_24hr_timeseries.py, fetch_whiterock_weather.py
  - Updated frontend: wind-stations.js, webcams.html
  - Fixes confusing naming where station moved from pier to East Beach but kept old ID

---

## Completed (2025-11-18)

✅ **Buoy Card Refinements**
  - Wave height significant figures (1 sig fig, except Crescent Beach keeps 2 decimals)
  - Show latest available wave data with timestamp
  - History table date column cleanup
  - Add Crescent Beach Ocean (CRPILE) to wave comparison chart
  - "Hide Details" now collapses both details AND history sections

✅ **Tofino tide station added**
  - Added DFO IWLS station near Tofino/Ucluelet
  - Corresponding GDSPS storm surge location added
  - Enables hindcast validation for west coast exposure

✅ **Storm surge hindcast plot improvements**
  - Simplified legend to highlight observed surge (black line)
  - Added subtitle explaining colored lines are historical forecast runs
  - Reduced legend clutter

---

## Completed (2025-11-11)

✅ **Condensed buoy cards refactor**
  - Reduced default card height by 60% (~150px vs ~300-400px)
  - Compact single-line wind/wave display
  - Expandable "Show Details" button for full metrics
  - Expandable "Show History (24h)" with 12 hourly observations
  - Added `degreesToCardinal()` for wind direction in history table
  - History table sorted newest-first
  - Collapsible region groups (click header to collapse/expand)
  - Default: Strait of Georgia expanded, others collapsed
  - Fixed Surrey attribution: "Surrey (FlowWorks)"
  - Added max-width: 450px constraint on cards

✅ **Peak storm surge display on storm surge page**
  - Added prominent "Peak Today" card above forecast chart
  - Shows absolute peak (positive or negative) for current Pacific day
  - Displays value (±X.XX m) and time (24h format)
  - Updates when switching stations
  - Hidden if no data available

---

## Completed (2025-11-09)

✅ **Combined water level forecasts (tide + storm surge)**
  - Created `export_combined_water_level.py` to merge astronomical tide predictions with storm surge forecasts
  - Exports to `site/data/combined-water-level.json` (1.5MB covering next 2 days)
  - Added multi-series ECharts visualization on tides.html:
    - Astronomical Tide (blue solid line)
    - Observations (green dots, today only)
    - Storm Surge (orange dashed line)
    - Total Water Level (purple bold line - tide + surge)
  - Implemented day navigation (today, tomorrow, +2 days) with arrow buttons
  - Automated via cron (every 5 min + after storm surge updates)
  - Stations: Point Atkinson, Campbell River, Crescent Beach

---

## Completed (2025-11-06)

✅ **Storm Surge Forecast Setup Guide**
  - Created comprehensive documentation (`docs/STORM_SURGE_SETUP.md`)
  - Improves upon Environment Canada's incomplete documentation
  - Includes prerequisites, installation, configuration, automation
  - Detailed troubleshooting and verification procedures
  - Explains GDSPS model, WMS queries, and hindcast analysis

---

## Completed (2025-11-05)

✅ **Warning banner improvements**
  - Variable dismiss durations by severity (Storm: 12h, Gale: 12h, Strong Wind: 6h)
  - Dismissal feedback toast messages
  - Enhanced visual hierarchy (border thickness by severity)
  - Mobile sticky positioning
  - Improved accessibility (ARIA labels, roles, live regions)

✅ **Documentation reorganization**
  - Split 1135-line CLAUDE.md into focused docs (71% reduction)
  - Created `docs/` subdirectories following industry standards
  - Created COMMANDS.md, DEPLOYMENT.md, TROUBLESHOOTING.md, ARCHITECTURE_DETAILED.md
  - Created FRONTEND_CHANGELOG.md
  - Updated all cross-references

---

## Completed (2025-11-04)

✅ **Marine forecasts & warning banners**
  - Created dedicated forecasts page (`site/forecasts.html`)
  - Dismissible warning banners across all pages
  - localStorage-based state management (24h expiry)
  - Severity-based color coding (Storm/Gale/Strong Wind)
  - Mobile-optimized compact layout (50% height reduction)
  - Smooth scroll-to-zone navigation
  - Framework evaluation (decided to stay vanilla JS)

---

## Completed (2025-11-02)

✅ **UI/UX enhancements**
  - Directional arrows on buoy cards (wind/wave directions)
  - Navigation links (card → map/charts)
  - Tide page padding reduction (38-50%)
  - Station metadata display (badges, coordinates, DFO codes)
  - Wave breaking threshold annotations

---

## Completed (2025-11-01)

✅ **Station registry system**
  - Created master `stations.json` with all station metadata
  - Unified buoy and tide station data
  - Interactive Leaflet map embedded on index.html
  - Color-coded markers (EC buoys, NOAA buoys, DFO tides)
  - Click markers to center map and view details

---

## Completed (2025-10-31)

✅ Implemented tide monitoring page (`site/tides.html`)
  - Station selector dropdown
  - Auto-loads Point Atkinson by default
  - Current observation and prediction display
  - High/low tide table
  - 28-hour ECharts tide prediction chart
  - Responsive design (mobile, tablet, desktop)

✅ Fixed chart horizontal compression issues
  - Standardized 1200px max-width across all chart sections
  - Moved inline styles to CSS files
  - Fixed grid layout with percentage-based margins

✅ Updated documentation (README.md and CLAUDE.md)
  - Added tide system architecture
  - Documented new scripts and cron jobs
  - Added frontend structure documentation


---

## UI/UX Improvements (Future)

### Mobile ECharts Issues
- [ ] Fix cursor/touch behavior on mobile for all ECharts plots
  - Currently "funky" - investigate specific issues
  - Affects: tide charts, buoy charts, wind charts, lightstation charts
  - May need to adjust ECharts touch/tooltip configuration
  - Test on actual mobile devices after fixing

---

