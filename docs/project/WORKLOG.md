# Worklog

Completed-work history (formerly the "Completed" sections of
`docs/project/TODO.md`). Open work lives in the repo-root `TODO.md`;
known issues in `docs/KNOWN_ISSUES.md`. Newest first.

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
