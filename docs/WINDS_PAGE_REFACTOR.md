# Winds Page Refactor Plan

Audit performed 2026-04-07, updated 2026-04-08. Covers `site/winds.html`, `site/assets/js/wind-stations.js`, `site/assets/js/winds-map.js`, `site/assets/js/wind-data.js`, and `site/assets/js/chart-utils-v4.js`.

## Stage 1 — Data Layer (DONE 2026-04-08)

### Unified data fetching
- Created `wind-data.js` — singleton `WindDataStore` that fetches all 5 JSON files once via `Promise.all`, normalizes buoy data to wind station format at load time
- `wind-stations.js` and `winds-map.js` now `await window.windData.ready` instead of fetching independently
- Network requests reduced from 8 → 5
- Retry-polling hack in `selectStationAndShowChart()` replaced with clean `await`

### Data normalization
- All `isBuoy` format branching eliminated from `filterWindTimeseriesData`, `renderWind24HourTable`, `renderWindChart`, and `loadWindStationsAndMarkers`
- Buoy latest data normalized to wind station field names (`wind_speed` → `wind_speed_kt`, `air_temp` → `air_temp_c`, etc.)
- Buoy timeseries unwrapped from `{data: [...]}` to flat `[...]` format
- Removed duplicate `fetchWithTimeout` and `degreesToCardinal` from `winds-map.js`

### UX improvements (also done in this session)
- Moved "Updated" column to second position (after Station name) in main table
- Time-first formatting: "14:00 4/8" instead of "4/8 14:00"
- Mobile shows time-only (no date) in both tables
- Added station name heading above the 24-hour data table
- Improved alternating row contrast (zebra stripe alpha: 0.03→0.06 light, 0.02→0.05 dark)

## Stage 2 — Code Organization

### Step 1. Extract shared utils (DONE 2026-04-09)
- Moved `degreesToCardinal`, `getDirectionalArrow`, `formatTimestamp`, `formatTimeOnly` to `chart-utils-v4.js`
- Consolidated `degreesToCompass()` (wind-chart-v4.js, wave-chart-v4.js) into `degreesToCardinal()`
- Fixed dark-mode bug: arrow color now uses `var(--color-primary-dark, #004b7c)` instead of hardcoded `#004b7c`
- Removed duplicates from `wind-stations.js`, `winds-map.js`, `main.js`
- Updated `comparison-chart-v4.js` call site
- `setSafeHTML()` left in place (site-wide concern, not winds-specific)

### Step 2. Unify marker functions (DONE 2026-04-09)
- Merged `addWindStationMarker()` + `addBuoyWindMarker()` into single `addWindMarker(station, currentData, isBuoy)`
- Extracted `getStationTypeLabel()` helper
- ~200 lines → ~110 lines in `winds-map.js`

### Step 3. Unify arrow data functions (DONE 2026-04-09)
- Unified `createWindDirectionArrows()` (wind-stations.js) and `createWindDirectionArrowData()` (wind-chart-v4.js) into single `createWindDirectionArrowData()` in `chart-utils-v4.js`
- Kept the sparse-data-aware sampling from wind-stations.js version + `colorOverride` param from wind-chart-v4.js version
- Accepts both `{time, value}` objects and raw values for speed/gust arrays

### Step 4. Clarify renderWindChart implementations (DONE 2026-04-09)
- Renamed wind-chart-v4.js version to `renderBuoyWindChart()` (used on index/buoy page)
- Updated call site in `charts-v4.js`
- Improved wind-stations.js `renderWindChart()` to use shared chart-utils patterns: `sanitizeSeriesData()`, `getResponsiveGridConfig()`, `formatCompactTimeLabel`, responsive title font, dark-mode area opacity, `try/catch` with `showChartError`

### Step 5. Move hardcoded mappings to config (DONE 2026-04-09)
- Added `source_url`, `short_name`, `flag` fields to `config/stations.json` for all wind and buoy stations
- Replaced ~70 lines of hardcoded `sourceLinks` object, `shortNames` object, and flag if/else logic with `getStationMeta(id)` lookup
- New stations only need config changes — no JS edits required

### Step 6. Extract constants (TODO)
- Stale/offline thresholds: `2` and `4` hours (used in 2 places each)
- Popup animation delay: `300ms` and `500ms` in winds-map.js / wind-stations.js
- Timestamp matching tolerance: `1800000` (30 min) in tooltip formatters
- Default visible rows: `12`

### Step 7. Break up oversized functions (TODO)
- `loadWindTable()` — still ~200 lines, does: process stations, render table, init sorting, render offline list, update footer
- `renderWind24HourTable()` — ~170 lines (includes 5 near-identical forEach loops for the dataByTime merge)

### Step 8. Address `window.*` globals (TODO)
- `viewStationChart`, `showStationOnMap`, `selectStationAndShowChart` manually attached to `window`
- `windsMap.focusStation` exported via `window.windsMap`
- Plan module boundaries and inter-file communication

### Step 9. Review all Stage 2 changes (TODO)
- Visual QA: take screenshots (light + dark) and compare against pre-refactor baseline
- Run full test suite and lint
- Verify no regressions on index page (buoy charts), winds page (table, map, chart, 24hr table), and other pages that share chart-utils
- Check mobile rendering (table short names, responsive charts, arrow sampling)
- Confirm source links, flags, and short names render correctly from config
- Verify dark mode arrow colors fixed across all pages

## Stage 3 — Polish (TODO)

### 1. ~165 lines of inline `<style>` in winds.html
- Table styles, time range buttons, mobile overrides, collapsed rows — move to CSS

### 2. Inconsistent mobile breakpoints
- `768px` in CSS, `600px` in JS arrow sampling, `600px`/`1000px` in chart-utils

### 3. Inline styles in JS
- Popup HTML in `winds-map.js`
- Offline stations list in `wind-stations.js`
- Repeated section wrapper in `winds.html` (4 occurrences)

### 4. Hardcoded colors in HTML
- `#e0e7ee` on map div border, `#ccc` on search input and dropdown

### 5. Dead code
- `arrowType` parameter in `getDirectionalArrow()` — resolved in Stage 2 Step 1 (moved to chart-utils with both wind+wave support)

### 6. Event listener hygiene
- Chart resize listener never cleaned up
- cursor/userSelect set in JS despite already being in CSS

### 7. Accessibility gaps
- Sortable headers lack `aria-sort`
- Time range buttons lack `aria-pressed`
- Collapse toggle lacks `aria-expanded`
- SVG arrows have no screen reader text

### 8. No error UI for map failures
- `loadWindStationsAndMarkers()` catches errors with only `console.error`
