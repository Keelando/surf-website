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

### Step 6. Extract constants (DONE 2026-04-11)
- `STALE_THRESHOLD_HOURS` (2) and `OFFLINE_THRESHOLD_HOURS` (4) in wind-stations.js
- `SCROLL_SETTLE_DELAY_MS` (500) in wind-stations.js, `MAP_FOCUS_DELAY_MS` (300) in winds-map.js
- `TOOLTIP_TIME_TOLERANCE_MS` (1800000 / 30 min) in wind-stations.js tooltip formatter
- `DEFAULT_VISIBLE_ROWS` (12) was already a local const — left as-is

### Step 7. Break up oversized functions (DONE 2026-04-11)
- Extracted `classifyStations(latestAll)` — station classification + stale/offline split from `loadWindTable()`
- Extracted `renderOfflineStationsList(offlineStations)` — offline callout box rendering from `loadWindTable()`
- Extracted `mergeTimeseriesByTime(fields)` — replaced 5 near-identical forEach loops in `renderWind24HourTable()`

### Step 8. Address `window.*` globals (DONE 2026-04-11)
- Removed `window.viewStationChart` and `window.showStationOnMap` — only called within `wind-stations.js`, no export needed
- Replaced `window.selectStationAndShowChart` with `winds:select-station` CustomEvent (winds-map.js dispatches, wind-stations.js listens)
- Replaced `window.windsMap.focusStation` with `winds:focus-station` CustomEvent (wind-stations.js dispatches, winds-map.js listens)
- Zero `window.*` exports remain between the two wind page modules

### Step 9. Review all Stage 2 changes (DONE 2026-04-11)
- Screenshots (light + dark): winds page renders correctly — table, map, chart, 24hr table, offline callout all intact
- Home page buoy charts verified — no regressions from chart-utils changes
- ESLint: 0 errors (39 warnings, all pre-existing unused-var warnings from non-module script scope)
- Playwright: 16/16 tests pass (Chromium + Firefox, all pages)

## Stage 3 — Polish (DONE 2026-04-13)

### Step 1. Extract inline styles to CSS (DONE 2026-04-13)
- Created `winds-v4.css` — all ~165 lines of inline `<style>` from `winds.html` moved to dedicated stylesheet
- Hardcoded `#e0e7ee` map border replaced with `var(--color-border-light)`
- Hardcoded `#ccc` input borders replaced with `var(--color-border)`
- All 4 `<section style="max-width...">` wrappers replaced with `.winds-section` class
- Inline styles on sort hint, station suggestion, map legend, station selector all replaced with CSS classes

### Step 2. Move inline styles from JS to CSS classes (DONE 2026-04-13)
- Map popup wind data card: replaced inline `style=` with `.popup-wind-card`, `.popup-wind-header`, `.popup-timestamp`, `.popup-station-details` classes
- Stale/fresh and buoy/station variants: `.popup-wind-card--stale`, `.popup-wind-card--fresh`, `.popup-wind-card--buoy`, `.popup-wind-card--station`
- Offline callout: replaced inline styles with `.offline-callout` class (responsive columns via CSS media query)
- Table action links: replaced inline styles with `.wind-table-action-link`, `.wind-table-action-separator` classes
- Toggle button cell: replaced 5 inline style assignments with `.wind-24hr-toggle-cell` class
- Table message cells: replaced inline `style="text-align: center; padding: 2rem;"` with `.table-message-cell` class
- Removed `view-data-btn` inline styles from `winds-map.js` — already styled via `stations-map-v4.css`

### Step 3. Mobile breakpoints review (DONE 2026-04-13)
- Reviewed: CSS `768px` (table column visibility) vs JS `600px` (chart font sizes, arrow density) vs `1000px` (chart grid spacing)
- These serve fundamentally different purposes — not a real inconsistency, left as-is

### Step 4. Event listener hygiene (DONE 2026-04-13)
- Removed `header.style.cursor = "pointer"` and `header.style.userSelect = "none"` from `initializeSortableTable()` — already handled by CSS `.sortable` rule
- Chart resize listener: page-level (lives for page lifetime), no cleanup needed

### Step 5. Accessibility (DONE 2026-04-13)
- `aria-sort="ascending|descending"` added to active sortable table headers, cleared on others
- `aria-pressed="true|false"` added to time range toggle buttons, updated on toggle
- `aria-expanded="true|false"` added to 24hr table collapse/expand button, updated on toggle
- `aria-hidden="true"` added to decorative SVG arrows (direction text already conveys the information)

### Step 6. Error UI for map failures (DONE 2026-04-13)
- `loadWindStationsAndMarkers()` now shows `.winds-map-error` overlay in map container on failure
- Message: "Unable to load station markers. Try refreshing the page."

### Step 7. Verification (DONE 2026-04-13)
- Screenshots (light + dark): winds page renders correctly — table, map, chart, 24hr table, offline callout all intact
- ESLint: 0 errors (39 warnings, all pre-existing unused-var warnings)
- Playwright: 16/16 tests pass (Chromium + Firefox, all pages)
