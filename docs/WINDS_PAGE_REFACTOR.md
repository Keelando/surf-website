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

## Stage 2 — Code Organization (TODO)

### 1. Function duplication across files
- `degreesToCardinal()` — still in `wind-stations.js` (winds-map.js copy removed, but chart-utils doesn't have it)
- `getDirectionalArrow()` — diverged implementations in `wind-stations.js` and `winds-map.js` (unused `arrowType` param vs `currentColor`)
- `setSafeHTML()` — duplicated in 11 JS files across the site
- `formatTimestamp()` / `formatTimeOnly()` — duplicated in 4 JS files
- Popup HTML generation — nearly identical in `addWindStationMarker()` and `addBuoyWindMarker()` in `winds-map.js` (~65 lines duplicated)

### 2. Oversized functions
- `loadWindTable()` — still ~250 lines, does 7 things (process stations, build source links, build short names, render table, init sorting, render offline list, update footer)
- `renderWindChart()` — ~190 lines
- `renderWind24HourTable()` — ~170 lines
- `addWindStationMarker()` and `addBuoyWindMarker()` — nearly identical ~100-line functions that should be unified

### 3. Hardcoded data that should be in config/stations.json
- `sourceLinks` object (37 lines of URLs)
- `shortNames` object
- Station type detection via ID prefix (`id.startsWith("4600")`, `id.startsWith("C")`, etc.) — fragile

### 4. Extract constants
- Stale/offline thresholds: `2` and `4` hours (used in 2 places each)
- Arrow sampling: `isMobile ? 6 : 3` hours, `maxArrows = isMobile ? 4 : 8`
- Popup animation delay: `300ms`
- Timestamp matching tolerance: `1800000` (30 min)

### 5. `window.*` global exports scattered throughout
- `viewStationChart`, `showStationOnMap`, `selectStationAndShowChart` manually attached to `window`
- `windsMap.focusStation` exported via `window.windsMap`
- Plan module boundaries and inter-file communication

### Recommended order
1. Extract shared utils — move `degreesToCardinal`, `getDirectionalArrow`, `fetchWithTimeout`, `setSafeHTML`, `formatTimestamp` to a shared module
2. Unify marker functions in `winds-map.js` — merge the two popup/marker builders
3. Move hardcoded mappings (`sourceLinks`, `shortNames`, flag logic) into `stations.json` or a config
4. Extract constants for thresholds, breakpoints, and magic numbers
5. Break up `loadWindTable()` and other oversized functions
6. Address `window.*` globals

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
- `arrowType` parameter in `getDirectionalArrow()` (wind-stations.js) never used

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
