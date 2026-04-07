# Winds Page Refactor Plan

Audit performed 2026-04-07. Covers `site/winds.html`, `site/assets/js/wind-stations.js`, `site/assets/js/winds-map.js`, and `site/assets/js/chart-utils-v4.js`.

## High Priority

### 1. Function duplication across 3 files
- `degreesToCardinal()` — exists in `wind-stations.js`, `winds-map.js`, and `chart-utils-v4.js`
- `fetchWithTimeout()` — exists in `wind-stations.js` and `winds-map.js` (chart-utils has a better version with retry logic)
- `getDirectionalArrow()` — similar implementations in `wind-stations.js` and `winds-map.js`
- Popup HTML generation — nearly identical in `addWindStationMarker()` and `addBuoyWindMarker()` in `winds-map.js` (~65 lines duplicated)

### 2. Oversized functions
- `loadWindTable()` — 314 lines, does 9 things (fetch, process wind stations, process buoys, build source links, build short names, render table, init sorting, render offline list, update footer)
- `renderWindChart()` — 199 lines
- `renderWind24HourTable()` — 170 lines
- `addWindStationMarker()` and `addBuoyWindMarker()` — nearly identical 100-line functions that should be unified

### 3. Hardcoded data that should be in config/stations.json
- `sourceLinks` object (37 lines of URLs, `wind-stations.js:328-365`)
- `shortNames` object (`wind-stations.js:368-385`)
- Station type detection via ID prefix (`id.startsWith("4600")`, `id.startsWith("C")`, etc.) — fragile

### 4. Buoy vs wind station format branching scattered everywhere
- The pattern `isBuoy && timeseries.field?.data ? ... : ...` appears in `filterWindTimeseriesData`, `renderWind24HourTable`, and `renderWindChart`
- Data should be normalized once at load time

## Medium Priority

### 5. Inconsistent mobile breakpoints
- `768px` in CSS, `600px` in JS arrow sampling (`wind-stations.js:666`), `600px`/`1000px` in chart-utils
- Should be shared constants

### 6. Magic numbers throughout
- Stale/offline thresholds: `2` and `4` hours (used in 2 places each)
- Arrow sampling: `isMobile ? 6 : 3` hours, `maxArrows = isMobile ? 4 : 8`
- Popup animation delay: `300ms`
- Timestamp matching tolerance: `1800000` (30 min)

### 7. Inline styles
- Popup HTML in `winds-map.js:204-271`
- Offline stations list in `wind-stations.js:487-496`
- Repeated section wrapper in `winds.html` (`max-width: 1200px; margin: 2rem auto; padding: 0 1rem;` appears 4 times)

### 8. Hardcoded color in HTML
- `winds.html:249` has `border: 1px solid #e0e7ee` on the map div instead of a CSS variable

## Low Priority

### 9. Dead code
- `arrowType` parameter in `getDirectionalArrow()` is never used

### 10. Event listener hygiene
- Chart resize listener (`wind-stations.js:1296`) never cleaned up
- cursor/userSelect set in JS despite already being in CSS

### 11. Accessibility gaps
- Sortable headers lack `aria-sort`
- Time range buttons lack `aria-pressed`
- Collapse toggle lacks `aria-expanded`
- SVG arrows have no screen reader text

## Recommended refactor order

1. **Extract shared utils** — move `degreesToCardinal`, `getDirectionalArrow`, `fetchWithTimeout` to a single shared module (or consolidate into `chart-utils-v4.js`)
2. **Unify marker functions** in `winds-map.js` — merge the two near-identical popup/marker builders
3. **Normalize data once** — transform buoy data to match wind station format at load time, eliminate all downstream branching
4. **Move hardcoded mappings** (`sourceLinks`, `shortNames`, flag logic) into `stations.json` or a separate config
5. **Extract constants** for thresholds, breakpoints, and magic numbers
6. **Break up `loadWindTable()`** into smaller functions
