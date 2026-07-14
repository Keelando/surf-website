# Shared Frontend Utils

Canonical ES-module home for helpers that were copy-pasted (and drifted)
across the classic global scripts. Created as step 1 of the ES-module
migration (`docs/project/MAINTAINABILITY_AUDIT_2026-07-14.md`, Priority 1).

Pattern follows `tides-modules/`: plain ES modules, no build step, loaded
via `<script type="module">` entry points.

Unit tests: `tests/js/*.test.mjs`, run with `npm run test:js` (node:test,
no dependencies). The `package.json` here (`"type": "module"`) exists so
node can import these files in tests; browsers ignore it.

## Modules

- `format-time.js` — Pacific-time formatters + `getAgeString`
- `staleness.js` — stale-data presentation (marker opacity, popup theme)
- `markers.js` — directional map marker, angular spread vector, ECharts arrow path

## Migration status

Legacy copies are deleted when their page converts to ES modules
(page-by-page; each conversion independently shippable).

| Legacy copy | File | Replaced by | Status |
|---|---|---|---|
| `createDirectionalMarker` | `stations-map.js` | `markers.js` | pending (index.html) |
| `createDirectionalMarker` | `winds-map.js` | `markers.js` | pending (winds.html) |
| `createDirectionalMarker` | `lightstation-map.js` | `markers.js` | pending (lightstations.html) — converges to themed label style |
| `createAngularSpreadVector` | `main.js` | `markers.js` | pending (index.html) |
| `createAngularSpreadVector` | `webcams-v4.js` | `markers.js` (`...Element` variant) | pending (webcams.html) |
| `DIRECTION_ARROW_PATH` | `chart-utils-v4.js` | `markers.js` | pending (all chart pages) |
| `formatTimestamp`, `formatTimeOnly`, `formatTimeAxis` | `chart-utils-v4.js` | `format-time.js` | pending |
| `formatTimestamp` | `forecasts.js` | `format-time.js` (`formatForecastTimestamp`) | pending — legacy used browser-local TZ; shared pins Pacific |
| `formatTimestamp`, `formatShortTimestamp` | `webcams-v4.js` | `format-time.js` | pending |
| `formatTimestamp` | `lightstation-charts.js` | `format-time.js` (`formatNumericDayTime`) | pending |
| `formatTime`, `getAgeString` | `tides-modules/utils.js` | `format-time.js` | pending (tides already ESM — trivial swap) |
| stale popup colours/header (inline ×3) | `stations-map.js` ×2, `lightstation-map.js` | `staleness.js` | pending |
