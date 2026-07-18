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

- `format-time.js` — Pacific-time formatters + `getAgeString`/`getShortAgeString`
- `staleness.js` — stale-data presentation (marker opacity, popup theme)
- `markers.js` — directional map marker, angular spread vector, ECharts arrow path

## Migration status

Legacy copies are deleted when their page converts to ES modules
(page-by-page; each conversion independently shippable).

Converted pages: tides (pre-existing), guide, webcams, forecasts
(2026-07-15), winds + lightstations (2026-07-16), storm_surge
(2026-07-17), index (2026-07-18). **All pages are now ES modules.**
Foundation scripts (theme-manager, logger, warning-banner, nav, footer,
sanitize-html, chart-utils-v4) stay classic for now; modules read their
globals bare. **All legacy dupes are gone as of 2026-07-18.**

| Legacy copy | File | Replaced by | Status |
|---|---|---|---|
| `createDirectionalMarker` | `stations-map.js` | `markers.js` | **done 2026-07-18** |
| `createDirectionalMarker` | `winds-map.js` | `markers.js` | **done 2026-07-16** |
| `createDirectionalMarker` | `lightstation-map.js` | `markers.js` | **obsolete** — legacy copy was dead code (never called), deleted 2026-07-16 |
| report time + age (inline ×2) | `lightstation-map.js`, `lightstation-page.js` | `format-time.js` (`formatWeekdayDayTime`, `getShortAgeString`) | **done 2026-07-16** |
| `createAngularSpreadVector` | `main.js` | `markers.js` | **done 2026-07-18** |
| `createAngularSpreadVector` | `webcams-v4.js` | `markers.js` (`...Element` variant) | **done 2026-07-15** |
| `DIRECTION_ARROW_PATH` | `chart-utils-v4.js` | `markers.js` | **done 2026-07-18** — chart-utils copy deleted; all chart pages import shared |
| `formatTimestamp`, `formatTimeOnly`, `formatTimeAxis` | `chart-utils-v4.js` | `format-time.js` | **done 2026-07-18** — dead copies deleted from chart-utils |
| `formatTimestamp` | `forecasts.js` | `format-time.js` (`formatForecastTimestamp`) | **done 2026-07-15** — legacy used browser-local TZ; shared pins Pacific |
| `formatTimestamp`, `formatShortTimestamp` | `webcams-v4.js` | `format-time.js` | **done 2026-07-15** |
| `formatTimestamp` | `lightstation-charts.js` | `format-time.js` (`formatNumericDayTime`) | **obsolete** — legacy copy was dead code (never called), deleted 2026-07-16 |
| `formatTime`, `getAgeString` | `tides-modules/utils.js` | `format-time.js` | **done 2026-07-18** |
| metadata `formatDate` + tooltip time (inline ×3) | `storm_surge_page.js` | `format-time.js` (`formatMonthDayTimeTZ`) | **done 2026-07-17**; `storm_surge_chart-v4.js` copy **done 2026-07-18** |
| model-run "Jul 14 12Z" block (inline ×2) | `storm_surge_page.js` | `format-time.js` (`formatModelRunTime`) | **done 2026-07-17**; `storm_surge_chart-v4.js` copy **done 2026-07-18** |
| stale popup colours/header (inline ×3) | `stations-map.js` ×2, `lightstation-map.js` | `staleness.js` | lightstation-map **done 2026-07-16**; stations-map ×2 **done 2026-07-18** |
| buoy-card `updated` timestamp (inline ×2) | `main.js` | `format-time.js` (`formatNumericDayTime`) | **done 2026-07-18** |
