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

- `format-time.js` — Pacific-time formatters + `getAgeString`/`getShortAgeString`.
  Model runs have **two** formatters, deliberately: `formatModelRunTime`
  ("Jul 14 12Z", UTC — the storm-surge pages) and `formatModelRunTimeLocal`
  ("Jul 14, 05:00 PDT (12Z)", Pacific-first — the wave forecast page). Same
  instant, different lead: the Z hour is what model documentation and the
  archive use, the local time is what a reader plans around.
- `staleness.js` — stale-data presentation (marker opacity, popup theme,
  `formatDataAge`)
- `markers.js` — directional map marker, angular spread vector, ECharts arrow path
- `map-fullscreen.js` — Leaflet fullscreen control (`addFullscreenControl`),
  used by all three maps; custom rather than a vendored plugin because the
  CSP blocks CDNs and the native Fullscreen API is enough
- `station-meta.js` — predicates over stations.json entries (NOAA/Surrey/pile,
  swell display, precision, sub-hourly) plus the displayed-field priorities
  (`waveHeightField`/`wavePeriodFields`/`pickWavePeriod`, shared by the card's
  compact line and the history table) — replaces inline station-ID checks;
  see `docs/project/BUOY_CARD_REFACTOR.md`
- `warning-zones.js` — the pure half of the sitewide warning banner: which
  zones may raise one (`DEFAULT_BANNER_ZONES` + `getBannerZones(stored,
  available)`, deliberately narrower than the zones we carry),
  `collectActiveWarnings` including the storm severity floor,
  `summarizeBannerWarnings` (how many zones the banner names and what it calls
  the rest), and the severity/icon mapping. Extracted 2026-08-20 so the banner could be tested
  at all; `warning-banner.js` keeps the DOM, dismissal and htmx wiring and
  became an ES module to import this. Tests: `tests/js/warning-zones.test.mjs`
- `warning-preferences.js` — reads and writes the reader's banner zone choice
  (`warning_banner_zones`, a JSON array so `[]` stays distinguishable from
  "never chosen"). Storage is injected, never reached for, so this is testable
  with a fake and `warning-zones.js` stays storage-free. Added 2026-08-23 with
  the per-zone opt-in (`docs/project/WARNING_ZONE_OPT_IN.md`). Tests:
  `tests/js/warning-preferences.test.mjs`
- `marine-zones.js` — marine zone vocabulary and ordering: `listZones`,
  `shortZoneLabel`, `pickerZoneLabel` (+ `PICKER_SHORT_NAMES`, display-only
  overrides for names too long for a picker row), `orderZonesForDisplay` (home
  area first) and `DEFAULT_ZONE_KEY`. Extracted from `forecasts.js` 2026-08-23
  so the zone `<select>` and the warning-zone picker beside it name and order
  zones identically. Tests: `tests/js/marine-zones.test.mjs`

Page-specific builders live beside their entry point rather than here:
`buoy-card.js` and `buoy-history.js` are index-only, and both take
`(data, meta)` so they stay pure and unit-testable.

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
