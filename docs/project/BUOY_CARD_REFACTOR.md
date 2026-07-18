# Buoy Card Refactor (P2 of the 2026-07-14 maintainability roadmap)

**Status:** Steps 1–2 DONE (2026-07-18); next is step 3 (inline styles → CSS)
**Target:** `site/assets/js/main.js` (1,288 → 736 lines) — the index-page buoy cards
**Origin:** agreed 2026-05-28 after the backend Surrey-channel config
consolidation; same disease on the frontend: per-station behavior hardcoded
as scattered station-ID checks instead of read from station metadata.

## The problem

The same station sets are re-derived under different names throughout
main.js (post-ESM line numbers, 2026-07-18):

| Inline check | Aliases | Sites |
|---|---|---|
| `CRPILE \|\| CRCHAN` | `isBoundaryBay` (×2), `isSurrey`, `isCrescentStation` | 341, 723, 1040, 1087 |
| `46087 \|\| 46088 \|\| 46267` | `isNOAA`, unnamed | 264, 276, 325, 520, (995 pairwise) |
| `46088 \|\| 46267` | `isNOAA` (history) | 394, 454, 995 |
| `46087` | `isNeahBay` | 376, 994, 1006, 1180 |
| `CRPILE \|\| CRCHAN \|\| COLEB` | unnamed (badges/borders) | 269, 278, 327 |
| `sourceLinks` map (11 URLs) | — | 184 |

`stations-map.js:460` carries one more copy of the NOAA trio.
Adding a station means finding every one of these. This is the frontend
version of the two-sources-of-truth problem we keep killing on the backend.

## The fix: drive behavior from stations.json

`config/stations.json` → (verbatim copy via `export_stations_json.py`) →
`site/data/stations.json` already ships `type`, `source`, `data_types`,
`update_frequency_minutes`, `source_url` per buoy. Verified 2026-07-18 that
metadata cleanly distinguishes every group the code checks by ID:

| Behavior | Today's check | Metadata driver |
|---|---|---|
| NOAA badge/border/spectral details/footnotes | ID trio | `source === "NOAA NDBC"` |
| Surrey badge/border | ID trio | `source === "Surrey FlowWorks"` |
| 2-decimal wave heights, 1-decimal temps | `isBoundaryBay`/`isSurrey`/`isCrescentStation` | `type === "pile_mounted_wave_station"` |
| Swell-based display (compact card, history, note) | `id === "46087"` | **new field** `wave_display: "swell"` |
| "Dominant" period tag + footnote | `46088 \|\| 46267` | NOAA && not swell-display |
| History table hourly filter | `isCrescentStation` | `update_frequency_minutes < 60` |
| Source links | `sourceLinks` map | `source_url` |

Notes:
- `wave_display: "swell"` is a display-preference fact (Neah Bay is open
  ocean, swell is the representative metric) that cannot be derived from
  `data_types` (46088 has identical sensors but shows sig/dominant). It
  lives in `config/stations.json` like every other station fact.
- The `sourceLinks` map is byte-identical to config `source_url` for all
  8 EC/NOAA stations (verified); Surrey stations get their FlowWorks URL
  added to config, then the map dies.
- COLEB never renders a card (not in `buoyGroups`); the Surrey source
  predicate covers it anyway if that changes.

## Steps

1. **Metadata-driven station helpers** ✅ 2026-07-18
   - `config/stations.json`: add `wave_display: "swell"` to 46087; add
     `source_url` to CRPILE/CRCHAN; re-export to `site/data/`.
   - New `site/assets/js/shared/station-meta.js`: pure predicates over a
     station-meta object — `isNoaaStation`, `isSurreyStation`,
     `isPileStation`, `usesSwellDisplay`, `usesDominantPeriod`,
     `waveHeightPrecision`, `reportsSubHourly`, `sourceUrl` — plus unit
     tests in `tests/js/`.
   - main.js: fetch `/data/stations.json` alongside the snapshot
     (`Promise.all`), keep a module-level meta lookup, replace every inline
     ID check; delete `sourceLinks`. renderHistoryTable reads the same
     lookup (it only receives `buoyId`).
   - stations-map.js:460: same swap for its NOAA-trio copy (it already
     fetches stations.json).
2. **Break up `loadBuoyData()`** ✅ 2026-07-18
   - New `site/assets/js/buoy-card.js`: pure HTML-string builders taking
     `(b, meta)` — `freshnessState`, `sourceBadge`, `applyCardBorder`,
     `buildNoDataCard`, `buildCardHeader`, `buildWindLine`, `buildWaveLine`,
     `buildCompactView`, `buildToggleButtons`, `buildStalenessCallout`,
     `buildNoaaWaveDetails`, `buildSpreadSection`, `buildEcWaveDetails`,
     `buildTempPressure`, `buildDetailsSection`, `buildNavLinks`,
     `buildSourceLink`, `buildBuoyCardHTML`, `wireBuoyCardEvents`.
     21 unit tests in `tests/js/buoy-card.test.mjs` (globals
     `getDirectionalArrow`/`degreesToCardinal` stubbed).
   - `formatDataAge` moved from main.js to `shared/staleness.js` (the hero
     panel uses it too), with tests.
   - main.js 1,288 → 736 lines; `loadBuoyData()` 693 → 105. It keeps the
     fetching, region grouping, and toggle callbacks.
   - **`buoyGroups` deliberately stays in main.js**: it encodes display
     ordering (region order, station order, which regions start collapsed),
     which is page layout rather than a station fact. Moving it to
     stations.json would mean inventing order/collapse fields only the
     index page reads. Note `4600206` carries a stray
     `region: "WEST COAST VANCOUVER ISLAND"` in config that nothing reads
     and whose casing differs from the code — clean up or adopt if this is
     ever revisited.
   - Verified no-op: the fully-expanded `#buoy-container` DOM (all 10 cards,
     details + history + spread open) is byte-identical before/after, with
     timestamps and ages normalized.
3. **Inline styles → CSS classes**: ~83 repeated `style="..."` blocks
   (table cells, buttons, callouts) onto the existing CSS-variable /
   dark-mode system.
4. **History table**: fold `renderHistoryTable`'s remaining structure into
   the step-2 builders where it overlaps.

Each step ships independently: format → lint → test:js → test:frontend →
runtime verify (`.claude/skills/verify/SKILL.md`) → bump `?v=` → commit
main + push.

## Invariants (regression checklist)

- Card renders identically for: EC buoy, 46087 (swell), 46088/46267
  (dominant + footnote), CRPILE/CRCHAN (2-dec heights, 1-dec temps,
  Surrey badge, hourly history rows).
- Offline/no-data and stale/down cards keep their badges and source links.
- History table: swell column for Neah Bay, `wave_period_sig` only for
  NOAA, 4-source fallback chain for the rest.
