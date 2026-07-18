---
name: verify
description: Drive the static site in a real browser to verify frontend changes at runtime (beyond the Playwright console-error suite).
---

# Verifying frontend changes (halibutbank.ca)

No build step — the site under `site/` is served as-is. The Playwright
test suite (`npm run test:frontend`) only checks for console errors per
page; behavioral verification needs a driving script.

## Recipe

1. Serve the site with the repo's own server (same one Playwright uses):
   `python3 tests/playwright/serve.py <port>` (cwd = repo root; serves
   `site/` with the right headers). Pick a port ≠ 4173 to avoid
   colliding with the test suite's webServer.
2. Drive with Playwright's library API from a scratch `.mjs` script.
   `@playwright/test` is not resolvable from outside the repo — import
   by absolute path:
   `import { chromium } from "/home/keelando/envcan_wave/node_modules/@playwright/test/index.mjs";`
3. Collect `page.on("console")` (type `error`) and `page.on("pageerror")`
   throughout — a clean run must end with zero.
4. `waitUntil: "networkidle"` is enough for data-driven pages; JSON
   fixtures live in `site/data/` (real production data, kept current by
   cron on this host).

## Proving a refactor changed nothing (DOM diff)

For a refactor that claims no behaviour change, tests passing isn't the
claim — the rendered output being identical is. Capture the container's
`innerHTML` on both sides and diff.

1. **Freeze the data first.** Cron rewrites `site/data/` every minute, so
   two captures minutes apart differ by live readings, not by your code.
   Snapshot the directory (`cp -r site/data <scratch>/fixtures`) and
   serve it from the page:

   ```js
   await page.route("**/data/**", (route) => {
     const path = new URL(route.request().url()).pathname.replace(/^\/data\//, "");
     const file = `${fixtures}/${path}`;
     if (!existsSync(file)) return route.fulfill({ status: 404, body: "" });
     route.fulfill({ contentType: "application/json", body: readFileSync(file) });
   });
   ```

2. **Expand everything first** — collapsed content isn't in the diff.
   On index.html: click `.region-header`s whose `.buoy-cards-grid` is
   `display: none`, then `.toggle-details-btn` / `.toggle-history-btn` /
   `.spread-info-btn`. Use `dispatchEvent("click")` rather than `click()`
   so hidden-but-present controls still fire.
3. Dump `#buoy-container` innerHTML, `.replace(/></g, ">\n<")` so `diff`
   is line-oriented and readable.
4. Capture the "before" by `git stash push -u` (`-u` matters — new
   modules are untracked and must disappear), re-running, then
   `git stash pop`. Compare against the *pre-refactor commit*, not an
   older capture.

A byte-identical DOM is engine-independent, so this substitutes for a
separate Firefox pass on pure-refactor changes (not on CSS changes —
those need real rendering in both engines).

## Flows worth driving per page

- **winds.html**: `#wind-conditions-table tbody tr` count + default
  speed-desc sort (`data-wind_speed_kt`); `.station-marker` count; map
  popup → "View Wind Chart" (`.view-data-btn[data-wind-station-id]`)
  must set `#wind-station-select` (CustomEvent wiring across modules);
  table "Map" link (`.wind-table-action-link[data-action="map"]`) must
  open a `.leaflet-popup` (allow ~1.2s for scroll settle + focus delay);
  48h toggle (`.wind-time-range-btn[data-wind-hours="48"]`) updates the
  section `h2`; deep link `#wind-<id>` selects that station on load
  (allow ~1.5s).
- **lightstations.html**: `.lightstation-card` count grouped in
  `.region-section`s; `.report-time` text matches
  `Report: <Weekday> <Mon> <D>, <HH:MM> (<age> ago)`;
  `#lightstation-station-select` has region optgroups, default
  MERRY ISLAND; `#lightstation-wind-chart canvas` +
  `#lightstation-24hr-body tr` rows; marker popup "View Data"
  (`.view-data-btn[data-lightstation-id]`) sets the dropdown (ID
  underscores → spaces); card "Show on Map" and the
  `#show-lightstation-on-map-btn` button open a `.leaflet-popup`
  (allow ~2.2s: 800ms scroll + 1000ms pan + 1100ms popup delay);
  deep link `#lightstation-<ID>` expands that region section.
- Chart pages render into ECharts: assert `<container> canvas` exists.

## Gotchas

- **Module double-execution**: a module loaded both via
  `<script type="module" src="...?v=X">` AND via a bare import
  specifier runs TWICE (module cache is keyed by full URL, query
  included). Symptom: "Map container is already initialized." Rule:
  each module is either a script-tag entry point or an import target,
  never both. Multi-module pages get ONE entry-point tag.

- `#timestamp` does not exist in any page HTML — several legacy scripts
  still reference it (null-guarded, dead). Don't assert on it.
- Leaflet markers need the map tiles *area*, not tile fetches, so
  offline tile failures don't block marker assertions.
