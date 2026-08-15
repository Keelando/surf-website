# Next Session Plan

**Last updated:** 2026-08-15
**Status:** Forecast upgrade ACTIVE — RDWPS waves phase 1 backend landed.
(Maintenance backlog from the 2026-07-19 audit is fully complete; completed
feature history lives in `WORKLOG.md` and the per-feature docs.)

---

## Where we're at (2026-08-15)

**RDWPS wave forecast — backend phase 1 done, not yet in cron:**

- Model catalogue + priorities: `FORECAST_MODELS.md` (all candidate
  models); decided order = ① RDWPS waves ② CIOPS-SalishSea water levels.
- Recon verified live (see `FORECAST_UPGRADE.md` recon section): new
  date-first Datamart layout with `/today/` alias, national 2.5 km grid
  resolves the Strait (~400 m from Halibut Bank buoy), GeoMet WMS
  `GetFeatureInfo` values are bit-identical to the raw GRIB2.
- Full 19-variable inventory with parse notes: `RDWPS_PARAMETERS.md`
  (directions are coming-FROM ° true; masked cells arrive as sentinel
  9999.0 over WMS; wind uses level tag `AGL-10m`; wind/Stokes are
  GRIB-only).
- `scripts/fetch/fetch_wave_forecast.py` implemented and live-tested:
  4 fields × 49 h at Halibut Bank (`4600146`) → `wave_forecast.sqlite`
  (every run kept, epoch timestamps, 60-day retention) +
  `site/data/wave_forecast/4600146.json`. Feed documented in
  `docs/DATA_FEEDS.md`.

**Decisions made:**

- **Preview strategy: unlisted pages, not a dev environment.** New
  (additive) pages go live unlinked on `main` — e.g.
  `site/forecast-waves.html` — with `<meta name="robots"
  content="noindex">`, out of the sitemap, out of the Playwright/
  screenshot lists. Promotion checklist: nav link + sitemap entry +
  remove noindex + add to test suites. The dev-subdomain backlog item is
  deferred to "risky changes to shared/existing surfaces" only. Verified
  safe: nothing auto-discovers pages; `update_asset_versions.py` globs
  all HTML so cache busting is automatic.
- Wave forecasts stay HTTP-polled (GeoMet WMS, like GDSPS) — **no sr3
  changes**; sr3 + GRIB2 is the later upgrade path only.

## Next session — pick up here

1. **Validation loop:** a few manual `fetch_wave_forecast.py` runs across
   different model runs, sanity-check against the buoy, then wire
   `config/crontab.txt` (4×/day, ~4 h after each 00/06/12/18Z run) via
   `scripts/install_crontab.sh`, and add the remaining EC buoys to
   `BUOY_IDS`.
2. **Unlisted forecast page:** scaffold `site/forecast-waves.html`
   (noindex, unlinked) rendering `site/data/wave_forecast/*.json` —
   forecast chart + buoy-observation overlay is the validation story.
3. **CIOPS-SalishSea recon** (priority ②): same treatment as RDWPS —
   Datamart layout, GeoMet layers, and the open question from
   `FORECAST_MODELS.md`: whether SSH includes tidal forcing (decides the
   surge calculation).
4. Small stuff when in the area: four frontend-polish backlog items in
   `TODO.md` (coming-soon flag pairs naturally with item 2).

## Gotchas worth keeping in mind

- **Module double-load:** a module loaded via both a
  `<script type="module" src="...?v=X">` tag AND a bare import runs
  twice (cache keyed on full URL incl. query). One entry-point tag per
  page; imported modules get no tag.
- **Verify visual changes in BOTH engines:** user's daily browser is
  Firefox; Playwright/screenshots default to Chromium. Pin timezones in
  checks (Chromium minute-padding bug, 2026-07-18).
- **GeoMet masked cells:** `GetFeatureInfo` returns 9999.0, not an empty
  feature — filter ≥ 9000 (already handled in `fetch_wave_forecast.py`;
  remember for CIOPS).
