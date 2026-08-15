# Next Session Plan

**Last updated:** 2026-08-15
**Status:** Forecast upgrade ACTIVE — RDWPS waves now running in cron on a
trial basis at Halibut Bank. Frontend-polish backlog from the 2026-08-15
review is cleared.

---

## Where we're at (2026-08-15, end of session)

**RDWPS wave forecast — live in cron (`a621316`):**

- `35 4,10,16,22 UTC` (run + 4 h 35 m), Halibut Bank (`4600146`) only while
  we get a feel for it. 4 variables × 33 tapered steps = 133 requests/run,
  532/day.
- **Timestep taper:** hourly out to 24 h, then 3-hourly to 48 h — 33 of the
  49 published steps, horizon unchanged. `taper_time_steps()` keys off the
  run hour, not wall clock, so a late fetch tapers at the same lead times.
- **Schema is tailored to forecast data** (3-D: run × valid × variable, with
  a verification lifecycle observations don't have):
  - `status` `'ok'`/`'masked'` with nullable `value` — a masked step is now
    recorded rather than vanishing, so a *missing* row can only mean a failed
    fetch. Migration rebuilds the table; SQLite can't relax the original
    `value REAL NOT NULL` with `ALTER`.
  - Index on `(valid_time - forecast_run_time)` — lead time is the axis every
    skill query groups by.
  - `wave_forecast_run` — per-run provenance and ok/masked/failed counts.
  - `wave_forecast_verification` — forecast↔observation pairs, **deliberately
    exempt from the 60-day retention.** Raw runs are bulky and disposable;
    these triples are the skill history and this summer is too calm to
    conclude anything from. *Nothing writes to this table yet.*
- Run time is 1 m 45 s, down from 3 m 17 s (session keep-alive + fewer steps).

**Validation status: inconclusive, and will stay that way until autumn.**
12Z run vs buoy gave bias −0.095 m, RMSE 0.102 m over 10 hours; the 18Z run
−0.064 m over 4. But the buoy quantises Hs to 0.1 m, so that bias is one
reporting step, and 90 days of history has nothing above 1.5 m. Peak period
runs 0.7–1.0 s below the buoy. Don't draw conclusions from calm-summer data.

**Fetch footprint is now documented** in `docs/DATA_FEEDS.md`: ~3,470 HTTP
requests/day to ECCC (4.0% of MSC's 86,400/day guidance) plus ~19,800
sr3 files/day. Update that table when adding or rescheduling a feed.

## Next session — pick up here

1. **Storm-surge taper** (`TODO.md`, has the measured numbers): our largest
   HTTP load at 2,894/day, and surge changes slowly enough that it's waste —
   hour-to-hour change averages 1.55 cm. Recommended shape is hourly to 48 h
   then 3-hourly to 240 h (−53%). Touches a live chart, so look at the page
   before committing. Plot smoothing is a separate, optional follow-up.
2. **Verification writer** — a small script on a lag behind the observations,
   pairing each past-valid forecast value with the nearest buoy observation
   into `wave_forecast_verification`. This is what makes the winter data
   worth having; the table exists and is empty.
3. **Unlisted `site/forecast-waves.html`** — noindex, unlinked, out of the
   sitemap and test suites (see the preview decision below). Forecast chart
   plus buoy-observation overlay is the validation story made visible. The
   "coming soon" callout now on `forecasts.html` is the thing it promotes;
   remove that callout and its page-local styles when waves ship.
4. **Policy cleanup** (`TODO.md`, quick): `fetch_lightstation.py` discovers
   data by walking Datamart directory listings, which MSC's usage policy
   explicitly forbids, and looks redundant with the sr3 subscription already
   delivering FPCN61. Pairs with the duplicate-fetching audit.
5. **CIOPS-SalishSea recon** (priority ②) — open question from
   `FORECAST_MODELS.md`: does SSH include tidal forcing? That decides the
   surge calculation.

Backlog beyond this lives in `TODO.md`; the four frontend-polish items from
this morning's review are done (`c723f47`).

## Decisions that shouldn't be relitigated

- **WMS for points, GRIB2 only for fields.** A Datamart file and a WMS query
  have the *same granularity* — one variable, one lead hour — so at one
  station they're an equal 196 requests, but 388 MB against 136 KiB. The
  bandwidth crossover is ~2,800 stations. GRIB2 becomes right only if we want
  a wave *map*, or the 4 GRIB-only variables (wind/Stokes — and HRDPS is the
  better wind source anyway). More buoys is *not* a reason to switch.
- **No batching exists.** GeoMet rejects multi-layer `LAYERS`, time ranges and
  comma-separated time lists, and RDWPS has no OGC API/EDR collection. One
  request per (variable, timestep) is the floor.
- **4 runs/day, not 2.** Run freshness buys more accuracy than time
  resolution, and the load difference is negligible at this scale.
- **Preview via unlisted pages, not a dev subdomain** — new additive features
  go live unlinked on `main` with noindex. Promotion = nav link + sitemap +
  drop noindex + add to test suites. The dev-subdomain backlog item is
  reserved for risky changes to shared or existing surfaces.

## Gotchas worth keeping in mind

- **Never discover EC data by guessing timestamps or walking directory
  listings** — MSC's usage policy forbids it; AMQPS is what that's for.
- **GRIB2 coordinates are sign-bit magnitude, not two's complement.** Reading
  La1 as a signed int gives −2135°, not −12.2575° — a plausible-looking grid
  in the wrong hemisphere.
- **GeoMet masked cells** come back as sentinel `9999.0`, not an empty feature
  — filter `>= 9000`. Remember for CIOPS.
- **Datamart publication is at run + 3 h 25 m**, measured to the minute on two
  consecutive runs; GeoMet ingests shortly after.
- **Module double-load:** a module loaded via both a `<script type="module">`
  tag AND a bare import runs twice (cache keyed on full URL incl. query). One
  entry-point tag per page.
- **Verify visual changes in BOTH engines** — user's daily browser is Firefox;
  Playwright and screenshots default to Chromium. Pin timezones.
