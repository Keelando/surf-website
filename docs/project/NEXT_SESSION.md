# Next Session Plan

**Last updated:** 2026-08-16
**Status:** Forecast upgrade ACTIVE — RDWPS waves are in cron *and now
rendering* on `forecasts.html` as an experimental preview. The next piece is
the verification writer, and it is time-sensitive.

---

## Session of 2026-08-16 — what shipped

Five commits, all on `main`, all suites green at each one:

| Commit | What |
|---|---|
| `20d3ac3` | Day-length change on the tides page (side quest) |
| `693f8ec` | RDWPS wave forecast preview on `forecasts.html` |
| `70e2d84` | Direction arrows, table expand controls, section reorder |
| `b8f5adc` | Footer-hamburger fix, mobile gutters, plot rework |

**Wave forecast preview** (`site/assets/js/wave-forecast.js`, page-local
styles in `forecasts.html`). Chart + table + provenance for Halibut Bank,
under an "Experimental — use at your own risk" badge. Decisions baked in:

- **Height axis floors at 0–1 m**, growing only past that. Summer forecasts
  sit under 0.2 m; auto-scaling renders a flat calm as a mountain range.
- **Time axis, not category.** Steps taper to 3-hourly after 24 h — even
  spacing would stretch the back half to look like the front half.
- **`hideOverlap: true` is what fixes crowded x labels**, not the tilt. A
  48 h span asks for more ticks than fit and ECharts will draw them on top
  of one another; the 30° tilt below 600 px just buys room for the
  survivors. Chart otherwise follows the buoy wave charts (shared
  `getResponsiveGridConfig` / `getResponsiveLegendBottom` /
  `getMobileOptimizedTooltipConfig`, axis furniture tinted per series).
- **Direction arrows sample by elapsed time, not array index** — the one
  deliberate divergence from `createWaveDirectionArrowData`. Buoy
  observations are evenly spaced; these are not, so an index stride would
  thin to one arrow per 9 h across the back half. They ride at 92 % of the
  axis rather than above the peak, because the axis has a fixed floor.
- **`wind_wave_height` is table-only, never plotted.** Where the model
  reports it, it equals total Hs within 1 mm in 88 of 96 DB rows — no swell
  reaches this fetch. Masked steps stay blank, never zero: the masked and
  unmasked bands overlap between 0.05 and 0.12 m, so zero-filling draws a
  sawtooth that is a plot artifact, not weather.
- **Table opens on 12 h with +12 / +24 / Show all / Collapse.** It was a
  fixed-height scrolling box; the user did not notice it scrolled at all,
  and would not have been the only one. The window is measured in hours, so
  it means the same thing either side of the taper.

**Two bugs found and fixed, both pre-existing and site-wide:**

1. `nav.js` used singular `querySelector` throughout, but the nav fragment
   is injected **twice** per page. Only the header hamburger was ever wired;
   the footer one drew its bars and did nothing. Now per-nav — and since the
   header nav is `position: sticky` below 600 px, the footer nav is hidden
   on mobile outright and kept on desktop.
2. `export_sunlight_times.py` only reached today+2. The 00:14 UTC cron fires
   at 17:14 PDT the *previous* Pacific day, so one of its four days was
   already yesterday, and the tides page's day navigation reaches today+2 —
   whose day-length-change line needs today+3. Raised to `days_ahead=5`.

**Mobile gutters** halved below 600 px on the major section wrappers. The
tides page already used 0.25 rem; this brings the rest in line rather than
inventing a new value.

## Where we're at (2026-08-15, partly superseded)

> **Since superseded — see [`FORECAST_UPGRADE.md`](FORECAST_UPGRADE.md) for the
> current state.** HRDPS wind joined the same fetch (2026-08-17), Crescent Beach
> Ocean (`CRPILE`) became a second point (2026-08-18) — so the counts below are
> now 464 requests/run and 1,856/day, not 133 and 532 — and the page grew a
> Waves/Wind segmented control (2026-08-18). Everything about the schema, the
> taper, and the validation status below still holds.

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

## Decision tree: verification, hindcast and skill

Raised 2026-08-16 — the user proposed letting readers flick back through
~14 days of past forecasts against what actually happened. Settled
vocabulary and design so this isn't re-derived:

**These are three different things. Keep the words apart.**

- **Hindcast** (modelling usage) = re-running the model over a past period.
  **We are not doing this** and cannot — we don't run RDWPS. Avoid the word
  in code, docs and UI or it will mislead.
- **Verification / forecast archive** = comparing *archived* forecasts to
  observations. This is what the user described, and what we can build.
  Qualitative: "does this look right?"
- **Skill score** = one number, relative to a reference:
  `SS = 1 − MSE_forecast / MSE_reference`. Ours must use **persistence**
  ("conditions stay as they are now") as the reference. A forecast that
  can't beat persistence at 24 h isn't worth displaying at 24 h — that is
  the number that sets the horizon, and EC's own verification products
  won't answer it for this station.

**Both views eat the same table.** `wave_forecast_verification` already has
`forecast_value`, `observed_value` and `reference_value`, and is exempt from
the 60-day retention. It has **0 rows**. The writer feeds the browser and
the score alike — building it is not a detour around the user's idea, it is
the shared foundation.

**The decision the browser forces.** "Flick back 14 days" is
under-specified: every past hour was forecast *many* times, once per run, at
different lead times. Two views, both wanted:

1. **Fixed lead time**, scrolled by day — "what the 24-hour-ahead forecast
   said." The user calls 24 h the gold standard to watch. Build this first;
   it matches how someone actually used the page.
2. **Fixed target day, all runs overlaid** — shows the forecast converging
   (or not) as an event approaches. User is keen, and it is well suited
   here: the wave horizon is **48 h** (33 steps), so a calendar day is
   covered by only ~8 runs — a legible number of lines. Contrast storm
   surge at **240 h / 10 days** (129 steps post-taper), where the same view
   would be 40 runs deep and unreadable.

**Measured facts that constrain this** (checked 2026-08-16, don't re-derive):

- Halibut Bank's buoy id is `4600146` — **identical to the forecast station
  id**. No mapping trap, unlike the NDBC-numbered stations.
- Buoy observations are hourly: 720 rows over 30 days, all with
  `wave_height_sig`.
- **The buoy quantises Hs to 0.1 m.** Thirty days of observations contain
  exactly **11 distinct values, 0.0–1.0 m**. Against a forecast of 0.139 m
  the observation carries ±0.05 m of rounding — **36 % of the value**. Any
  bias or RMSE computed this month measures the buoy's reporting step, not
  the model. This is the single most important caveat: a browser opened in
  August draws a smooth curve against a staircase and says nothing true.
- **Retention is asymmetric and will silently eat history**: buoy
  observations 30 days, forecasts 60, verification pairs permanent. Anything
  reading the raw tables loses its past at day 30. Only the writer's output
  survives.
- Volume is a non-issue: 4 variables × 33 steps × 4 runs/day ≈ 530 rows/day,
  under 200 k/year.

**Therefore the order is: writer → accumulate → fixed-lead browser → skill
score → convergence view.** Every day without the writer is a day of autumn
data that cannot be recovered.

## Next session — pick up here

Theme: start the verification writer; it is the time-sensitive one.

0. **Verification writer** — promoted to the top (was item 2 below, detail
   there). The reasoning is in the decision tree above: the user's forecast
   browser and the skill score are the same build, and the pairs only
   accumulate going forward. Nothing else here expires.
1. **Storm-surge taper + delay** (`TODO.md`, has all the measured numbers):
   our largest HTTP load at 2,894/day, and surge changes slowly enough that
   it's waste — hour-to-hour change averages 1.55 cm. Chosen shape is hourly
   to 72 h then 3-hourly to 240 h (129 of 241 steps, −47%). **Raise
   `FETCH_DELAY` to ~2 s at the same time** — it currently runs at 1.05 req/s
   for 23 minutes, which is the more important problem. Touches a live chart,
   so look at the page before committing. Plot smoothing is a separate,
   optional follow-up, with the overshoot caveat in `TODO.md`.
2. **Verification writer** — a small script on a lag behind the observations,
   pairing each past-valid forecast value with the nearest buoy observation
   into `wave_forecast_verification`. This is what makes the winter data worth
   having; the table exists and is empty. It has a `reference_value` column
   for the buoy reading at the model run hour — fill it, because a **skill
   score against persistence** ("do we beat 'conditions stay as they are'?")
   is what decides how far out the forecast is worth displaying, and EC's own
   verification won't answer it (see `RDWPS_PARAMETERS.md`).
3. ~~**Unlisted `site/forecast-waves.html`**~~ — **DONE 2026-08-16, but not
   as a separate page.** User's call: the preview lives at the bottom of the
   existing `forecasts.html` under an "Under development" badge, which is
   the dev environment. `site/assets/js/wave-forecast.js` renders chart +
   table + provenance from `site/data/wave_forecast/4600146.json`; the old
   "coming soon" callout and its page-local styles are gone, since the thing
   it promoted is now on the same page.
   - **Height axis floors at 0–1 m** and only grows past it. Summer
     forecasts sit under 0.2 m and auto-scaling renders a flat calm as a
     mountain range.
   - **Time axis, not category.** The fetch tapers to 3-hourly after 24 h;
     evenly spacing those steps would stretch the back half of the forecast
     to look like the front half.
   - **`wind_wave_height` is table-only, never plotted.** Where present it
     equals total Hs within 1 mm in 88 of 96 DB rows — the Strait has no
     swell at this fetch, so a second line would just hide under the first.
   - Still to come here: the buoy-observation overlay, which is the
     validation story made visible, and which wants item 2 first.
4. **Policy cleanup** (`TODO.md`, quick): `fetch_lightstation.py` discovers
   data by walking Datamart directory listings, which MSC's usage policy
   explicitly forbids, and looks redundant with the sr3 subscription already
   delivering FPCN61. Pairs with the duplicate-fetching audit.
5. **CIOPS-SalishSea recon** (priority ②) — **does SSH include tidal
   forcing?** Still open, and it decides the arithmetic: if tides are in the
   field it *is* total water level and we stop adding the DFO prediction to
   it; if not, it is a surge-like field we add as we do today. ECCC's layer
   abstract does not say, so this needs the CIOPS technical note or a
   comparison of the field against a known tide curve at a station.

   Settled by GetCapabilities 2026-08-16, so don't re-derive:
   - Layer `CIOPS-SalishSea_500m_SeaSfcHeight`, **500 m**, title *"Sea
     surface height above geoid [m]"* — a **geoid** datum, where DFO
     predictions are on chart datum. The offset is a second open item and is
     independent of the tide question.
   - Bounding box −126.204…−121.109 E, 46.998…50.994 N — covers every station
     we carry.
   - 48 hourly steps (`PT1H`), two reference times online at once (06Z, 12Z
     when checked), 4 runs/day. So one station is 48 requests/run, 192/day;
     six stations ~1,150/day untapered — affordable against the headroom the
     storm-surge taper just freed, but not free.
   - Atmospheric forcing is HRDPS, which is also our candidate wind upgrade.

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
- **Burst rate, not daily total, is the ECCC constraint.** The guidance is
  "about 1 request per second"; our daily totals are ~4% of it. `FETCH_DELAY`
  controls the rate, tapering timesteps controls the total — thinning steps
  shortens a burst but does not slow it. See `docs/DATA_FEEDS.md`.
- **Skill score ≠ bias/RMSE.** A skill score is relative to a reference
  (`SS = 1 − MSE_fc / MSE_ref`); ours should be persistence.
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
- **The nav fragment is injected twice per page** (header and above the
  footer). Any `document.querySelector` in `nav.js` silently wires only the
  header copy — this is exactly how the footer hamburger sat dead. Iterate
  `.main-nav` and scope lookups to each one.
- **ECharts renders to `<canvas>`, not SVG.** There are no `<text>` nodes,
  so axis-label overlap cannot be asserted from the DOM — a probe for them
  returns 0 and looks like a pass. Verify chart legibility by screenshot.
- **`isMobile` in Playwright is Chromium-only**; passing it to Firefox
  throws at `newContext`. Set the viewport for both, and gate
  `isMobile`/`hasTouch` on the engine.
- **A cron job's "today" is Pacific-yesterday** for anything running before
  08:00 UTC. `export_sunlight_times.py` lost a whole day of its forward
  window to this. Count the days a consumer actually needs, then add the
  offset — don't assume `days_ahead` means days ahead of the viewer.
- **New ESLint module files need adding to `.eslintrc.json`.** The module
  `sourceType` override is an explicit file allowlist; a new ES module fails
  with "'import' and 'export' may appear only with sourceType: module" until
  it is listed.
- **Verify visual changes in BOTH engines** — user's daily browser is Firefox;
  Playwright and screenshots default to Chromium. Pin timezones.
