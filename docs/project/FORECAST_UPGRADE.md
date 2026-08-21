# Forecast Section Upgrade — Salish Sea–Resolving Models

**Status:** Planning (lead captured 2026-07-19; details unverified)
**Goal:** Replace/augment the current forecast offering with model data that
actually resolves the Strait of Georgia, feeding a better forecast section.

> **Model specs:** the full catalogue of candidate models (domains,
> horizons, cadences, parameters, access paths — including GDWPS/GEWPS,
> CIOPS-West, HRDPS, SalishSeaCast, and validation anchors) now lives in
> [`FORECAST_MODELS.md`](FORECAST_MODELS.md), captured 2026-08-15. Where
> the two disagree on numbers (e.g. CIOPS-SalishSea is **500 m**, not
> ~2.5 km as first noted below), the catalogue is the better reference.

**Priorities (decided 2026-08-15):**

1. **Waves first** — the most fine-grained wave forecast available for the
   Salish Sea, i.e. **RDWPS Pacific North-East** (~2.5 km regridded).
2. **Water levels second** — the new high-resolution water-level source,
   i.e. **CIOPS-SalishSea** (500 m) sea surface height, pending the tide
   treatment check in `FORECAST_MODELS.md` §2.

Everything else in the catalogue (HRDPS wind, GDWPS/GEWPS long-range,
GDSPS retention) is downstream of these two.

## The lead: two-pronged regional model setup

### 1. Surface waves — RDWPS

- **What:** Regional Deterministic Wave Prediction System (WAVEWATCH III
  engine, driven by high-resolution Canadian wind models).
- **Why:** 2.5 km grid — granular enough to resolve wave height, period, and
  direction in the main basin of the Strait of Georgia. The current site has
  no wave *forecast* at all (observations only).
- **Where:** MSC Datamart, <https://dd.weather.gc.ca/model_rdwps/>
  (look inside the `national/2.5km/` or pacific directories).

### 2. Storm surge & water levels — CIOPS-SalishSea

- **What:** Coastal Ice-Ocean Prediction System for the Salish Sea.
- **Why:** Upgrade over the current GDSPS feed (3–9 km, struggles with
  complex coastlines). CIOPS-SalishSea is **500 m** (0.008 × 0.005°), nested
  specifically to Salish Sea / Strait of Georgia bathymetry. Also provides
  ocean currents.
- **Beware the 1/36° (~2 km) figure** — it is everywhere, including in
  ECCC's own layer abstract, and it is *not* the Salish Sea number. The
  abstract opens with family-level boilerplate ("different domains … at
  1/36° resolution") and only corrects itself in its last sentence: East and
  West are 2 km, and "the CIOPS-West system also outputs a regional
  enhancement at 500m resolution (0.008 x 0.005 degree) that covers the
  Salish Sea region only." Anyone quoting the opening sentence will hand you
  2 km. Verified against GeoMet GetCapabilities 2026-08-16; the layer name
  itself settles it: `CIOPS-SalishSea_500m_SeaSfcHeight`.
- **Where:** MSC Datamart, <https://dd.weather.gc.ca/model_ciops/>
  (look for the `salishsea` subdirectories).

**Division of labour:** CIOPS-SalishSea for ultra-local water levels, currents,
and surge; RDWPS for wind-driven wave height/period/direction.

## Fit with existing infrastructure

- Both are on Datamart, so per our data-delivery convention (EC sources =
  AMQP push) they can arrive via **sr3 subscriptions** in `config/sr3/` —
  no new polling cron jobs. See `docs/SR3_MANAGEMENT.md`.
- The storm-surge pipeline (`fetch_storm_surge.py` → SQLite → JSON →
  `storm_surge_page.js`) is the template for the ingest/display pattern.
  **Prerequisite:** deduplicate the twin chart functions in
  `storm_surge_page.js` first (see `TODO.md`) so the new page inherits clean
  code rather than cloning the duplication.
- GDSPS need not be ripped out immediately — CIOPS can run alongside for
  comparison/hindcast continuity before switchover.

## RDWPS recon — verified live 2026-08-15

- **Datamart layout has changed** to date-first:
  `https://dd.weather.gc.ca/YYYYMMDD/WXO-DD/model_rdwps/…`, with a stable
  `https://dd.weather.gc.ca/today/…` alias. The old `/model_rdwps/` root
  cited elsewhere 404s.
- **There is no Pacific subdomain.** Domains are the four Great Lakes plus
  `national/2.5km/` — and the national rotated grid (0.0225°, 1286×2536,
  lat 27–71, lon −153…−41) covers the Pacific and resolves the Strait:
  nearest cell to the Halibut Bank buoy is ~400 m away and is a valid sea
  point.
- **Files:** one GRIB2 per variable per lead hour, ~1.9 MB each;
  19 variables × 49 lead hours (0–48 h) = 931 files/run. Variables include
  HTSGW, WVHGT, swell partitions (SWHF/SSWEL, PWPF/SSWEL, MWDF/SSWEL),
  periods (PWPER, MZWPER, PPERWW), directions (WVDIR, PWAVEDIR, WWSDIR),
  wind (UGRD/VGRD), Stokes drift (USSD/VSSD), ICEC. Runs 00/06/12/18Z
  (12Z files landed ~15:24 UTC, so latency ≈ 3.5 h).
- **GeoMet WMS works for point extraction** and matches the GRIB exactly:
  `GetFeatureInfo` on layer `RDWPS_2.5km_SignificantWaveHeight` at Halibut
  Bank returned 0.11212 m for 2026-08-16T12Z — bit-identical to the value
  pygrib read from the corresponding GRIB2 file. 15 national layers exist
  (`RDWPS_2.5km_*`: SignificantWaveHeight, PeakWavePeriod, PeakWaveDir,
  MeanWaveDir, MeanZeroCrossingWavePeriod, wind-wave + first/second swell
  height/period/direction, IceFraction). JSON responses carry `value`,
  `time`, and `dim_reference_time` (model run).
- **Decoding GRIB2 needs pygrib** (or cfgrib/eccodes) — not currently in
  the project venv; pygrib 2.1.8 installs clean from wheels and reads the
  files without system deps.

**Implemented 2026-08-15:** `scripts/fetch/fetch_wave_forecast.py` fetches
4 fields × 49 timesteps at Halibut Bank via GeoMet `GetFeatureInfo`,
stores every run to `wave_forecast.sqlite` (epoch timestamps, 60-day
retention) and exports `site/data/wave_forecast/<buoy>.json`. Full
parameter inventory + parse notes in
[`RDWPS_PARAMETERS.md`](RDWPS_PARAMETERS.md). In cron 4×/day since 2026-08-15.

**Extended 2026-08-17 — HRDPS wind in the same fetch.** 3 more fields
(`wind_speed`, `wind_direction`, `wind_gust` from WSPD/WD/WGX), because RDWPS
publishes no wind WMS layer and the wave series alone cannot be sanity-checked:
a 0.72 m overnight peak read as implausible until it was cross-checked against
17 kt of model wind. Two models in one archive, so every row now carries a
`model` and `wave_forecast_run` keys on it. 932 requests/day, 2.9% of the MSC
guidance, same 0.51 req/s burst rate. Backend only at the time; the page picked
these fields up on 2026-08-18 (see "Wind on the page" below).

**Second station, 2026-08-18 — Crescent Beach Ocean (`CRPILE`).** The selection
rule: *a forecast point earns its place by having a co-located sensor to verify
against.* CRPILE went first for that reason rather than because the model does
well there — a 2.5 km cell over a bay that dries is exactly where RDWPS should
struggle, and Surrey's sensor at the same spot is how we find out by how much.
464 requests/run, 1,856/day, 4.0% of the guidance; burst rate unchanged.

The page grew a station picker driven by `site/data/wave_forecast/index.json`,
which the fetcher writes from the stations that actually produced a file — so a
station added to `BUOY_IDS` appears in the picker and in the map popups with no
frontend edit. Map popups link to `/forecasts.html#wave-<station>`, and the
table thins to 3-hourly for display while the chart and database keep the hourly
steps.

**Remaining candidate points, in the user's priority order:** ① Boundary Bay /
Crescent Beach *(done)* ② Hein Bank ③ Sombrio Beach ④ Long Beach, Tofino. All
four were probed over WMS on 2026-08-18 and **none is land-masked** — every one
returns real values. Hein Bank is the natural next one: NOAA `46088` is labelled
"New Dungeness / Hein Bank" and is already fetched, so it comes with free
verification. For Sombrio and Long Beach, site the point a few kilometres
*offshore* — a shore cell is part land, and RDWPS solves offshore sea state, not
breaking surf.

**② Hein Bank is not blocked at all** — corrected 2026-08-18. Hein Bank is the
old name for the New Dungeness buoy: `46088` is already in `stations.json` as
`"name": "New Dungeness", "location": "Hein Bank"`, already fetched hourly, and
already reporting. So it is the same one-line `BUOY_IDS` change CRPILE was.

Use `get_buoy("46088")` and let the registry supply the coordinates —
48.333, -123.167. A hand-picked "Hein Bank" point taken from the shoal's own
position lands 48.35, -123.03, **10.3 km away** and on the wrong side of the
eastern Strait of Juan de Fuca. That is the silent-wrong-station trap; the
registry is the defence.

It is also the *better* verification target of the two we have. `46088` reports
spectral separation — `wind_wave_height`/`period`/`direction` and swell apart
from each other — so it can score RDWPS's `WindWavesSignificantHeight` partition
directly, which the EC buoy at Halibut Bank cannot. It reports wind and gust
too, so it scores the HRDPS side in the same pass.

A single spot-check on 2026-08-18 (model vs a 73-minute-old observation, one
hour, one sample — not a validation): model wind-wave 0.203 m vs 0.2 m observed,
Hs 0.212 m vs 0.3 m (the buoy quantises to 0.1 m), mean wave direction 252° vs
232° observed average, wind 9.3 km/h vs 5.8 km/h. Encouraging, and nothing more
than that until autumn.

**Blocker for ③–④ only:** Sombrio Beach and Long Beach are not in
`config/stations.json` and are not instruments — adding them under `buoys` would
put phantom stations on the map and in the buoy cards. Those two need a
forecast-only registry section first. Request budget is *not* the blocker (user
confirmed 2026-08-18 that volume is fine).

**Wind on the page — built 2026-08-18.** The open design question this replaces
was how wind should arrive on the page: the user had spitballed "a selector bar
spanning the main div with two buttons, Wind and Wave, showing one or the
other," and the counter-proposal here was to toggle the *chart* only while
keeping one table carrying both sets of columns, on the grounds that RDWPS is
WW3 forced by HRDPS wind — the two series are cause and effect, not peers, and
an exclusive either/or works against the one thing the pairing buys. Settled by
building it: a **segmented control** (a row of joined buttons, exactly one
active) switching the section between **Waves** (default) and **Wind**. Two
copies of the control, above the chart and below the table, kept in sync by
`setForecastMode()` — the same duplicate-and-sync pattern as the 24h/48h toggle
on `winds.html`.

The toggle governs **both the chart and the table columns** — the user's call
over that chart-only compromise, and the right one for two reasons:

- **The 8-column table did not fit a phone.** Waves and wind together measured
  ~800 px against 356 px of usable width. Split by mode, the wind view is 356 px
  — it fits exactly, no sideways scroll — after trimming cell padding and font
  size under 600 px. The waves view is 499 px and still scrolls inside its
  wrapper, which is what that wrapper is for.
- **A control duplicated below the table has to govern the table.** Otherwise
  the lower copy silently drives a chart that is scrolled off-screen above it.

The cause-and-effect caution still stands, and is answered in the page copy
rather than in the layout: each panel names its own model (`Waves — RDWPS`,
`Wind at 10 m — HRDPS`), and the provenance block prints one `Model: … — run …`
line per model, from the payload's `models` array, so a run divergence between
the two is visible rather than silent. The stacked-meteogram idea is *not* dead
— it is the natural wide-screen upgrade, and nothing here forecloses it.

**Combining the two tables is parked, not rejected** (user, 2026-08-18: "nearly!
although the wind and wave directions dont completely add up"). That divergence
is physics, not a data fault: wave direction tracks wind direction only in a
pure wind-sea; any swell component pulls the two apart, which is precisely what
the wind-wave-height partition column reports.

Implementation notes worth keeping:

- **ECharts cannot be initialised inside a hidden panel.** It measures its
  container at init, so a chart built while `hidden` gets a zero-sized canvas
  that a later `resize()` does not fully recover. The hidden chart is therefore
  not initialised until its panel is shown, and each chart is redrawn every time
  it becomes visible — which also means a theme change while a chart was hidden
  cannot leave stale palette colours behind.
- **Plain buttons with `aria-pressed`, not tab roles.** A `tablist` duplicated
  on the page would have two tabs claiming `aria-controls` over one panel.
  Panels use the `hidden` attribute. Verified against the axe suite in both
  themes.
- **Wind is stored km/h and displayed knots**, converted at parse time in
  `toSortedRows()` — the site-wide convention, and the reason the JSON carries
  km/h while every label says kt.
- **Gusts are a scatter series, never a line.** HRDPS masks the gust at most
  hours, and a line would draw segments across hours where no gust was
  diagnosed. The tooltip says "none forecast" there rather than showing a gap.
- **Control sizing** (user, 2026-08-18): the segmented control and the station
  picker share one box — 40 vw centred on desktop, 75 vw under 600 px, matched
  left and right edges. 40 vw is only ~156 px on a 390 px phone, too cramped for
  two labels and a poor tap target.

Pre-existing and unrelated: the page scrolls horizontally between roughly
700–950 px viewport width, caused by `.nav-actions`/`.theme-toggle` in the
shared nav reaching 912 px. Verified byte-identical before and after this work.

**Verification writer — built 2026-08-19.**
`scripts/monitoring/verify_wave_forecast.py`, daily at 03:40 UTC. It reads the
forecast archive, finds values whose valid time has passed and that aren't yet
scored, and writes one `wave_forecast_verification` row per pair: what the
model said, what the buoy measured, and what the buoy read at run time (the
persistence baseline). Local SQLite only — no network.

Both forecast stations turn out to share one observation source:
`buoy_data.sqlite`'s `buoy_observation` holds 4600146 *and* CRPILE, because the
Surrey fetch writes into the same table. So there is no cross-database join
here at all.

The design questions that mattered, and how they were settled:

- **"The forecast changes" is not a problem to solve** — it is already solved
  by `wave_forecast`'s primary key. Every run's prediction for a given valid
  time is its own row, never overwritten, so `lead_hours` is just
  `valid_time - forecast_run_time` and nothing needs reconciling.
- **Score every lead, not a 24-hour window.** The user's opening instinct was
  to look at hours 24–30 of each run. But the taper means each run yields
  *exactly one* `lead_hours = 24` row per station/variable, not a range — so
  "24 h skill" is `WHERE lead_hours = 24`, and the entire 0–48 h curve is
  already in the archive for free. Restricting the writer would have thrown
  away the lead-time decay curve, which is the more interesting result.
- **Directions must use circular arithmetic.** New in
  `lib/directions.py`: `circular_difference()`. A 010° forecast against a 350°
  observation is a 20° error; plain subtraction scores it as −340°, and one
  such pair dominates an RMSE. Northerlies straddle 0° here constantly. Two
  tests guard the mapping in both directions, so a newly added bearing cannot
  default to scalar arithmetic.
- **The 0.5 m event gate is applied at read time, not write time** (user,
  2026-08-19). Sub-threshold rows are still stored: a 0.7 m forecast that never
  materialised is a *false alarm*, and dropping it at write time would make the
  model look better the worse it got. The gate is
  `forecast >= 0.5 OR observed >= 0.5` — `OR`, because gating on the
  observation alone discards false alarms and gating on the forecast alone
  discards misses, which are the two failures the score most needs to catch.
  The summary prints gated and ungated side by side.
- **`wind_wave_height` is unverifiable and says so.** The column exists in
  `buoy_observation` but neither station has ever populated it (0 of 720 at
  4600146, 0 of 4312 at CRPILE) — the wind-sea/swell partition is a model
  diagnostic our instruments don't report. Mapping it to an always-NULL column
  would read as a permanent data outage; `OBSERVATION_COLUMNS` maps it to
  `None` instead. `wave_direction` is likewise absent at CRPILE, but that is
  per-station and needs no special case: no observation simply means no pair.
- **A written pair is never revised** (INSERT OR IGNORE), so a late buoy
  backfill cannot rewrite a past score. A pair with no observation yet writes
  *nothing* and is retried until it falls out of the 7-day lookback.

**Peak timing is deliberately not handled here, and is the next metric.** Wind
and wave events are peaky, and exact point matching penalises a phase-shifted
forecast twice — once for the peak that didn't happen, once for the peak it
missed. The fix is *not* a fuzzy valid-time join, which would reward a forecast
that is wrong at every specific hour and defeat the question the 3-hourly rows
past +24 h exist to answer. It is a separate metric over the same archive:
model peak value and its time vs observed peak value and its time, recording
`timing_error_hours` and `magnitude_error` **separately** — "does the model
know how big" and "does it know when" are the two things point-RMSE fuses into
one number. Parked until there are peaks: a peak-timing metric with an empty
archive is untested code. With September close, that wait should be short.

**First backfill, 2026-08-19:** 2,420 pairs from four days of archive (78
awaiting observations, 329 skipped as unverifiable — all `wind_wave_height`).
The headline number is a self-validation, not a result: **HRDPS wind bias at
Halibut Bank is ≈0 at 0–12 h lead (−2 to +5 km/h at every hour of the day) and
grows to +12.4 km/h at 25–48 h**, peaking near +23 km/h overnight. A units,
timestamp, or station-matching bug would have shown up as error at *every*
lead; near-zero short-lead bias is what says the pipeline is sound. The
long-lead over-forecast is a real (tiny-sample, four-day, calm-season) signal
to re-check after an actual event.

Two caveats to carry into any published version of these numbers:

- **Direction errors are noise at low wind.** Wave direction RMSE of 64–94° and
  wind direction ~100° at 4600146 are mostly a calm-summer artefact — a bearing
  is meaningless when the speed is near zero. A companion-variable gate (score
  direction only above some speed) is the fix; not built, because it needs a
  threshold picked for a reason rather than guessed.
- **The `wind_gust` sample is self-selected.** HRDPS only diagnoses a gust where
  there is one, so gust rows exist only at hours the model thought were windy —
  precisely the hours it was most wrong. The +27 km/h gust bias is that
  selection effect, not a conversion error: the forecast gust/sustained ratio is
  a consistent 1.18–1.29, and the sustained wind is over-forecast by the same
  margin at those hours.

**Forecast verification plot — built 2026-08-19.**
`scripts/export/export_forecast_verification.py` →
`site/data/wave_forecast/verification/{station}.json`, every 10 minutes; the
**Forecast Verification** section at the bottom of `forecasts.html` draws it.
Last 48 h, observed against what the model said about a day ahead.

**Terminology, settled 2026-08-19.** This is *not* a hindcast, and "backcast"
is not a real term of art. A hindcast re-runs a model over past dates, usually
with better forcing than a live forecast had. What we are drawing is forecasts
that were genuinely *issued in advance* and archived at the time, compared
against observations — textbook **forecast verification**. The storm-surge
"hindcast" on `storm_surge.html` is the same thing under the same wrong name;
renaming it is a separate change (live page, public JSON path) and is not done
here.

**The model line is a 19–24 h lead band, not a flat 24 h.** For each past hour
the export takes the largest available lead ≤ 24 h. It cycles 19→24 and drops
back, because runs are 6-hourly *and* the fetch taper (hourly to +24 h, then
3-hourly) means leads 25 and 26 do not exist in the archive at all — so one
hour past a run boundary the next candidate down is 19 h, not 25 h. Capping at
24 rather than taking the *nearest* lead to 24 is deliberate: it guarantees the
curve never claims more notice than it had. Every point carries its own
`lead_hours`, the payload publishes the band, and the page caption explains the
six-hourly seams rather than smoothing them.

Design notes:

- **Nothing is paired here.** Two independent series on a shared time axis; the
  chart aligns them. Pairing is the verification *writer's* job and answers a
  different question — a plot wants every observation it can draw, including
  the last few hours the writer's settle window holds back. Both read the same
  `wave_forecast` archive, so chart and scores cannot disagree.
- **The observation half was nearly free.** Both stations live in
  `buoy_data.sqlite`'s `buoy_observation` (the Surrey fetch writes CRPILE
  there), and `OBSERVATION_COLUMNS` is imported from the verification writer so
  one place decides which instrument measures which variable.
- **One comparison per view.** Significant wave height and sustained wind only.
  Period and direction would put four lines on a chart whose whole job is to
  make one gap obvious. Gusts are excluded for a stronger reason: HRDPS masks
  the gust at most hours, so the forecast side would be a near-empty line
  beside a continuous observed one — reading as "the model predicted calm"
  rather than "the model was not asked".
- **No new control.** The two panels reuse the existing `data-panel`
  values, so the segmented control already drives them; a third copy of the
  control sits directly above the plot (user, 2026-08-19) and needed no JS,
  since `setForecastMode()` syncs every `.forecast-mode-btn` on the page.

**Legend bug, fixed 2026-08-19** (user-reported, pre-existing on the forward
charts): ECharts draws the legend swatch from the series `itemStyle`, not from
`lineStyle` or from a per-*point* `itemStyle`. Every series here coloured only
its line — or, for the direction arrows, only its individual points — so the
swatch fell through to ECharts' default palette: series index 1 got `#91cc75`
(the green circle on the dashed peak-period line) and index 2 got `#fac858`
(the yellow direction marker that matched nothing on the chart). Fixed by
setting a series-level `itemStyle` on all seven series across the three charts.

**Mobile dev-note, fixed 2026-08-19** (user-reported): `.dev-note` was a
non-wrapping flex row with a `flex-shrink: 0` badge, so on a 390 px phone the
text was squeezed into a ~215 px column and the note ran 252 px tall. Now
`flex-wrap: wrap` with `flex: 1 1 18rem` on the text — a width threshold rather
than a media query, so it also covers a narrow desktop window. 252 px → 193 px,
desktop layout unchanged.

**Next:** the peak-timing metric once an event lands, then the forecast-only
station registry, and Hein Bank behind it. Open question from the user
(2026-08-19): whether to put **more than one lead time** on the verification
plot — see below.

## Next: add English Bay (4600304) as a third extraction point

Decided 2026-08-21, to build next session. Wreck Beach was the original idea —
it is the more interesting spot — but English Bay wins because it sits on a
buoy, and a forecast point without an instrument under it cannot be scored.

**Why this point**

- `4600304` reported **711 significant-wave-height observations in the last 30
  days**, the same cadence as Halibut Bank, so the verification writer scores
  it from the first run rather than after a bring-up period.
- It is **27 km from Halibut Bank** — a genuinely different sea state, not a
  near-duplicate of a point we already carry.
- Wreck Beach is only **8.3 km** away but sits behind Point Grey. A 2.5 km
  regional model in a sheltered nearshore corner is exactly where it is least
  trustworthy, and there is no instrument there to catch it being wrong.

**Known cost** (the per-point figure in `docs/DATA_FEEDS.md`, unchanged)

- +231 requests/run, +924/day. ECCC total goes 3,406 → 4,330/day, 3.9% → 5.0%
  of the 86,400 guidance.
- Run length ~15 → ~22.6 min. Still well inside both binding constraints: the
  6 h gap to the next run, and the 1 h stale-lock threshold in
  `acquire_lock()`. **Do not raise `FETCH_DELAY`** — the burst rate is what
  matters and it stays at 0.51 req/s.

**Steps**

1. Add `"4600304"` to `BUOY_IDS` in `scripts/fetch/fetch_wave_forecast.py`.
2. Confirm the RDWPS/HRDPS grids resolve at 49.30/-123.36 — a GetFeatureInfo
   over open water there should return values, not nulls or a land mask.
3. Update the `fetch_wave_forecast.py` row and the running total in
   `docs/DATA_FEEDS.md` **in the same commit** (the table is hand-maintained
   by decision; that is the whole discipline holding it together).
4. Check the station picker on `forecasts.html` picks it up — it reads
   `data/wave_forecast/index.json` and stays empty while only one station has
   a forecast, so this is the first run with three.
5. Let verification accumulate a few weeks before judging.

**What it decides.** If RDWPS scores well at English Bay, that is the
evidence-based case for a Wreck Beach point. If it scores badly at a *buoy*,
that is also the answer about the sheltered spot — and cheaper to learn here.

## Open: more forecast runs on the verification plot

Today the plot draws one stitched lead band (19–24 h). The archive holds every
run, so drawing several is only a rendering decision, not a data one. Worth
weighing when we pick it up:

- **What it buys.** A 6 h band beside the 24 h band beside the observation
  shows *convergence* — whether the model walked toward the truth as the event
  approached, which is the single most useful thing a reader can learn about
  how far ahead to trust it. It also makes a timing error legible: three leads
  all predicting the same peak three hours early is a very different story from
  three leads disagreeing with each other.
- **What it costs.** Each extra band is another dashed line on a chart that
  currently makes one gap obvious. Three or four lines risks the storm-surge
  hindcast's problem, where the eye cannot separate the runs.
- **Likely shape.** Two bands, not four: ~6 h ("what you'd have seen that
  morning") and ~24 h ("what you'd have seen the day before"), with observed
  as the solid anchor. Lead bands would become a list in the exporter rather
  than one constant, which the payload's `lead_band` key already anticipates.
- **Decide after an event.** On four calm days every lead looks alike; the
  question answers itself the first time a gale runs through the archive.

**Implication — starting architecture:** clone the
`fetch_storm_surge.py` GeoMet pattern (owslib `getfeatureinfo` per station
per timestep) for the wave variables at the four EC buoys + chosen surf
points. Near-zero new code, no GRIB tooling, JSON in. Mind request volume:
points × variables × 49 timesteps per run — trim to ~4 variables and
consider 3-hourly steps beyond +24 h. The sr3 + GRIB2 route stays as the
later upgrade path if we outgrow WMS (whole-strait fields for a map layer,
or WMS flakiness).

## To verify before building

- Exact Datamart directory layout, file formats (GRIB2?), and variables for
  both models; whether RDWPS has a Pacific/Salish subdomain or only national.
- Model run cadence and latency for each.
- AMQP topic paths for sr3 subscription configs.
- Grid-point extraction approach (nearest-point vs interpolation) for our
  station locations; GRIB tooling for the venv.
- Hindcast/validation plan: compare model output against our own buoy
  observations (we uniquely have the ground truth for this).
