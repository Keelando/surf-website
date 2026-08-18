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

**Next:** the verification writer — which wind unblocks, since it varies enough
to score this summer where the waves do not. Then the forecast-only station registry, and
Hein Bank behind it.

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
