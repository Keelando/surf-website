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
[`RDWPS_PARAMETERS.md`](RDWPS_PARAMETERS.md). Not yet in cron.

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
