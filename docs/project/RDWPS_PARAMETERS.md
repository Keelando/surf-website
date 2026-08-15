# RDWPS Parameters — Full Enumeration at Halibut Bank

**Captured:** 2026-08-15, from the 12Z national run, lead hour +24
(valid 2026-08-16T12:00Z), decoded with pygrib 2.1.8.
**Grid cell used:** (358, 395) on the national rotated grid —
lat 49.33356, lon −123.72941, ~400 m from the Halibut Bank buoy
(`4600146` at 49.337, −123.731). The cell is open water (unmasked).

Purpose: the complete inventory of what RDWPS publishes per lead hour, so
we can choose fields deliberately. Fetch architecture and priorities live
in [`FORECAST_UPGRADE.md`](FORECAST_UPGRADE.md).

## All 19 GRIB2 variables (one file each per lead hour)

Filename pattern:
`{run}T{HH}Z_MSC_RDWPS_{VAR}_{LEVEL}_RLatLon0.0225_PT{hhh}H.grib2`
under `https://dd.weather.gc.ca/today/model_rdwps/national/2.5km/{HH}/`.
`LEVEL` is `Sfc` for everything except wind, which is `AGL-10m`.

| # | VAR code | GRIB name | Units | GeoMet WMS layer | Sample @ Halibut Bank |
|---|---|---|---|---|---|
| 1 | `HTSGW` | Significant height of combined wind waves and swell | m | `RDWPS_2.5km_SignificantWaveHeight` | 0.1121 |
| 2 | `WVHGT` | Significant height of wind waves | m | `RDWPS_2.5km_WindWavesSignificantHeight` | 0.1121 |
| 3 | `SWHFSWEL` | Significant wave height, first swell partition | m | `RDWPS_2.5km_FirstSwellSignificantWaveHeight` | *masked* |
| 4 | `SWHSSWEL` | Significant wave height, second swell partition | m | `RDWPS_2.5km_SecondSwellSignificantWaveHeight` | *masked* |
| 5 | `PWPER` | Peak wave period | s | `RDWPS_2.5km_PeakWavePeriod` | 1.9469 |
| 6 | `PPERWW` | Peak wave period of wind waves | s | `RDWPS_2.5km_WindWavesPeakPeriod` | 1.9473 |
| 7 | `PWPFSWEL` | Peak wave period, first swell partition | s | `RDWPS_2.5km_FirstSwellPeakWavePeriod` | *masked* |
| 8 | `PWPSSWEL` | Peak wave period, second swell partition | s | `RDWPS_2.5km_SecondSwellPeakWavePeriod` | *masked* |
| 9 | `MZWPER` | Mean zero-crossing wave period | s | `RDWPS_2.5km_MeanZeroCrossingWavePeriod` | 1.4414 |
| 10 | `WWSDIR` | Mean wave direction (wind waves + swell combined) | ° true, FROM | `RDWPS_2.5km_MeanWaveDir` | 135.85 |
| 11 | `WVDIR` | Direction of wind waves | ° true, FROM | `RDWPS_2.5km_WindWavesDir` | 135.86 |
| 12 | `PWAVEDIR` | Peak wave direction (total) | ° true, FROM | `RDWPS_2.5km_PeakWaveDir` | 137.59 |
| 13 | `MWDFSWEL` | Mean wave direction, first swell partition | ° true, FROM | `RDWPS_2.5km_FirstSwellMeanWaveDir` | *masked* |
| 14 | `MWDSSWEL` | Mean wave direction, second swell partition | ° true, FROM | `RDWPS_2.5km_SecondSwellMeanWaveDir` | *masked* |
| 15 | `UGRD` | 10 m U wind component (level `AGL-10m`) | m/s | — none | −1.591 |
| 16 | `VGRD` | 10 m V wind component (level `AGL-10m`) | m/s | — none | 1.732 |
| 17 | `USSD` | U-component surface Stokes drift | m/s | — none | −0.0045 |
| 18 | `VSSD` | V-component surface Stokes drift | m/s | — none | 0.0058 |
| 19 | `ICEC` | Sea ice area fraction | 0–1 | `RDWPS_2.5km_IceFraction` | 0.0 |

## Parse notes (verified against live data)

- **WMS values are bit-identical to the GRIB.** `GetFeatureInfo` on the
  layers above returned the exact pygrib values (e.g. HTSGW 0.11212,
  PWPER 1.9469037, WWSDIR 135.85182) — WMS point extraction loses nothing
  vs. downloading the grids.
- **Directions are coming-FROM, degree true** — matching the site-wide
  meteorological convention. Verified empirically: the model's own 10 m
  wind (UGRD −1.59, VGRD 1.73 → wind *from* 137°) agrees with WVDIR
  135.9° on a pure wind-sea day. No conversion needed.
- **Masked ≠ error.** Partition fields are masked wherever that partition
  doesn't exist — swell is often absent inside the Strait (today
  WVHGT = HTSGW, i.e. 100 % wind sea), and WVHGT itself masks out during
  glassy-calm hours. Land cells are masked in every field. Store as
  NULL / omit from JSON.
- **Over WMS, masked cells come back as the sentinel `9999.0`**, not as an
  empty feature list — filter `value >= 9000` before storing (live-tested:
  22 of 49 wind-wave-height timesteps were 9999.0 on a calm night).
- **Wind files use level tag `AGL-10m`, not `Sfc`** — a naive
  `_Sfc_`-pattern fetch 404s on UGRD/VGRD.
- **UGRD/VGRD/USSD/VSSD have no GeoMet WMS layer** — GRIB2-only. If we
  ever want the model's forcing wind or Stokes drift at a point, that
  forces the sr3 + GRIB route (or HRDPS layers, which GeoMet does carry).
- **GeoMet time dimensions** (per layer, from single-layer
  `GetCapabilities` — multi-layer filtering is not supported):
  `time` = 49 hourly steps spanning the current run's 0–48 h;
  `reference_time` = last 5 runs at PT6H (00/06/12/18Z). Responses carry
  `dim_reference_time`, so the actual model run is known per value.
- **Datamart cadence:** 4 runs/day; the 12Z files landed ~15:24 UTC
  (≈3.5 h latency). ~1.9 MB per variable per lead hour.

## Transport: why WMS points, not GRIB2 (settled 2026-08-15)

**A Datamart file and a WMS point query have the same granularity — one
variable at one lead hour.** That single fact decides the architecture, and
it's the thing most likely to be re-litigated, so: a run directory holds
19 variables × 49 lead hours = **931 files**, one field each. There is no
bundled per-run file, and the time axis cannot be collapsed on either path.

For our 4 fields × 49 hours, both routes are therefore **196 requests**:

| | requests | bytes | values kept |
|---|---|---|---|
| WMS `GetFeatureInfo` | 196 | 136 KiB (710 B each) | 196 |
| GRIB2 download | 196 | 388 MB (1.98 MB each) | 196 |

GRIB2's cost is flat in stations — one national file serves every marine point
in Canada — but the crossover where that pays off is **~2,800 stations**
(`stations × 710 B > 1.98 MB`). At the 8 marine stations in our registry it's
4.5 MB/day versus 1,550 MB/day. **More buoys is not a reason to switch.**

GRIB2 becomes the right tool only for:
1. **Whole fields** — a wave map we render ourselves (GeoMet `GetMap` tiles
   may cover that anyway without touching a GRIB), or spatial analysis.
2. **The 4 GRIB-only variables** — UGRD/VGRD/USSD/VSSD have no WMS layer, and
   HRDPS is the better wind source regardless.

Two further constraints: MSC's usage policy says systematic retrieval **must**
use AMQPS rather than HTTP, so a GRIB2 route would have to go through sr3, not
polling. And decoding needs `pygrib`/`cfgrib` over native eccodes — by far the
heaviest thing that would enter the deliberately pruned venv.

### No batching exists (all three tested live, all rejected)

- Multi-layer `LAYERS`/`QUERY_LAYERS` → `InvalidLayersParameter`
- `TIME` as an interval (`start/end/PT1H`) → `NoMatch`, wants one instant
- `TIME` as a comma-separated list → same `NoMatch`
- RDWPS is **not** among the 104 collections on `api.weather.gc.ca`, so there
  is no OGC API / EDR position query returning a series in one call

One request per (variable, timestep) is the floor. The only real lever is
fetching fewer timesteps — see `taper_time_steps()`.

### Anatomy of a 2 MB GRIB2 file

Should we ever build the GRIB path, this is what one file contains — verified
by parsing sections directly on `HTSGW_Sfc_..._PT024H.grib2`:

| Section | Size | Contents |
|---|---|---|
| 1 | 21 B | centre 54 (CMC), reference time |
| 3 | 84 B | rotated lat/lon, 2536 × 1286 = 3,261,296 points, 0.0225° (~2.5 km), south pole at (−36.09, 245.31) |
| 4 | 34 B | category 0 / parameter 3, forecast time +24 h |
| 5 | 23 B | 1,156,051 data points, JPEG 2000 packing, 20 bits/value |
| 6 | 408 KB | bitmap: 1 bit per grid point, wet or dry |
| 7 | 1.57 MB | the packed field, ~10.85 bits/wet point after compression |

Only **35.4%** of the grid is water. Section 7 is a single JPEG 2000
codestream over the whole grid, so **HTTP range requests cannot subset it** —
there is no "just fetch the Strait's corner" middle ground.

- **⚠️ Coordinates are sign-bit magnitude, not two's complement.** Reading La1
  as a normal signed int gives −2135.2261°; masking the high bit gives the
  real −12.2575°. Same for the pole latitude. This produces a plausible-looking
  grid silently in the wrong hemisphere.

### Measured timings (2026-08-15)

- **Datamart publication: run + 3 h 25 m.** Files for both the 12Z and 18Z runs
  landed at HH:23–HH:26, to the minute. GeoMet had the 18Z run by 21:47.
  Cron is set at run + 4 h 35 m for margin.
- **WMS latency:** 493 ms per request cold, 376 ms with a reused session —
  hence `SESSION` in the fetcher.
- **Datamart throughput:** ~3 MB/s, so 388 MB would be ~130 s per run. GRIB2 is
  not *slower* than WMS; the waste is bytes, not wall-clock.

## Verification: ours to compute, not EC's

ECCC verifies RDWPS against buoys and publishes scorecards in the
[RDWPS technical note](https://collaboration.cmc.ec.gc.ca/cmc/CMOI/product_guide/docs/tech_notes/technote_rdwps_e.pdf),
but it doesn't answer our question: results are **aggregated by domain**
(Great Lakes, ocean) rather than per-buoy, expressed largely as **percent
change in scatter index versus the previous model version** rather than
absolute error, and published **at version upgrades** rather than
continuously. *(Characterised from search results — the PDF was not parsed
locally, no poppler on this host. Confirm before citing.)*

**Bias and RMSE are error metrics, not skill scores.** A skill score is
relative to a reference: `SS = 1 − MSE_forecast / MSE_reference`. The
reference that matters here is **persistence** — "conditions stay as they are
now" — because that decides whether the forecast beats simply showing the
current buoy reading, and therefore how far out the page should display it.
Persistence is hard to beat at short leads and should lose badly by 36 h;
finding the crossover lead time is the genuinely useful number.

That is why `wave_forecast_verification` carries `reference_value` (the buoy
reading at the model run hour) alongside `forecast_value` and
`observed_value` — computing it later means re-deriving every past t0.

**Don't trust calm-summer numbers.** First comparison (12Z run, n=10) gave
bias −0.095 m, RMSE 0.102 m — but the buoy quantises Hs to 0.1 m (the only
distinct values in a week were 0.1–0.7), so that bias is one reporting step,
and 90 days of history has nothing above 1.5 m. Peak period ran 0.7–1.0 s
below the buoy. Real skill numbers need autumn.

## Field selection for phase 1 (fetched by `scripts/fetch/fetch_wave_forecast.py`)

| Our field | VAR / layer | Why |
|---|---|---|
| `wave_height` | HTSGW / SignificantWaveHeight | headline number; buoy `VWH$` comparable |
| `peak_period` | PWPER / PeakWavePeriod | buoy `VTP$` comparable |
| `wave_direction` | WWSDIR / MeanWaveDir | display arrow; combined sea state |
| `wind_wave_height` | WVHGT / WindWavesSignificantHeight | with HTSGW, tells wind-sea vs swell mix |

Deliberately skipped for now: swell partitions (mostly masked in-Strait —
revisit for outer-coast points), MZWPER/PPERWW (redundant with PWPER for
display), ICEC (not relevant), wind/Stokes (GRIB-only; HRDPS is the
better wind source anyway).
