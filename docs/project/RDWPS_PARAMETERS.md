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
