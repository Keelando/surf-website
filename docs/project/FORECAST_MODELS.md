# Salish Sea Marine Forecast Models — Reference

**Status:** Reference catalogue (captured 2026-08-15)
**Purpose:** A working catalogue of the numerical models worth considering
for local wave, wind, water-level and storm-surge forecasting in the Salish
Sea / Strait of Georgia — what each is, *where* it's valid, *when* it's
valid (forecast horizon + update cadence), *which parameters* it carries,
and how to access it.

**Region of interest:** Salish Sea, Strait of Georgia, Boundary Bay,
Juan de Fuca Strait.

**Related:** implementation plan in [`FORECAST_UPGRADE.md`](FORECAST_UPGRADE.md);
current feed inventory in [`../DATA_FEEDS.md`](../DATA_FEEDS.md).

> **Note on specs:** Forecast horizons, resolutions and run cadences change
> between model versions. The figures below are current to the best
> available documentation, but confirm the exact spec on each system's MSC
> Open Data "readme" page before locking anything into production. Acronyms
> are spelled out on first use.

---

## 1. Waves

| Model | Where (domain / resolution) | When (horizon / cadence) | Parameters | Access |
|---|---|---|---|---|
| **RDWPS — Pacific North-East** (Regional Deterministic Wave Prediction System) | NE Pacific + BC coast. Native unstructured **1–5 km** grid; national product regridded to **~2.5 km** rotated lat/lon (0.0225°). Resolves inner coastal waters far better than any global model. | **48 h** horizon; runs several times daily (00/06/12/18Z — confirm cadence for the ocean domain). | Significant wave height, peak period, mean/primary wave direction, **combined wind-wave & swell direction (WWSDIR)**, partitioned swell vs. wind-sea, **Stokes drift U/V (USSD/VSSD)**. | GRIB2 on MSC Datamart (`/model_rdwps`); MSC GeoMet Web Map Service (WMS). |
| **GDWPS** (Global Deterministic Wave Prediction System) | Global, **25 km** (0.25°). Cells are larger than the Strait, so it is effectively blind inside enclosed basins; strength is open-ocean swell approaching Juan de Fuca. | Medium-range (**~5–10 days**; documentation has cited 120 h and 240 h across versions — confirm); twice daily. | Significant wave height, peak/mean period, wave direction, swell partitions, Stokes drift. | GRIB2 on MSC Datamart (`/model_gdwps/25km`). |
| **GEWPS** (Global Ensemble Wave Prediction System) | Global, **25 km**. 20 members + control, forced by the Global Ensemble Prediction System (GEPS). | Probabilistic, out to **~16 days**; twice daily. | Ensemble-mean significant wave height, ensemble spread, probability-of-exceedance for thresholds. | GRIB2 on MSC Datamart (`/model_gewps/25km`). |

**Use when:**

- **RDWPS** = your primary local wave engine (0–48 h). This is the one to
  validate against the buoys.
- **GDWPS** = the 2–10 day open-coast swell outlook, for after RDWPS's 48 h
  runs out.
- **GEWPS** = uncertainty and extreme-event risk framing ("how confident /
  how bad could it get") — the probabilistic angle that's often useful for
  coastal-flood / emergency planning conversations.

---

## 2. Water Level & Storm Surge

| Model | Where (domain / resolution) | When (horizon / cadence) | Parameters | Access |
|---|---|---|---|---|
| **GDSPS** (Global Deterministic Storm Surge Prediction System) *— current production model* | Global, **1/12° (~3–9 km)**. Covers the Salish Sea but coarse inshore. Modified NEMO ocean model; surge elevation derived from total water level by harmonic analysis (t_tide). | **240 h (10 days)**; twice daily. | Storm surge elevation (ETAS), total water level / sea surface height. | NetCDF on MSC Datamart (`/model_gdsps`); MSC GeoMet WMS (layer `GDSPS_15km_StormSurge`). |
| **CIOPS-SalishSea** (Coastal Ice-Ocean Prediction System — Salish Sea) | **Salish Sea only**, **500 m** (0.008 × 0.005°). Regional enhancement of CIOPS-West. Full 3-D ocean (NEMO), 39 depth levels. Finest water-level field available for your water. | **48 h**; four times daily. | **Sea surface height above geoid (total water level)**, 3-D temperature, salinity, currents, sea ice. | NetCDF on MSC Datamart (`/model_ciops/salish-sea`); MSC GeoMet WMS (layer `CIOPS-SalishSea_500m_SeaSfcHeight`). |
| **CIOPS-West** (Coastal Ice-Ocean Prediction System — West Coast) | NE Pacific + BC coast, **2 km**. Covers the outer coast and Juan de Fuca approaches beyond the 500 m Salish box. | **48 h**; four times daily. | Same family as CIOPS-SalishSea (sea surface height, temperature, salinity, currents, ice). | NetCDF on MSC Datamart (`/model_ciops/west`); MSC GeoMet WMS (layer `CIOPS-West_2km_SeaSfcHeight`). |
| **RDSPS / RESPS** (Regional Deterministic / Ensemble Storm Surge Prediction System) — **NOT VALID FOR THIS REGION** | **East coast of Canada + NE US only** (DalCoast / Princeton Ocean Model). Listed here so nobody sends the project down this path — there is **no Pacific equivalent**. | Hours to ~10 days (Atlantic). | Storm surge / water level. | MSC Datamart (`/model_rdsps`, `/model_resps`). |

**Use when:**

- **GDSPS** = keep as the broad-area storm-surge forecast (10-day horizon,
  whole region).
- **CIOPS-SalishSea** = high-resolution local total water level and
  **tidal currents**; the strongest regional water-level source for the
  Strait. Plugs straight into an existing GeoMet WMS point-extraction
  pipeline.
- **CIOPS-West** = water level / currents on the outer coast and the
  Juan de Fuca approaches.
- ⚠️ **CIOPS caveat:** sea surface height above geoid is *total* water
  level, not surge alone. Isolating the surge depends on whether CIOPS runs
  with tidal forcing — if it includes tides, subtract predicted tide
  (observed-minus-predicted residual method); if tideless, the height
  already approximates the surge. **Confirm the tide treatment before
  labelling it "storm surge."**

---

## 3. Wind & Atmosphere (drives the waves; feeds a wind-window / frontal-passage tool)

| Model | Where (domain / resolution) | When (horizon / cadence) | Parameters | Access |
|---|---|---|---|---|
| **HRDPS** (High Resolution Deterministic Prediction System) | Continental Canada incl. BC coast, **2.5 km**. This is the wind field that forces RDWPS. | **48 h**; four times daily. | 10 m wind speed & direction, gusts, mean sea-level pressure, air temperature, precipitation, and more. | GRIB2 on MSC Datamart (`/model_hrdps/continental`); MSC GeoMet WMS. |
| **RDPS** (Regional Deterministic Prediction System) | Canada + adjacent waters, **10 km**. Broader / coarser companion to HRDPS. | ~84 h; several times daily. | Same atmospheric family as HRDPS at coarser scale. | GRIB2 on MSC Datamart (`/model_rdps`); MSC GeoMet WMS. |

**Use when:**

- **HRDPS** = the actual gridded wind forecast to replace or augment the
  pasted text bulletins, and the pressure field for a frontal-passage /
  "should I head to the beach now" wind-window tool (see
  [`PRESSURE_FRONTS_PAGE.md`](PRESSURE_FRONTS_PAGE.md)).

---

## 4. Academic / Research Model

| Model | Where (domain / resolution) | When (horizon / cadence) | Parameters | Access |
|---|---|---|---|---|
| **SalishSeaCast** (University of British Columbia / CIOOS) | Salish Sea, **~500 m** (NEMO-based research model, with a wave and biogeochemistry component). | Nowcast + short forecast (typically ~1–2 days — confirm on their server). | Currents, sea surface height, temperature, salinity; biology (nitrate, phytoplankton, dissolved oxygen); waves. | **ERDDAP** (Easier Access to Scientific Data) RESTful API — **not** on MSC Datamart. |

**Use when:** research-grade cross-check, biology/water-quality, or as the
natural hook for a UBC collaboration. Not an operational service with the
same uptime guarantees as MSC systems.

---

## 5. Ground Truth — Validation Anchors (observations, not forecasts)

Any model above is only trustworthy once scored against real observations.
Available anchors (most already flowing through this project's pipelines —
see `../DATA_FEEDS.md`):

| Source | What it validates | Notes |
|---|---|---|
| **ECCC wave buoys** — Halibut Bank, English Bay, Southern Georgia Strait, Sentry Shoal | Waves, wind, sea/air temp, pressure | Reporting intervals vary (10 min to hourly). |
| **NDBC buoy 46087 (Neah Bay)** | Waves, swell partitions, wind | Offshore Juan de Fuca entrance. |
| **DFO IWLS tide gauges** (Point Atkinson, etc.) | Water level / surge | Observed − predicted = surge residual. |
| **NOAA CO-OPS 9443090 (Neah Bay)** — fixed water-level station (a.k.a. NDBC NEAW1) | Water level (+ met) at the Juan de Fuca mouth | Boundary condition for surge propagating into the Strait. |
| **Surrey FlowWorks — Crescent Beach** | Local water level / waves in Boundary Bay | Validated tidal-residual channel already in use. |

**Scoring method:** for each model point nearest a buoy/gauge, join forecast
to observed at valid time; compute **bias** and **root-mean-square error
(RMSE)** over several weeks. That's how you answer "does Environment Canada
beat the commercial apps in our backyard" with evidence rather than vibes.

---

## Quick Decision Guide

| I want… | Reach for… |
|---|---|
| Local wave forecast, next 0–48 h | **RDWPS Pacific NE** |
| Open-coast swell, 2–10 days out | **GDWPS** (add **GEWPS** for uncertainty) |
| Storm-surge forecast, whole region | **GDSPS** (current model) |
| Finest local water level & tidal currents | **CIOPS-SalishSea (500 m)** |
| Water level on the outer coast / approaches | **CIOPS-West (2 km)** |
| Gridded wind + pressure (wind windows, fronts) | **HRDPS (2.5 km)** |
| Research cross-check / biology / UBC tie-in | **SalishSeaCast** |
| Truth to score everything against | **Buoys + tide gauges (bias / RMSE)** |

## Suggested Starting Stack (lowest effort → highest value)

1. **RDWPS** wave point-extraction at buoy cells first (validate), then at
   ungauged surf points (e.g. Wreck Beach, Point Roberts south shore).
2. **CIOPS-SalishSea sea surface height** via the existing GeoMet WMS
   point-extraction pipeline — near-zero new code; confirm tide treatment.
3. **HRDPS** wind/pressure to modernize the forecast page and seed a
   frontal-passage tool.
4. **GDWPS/GEWPS** for the longer-range and probabilistic outlook.
5. Keep **GDSPS** as the surge baseline; layer tide-gauge residuals on top
   for validation.

---

*Prepared as a discussion reference. All listed MSC systems are Government
of Canada open data (Meteorological Service of Canada Datamart / GeoMet);
SalishSeaCast is University of British Columbia / CIOOS open data via
ERDDAP.*
