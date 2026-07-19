# Forecast Section Upgrade — Salish Sea–Resolving Models

**Status:** Planning (lead captured 2026-07-19; details unverified)
**Goal:** Replace/augment the current forecast offering with model data that
actually resolves the Strait of Georgia, feeding a better forecast section.

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
  complex coastlines). CIOPS-SalishSea is 1/36° (~2.5 km), nested
  specifically to Salish Sea / Strait of Georgia bathymetry. Also provides
  tidal heights and ocean currents.
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

## To verify before building

- Exact Datamart directory layout, file formats (GRIB2?), and variables for
  both models; whether RDWPS has a Pacific/Salish subdomain or only national.
- Model run cadence and latency for each.
- AMQP topic paths for sr3 subscription configs.
- Grid-point extraction approach (nearest-point vs interpolation) for our
  station locations; GRIB tooling for the venv.
- Hindcast/validation plan: compare model output against our own buoy
  observations (we uniquely have the ground truth for this).
