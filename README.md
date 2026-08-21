# Salish Sea Wave Conditions

Real-time marine weather monitoring system for the Salish Sea region.

**Live site:** [halibutbank.ca](https://halibutbank.ca)

---

## What It Does

Collects data from multiple sources, stores it in SQLite, and exports JSON files that the static frontend reads directly.

**Data sources:** EC and NOAA wave buoys · wind stations (EC SWOB-ML, NWS airports, Jericho Sailing Centre) · DFO tide stations · Surrey FlowWorks · White Rock weather · 23 lightstations · webcams · marine forecasts · GeoMet model forecasts (RDWPS waves, GDSPS storm surge) — full registry in `config/stations.json`

**Stack:** Python · SQLite · Sarracenia (sr3) · Caddy

---

## Forecast Models

Alongside the live observations, two Environment Canada models are pulled from
GeoMet by point extraction — one value per location per hour, rather than whole
grids:

**RDWPS** (Regional Deterministic Wave Prediction System, 2.5 km) — what the
waves are expected to do. Significant wave height, peak period, mean direction,
and wind-wave height out to 48 hours. Runs four times a day; we fetch hourly
detail for the first 24 hours, then every 3 hours. Currently Halibut Bank only,
and every run is kept so the forecast can be scored against what the buoy
actually measured.

**GDSPS** (Global Deterministic Storm Surge Prediction System, 15 km) — how much
higher or lower the water will sit than the tide table says. Ten days out, twice
a day, for six stations around the Salish Sea and outer coast. Added to the DFO tide
prediction, this is the water level the tides page shows.

Details: `docs/project/FORECAST_MODELS.md`, `docs/STORM_SURGE_SETUP.md`.

---

## Quick Start

```bash
cd ~/envcan_wave
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-lock.txt   # exact versions this server runs
pip install -e .                       # make lib/ importable

# Start Sarracenia subscriptions (EC data push)
sr3 start subscribe/bc_buoys
sr3 start subscribe/bc_wind_stations
sr3 start subscribe/marine_forecast
```

sr3 configs are tracked in `config/sr3/` and deployed to
`~/.config/sr3/subscribe/` — see `docs/SR3_MANAGEMENT.md`.

See `docs/DEPLOYMENT.md` for full setup including cron jobs.

---

## Directory Structure

```
envcan_wave/
├── site/                 # Static frontend (HTML/JS/CSS, served by Caddy, no build step)
│   ├── assets/js/        # Page scripts (ES modules, no build step)
│   │   └── shared/       # Shared ES-module utils (formatting, staleness, markers)
│   ├── data/             # JSON exports read by the frontend
│   └── docs/             # Frontend documentation
├── scripts/
│   ├── fetch/            # HTTP-polled sources (NOAA, Surrey, DFO, webcams, etc.)
│   ├── parse/            # XML/text parsers → SQLite
│   ├── export/           # SQLite → JSON exporters
│   ├── pipelines/        # Cron orchestrators (buoy, lightstation chains)
│   └── monitoring/       # health_check.py, daily digest
├── lib/                  # Shared utilities (config, stations, logging)
├── config/
│   ├── stations.json     # Master station registry (buoys, winds, tides, …)
│   ├── webcams.json      # Webcam registry (read by fetch + monitoring)
│   ├── crontab.txt       # Canonical crontab (install via scripts/install_crontab.sh)
│   └── sr3/              # Sarracenia configs (source of truth, deployed to ~/.config/sr3/subscribe/)
├── docs/                 # Backend documentation
├── data/                 # Raw XML/text files from EC (auto-purged after 2 days)
└── tests/                # pytest + JS unit tests (tests/js) + Playwright (tests/playwright)
```

**Databases** live in `~/.local/share/` (not in the repo):
- `buoy_data.sqlite` — wave buoy observations
- `wind_data.sqlite` — wind station observations
- `tide_data.sqlite` — tide observations, predictions, high/low events
- `storm_surge_forecast.sqlite` — GDSPS storm surge forecasts
- `wave_forecast.sqlite` — RDWPS wave forecasts (every run, for scoring)
- `lightstation_data.sqlite` — lightstation reports
- `weather_data.sqlite` — White Rock weather station

---

## Key Scripts

| Script | What it does |
|--------|-------------|
| `scripts/parse/buoy_to_sqlite.py` | Parse EC SWOB-ML buoy XMLs → SQLite |
| `scripts/fetch/fetch_noaa_buoy.py` | Fetch NOAA met + spectral feeds |
| `scripts/fetch/fetch_surrey_wave_v2.py` | Fetch Surrey FlowWorks wave data |
| `scripts/parse/wind_to_sqlite.py` | Parse EC wind station XMLs → SQLite |
| `scripts/fetch/fetch_jericho_wind.py` | Fetch Jericho Sailing Centre wind |
| `scripts/parse/tide_to_sqlite.py` | Fetch DFO IWLS tide data |
| `scripts/fetch/fetch_storm_surge.py` | Fetch GeoMet GDSPS storm surge |
| `scripts/fetch/fetch_wave_forecast.py` | Fetch GeoMet RDWPS wave forecast |
| `scripts/parse/parse_lightstation.py` | Parse lightstation text → SQLite |
| `scripts/parse/parse_marine_forecast.py` | Parse EC marine forecast XMLs → JSON |
| `scripts/fetch/fetch_webcam.py` | Capture webcam snapshots |
| `scripts/pipelines/buoy_pipeline.py` | Orchestrate buoy parse → export chain (cron) |
| `scripts/pipelines/lightstation_pipeline.py` | Orchestrate lightstation fetch → parse → export chain (cron) |
| `scripts/export/sqlite_to_json.py` | Export latest buoy snapshot |
| `scripts/export/export_tide_json.py` | Export tide data (latest, timeseries, high/low) |
| `scripts/export/water_level_export.py` | Export combined water level forecast + observed storm surge |
| `scripts/export/export_wind_json.py` | Export latest wind readings |
| `scripts/export/export_lightstation_json.py` | Export latest lightstation conditions |
| `scripts/monitoring/health_check.py` | Hourly system health → `site/data/system_health.json` |

---

## Development

```bash
.venv/bin/pytest tests/       # Backend tests (also run by the pre-commit hook)
npm run test:js               # Frontend unit tests (node:test, no deps)
npm run test:frontend         # Playwright — console health on all pages
npm run lint:js               # ESLint (kept at 0 errors / 0 warnings)
npm run format:js             # Biome formatter — run after editing JS
npm run screenshots           # Capture all pages × light/dark for visual QA
```

Frontend has no build step; all pages are ES modules as of 2026-07-18
(module inventory: `site/assets/js/shared/README.md`). Cron jobs are managed
via `config/crontab.txt` + `scripts/install_crontab.sh`.

---

## Documentation

| Doc | Contents |
|-----|----------|
| `docs/COMMANDS.md` | All common commands — db queries, pipeline testing, sr3 management |
| `docs/DATA_FEEDS.md` | Every external API/feed with URLs, auth, and schedule |
| `docs/DEPLOYMENT.md` | Full server setup, cron schedule, Caddy config |
| `docs/TROUBLESHOOTING.md` | Common issues and fixes |
| `docs/KNOWN_ISSUES.md` | Open bugs and resolved issue history |
| `docs/SURREY.md` | Surrey FlowWorks integration guide |
| `docs/SURREY_CHANNELS.md` | Active FlowWorks channel IDs |
| `docs/STORM_SURGE_SETUP.md` | GeoMet GDSPS setup and methodology |
| `docs/SR3_MANAGEMENT.md` | Sarracenia subscription management |

---

## Acknowledgements

**Environment Canada** — SWOB-ML buoy/wind data, GeoMet RDWPS wave and GDSPS storm surge forecasts, marine forecasts
**NOAA NDBC** — Spectral and meteorological feeds
**DFO** — IWLS tide data, lightstation weather reports
**City of Surrey** — FlowWorks wave and tide data
**Jericho Sailing Centre** — Real-time English Bay wind data
