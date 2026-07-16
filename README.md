# Salish Sea Wave Conditions

Real-time marine weather monitoring system for the Salish Sea region.

**Live site:** [halibutbank.ca](https://halibutbank.ca)

---

## What It Does

Collects data from multiple sources, stores it in SQLite, and exports JSON files that the static frontend reads directly.

**Data sources:** 4 EC wave buoys · 3 NOAA buoys · 10 EC wind stations · 2 Surrey FlowWorks stations · 12 DFO tide stations · 23 lightstations · webcams · marine forecasts · GeoMet GDSPS storm surge

**Stack:** Python · SQLite · Sarracenia (sr3) · Caddy

---

## Quick Start

```bash
cd ~/envcan_wave
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

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
│   ├── assets/js/        # Page scripts (migrating to ES modules)
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
│   ├── stations.json     # Master station registry
│   ├── tide_stations.json
│   ├── webcams.json      # Webcam registry (read by fetch + monitoring)
│   ├── crontab.txt       # Canonical crontab (install via scripts/install_crontab.sh)
│   └── sr3/              # Sarracenia configs (source of truth, deployed to ~/.config/sr3/subscribe/)
├── docs/                 # Backend documentation
├── data/                 # Raw XML/text files from EC (auto-purged after 2 days)
├── tests/                # pytest + JS unit tests (tests/js) + Playwright (tests/playwright)
└── archive/              # Deprecated scripts and old docs
```

**Databases** live in `~/.local/share/` (not in the repo):
- `buoy_data.sqlite` — wave buoy observations
- `wind_data.sqlite` — wind station observations
- `tide_data.sqlite` — tide observations, predictions, high/low events
- `storm_surge_forecast.sqlite` — GDSPS storm surge forecasts
- `lightstation_data.sqlite` — lightstation reports

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
| `scripts/fetch/fetch_lightstation.py` | Fetch DFO lightstation bulletins |
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

Frontend has no build step; pages are migrating to ES modules
(status table: `site/assets/js/shared/README.md`). Cron jobs are managed
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

**Environment Canada** — SWOB-ML buoy/wind data, GeoMet GDSPS storm surge, marine forecasts
**NOAA NDBC** — Spectral and meteorological feeds
**DFO** — IWLS tide data, lightstation weather reports
**City of Surrey** — FlowWorks wave and tide data
**Jericho Sailing Centre** — Real-time English Bay wind data
