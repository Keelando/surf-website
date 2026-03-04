# Salish Sea Wave Conditions — Backend

Real-time marine weather monitoring system for the Salish Sea region.

**Live site:** [halibutbank.ca](https://halibutbank.ca) · **Frontend:** `site/`

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

See `docs/DEPLOYMENT.md` for full setup including cron jobs.

---

## Directory Structure

```
envcan_wave/
├── scripts/
│   ├── fetch/        # Data fetching scripts (NOAA, Surrey, DFO, webcams, etc.)
│   └── parse/        # XML/text parsers → SQLite
├── lib/              # Shared utilities (config, stations, logging)
├── config/
│   ├── sr3/          # Sarracenia subscription configs
│   ├── stations.json # Master station registry
│   └── tide_stations.json
├── site/             # Static frontend (HTML/JS/CSS, served by Caddy)
├── data/             # Raw XML/text files from EC (auto-purged after 2 days)
├── docs/             # Documentation
├── tests/
└── archive/          # Deprecated scripts and old docs
```

**Databases** live in `~/.local/share/` (not in the repo):
- `buoy_data.sqlite` — wave buoy observations
- `wind_data.sqlite` — wind station observations
- `tide_data.sqlite` — tide observations, predictions, high/low events
- `storm_surge_forecast.sqlite` — GDSPS storm surge forecasts
- `lightstation_data.sqlite` — lightstation reports

**JSON exports** go to `site/data/` for the frontend to read.

---

## Key Scripts

| Script | What it does |
|--------|-------------|
| `scripts/parse/buoy_to_influx_sqlite.py` | Parse EC SWOB-ML buoy XMLs → SQLite |
| `scripts/fetch/fetch_noaa_buoy.py` | Fetch NOAA met + spectral feeds |
| `scripts/fetch/fetch_surrey_wave_v2.py` | Fetch Surrey FlowWorks wave data |
| `scripts/parse/wind_to_sqlite.py` | Parse EC wind station XMLs → SQLite |
| `scripts/fetch/fetch_jericho_wind.py` | Fetch Jericho Sailing Centre wind |
| `scripts/fetch/tide_to_sqlite.py` | Fetch DFO IWLS tide data |
| `scripts/fetch/fetch_storm_surge.py` | Fetch GeoMet GDSPS storm surge |
| `scripts/fetch/fetch_lightstation.py` | Fetch DFO lightstation bulletins |
| `scripts/parse/parse_lightstation.py` | Parse lightstation text → SQLite |
| `scripts/parse/parse_marine_forecast.py` | Parse EC marine forecast XMLs → JSON |
| `scripts/fetch/fetch_webcam.py` | Capture webcam snapshots |
| `sqlite_to_json.py` | Export latest buoy snapshot |
| `export_tide_json.py` | Export tide data (latest, timeseries, high/low) |
| `export_combined_water_level.py` | Export tide + storm surge combined |
| `export_wind_json.py` | Export latest wind readings |
| `export_lightstation_json.py` | Export latest lightstation conditions |
| `influx_to_mqtt.py` | Publish to Home Assistant via MQTT (optional) |

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
| `docs/project/TODO.md` | Upcoming and completed work |

---

## Acknowledgements

**Environment Canada** — SWOB-ML buoy/wind data, GeoMet GDSPS storm surge, marine forecasts
**NOAA NDBC** — Spectral and meteorological feeds
**DFO** — IWLS tide data, lightstation weather reports
**City of Surrey** — FlowWorks wave and tide data
**Jericho Sailing Centre** — Real-time English Bay wind data
