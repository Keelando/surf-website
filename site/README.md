# halibutbank.ca — Frontend

Static frontend for the Salish Sea marine weather monitoring system.

## Architecture

This frontend is a static site served by Caddy. It reads JSON data files exported by the backend Python scripts in the parent directory.

- **Backend:** Parent directory (`../`) — Python data collection and export scripts
- **Data bridge:** Backend exports JSON to `site/data/`, frontend reads from there
- **Live site:** https://halibutbank.ca
- **See:** `docs/BACKEND_CONNECTION.md` for full integration details

## Pages

| Page | URL | Data Source | Update Frequency |
|------|-----|-------------|-----------------|
| Buoys | `/` | `latest_buoy_v2.json`, `timeseries_*.json` | Every minute |
| Tides | `/tides.html` | `tide-latest.json`, `tide-timeseries.json`, `tide-hi-low.json` | Every 30 min |
| Winds | `/winds.html` | `stations.json` + Environment Canada feeds | Hourly |
| Forecasts | `/forecasts.html` | Environment Canada marine text forecasts | Hourly |
| Storm Surge | `/storm_surge.html` | `data/storm_surge/*.json` | Every 6 hours |
| Webcams | `/webcams.html` | `data/wrcam/`, `data/bbcam/`, etc. | Varies |
| Lightstations | `/lightstations.html` | `stations.json` + Environment Canada feeds | Every 3 hours |

## Production Environment

**This directory (`site/`) is the live production environment.**

Changes are immediately live — there is no staging step. The daily cron auto-commits and pushes the monorepo to git.

## Documentation

- `docs/DEVELOPMENT_GUIDELINES.md` — code standards, arrow rotation gotchas, permissions
- `docs/FUTURE_ENHANCEMENTS.md` — planned features and dependency upgrades
- `docs/FRONTEND_CHANGELOG.md` — feature history
- `docs/BACKEND_CONNECTION.md` — backend/frontend data flow
- `docs/VERIFICATION_METHODOLOGY.md` — storm surge forecast verification details
