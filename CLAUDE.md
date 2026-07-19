# CLAUDE.md

Guidance for Claude Code in this repo. Keep this file thin and accurate —
detail lives in `docs/` (backend) and `site/docs/` (frontend); when things
move, fix pointers here rather than copying content in.

## What this is

Real-time marine weather monitoring for the Salish Sea, live at
<https://halibutbank.ca>. A Python backend collects buoy, wind, tide,
lightstation, storm-surge, marine-forecast, weather, and webcam data; a
static no-build-step frontend (`site/`) renders the JSON exports. Solo
project: commit straight to `main` (branch by exception, `--ff-only` merges).

## Layout

- `scripts/` — pipeline entry points: `fetch/` (HTTP-polled sources),
  `parse/` (XML/text → SQLite), `export/` (SQLite → JSON), `pipelines/`
  (cron orchestrators), `monitoring/`, `hooks/`, `utils/`
- `lib/` — shared Python: `config.py` (canonical paths — use it, don't
  hardcode), `stations.py` (registry access), units, directions, logging
- `config/` — `stations.json` (canonical station registry),
  `crontab.txt` (canonical crontab), `sr3/` (AMQP subscription configs,
  deployed to `~/.config/sr3/subscribe/`)
- `site/` — static frontend; all pages are ES modules, shared helpers in
  `site/assets/js/shared/` (see its README for the module inventory)
- `tests/` — pytest (backend + crontab validation), `tests/js/` node unit
  tests, `tests/playwright/` browser suite
- `docs/` / `site/docs/` — backend / frontend docs; plans in `docs/project/`

## Data flow

fetch → parse → SQLite → export JSON → `site/data/` → Caddy static serving.

Two delivery mechanisms, determined by source (see `docs/DATA_FEEDS.md`):
**sr3/Sarracenia AMQP push** for Environment Canada feeds (systemd services,
`docs/SR3_MANAGEMENT.md`); **cron HTTP polling** for everything else (NOAA,
DFO, Surrey, Jericho, White Rock, webcams).

Databases in `~/.local/share/` (never in the repo):
`buoy_data.sqlite`, `wind_data.sqlite`, `tide_data.sqlite`,
`lightstation_data.sqlite`, `storm_surge_forecast.sqlite`, and
`weather_data.sqlite` (White Rock weather).

## Critical conventions

- Meteorological directions = coming FROM, not going to.
- Units: store km/h, display knots.
- Per-field freshness: 2-hour window, each metric independent.
- Timestamps: Unix epoch in SQLite, ISO 8601 UTC in JSON.
- NOAA pressure ~999 hPa is valid data, not a missing-value marker.
- Single source of truth: read canonical files (`config/stations.json`,
  `config/crontab.txt`, `config/webcams.json`) — never duplicate their
  contents into code or docs.

## Commands

- Python: always the project venv — `.venv/bin/pytest -q`,
  `.venv/bin/ruff check .` — never `--break-system-packages`.
- Frontend: `npm run lint:js` (0 errors, 0 warnings), `npm run format:js`
  (run after any JS edit), `npm run test:js`, `npm run test:frontend`,
  `npm run screenshots`.
- Cache busting is still manual: bump `?v=` on `<script>`/`<link>` tags in
  every HTML file that references a modified asset (automation is on
  `TODO.md`).
- Crontab: edit `config/crontab.txt`, apply with
  `scripts/install_crontab.sh` (`--diff-only` to preview). Never edit the
  live crontab as the primary copy.

## Key docs

| Doc | Contents |
|-----|----------|
| `docs/COMMANDS.md` | Common commands, db queries, pipeline testing |
| `docs/DATA_FEEDS.md` | Every external feed: URLs, auth, schedule |
| `docs/DEPLOYMENT.md` | Server setup, cron schedule, Caddy |
| `docs/SR3_MANAGEMENT.md` | Sarracenia subscription management |
| `docs/TROUBLESHOOTING.md` | Common issues and fixes |
| `docs/ARCHITECTURE_DETAILED.md` | Database schemas, script details |
| `site/assets/js/shared/README.md` | Frontend shared-module inventory |
| `docs/project/` | Active plans (forecast upgrade, pressure page, …) |
| `TODO.md` | Mandatory maintenance backlog (2026-07-19 audit) |
