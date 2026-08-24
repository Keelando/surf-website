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
  hardcode), `stations.py` (registry access), units, directions, logging,
  `daylight.py` (astral sunrise/sunset), `water_level_stations.py`, `webcam/`
- `config/` — `stations.json` (canonical station registry, tides included),
  `webcams.json` (canonical webcam registry, gitignored — schema in
  `webcams.example.json`), `crontab.txt` (canonical crontab), `sr3/` (AMQP
  subscription configs, deployed to `~/.config/sr3/subscribe/`)
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
`lightstation_data.sqlite`, `storm_surge_forecast.sqlite`,
`weather_data.sqlite` (White Rock weather), and `reporting_lag.sqlite`
(instrument → database → website latency, written by the exports via
`lib/reporting_lag.py`).

## Critical conventions

- Meteorological directions = coming FROM, not going to.
- Units: store km/h, display knots.
- Per-field freshness: 2-hour window, each metric independent.
- Timestamps: Unix epoch in SQLite, ISO 8601 UTC in JSON.
- NOAA pressure ~999 hPa is valid data, not a missing-value marker.
- Single source of truth: read canonical files (`config/stations.json`,
  `config/crontab.txt`, `config/webcams.json`) — never duplicate their
  contents into code or docs.
- Credentials go in `config/.env` (gitignored) and are read via
  `lib/env.py`. The repo is public and the nightly cron commits unattended,
  so no secret may enter a tracked file — including `config/crontab.txt`.
  See `docs/SECRETS.md`.
- **Two public surfaces, not one.** The git repo publishes *tracked* files;
  `site/data/` publishes *everything Caddy serves*. `site/data/` is
  gitignored, so the pre-commit scan and `tests/test_secrets.py` never look
  at it — whatever an export or monitor writes there goes straight to
  halibutbank.ca unchecked. Never write an upstream API response into
  `site/data/` wholesale; copy an explicit allowlist of fields.
- **The crontab is a third way in.** `scripts/backup_crontab.sh` dumps the
  *whole* user crontab into the tracked `config/crontab.txt` nightly, and one
  user has one crontab — so any job scheduled on this host publishes itself
  here. Host jobs unrelated to this project belong in `/etc/cron.d/<name>`
  (with a `keelando` user field), which `crontab -l` cannot see. The script
  enforces this and keeps a small reasoned allowlist.

## Commands

- Python: always the project venv — `.venv/bin/pytest -q`,
  `.venv/bin/ruff check .` — never `--break-system-packages`.
- Frontend: `npm run lint:js` (0 errors, 0 warnings), `npm run format:js`
  (run after any JS edit), `npm run test:js`, `npm run test:frontend`,
  `npm run screenshots`.
- Everything: `npm test` (pytest + JS unit tests + Playwright);
  `npm run test:python` for the pytest suite alone.
- Cache busting is automated: `scripts/update_asset_versions.py` rewrites
  `?v=` to content hashes (pre-commit hook runs it and stages the result —
  never bump versions by hand). See `site/docs/CACHE_BUSTING.md`.
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
| `docs/SECRETS.md` | Credential handling — `config/.env`, `lib/env.py`, the pre-commit secret scan |
| `docs/TROUBLESHOOTING.md` | Common issues and fixes |
| `docs/ARCHITECTURE_DETAILED.md` | Database schemas, script details |
| `docs/MSC_REFERENCE_TABLES.md` | MSC code→name lookups (marine regions, SWOB stations) — dated snapshot, not authoritative |
| `site/assets/js/shared/README.md` | Frontend shared-module inventory |
| `docs/project/` | Active plans (forecast upgrade, pressure page, …) |
| `TODO.md` | Feature backlog (the 2026-07-19 maintenance items are all done) |
