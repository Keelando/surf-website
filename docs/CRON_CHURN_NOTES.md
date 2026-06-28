# Cron Churn — Investigation Note & Top Suspects

**Date:** 2026-06-28
**Context:** Host (`surfserver`) temperature investigation traced steady background load to this
project's cron setup. Captured here so it can be tackled in the surf-website repo.

## TL;DR

The data pipeline runs as **~50 standalone crontab lines, 38 of which cold-start a fresh
`.venv/bin/python3` interpreter** every few minutes. This is the "lazy cron method": no
long-running scheduler, no pipelining, no overlap guards. It works, but it pays full
interpreter + import startup cost dozens of times an hour and risks run pile-ups.

> Honesty note on the temp angle: host CPU load was actually **flat** (~15%, atop) across the
> reported temp jump window, and the elevated on-disk temp log is the **NVMe NAND flash**
> (~70°C since May), not the CPU. So cron churn is an **efficiency / maintainability**
> problem to fix on its own merits — it is *not* yet a proven cause of the temp jump. Don't
> let fixing this close out the thermal ticket.

## What's running now

- **50** active cron jobs; **38** spawn `python3` from this repo's venv.
- Busiest cadences:
  - `*/3` — `parse/buoy_to_sqlite.py`, `export/sqlite_to_json.py`, `parse/wind_to_sqlite.py`
  - `*/5` — `export/export_wind_json.py`
  - `*/10` ×5 — timeseries exports, NWS fetch, whiterock, tide export, water level
  - plus `*/15`, `*/20`, `*/30` fetch/export jobs and many hourly ones
- Separately, **4 long-running `sarracenia` subscriber processes** (bc_buoys, bc_wind_stations,
  marine_forecast, bc_lightstation_obs). These are the steady CPU users (PID with ~12h CPU over
  ~11 days) but they're architecturally fine — they're *not* the cron-churn problem.

## Top suspects (in priority order)

1. **Interpreter + import cold-start, 38× over.** Every cron line boots a new Python and
   re-imports the full stack (pandas/numpy/lxml/requests if imported at module top). Several
   fire in the same minute. → Biggest, cheapest win is consolidation.

2. **The `*/3` fan-out isn't pipelined.** `buoy_to_sqlite` → `sqlite_to_json` and
   `wind_to_sqlite` run as independent jobs on the same 3-min tick. They open/close SQLite and
   start interpreters separately, and `sqlite_to_json` may run before/after the parse with no
   ordering guarantee. → Chain fetch→parse→export per source into **one** process per tick.

3. **No overlap protection.** If any `*/3` or `*/5` job ever runs longer than its interval
   (slow upstream fetch, big export), runs stack up and compete. → Wrap each job in
   `flock -n` on a per-job lockfile.

4. **Likely full re-exports every run.** Exports like `sqlite_to_json`, `export_24hr_timeseries`,
   `export_wind_24hr_timeseries` probably re-read and re-serialize the whole dataset each tick
   regardless of whether new data arrived. → Make them **change-gated / incremental** (skip if
   source mtime/rowcount unchanged).

5. **Top-level heavy imports.** Confirm whether the parse/export scripts import pandas/numpy/
   geopandas at module load. If so, every invocation pays it; a resident worker removes that.

## Suggested direction

- **Short term (low risk):** add `flock` guards; merge the obvious per-source fetch→parse→export
  chains into single scripts so one tick = one interpreter; gate exports on "did data change?".
- **Medium term:** replace the cron wall with **one long-running scheduler** (APScheduler, or a
  small asyncio loop) that imports the stack once and runs jobs in-process on their cadences.
  Keeps the venv warm, gives you central logging/locking, and collapses 38 interpreter starts
  into ~1 resident process.

## Quick repro / measurement

```bash
crontab -l | grep -c '.venv/bin/python3'          # count venv-spawning jobs
crontab -l | grep -vE '^#|^$|^[A-Z_]+=' | awk '{print $1}' | sort | uniq -c | sort -rn
# Time a representative cold start vs. just the import cost:
time .venv/bin/python3 scripts/export/sqlite_to_json.py
time .venv/bin/python3 -c "import pandas, numpy, lxml, requests"
```
