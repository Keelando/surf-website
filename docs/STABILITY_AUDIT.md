# Stability Audit

A periodic review to catch slow-burn issues before they cause outages.

## Last Run: 2026-08-02

| Check | Status | Notes |
|-------|--------|-------|
| Disk: `/` | OK | 18% used (182G free) |
| Log rotation | **Fixed** | logrotate healthy, but 540 MB of pre-2026-03-10 orphans found and removed — see below |
| Python venv | **Fixed** | Live `.venv` pruned to match `requirements-lock.txt` (84 → 34 packages, 246 → 139 MB) |
| sr3 subscriptions | OK | 4/4 active after the venv swap, no errors in `journalctl` |
| Tests | OK | 267 passed, `ruff check .` clean |

### Actions Taken

**Log orphans (540 MB reclaimed).** `logs/` had grown to 1.1 GB. The cause was a
one-time artifact of enabling logrotate on 2026-03-10: the rotation set up that
day uses `compress`, so it began a fresh `foo.log.N.gz` chain and **never again
touched the uncompressed `foo.log.N` files left by the previous scheme**.
logrotate rotates `.2.gz → .3.gz → .4.gz → deleted` and simply does not see the
plain siblings, so they sat outside any retention policy indefinitely. Removed
26 orphaned `*.log.2`–`.4` plus 8 `*.log.5` files (the latter also stranded
beyond `rotate 4`), all holding Dec 2025 – Mar 2026 content. `logs/` is now
507 MB and fully under logrotate's control.

*This will not recur* — it was residue from the transition, not an ongoing leak.
The remaining 315 MB of `*.log.1` is legitimate (`delaycompress` leaves the most
recent rotation uncompressed) and compresses on the next cycle.

**Venv prune.** Executed the rebuild-and-swap staged in
`docs/project/VENV_PRUNE.md`; see that doc for the outcome and the retained
`.venv.old` rollback.

**Surrey tide observations were silently not updating.** Investigating the log
error ranking surfaced a real data bug. `fetch_surrey_tides.py` requested a
2-hour observation window, but Surrey's upstream FlowWorks sensors lag ~3+ hours
behind real time — so the requested window sat entirely *after* the newest
available reading and returned zero points on every run, every 20 minutes. The
empty-result branch logged at `DEBUG`, so it was invisible. Crescent Channel
Ocean had gone 17 h without an observation; Crescent Beach Ocean 3.7 h.

Note the page still *looked* healthy throughout, because `tide-latest.json`
carries both `prediction_now` (fetched daily, 96 h ahead — always current) and
`observation`. Only the latter was stalled. **When judging Surrey tide health,
check `observation.stale`, not whether the page renders.**

Fixes: window 2 h → 24 h (inserts are `INSERT OR REPLACE`, so overlap is free);
empty result now logs `WARNING`. First run backfilled 15 h for Crescent Channel.

Also cleared two chronic error sources found in the same ranking:

- **548 errors/day** — channels 2454/2455 (`geodiff_*_vs_radar`) were retired
  upstream and 404 on every call; last real value was 2026-01-21. Removed from
  `config/stations.json`. Nothing reads them: no frontend reference, and the
  historical rows stay in `surrey_geodetic_data`.
- **380 errors/day** — `tide_to_sqlite.py` was passing Surrey's internal ids
  (`surrey_crescent_ocean`) to the DFO IWLS API, which 400s them. Now filtered
  on the registry's own `type != "SURREY_FLOWWORKS"` rather than a hardcoded list.
- **Log volume halved** for `surrey_tide_sync.log` (7 MB). See the systemic fix
  below — this turned out to affect 12 scripts, not one.

### Duplicate log lines: systemic fix

Auditing every cron job against its script's logger found **12 of 25 writing
every line twice**, including both pipeline orchestrators. The mechanism:
`setup_logging()` added a console handler unconditionally, and cron redirects
stdout into the script's own log file — so the file handler and the console
handler both wrote to the same file.

`lib/logging_config.py` now defaults `console=None`, meaning **auto-detect via
`sys.stdout.isatty()`**: console output when run interactively, file-only under
cron. Explicit `True`/`False` still override. This makes the duplication
structurally impossible for existing and future scripts alike. The 8 scripts
that already passed `console=False` were exactly the 8 that never duplicated.

Cron redirects are deliberately **kept** — with the console handler gone they
capture only genuine stderr and tracebacks that crash before logging starts,
which is what they are actually useful for.

Five scripts had a cron redirect target that differed from their logger's
filename, so that second file was populated *entirely* by console output and
would have gone empty. Resolved by making the logger own the split:

- `tide_to_sqlite.py` runs in three cron modes but logged everything to
  `tide_obs.log`; `tide_pred.log` and `tide_highlow.log` existed only as console
  spill. It now picks its log file from argv, so each mode writes its own file
  through the file handler.
- The other four (`export_stations_json`, `export_wind_json`,
  `fetch_jericho_wind`, `fetch_surrey_wave_v2`) had their crontab redirect
  pointed at the logger's real filename, so each log has exactly one writer.

Applied with `scripts/install_crontab.sh` (live crontab backed up first).

### Follow-up: verify on 2026-08-03

Everything below was changed on 2026-08-02 and needs one clean day before it can
be called done. Run this after the 06:02 UTC digest:

```bash
cd ~/envcan_wave

# 1. Overall state vs. the 2026-08-02 baseline
.venv/bin/python3 scripts/monitoring/daily_digest.py --verbose
tail -4 logs/health_check.log

# 2. Surrey observations staying current (was: silently 17h stale)
.venv/bin/python3 -c "
import json;d=json.load(open('site/data/tide-latest.json'))['stations']
for k in ('crescent_beach_ocean','crescent_channel_ocean'):
    o=d[k]['observation']; print(k, o['age_minutes'],'min', 'STALE' if o['stale'] else 'ok')"

# 3. The 928/day error sources should be gone
grep -c '404 Client Error' logs/surrey_tide_sync.log
grep -c '400 Client Error' logs/tide_obs.log

# 4. No log is double-written. Handler duplication doubles EVERY line, so
#    compare unique vs total. Two filters matter or this false-positives:
#    only look at timestamped logger-format lines (several logs are bare
#    print() or raw shell output that repeats identically every run), and
#    drop "=====" separators (legitimately repeat within one second).
for f in logs/*.log; do
  b=$(tail -40 "$f" | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8} - .+ - [A-Z]+ - ' | grep -v '===')
  t=$(echo "$b" | grep -c .); u=$(echo "$b" | sort -u | grep -c .)
  [ "$t" -gt 20 ] && [ "$u" -lt $((t / 2)) ] && echo "DOUBLED: $f ($u unique of $t)"
done; echo "duplicate scan done"
```

Note the scan reads the *tail*, so a log whose last run predates a fix will
still show old doubled lines — check the timestamps before concluding anything.

**Expected:** digest `0 cascade risks` and the `tides` warning gone; stale count
down from 9 to 7 (lightstations only); both grep counts unchanged from their
2026-08-02 totals (i.e. no *new* errors); duplicate scan silent.

Specific items, in order of how much they actually need watching:

1. **`tide_to_sqlite.py` mode split — first real run at 10:06 / 10:11 UTC.**
   This is new code that has never executed under cron. Confirm `tide_pred.log`
   and `tide_highlow.log` each got a fresh, non-doubled run written by the
   *logger* (not console spill), and that `tide_obs.log` no longer collects all
   three modes.
2. **Nine logging scripts not yet observed post-fix.** Six run every 10–15 min
   and self-verify within the hour; `export_storm_surge_verification` (02:13) and
   `fetch_storm_surge` (01:31) need overnight. Covered by the duplicate scan.
3. **`.venv.old` (246 MB)** — delete once a full day of pipelines has run clean.
   This is the gate from `docs/project/VENV_PRUNE.md`.
4. **`*.log.1` (315 MB)** should compress at the next weekly logrotate, taking
   `logs/` from ~507 MB to roughly 200 MB. No action, just confirm.

Not fixed, and expected to still be flagged: the **7 stale lightstations**
(Pine Island 120 h, four at 63.7 h, Egg Island never reported). Separate EC
bulletin-feed issue, own session.

## Previous Run: 2026-03-10

| Check | Status | Notes |
|-------|--------|-------|
| Cron jobs | OK | All configured and running |
| Disk: `/` | OK | 16% used (188G free) |
| Disk: `/mnt/storage` | OK | 4% used (200G free), webcams at 3.9G |
| SQLite integrity | OK | All 5 databases pass (buoy, tide, lightstation, wind, storm_surge_forecast) |
| sr3 subscriptions | OK | 3/3 configs running (bc_buoys, bc_wind_stations, marine_forecast) |
| JSON freshness | OK | All active exports <30min old; 3 stale legacy files removed |
| Caddy errors | OK | No errors in last 24h |
| Log rotation | OK | logrotate configured (`/etc/logrotate.d/envcan-wave`), 46 logs rotated |
| Webcam archives | OK | Rotating properly, largest is whiterock at 2.3G |
| Uptime-Kuma | Not tested | Requires deliberate outage — deferred |

### Actions Taken
- Removed 3 stale legacy JSON files: `buoy_timeseries_24h.json`, `wind_timeseries_24hr.json`, `latest_tide_v2.json`
- Set up logrotate: weekly rotation, 4 kept, compress, copytruncate, 10M size threshold
- Archived 6 outdated docs to `archive/docs/`

## Checklist

- [x] Verify all cron jobs are running and producing fresh output (check log mtimes)
- [x] Check disk usage on `/` and `/mnt/storage` (webcam archives)
- [x] SQLite integrity check on all databases (`PRAGMA integrity_check`)
- [x] Confirm sr3 subscriptions are active and receiving data
- [x] Review log sizes and logrotate effectiveness across all logs
- [x] Check for stale JSON exports (data freshness vs. cron schedule)
- [ ] Verify Uptime-Kuma alerts are firing correctly (test a deliberate outage)
- [x] Review Caddy error logs for recurring 5xx or unexpected patterns
- [x] Confirm webcam archives are rotating properly and not filling `/mnt/storage`

## Databases

| Database | Location | Notes |
|----------|----------|-------|
| buoy_data.sqlite | `~/.local/share/` | 7.1M |
| tide_data.sqlite | `~/.local/share/` | 83M |
| wind_data.sqlite | `~/.local/share/` | 64M |
| lightstation_data.sqlite | `~/.local/share/` | 3.0M |
| storm_surge_forecast.sqlite | `~/.local/share/` | 8.8M |
