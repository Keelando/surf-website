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
