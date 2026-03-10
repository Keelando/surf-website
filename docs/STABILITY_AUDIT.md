# Stability Audit

A periodic review to catch slow-burn issues before they cause outages.

## Last Run: 2026-03-10

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
