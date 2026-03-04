# Monorepo Migration Log

## Summary

On 2026-03-04, the frontend repository (`surf-website-front-end`) was merged into the backend repository (`envcan_wave`) under `site/`. This consolidates all code into a single monorepo.

**Merge commit:** `b7a44e0` — "Merge frontend into monorepo under site/"
**Fix commit:** `031d477` — "Fix hardcoded ~/site paths broken by monorepo merge"

## Why

- Single repo simplifies deployment, CI, and development workflow
- Backend scripts already exported JSON directly to `~/site/data/`
- No build step or package manager in the frontend — just static files
- Eliminates need to keep two repos in sync

## What Changed

### Directory Structure

```
Before:                          After:
~/envcan_wave/   (backend)       ~/envcan_wave/          (monorepo)
~/site/          (frontend)      ~/envcan_wave/site/     (frontend)
```

### Config Changes Made

1. **Caddy** (`/etc/caddy/Caddyfile`): Updated `root` directive from `~/site` to `~/envcan_wave/site`
2. **Crontab**: Updated all `~/site/` references to `~/envcan_wave/site/`
3. **EXPORT_DIR**: Updated in all export scripts (11 scripts fixed)
4. **Documentation**: ~20 docs files updated to reflect new paths

### Scripts Fixed (commit `031d477`)

11 scripts had hardcoded `~/site` paths updated:
- `sqlite_to_json.py`
- `export_24hr_timeseries.py`
- `export_wind_json.py`
- `export_wind_24hr_timeseries.py`
- `export_tide_json.py`
- `export_hindcast_json.py`
- `parse_marine_forecast.py`
- `fetch_storm_surge.py`
- `capture_webcam.sh`
- `capture_webcam_coxbay.sh`
- `capture_mudbay_webcam.sh`

## Remaining Next Steps

- [ ] Create symlink `~/site` → `~/envcan_wave/site` as safety net for any missed references
- [ ] Remove old `~/site/` directory after 24-hour verification cycle
- [ ] Rename GitHub repo `surf-website` → `halibutbank` (or similar)
- [ ] Archive `surf-website-front-end` GitHub repo
- [ ] Fix 2 test scripts with hardcoded paths (`tests/`)
- [ ] Update crontab backup reference (`config/crontab.txt`)
