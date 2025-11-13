# Phase 0 Quick Checklist

Print this and check off as you go!

## Pre-Execution

- [ ] Backup created: `tar -czf ~/envcan_wave_backup_$(date +%Y%m%d).tar.gz ~/envcan_wave/`
- [ ] Git is clean: `git status` shows no uncommitted changes
- [ ] Current scripts work: `python3 sqlite_to_json.py` runs without errors

## Execution

- [ ] Scaffolding copied to server (if needed)
- [ ] New modules copied to root: `cp phase0_scaffolding/new_modules/*.py .`
- [ ] New modules tested: `python3 phase0_scaffolding/validation/test_new_modules.py` passes
- [ ] Migration previewed: `python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --dry-run`
- [ ] Migration applied: `python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --backup`
- [ ] Scripts validated: `bash phase0_scaffolding/validation/validate_scripts.sh` passes

## Manual Testing

- [ ] Buoy pipeline: `python3 buoy_to_influx_sqlite.py` works
- [ ] JSON export: `python3 sqlite_to_json.py` generates latest_buoy_v2.json
- [ ] NOAA fetch: `python3 fetch_noaa_buoy.py` works
- [ ] Tide fetch: `python3 tide_to_sqlite.py --observations` works
- [ ] Tide export: `python3 export_tide_json.py` works
- [ ] Timeseries: `python3 export_24hr_timeseries.py` works
- [ ] MQTT: `python3 influx_to_mqtt.py` works (if using Home Assistant)

## Validation

- [ ] JSON files have recent timestamps: `ls -lh ~/site/data/latest_buoy_v2.json`
- [ ] JSON structure correct: `cat ~/site/data/latest_buoy_v2.json | jq '.["4600146"]'`
- [ ] Database writes working: Check row counts in SQLite
- [ ] Website displays data: Check https://halibutbank.ca

## Git Commit

- [ ] Changes staged: `git add units.py directions.py config.py *.py`
- [ ] Commit message written (see PHASE0_EXECUTION_GUIDE.md for template)
- [ ] Committed: `git commit -m "..."`
- [ ] Pushed: `git push origin main`

## 24-Hour Monitoring

- [ ] Logs checked: `tail -f ~/envcan_wave/*.log` (no errors)
- [ ] JSON exports updating: `watch -n 60 'ls -lh ~/site/data/latest_buoy_v2.json'`
- [ ] Database writes continuing: Check observation counts hourly
- [ ] Website working: Check site loads and displays fresh data
- [ ] No errors after 24 hours

## Cleanup (After 24 Hours)

- [ ] Remove backup files: `rm *.backup`
- [ ] Remove scaffolding: `rm -rf phase0_scaffolding` (optional, keep in git)
- [ ] Tag milestone: `git tag -a phase0-complete -m "Phase 0 complete"`
- [ ] Push tag: `git push origin phase0-complete`

## Rollback (If Needed)

If something goes wrong:

- [ ] Restore from backups: `for f in *.backup; do cp "$f" "${f%.backup}"; done`
- [ ] Remove new modules: `rm units.py directions.py config.py`
- [ ] Test original scripts: `python3 sqlite_to_json.py`
- [ ] Commit rollback: `git add -A && git commit -m "Rollback Phase 0"`
- [ ] Investigate issue before retrying

---

## Quick Commands Reference

```bash
# Copy modules
cp phase0_scaffolding/new_modules/*.py .

# Test modules
python3 phase0_scaffolding/validation/test_new_modules.py

# Preview migration
python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --dry-run

# Apply migration
python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --backup

# Validate
bash phase0_scaffolding/validation/validate_scripts.sh

# Test a script
python3 sqlite_to_json.py

# Check logs
tail -f ~/envcan_wave/*.log

# Commit
git add -A
git commit -m "Phase 0: Extract shared utilities"
git push
```

---

**Status:** ___________ (Not Started / In Progress / Complete / Rolled Back)

**Date Started:** ___________

**Date Completed:** ___________

**Issues Encountered:**

_____________________________________________________________

_____________________________________________________________

_____________________________________________________________

**Notes:**

_____________________________________________________________

_____________________________________________________________

_____________________________________________________________
