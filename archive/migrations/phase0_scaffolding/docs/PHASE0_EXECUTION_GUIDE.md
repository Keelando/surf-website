# Phase 0 Execution Guide

**Goal:** Extract shared utilities to reduce duplication BEFORE directory structure migration
**Duration:** 1-2 hours (including testing)
**Risk:** LOW (no file moves, just extraction + import updates)
**Downtime:** None required

---

## Overview

This guide walks you through executing Phase 0 migration on your production server. Phase 0 creates three shared utility modules and updates all scripts to use them:

1. **units.py** - Unit conversion functions (kmh_to_knots, ms_to_kmh, etc.)
2. **directions.py** - Direction utilities (degrees_to_cardinal, parse_direction, etc.)
3. **config.py** - Centralized configuration (database paths, freshness windows, etc.)

**Benefits:**
- Eliminates ~100 lines of duplicated code
- Single source of truth for database paths
- Prepares codebase for Phase 1-2 (structure migration)
- Makes future additions easier (wind database will reuse these!)

---

## Prerequisites

### 1. Backup Current State

```bash
# On your production server
cd ~/envcan_wave

# Create backup
tar -czf ~/envcan_wave_backup_$(date +%Y%m%d).tar.gz \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='data' \
    ~/envcan_wave/

# Verify backup created
ls -lh ~/envcan_wave_backup_*.tar.gz
```

### 2. Ensure Git is Clean

```bash
# Check status
git status

# Commit any uncommitted changes
git add -A
git commit -m "Pre-Phase 0: checkpoint before refactoring"
git push origin main
```

### 3. Test Environment

```bash
# Ensure Python and venv work
source .venv/bin/activate
python3 --version  # Should be Python 3.8+

# Test a script runs
python3 buoy_to_influx_sqlite.py  # Should run without errors
```

---

## Step-by-Step Execution

### Step 1: Copy Scaffolding Files to Server

On your **local machine** (where you have the `phase0_scaffolding/` directory):

```bash
# Option A: Using scp (if you have SSH access)
scp -r phase0_scaffolding/ yourserver:~/envcan_wave/

# Option B: Using git (if phase0_scaffolding is committed to repo)
# (Already committed if you're reading this!)
# On server:
git pull origin <branch-name>
```

On your **production server**:

```bash
cd ~/envcan_wave

# Verify scaffolding copied
ls -la phase0_scaffolding/
# Should see: new_modules/, migration_scripts/, validation/, docs/
```

---

### Step 2: Copy New Modules to Root

```bash
cd ~/envcan_wave

# Copy the three new modules to root
cp phase0_scaffolding/new_modules/units.py .
cp phase0_scaffolding/new_modules/directions.py .
cp phase0_scaffolding/new_modules/config.py .

# Verify they're in root
ls -la *.py | grep -E "(units|directions|config)"
```

---

### Step 3: Test New Modules

```bash
# Test units.py
python3 units.py
# Should print "✅ All conversions working correctly!"

# Test directions.py
python3 directions.py
# Should print "✅ All conversions working correctly!"

# Test config.py
python3 config.py
# Should print configuration summary

# Run comprehensive test suite
python3 phase0_scaffolding/validation/test_new_modules.py
# Should show all tests passing
```

**If any tests fail, STOP HERE and debug before proceeding.**

---

### Step 4: Preview Migration (Dry Run)

```bash
# Run migration in dry-run mode (no changes applied)
python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --dry-run

# Review output carefully:
# - Which files will be modified
# - What changes will be made
# - How many migrations applied per file
```

**Expected output:**
```
============================================================
Phase 0 Migration - DRY RUN
============================================================

Processing: sqlite_to_json.py
  ✅ Applied 4 migrations:
     - Add units import to sqlite_to_json.py (1 matches)
     - Add directions import to sqlite_to_json.py (1 matches)
     - Remove kmh_to_knots function from sqlite_to_json.py (1 matches)
     - Replace buoy DB path in sqlite_to_json.py (1 matches)

Processing: influx_to_mqtt.py
  ✅ Applied 5 migrations:
     ...

[... more files ...]

Migration Summary:
  Files processed: 11
  Migrations applied: 35
  Errors: 0

⚠️  This was a DRY RUN - no files were modified
    Run without --dry-run to apply changes
============================================================
```

**If output looks good, proceed to Step 5. If errors, STOP and investigate.**

---

### Step 5: Apply Migration (With Backups)

```bash
# Apply migration with automatic backups
python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --backup

# Verify backups created
ls -la *.backup
# Should see .backup files for all modified scripts
```

**Expected output:**
```
============================================================
Phase 0 Migration - LIVE MIGRATION
============================================================

Processing: sqlite_to_json.py
  ✅ Applied 4 migrations:
     - Add units import to sqlite_to_json.py (1 matches)
     ...

[... more files ...]

Migration Summary:
  Files processed: 11
  Migrations applied: 35
  Errors: 0

✅ Migration complete!
   Backups created with .backup extension
============================================================
```

**If any errors, see Rollback section below.**

---

### Step 6: Validate Scripts

```bash
# Run validation script
bash phase0_scaffolding/validation/validate_scripts.sh
```

**Expected output:**
```
==========================================
Phase 0 Script Validation
==========================================

Testing scripts modified in Phase 0:

--- Unit Conversion Scripts ---
Testing: sqlite_to_json.py
  ✅ Syntax check passed
  ✅ Import successful

Testing: influx_to_mqtt.py
  ✅ Syntax check passed
  ✅ Import successful

[... more scripts ...]

==========================================
Validation Summary
==========================================
  Total scripts tested: 14
  Passed: 14
  Failed: 0
  Warnings: 0
==========================================

✅ ALL VALIDATIONS PASSED!
```

**If any failures, see Troubleshooting section below.**

---

### Step 7: Manual Smoke Testing

Test a few critical scripts manually to ensure they work end-to-end:

```bash
source .venv/bin/activate

# Test buoy data pipeline
echo "Testing buoy_to_influx_sqlite.py..."
python3 buoy_to_influx_sqlite.py
# Should process XMLs without errors

echo "Testing sqlite_to_json.py..."
python3 sqlite_to_json.py
# Should generate ~/site/data/latest_buoy_v2.json

# Check JSON file updated
ls -lh ~/site/data/latest_buoy_v2.json
# Timestamp should be recent

# Verify JSON structure unchanged
cat ~/site/data/latest_buoy_v2.json | jq '.["4600146"]' | head -20
# Should show buoy data with all expected fields

# Test NOAA fetcher
echo "Testing fetch_noaa_buoy.py..."
python3 fetch_noaa_buoy.py
# Should fetch NOAA data without errors

# Test tide pipeline
echo "Testing tide_to_sqlite.py..."
python3 tide_to_sqlite.py --observations
# Should fetch tide observations

echo "Testing export_tide_json.py..."
python3 export_tide_json.py
# Should generate tide JSONs

# Test timeseries export
echo "Testing export_24hr_timeseries.py..."
python3 export_24hr_timeseries.py
# Should generate timeseries JSON

# Test MQTT (if using Home Assistant)
echo "Testing influx_to_mqtt.py..."
python3 influx_to_mqtt.py
# Should publish to MQTT without errors
```

**All scripts should run without import errors or crashes.**

---

### Step 8: Review Changes

```bash
# View what changed in a file
git diff sqlite_to_json.py

# Review all changes
git diff | less

# Expected changes:
# - New imports at top (from units import ...)
# - Removed function definitions (def kmh_to_knots...)
# - Removed BUOYS dictionary
# - Replaced database paths (SQLITE_PATH = BUOY_DATABASE)
```

---

### Step 9: Commit Changes

```bash
# Stage all changes
git add units.py directions.py config.py
git add *.py  # All modified scripts

# Commit with detailed message
git commit -m "Phase 0: Extract shared utilities and eliminate duplication

Create three shared utility modules:
- units.py: Unit conversion functions (kmh_to_knots, ms_to_kmh, etc.)
- directions.py: Direction utilities (degrees_to_cardinal, parse_direction, etc.)
- config.py: Centralized database paths and configuration

Update 11 scripts to use shared utilities:
- sqlite_to_json.py
- influx_to_mqtt.py
- export_24hr_timeseries.py
- fetch_noaa_buoy.py
- fetch_surrey_wave_v2.py
- buoy_to_influx_sqlite.py
- tide_to_sqlite.py
- export_tide_json.py
- calculate_storm_surge_observed.py
- export_combined_water_level.py
- compare_surrey_dfo_water_levels.py

Eliminate ~100 lines of duplicated code:
- Remove duplicated unit conversion functions (5 functions × 2-3 copies)
- Remove duplicated direction functions (2 functions × 2 copies)
- Remove hardcoded BUOYS dictionaries (3 scripts now use stations.py)
- Remove hardcoded database paths (11 scripts now use config.py)

Benefits:
- Single source of truth for database paths
- Consistent utilities across all scripts
- Easier to add new data sources (wind database coming soon)
- Prepares codebase for Phase 1-2 structure refactoring

All scripts tested and validated. No functional changes to pipeline.
Cron jobs do not need updating (scripts still in same locations).

Phase 0 of 3-phase refactoring complete."

# Push to remote
git push origin main
```

---

### Step 10: Monitor Production (24 Hours)

After deployment, monitor for 24 hours to ensure stability:

```bash
# Watch cron logs for errors
tail -f ~/envcan_wave/*.log

# Check JSON exports still updating
watch -n 60 'ls -lh ~/site/data/latest_buoy_v2.json'

# Check database writes
sqlite3 ~/.local/share/buoy_data.sqlite \
  "SELECT COUNT(*) FROM buoy_observation WHERE timestamp > $(date -d '1 hour ago' +%s);"
# Should show new observations

# Check website
curl https://halibutbank.ca/data/latest_buoy_v2.json | jq '.["4600146"].timestamp'
# Should show recent timestamp
```

**If all looks good after 24 hours, Phase 0 is complete! 🎉**

---

## Troubleshooting

### Import Error: "No module named 'units'"

**Problem:** Script can't find new modules

**Solution:**
```bash
# Verify modules in correct location
ls -la units.py directions.py config.py
# Should be in ~/envcan_wave/ (root directory)

# Check PYTHONPATH
echo $PYTHONPATH
# Should include ~/envcan_wave or be empty (defaults to current dir)

# Try running from correct directory
cd ~/envcan_wave
python3 sqlite_to_json.py
```

---

### Import Error: "No module named 'stations'"

**Problem:** stations.py not found (should already exist)

**Solution:**
```bash
# Check if stations.py exists
ls -la stations.py

# If missing, it should be in root (not migrated yet)
# This is a pre-existing file, should already be there
```

---

### ValueError: Path does not exist

**Problem:** config.py references paths that don't exist yet

**Solution:**
```bash
# config.py creates directories automatically
# But check your environment variables:
echo $MARINE_DATA_DIR  # Should be empty (uses default ~/.local/share)
echo $SITE_DIR         # Should be empty (uses default ~/site)

# If you use custom paths, set environment variables:
export MARINE_DATA_DIR="/your/custom/data/dir"
export SITE_DIR="/your/custom/site/dir"

# Or edit config.py to use your paths directly
```

---

### Script Runs But Produces Wrong Output

**Problem:** Logic changed unintentionally

**Solution:**
```bash
# Compare with backup
diff sqlite_to_json.py sqlite_to_json.py.backup

# If function logic differs, the migration may have gone wrong
# Rollback and investigate:
cp sqlite_to_json.py.backup sqlite_to_json.py

# Review what migration script did:
python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --dry-run
```

---

## Rollback Procedure

If Phase 0 causes issues, rollback using .backup files:

```bash
cd ~/envcan_wave

# Restore all backup files
for file in *.backup; do
    original="${file%.backup}"
    echo "Restoring $original from $file"
    cp "$file" "$original"
done

# Remove new modules
rm units.py directions.py config.py

# Test original scripts work
python3 sqlite_to_json.py
python3 influx_to_mqtt.py

# If working, commit rollback
git add -A
git commit -m "Rollback Phase 0: restored original scripts"
git push origin main

# Monitor logs
tail -f ~/envcan_wave/*.log
```

**After rollback, investigate what went wrong before retrying.**

---

## Common Migration Script Errors

### "Pattern not found" warnings

**Not a problem** - Some patterns may not exist in all scripts. For example:
- Scripts that never had `kmh_to_knots` won't match that pattern
- This is expected and safe to ignore

### "Multiple matches found"

**May be a problem** - If a function appears multiple times in one script:
- Review the script manually
- May need hand-editing to fix

### "Regex failed to match"

**Investigate** - Migration pattern may need adjustment:
- Check the script manually to see actual code format
- May need to update migrate_phase0.py patterns
- Report issue if patterns don't match expected code

---

## Cleanup (After Successful 24-Hour Monitoring)

Once Phase 0 is stable and validated:

```bash
cd ~/envcan_wave

# Remove backup files
rm *.backup

# Remove scaffolding directory (keep in git for reference)
rm -rf phase0_scaffolding

# Optional: Tag this milestone
git tag -a phase0-complete -m "Phase 0 complete: shared utilities extracted"
git push origin phase0-complete
```

---

## Next Steps

After Phase 0 is complete and stable:

1. **Review Phase 1 plan** (Documentation & Configuration reorganization)
2. **Schedule Phase 1 execution** (Low risk, no downtime)
3. **Prepare for Phase 2** (Module structure + script migration)

**Phase 0 → Phase 1 gap:** Can be immediate or wait a few days to ensure stability

**Expected timeline:**
- Phase 0 complete: Week 1
- Phase 1: Week 2
- Phase 2: Weeks 3-4
- Phase 3 (optional polish): Week 5

---

## Success Criteria Checklist

Phase 0 is successful when:

- [ ] All 3 new modules created (units.py, directions.py, config.py)
- [ ] All 11 scripts updated to use shared utilities
- [ ] All validation tests pass (test_new_modules.py, validate_scripts.sh)
- [ ] Manual smoke tests pass (scripts run without errors)
- [ ] JSON exports still generated correctly
- [ ] Database writes still working
- [ ] MQTT messages publishing (if using Home Assistant)
- [ ] No errors in logs for 24 hours
- [ ] Website displays data correctly
- [ ] Changes committed to git

**If all checkboxes ✅, Phase 0 is complete!**

---

## Questions / Issues

If you encounter issues not covered in this guide:

1. Check error messages carefully
2. Review the Troubleshooting section
3. Compare with .backup files
4. Test new modules individually (python3 units.py)
5. If needed, rollback and investigate

**Remember:** Phase 0 is low-risk. Worst case, rollback using .backup files and retry after fixing issues.

---

## Summary

**What Phase 0 Achieves:**
- ✅ 3 new shared utility modules created
- ✅ ~100 lines of duplicated code eliminated
- ✅ Database paths centralized
- ✅ Station data centralized
- ✅ Codebase prepared for Phase 1-2 (structure refactoring)
- ✅ Future additions easier (wind database will reuse these modules!)

**Time invested:** 1-2 hours
**Risk level:** LOW
**Downtime:** None
**Reward:** Cleaner codebase, easier maintenance, ready for structure refactoring

**Next:** Phase 1 (Documentation & Configuration reorganization) - even easier with Phase 0 complete!

---

**Good luck with Phase 0! 🚀**
