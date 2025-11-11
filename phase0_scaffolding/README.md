# Phase 0 Scaffolding Package

Complete toolkit for executing Phase 0 refactoring (shared utilities extraction).

## What's Inside

```
phase0_scaffolding/
├── new_modules/              # Ready-to-deploy utility modules
│   ├── units.py             # Unit conversion functions
│   ├── directions.py        # Direction utilities
│   └── config.py            # Centralized configuration
│
├── migration_scripts/        # Automated migration tooling
│   └── migrate_phase0.py    # Auto-updates all scripts to use new modules
│
├── validation/               # Testing and validation tools
│   ├── test_new_modules.py  # Test suite for new modules
│   └── validate_scripts.sh  # Validation script for migrated code
│
└── docs/                     # Execution documentation
    └── PHASE0_EXECUTION_GUIDE.md  # Step-by-step instructions
```

## Quick Start

### On Your Production Server:

1. **Copy this directory to server:**
   ```bash
   # Already done if you pulled from git!
   cd ~/envcan_wave
   ```

2. **Follow the execution guide:**
   ```bash
   cat phase0_scaffolding/docs/PHASE0_EXECUTION_GUIDE.md
   ```

3. **Or use this quick reference:**

   ```bash
   # Step 1: Copy new modules
   cp phase0_scaffolding/new_modules/*.py .

   # Step 2: Test modules
   python3 phase0_scaffolding/validation/test_new_modules.py

   # Step 3: Preview migration
   python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --dry-run

   # Step 4: Apply migration
   python3 phase0_scaffolding/migration_scripts/migrate_phase0.py --backup

   # Step 5: Validate
   bash phase0_scaffolding/validation/validate_scripts.sh

   # Step 6: Test manually
   python3 sqlite_to_json.py
   python3 influx_to_mqtt.py

   # Step 7: Commit
   git add -A
   git commit -m "Phase 0: Extract shared utilities"
   git push
   ```

## What Phase 0 Does

**Creates:**
- `units.py` - Unit conversions (kmh_to_knots, ms_to_kmh, etc.)
- `directions.py` - Direction utilities (degrees_to_cardinal, parse_direction, etc.)
- `config.py` - Database paths, freshness windows, field definitions

**Updates (11 scripts):**
- `sqlite_to_json.py`
- `influx_to_mqtt.py`
- `export_24hr_timeseries.py`
- `fetch_noaa_buoy.py`
- `fetch_surrey_wave_v2.py`
- `buoy_to_influx_sqlite.py`
- `tide_to_sqlite.py`
- `export_tide_json.py`
- `calculate_storm_surge_observed.py`
- `export_combined_water_level.py`
- `compare_surrey_dfo_water_levels.py`

**Eliminates:**
- ~100 lines of duplicated code
- 3 hardcoded BUOYS dictionaries
- 11 hardcoded database paths
- 5+ duplicated function definitions

## Safety Features

- **Dry-run mode** - Preview changes before applying
- **Automatic backups** - Creates .backup files for all modified scripts
- **Validation suite** - Tests syntax and imports after migration
- **Rollback procedure** - Easy restoration from .backup files

## Timeline

- **Execution:** 30-60 minutes
- **Testing:** 30 minutes
- **Monitoring:** 24 hours
- **Total:** ~1.5 hours active work

## Risk Level: LOW

- No file moves (scripts stay in same location)
- No cron changes needed
- Easy rollback (just restore .backup files)
- All changes are pure refactoring (no logic changes)

## Success Criteria

✅ All tests pass (test_new_modules.py)
✅ All validations pass (validate_scripts.sh)
✅ Scripts run without errors
✅ JSON exports still generated
✅ No errors in logs for 24 hours

## Files You'll Create

After Phase 0, your root directory will have:

```
~/envcan_wave/
├── units.py              # NEW - Unit conversions
├── directions.py         # NEW - Direction utilities
├── config.py             # NEW - Configuration
├── ... all existing scripts (now using above modules)
```

## Next Steps

After Phase 0:
1. Monitor for 24 hours
2. Clean up .backup files (once stable)
3. Proceed to Phase 1 (Documentation reorganization)

## Support

- **Execution Guide:** `docs/PHASE0_EXECUTION_GUIDE.md` (detailed step-by-step)
- **Troubleshooting:** See guide's Troubleshooting section
- **Rollback:** See guide's Rollback Procedure section

## Questions?

Before executing:
- Read `docs/PHASE0_EXECUTION_GUIDE.md` thoroughly
- Run `--dry-run` first to preview changes
- Make backups of your current state

During execution:
- If tests fail, STOP and investigate
- If validation fails, review errors
- If unsure, don't proceed - ask for help

After execution:
- Monitor logs for 24 hours
- Test critical functionality
- Keep .backup files until stable

---

**Ready to proceed?** Start with `docs/PHASE0_EXECUTION_GUIDE.md`
