# Backend Testing Results - 2025-11-22

## Summary

Comprehensive backend testing was performed using example/test data to identify potential issues before they impact production.

## Test Databases Generated

- **Tide database**: `tests/databases/tide_data_test.sqlite`
  - 82 observations, 1,444 predictions
  - 2 stations: Point Atkinson, Kitsilano

- **Wind database**: `tests/databases/wind_data_test.sqlite`
  - 75 observations (25 per station)
  - 3 stations: Point Atkinson (CWSB), Sisters Island (CWGT), Tsawwassen (CVTF)

- **Storm surge data**: `tests/data/storm_surge/*.json`
  - 3 stations with forecast data

## Issues Found and Fixed

### 1. Hardcoded Path in stations.py (FIXED)

**File**: `stations.py:25`

**Issue**:
```python
# Before:
STATIONS_FILE = Path("~/envcan_wave/config/stations.json").expanduser()
```

**Problem**:
- Path was hardcoded to `~/envcan_wave/config/stations.json`
- Only worked in production environment (`/home/keelando/envcan_wave/`)
- Failed in test/dev environments or when run from different directories
- Caused `FileNotFoundError` for all scripts importing the stations module

**Fix**:
```python
# After:
from config import PROJECT_ROOT
STATIONS_FILE = PROJECT_ROOT / "config" / "stations.json"
```

**Impact**:
- Makes code portable across environments
- Works regardless of working directory or user account
- Critical for development, testing, and CI/CD workflows

**Why it wasn't noticed before**:
- Production scripts run from `/home/keelando/envcan_wave/` where the hardcoded path was correct
- Website uses `~/site/data/stations.json` directly (not via Python stations module)
- No one had run the scripts from a different directory before

**Commit**: `25b98b4` - "Fix hardcoded path in stations.py causing FileNotFoundError"

## Module Import Tests

✅ All 16 core modules import successfully:
- config, logging_config, stations, units, directions
- tide_to_sqlite, wind_to_sqlite, buoy_to_influx_sqlite
- parse_marine_forecast
- export_tide_json, export_wind_json, export_wind_24hr_timeseries
- export_24hr_timeseries, export_combined_water_level
- export_hindcast_json, export_observed_storm_surge

## Utility Function Tests

✅ All utility functions working correctly:
- Unit conversions (km/h ↔ knots, m/s ↔ km/h, meters ↔ feet)
- Direction conversions (degrees ↔ cardinal directions)
- Proper None/null value handling

## Station Registry

✅ Successfully loads metadata:
- 12 buoy stations
- 12 tide stations
- No parsing errors

## Observations (Not Bugs)

### Test Mode Support

Currently only `export_combined_water_level.py` has `--test-mode` flag support. Other export scripts use production database paths from `config.py`:
- `export_tide_json.py` - No test mode
- `export_wind_json.py` - No test mode
- `export_wind_24hr_timeseries.py` - No test mode
- `export_24hr_timeseries.py` - No test mode
- `export_hindcast_json.py` - No test mode
- `export_observed_storm_surge.py` - No test mode

**Note**: This is by design. Scripts expect production databases in `/root/.local/share/` or `~/.local/share/`. Test mode flags would be an enhancement, not a bug fix.

### Missing Dependencies in Test Environment

- `fetch_storm_surge.py` requires `owslib` module
- Not installed in this test environment
- Likely present in production

### stations.json Deployment

From `docs/DEPLOYMENT.md`, stations.json exists in TWO locations:

1. **Backend**: `~/envcan_wave/config/stations.json`
   - Used by Python scripts via `stations.py` module
   - Now correctly references via `PROJECT_ROOT`

2. **Frontend**: `~/site/data/stations.json`
   - Used directly by website (fetch/AJAX)
   - Independent of Python stations module
   - TODO: Investigate frontend usage patterns

**Action Item**: Review frontend code to understand:
- How it consumes stations.json
- Whether it needs metadata updates
- If backend should export/sync to frontend location

## Syntax Validation

✅ No Python syntax errors found in any `.py` files (excluding archive/)

## Test Data Validation

✅ Test databases contain valid data:
- Proper schema matching production
- Realistic timestamps (auto-generated as recent)
- Appropriate data ranges and variations

## Recommendations

1. ✅ **DONE**: Fix hardcoded path in stations.py
2. Consider adding `--test-mode` flags to other export scripts (low priority)
3. Review frontend stations.json usage and document the workflow
4. Consider adding automated tests using the test database infrastructure

## Related Files

- Test infrastructure: `tests/README.md`
- Wind testing: `tests/README_WIND.md`
- Offline testing summary: `tests/OFFLINE_TESTING_SUMMARY.md`
- Test database generators:
  - `tests/create_test_tide_database.py`
  - `tests/create_test_wind_database.py`
  - `tests/create_test_storm_surge.py`
