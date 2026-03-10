# Test Data Infrastructure

This directory contains test fixtures and tools for offline development and testing of the marine weather monitoring system.

## Overview

The test infrastructure allows you to:
- Develop and test without requiring live API access
- Work with consistent, reproducible data
- Test edge cases and specific scenarios
- Build and iterate faster without external dependencies

## Directory Structure

```
tests/
├── fixtures/              # Sample API responses and data files
│   ├── dfo_iwls/         # DFO IWLS tide API responses
│   ├── ec_buoy/          # Environment Canada buoy XMLs (TODO)
│   ├── noaa_buoy/        # NOAA buoy feeds (TODO)
│   ├── marine_forecast/  # Marine forecast XMLs (TODO)
│   └── storm_surge/      # Storm surge WMS responses (TODO)
├── databases/            # Pre-populated test databases
│   └── tide_data_test.sqlite
├── create_test_tide_database.py  # Database generator script
├── run_tide_test_workflow.sh     # Complete test workflow
└── README.md             # This file
```

## Quick Start

### 1. Generate Test Database

```bash
python3 tests/create_test_tide_database.py
```

This creates `tests/databases/tide_data_test.sqlite` with:
- Recent tide observations (last 4 hours)
- Tide predictions (matching time range)
- High/low tide events

### 2. Calculate Tide Offsets (Test Mode)

```bash
python3 calculate_storm_surge_observed.py --test-mode
```

This calculates observed storm surge (observed - predicted) using the test database.

### 3. Run Complete Workflow

```bash
./tests/run_tide_test_workflow.sh
```

## Frontend Console Tests (Playwright)

Automated browser checks live in `tests/playwright` and make sure every public page loads without throwing console errors. Each test captures the browser console output and fails when `console.error` or `pageerror` events appear.

### Setup

```bash
npm install
npx playwright install chromium firefox   # Run once to pull both browser binaries
```

### Run

```bash
npm run test:frontend
```

The command automatically starts a local static server from `site/` and visits key routes (`/`, `/tides.html`, `/winds.html`, etc.) in both Chromium and Firefox projects. Console output for each page is attached to the Playwright report (`playwright-report/`). Failures will include the offending console text right in the test output. Playwright tears down the Python server after the suite finishes, but if a run is interrupted you can double-check the port is free with `lsof -i :4173`.

To audit additional pages, edit `tests/playwright/console.spec.js` and add the route to the `monitoredRoutes` array.

## Test Fixtures

### Tide Data (DFO IWLS)

**Location:** `tests/fixtures/dfo_iwls/`

**Stations:**
- `point_atkinson_*.json` - Point Atkinson (07795)
- `kitsilano_*.json` - Kitsilano (07707)

**Data types:**
- `*_observations.json` - Water level observations (wlo)
- `*_predictions.json` - Astronomical tide predictions (wlp)
- `*_highlow.json` - High/low tide events (wlp-hilo)

**Timestamp handling:**
The database generator automatically updates fixture timestamps to be recent (within last 4 hours), so the data never goes stale.

**Sample data characteristics:**
- Point Atkinson: ~+0.06m average storm surge offset
- Kitsilano: ~+0.02m average storm surge offset
- Represents mild positive surge conditions (calm to moderate weather)

## Modifying Scripts for Test Mode

Scripts support `--test-mode` flag to use test databases instead of production:

```python
import argparse
from pathlib import Path

DB_PATH = Path("~/.local/share/tide_data.sqlite").expanduser()
TEST_DB_PATH = Path(__file__).parent / "tests" / "databases" / "tide_data_test.sqlite"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-mode', action='store_true')
    args = parser.parse_args()

    db_path = TEST_DB_PATH if args.test_mode else DB_PATH
    # ... rest of script
```

## Creating New Test Fixtures

### Capturing Real API Responses

For tide data:
```bash
# Capture observation data (requires API access)
curl -H "User-Agent: your@email.com" \
  "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/07795/data?time-series-code=wlo&from=2024-01-15T08:00:00Z&to=2024-01-15T12:00:00Z" \
  > tests/fixtures/dfo_iwls/point_atkinson_observations.json
```

### Creating Synthetic Data

For testing edge cases, you can create synthetic fixtures:

```python
import json
from datetime import datetime, timedelta, timezone

# Generate synthetic tide data
base_time = datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
data = []

for i in range(40):
    timestamp = base_time + timedelta(minutes=6 * i)
    # Simple sinusoidal tide pattern
    water_level = 3.0 + 1.5 * sin(i * 0.1)

    data.append({
        "eventDate": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "value": round(water_level, 3),
        "qcFlagCode": "1"
    })

with open("tests/fixtures/dfo_iwls/synthetic_observations.json", "w") as f:
    json.dump(data, f, indent=2)
```

## Test Database Schema

The test database mirrors the production schema exactly:

**Tables:**
- `tide_observation` - Real-time water levels
- `tide_prediction` - Astronomical tide predictions
- `tide_highlow` - High/low tide events
- `tide_offset` - Calculated storm surge (obs - pred)

See `tests/create_test_tide_database.py` for complete schema.

## Working with Test Data

### Query Test Database

Using Python:
```python
import sqlite3

conn = sqlite3.connect("tests/databases/tide_data_test.sqlite")
cur = conn.cursor()

# Get all offsets
cur.execute("SELECT * FROM tide_offset ORDER BY observation_time DESC LIMIT 10")
for row in cur.fetchall():
    print(row)

# Get summary statistics
cur.execute("""
    SELECT
        station_name,
        COUNT(*) as records,
        AVG(offset) as avg_offset,
        MIN(offset) as min_offset,
        MAX(offset) as max_offset
    FROM tide_offset
    GROUP BY station_name
""")
for row in cur.fetchall():
    print(f"{row[0]}: {row[1]} records, avg={row[2]:+.3f}m")
```

### Inspect with sqlite3 CLI

```bash
# Install sqlite3 if needed
sqlite3 tests/databases/tide_data_test.sqlite

# Example queries
sqlite> SELECT COUNT(*) FROM tide_offset;
sqlite> SELECT * FROM tide_offset WHERE station_id = 'point_atkinson' LIMIT 5;
sqlite> .schema tide_offset
```

## Testing Different Scenarios

### Scenario 1: Moderate Positive Surge
**Current default fixtures** - Point Atkinson +0.06m avg

### Scenario 2: Strong Storm Surge
Modify observations in fixtures to be +0.5m higher than predictions:
```python
# In fixture file, add 0.5 to all observation values
obs["value"] += 0.5
```

### Scenario 3: Negative Surge (High Pressure System)
Modify observations to be -0.2m lower than predictions

### Scenario 4: Missing/Sparse Data
Remove some observation records to test interpolation logic

## Future Enhancements

### Storm Surge Test Fixtures (✅ AVAILABLE)

**Location:** `tests/fixtures/storm_surge/`

**Files:**
- `hindcast.json` - Clean hindcast data (no duplicates)
- `hindcast_with_duplicates.json` - Test data with pre-Nov-7 duplicates (mimics production)
- `Point_Atkinson.json` - Individual station forecast
- `Campbell_River.json` - Individual station forecast
- `Crescent_Beach_Channel.json` - Individual station forecast

**Test scripts:**
- `setup_offline_test.sh` - Copy fixtures to `~/site/data/storm_surge/`
- `validate_hindcast_timestamps.py` - Validate timestamp format and calendar alignment
- `diagnose_hindcast_duplicates.py` - Detect duplicate data across stations

**See:** `tests/NEXT_STEPS.md` for usage guide

### TODO: Additional Fixtures Needed

1. **EC Buoy XMLs** (`tests/fixtures/ec_buoy/`)
   - SWOB-ML format
   - Multiple buoy stations
   - Wave/wind/temperature data

2. **NOAA Buoy Feeds** (`tests/fixtures/noaa_buoy/`)
   - `.txt` files (meteorological data)
   - `.spec` files (spectral wave data)
   - Station 46087 (Neah Bay)
   - Station 46088 (New Dungeness)

3. **Marine Forecast XMLs** (`tests/fixtures/marine_forecast/`)
   - Strait of Georgia north/south zones
   - Various warning types

### TODO: Test Mode Support

Add `--test-mode` support to additional scripts:
- [ ] `tide_to_sqlite.py`
- [ ] `buoy_to_influx_sqlite.py`
- [ ] `fetch_noaa_buoy.py`
- [ ] `fetch_storm_surge.py`
- [ ] `parse_marine_forecast.py`

### TODO: Integration Tests

Create automated test suite:
```python
# tests/test_tide_offsets.py
def test_offset_calculation():
    # Generate test DB
    # Run calculation
    # Verify expected offsets
    assert avg_offset == pytest.approx(0.06, abs=0.01)
```

## Best Practices

1. **Always regenerate test database** before testing to ensure fresh timestamps
2. **Don't commit the test database** to git (add to `.gitignore`)
3. **Document fixture characteristics** in comments (what scenario they represent)
4. **Use meaningful fixture filenames** that indicate content
5. **Keep fixtures small** - just enough data to test functionality
6. **Version control fixtures** - commit JSON/XML files, not binary databases

## Troubleshooting

### Test database is empty
```bash
# Regenerate it
python3 tests/create_test_tide_database.py
```

### Offsets being purged immediately
The data is too old. Regenerate the database - it will automatically use recent timestamps.

### Fixture not found
Check the path in `create_test_tide_database.py` matches your fixture filename.

### Script doesn't recognize --test-mode
The script hasn't been updated yet. See "Modifying Scripts for Test Mode" above.

## Contributing

When adding new test fixtures:

1. Create fixture files in appropriate subdirectory
2. Update `create_test_*_database.py` if needed
3. Document the fixture characteristics in this README
4. Test the complete workflow end-to-end
5. Commit fixtures (JSON/XML) but not databases

## References

- [DFO IWLS API Documentation](https://api-iwls.dfo-mpo.gc.ca/swagger-ui/index.html)
- [NOAA NDBC Data Formats](https://www.ndbc.noaa.gov/docs/)
- [Environment Canada SWOB-ML](https://collaboration.cmc.ec.gc.ca/cmc/cmos/public_doc/msc-data/obs_station/obs_station_swob-xml_en.pdf)
