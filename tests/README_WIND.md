# Wind Data Offline Testing

Test fixtures and database generation for offline development of wind data pipeline.

## Quick Start

```bash
# Generate test database and setup offline environment
./tests/setup_offline_wind_test.sh
```

## What's Included

### Test Fixtures
- **Location:** `tests/fixtures/wind/`
- **Format:** SWOB-ML XML files (simplified but realistic)
- **Stations:** 3 sample stations (CWSB, CWGT, CVTF)
- **Data:** Wind speed, gust, direction, temperature, pressure, humidity, dewpoint, rainfall

### Test Database
- **Generated:** `tests/databases/wind_data_test.sqlite`
- **Records:** 75 observations (25 per station, hourly for 24 hours)
- **Schema:** Matches production `wind_observation` table
- **Timestamps:** Auto-generated as recent data (last 24 hours)

### Scripts
| Script | Purpose |
|--------|---------|
| `create_test_wind_database.py` | Generate populated test database |
| `setup_offline_wind_test.sh` | Complete setup script |

## Testing Workflow

### 1. Generate Test Data
```bash
python3 tests/create_test_wind_database.py
```

### 2. Test Parser
```bash
# Copy test database to standard location
cp tests/databases/wind_data_test.sqlite ~/.local/share/wind_data.sqlite

# Test wind parser (reads from fixtures)
python3 wind_to_sqlite.py
```

### 3. Test Exports
```bash
# Export JSON files
python3 export_wind_json.py
python3 export_wind_24hr_timeseries.py

# Check outputs
cat ~/site/data/latest_wind.json | jq '.CWSB'
cat ~/site/data/wind_timeseries_24hr.json | jq '.CWSB | length'
```

### 4. Test Frontend
```bash
# Open winds.html in browser
# Data should load from exported JSON files
```

## Test Data Characteristics

### Station Coverage
- **CWSB** (Point Atkinson) - Coastal marine station
- **CWGT** (Sisters Island) - Exposed strait location
- **CVTF** (Tsawwassen) - Sheltered southern location

### Data Variation
- Wind speeds: 15-35 km/h with ±10% random variation
- Gusts: 20-45 km/h
- Directions: Randomized with ±30° variation
- Realistic atmospheric conditions (temp, pressure, humidity)

### Realistic Features
- Some stations may have missing data (MSNG)
- Timestamps span full 24-hour period
- Hourly resolution matches production
- All units match production (km/h, °C, hPa)

## Database Schema

```sql
CREATE TABLE wind_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    station_name TEXT,
    observation_time INTEGER NOT NULL,
    wind_speed_kmh REAL,
    wind_gust_kmh REAL,
    wind_direction_deg INTEGER,
    air_temp_c REAL,
    pressure_hpa REAL,
    rainfall_1hr_mm REAL,
    rainfall_6hr_mm REAL,
    humidity_percent REAL,
    dewpoint_c REAL,
    pressure_mslp_hpa REAL,
    visibility_km REAL,
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);
```

## Extending Test Data

### Add More Stations
1. Create new SWOB-ML fixture in `tests/fixtures/wind/sample_STATION.xml`
2. Add station to `TEST_STATIONS` dict in `create_test_wind_database.py`
3. Regenerate database

### Modify Sample Data
1. Edit XML fixtures to change wind speeds, directions, etc.
2. Adjust variation parameters in `populate_station_data()` function
3. Regenerate database

## Related Documentation

- `docs/ARCHITECTURE_DETAILED.md` - Wind database schema details
- `docs/project/CLAUDE.md` - Wind station information
- `README.md` - Updated with wind pipeline architecture
