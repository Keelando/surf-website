# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time marine weather monitoring system for the Salish Sea region (Strait of Georgia, English Bay, Neah Bay). Collects data from Environment Canada and NOAA buoys, stores in SQLite, publishes to MQTT/Home Assistant, and exports JSON for static website rendering.

**Live site:** https://halibutbank.ca

## Core Architecture

### Data Flow Pipeline

```
Environment Canada XML feeds → buoy_to_influx_sqlite.py → SQLite Database
NOAA text/spectral feeds     → fetch_noaa_buoy.py       →      ↓
                                                         ├→ sqlite_to_json.py → ~/site/data/*.json (website)
                                                         ├→ export_24hr_timeseries.py → 24h charts
                                                         └→ influx_to_mqtt.py → Home Assistant
```

### Key Design Principles

1. **SQLite as primary persistence**: Single source of truth at `~/.local/share/buoy_data.sqlite`
2. **InfluxDB as optional sink**: Soft dependency (code works without it)
3. **Per-field freshness**: Each metric (wave height, wind speed, etc.) independently queries for most recent non-null value within 2-hour window
4. **Deduplication**: Unique index on `(buoy_id, observation_time)` prevents duplicate records
5. **Unit conversions**: Wind stored as km/h internally, displayed as knots; NOAA m/s → km/h on ingest

### Database Schema

**Table:** `buoy_observation`

```sql
CREATE TABLE buoy_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buoy_id TEXT NOT NULL,
    observation_time INTEGER NOT NULL,  -- Unix timestamp
    -- Wave metrics
    wave_height_sig, wave_height_peak, wave_period_sig, wave_period_avg, wave_period_peak,
    wave_direction_avg, wave_direction_peak REAL,
    -- NOAA spectral (Neah Bay, New Dungeness)
    swell_height, swell_period, swell_direction,
    wind_wave_height, wind_wave_period, wind_wave_direction REAL,
    -- Meteorological
    wind_speed, wind_gust, wind_direction, air_temp, sea_temp, pressure REAL,
    -- Metadata
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX uniq_buoy_ts ON buoy_observation(buoy_id, observation_time);
```

## Monitored Buoys

**Environment Canada (EC):**
- `4600146` - Halibut Bank
- `4600303` - Southern Georgia Strait
- `4600304` - English Bay
- `4600131` - Sentry Shoal

**NOAA:**
- `46087` - Neah Bay (includes spectral wave separation: swell vs wind waves)
- `46088` - New Dungeness (Hein Bank)

## Development Commands

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Manual Script Execution

```bash
# Activate venv first
source .venv/bin/activate

# Fetch Environment Canada data (parses XMLs in data/buoy/)
python3 buoy_to_influx_sqlite.py

# Fetch NOAA 5-day feeds (meteorological + spectral)
python3 fetch_noaa_buoy.py

# Export latest snapshot to JSON
python3 sqlite_to_json.py

# Export 24-hour timeseries
python3 export_24hr_timeseries.py

# Push to Home Assistant via MQTT
python3 influx_to_mqtt.py

# Fetch storm surge forecast from GeoMet
python3 fetch_storm_surge.py

# Fetch tide data from NOAA
python3 tide_to_sqlite.py
```

### Database Inspection

```bash
# Check latest observations per buoy
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT buoy_id, datetime(observation_time, 'unixepoch') AS last_obs,
         (strftime('%s','now') - observation_time)/3600.0 AS hours_ago
  FROM buoy_observation
  WHERE observation_time IN (
    SELECT MAX(observation_time) FROM buoy_observation GROUP BY buoy_id
  );"

# View recent records for a specific buoy
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT datetime(observation_time, 'unixepoch'), wave_height_sig, wind_speed
  FROM buoy_observation
  WHERE buoy_id='46087'
  ORDER BY observation_time DESC LIMIT 10;"
```

### Log Monitoring

```bash
tail -f ~/envcan_wave/*.log
```

## Cron Schedule

Production system runs on cron (see `cron.txt`):

- **Every minute**: Parse EC XMLs, export JSON, push MQTT
- **Every 5 min**: Export 24h timeseries
- **Every 20 min**: Fetch NOAA data (5,25,45 min)
- **Every 30 min**: Fetch tide data
- **Every 6 hours**: Fetch storm surge forecast (1,7,13,19h)
- **Hourly**: Purge XML files older than 2 days
- **Daily 11 PM**: Auto-commit and push to git

## Configuration Files

### InfluxDB + MQTT Credentials

**Location:** `~/.config/buoy_influx_1.env`

```
INFLUX_HOST=192.168.1.98
INFLUX_PORT=8086
INFLUX_USER=your_user
INFLUX_PASS=your_password
INFLUX_DB=buoy_data
MQTT_HOST=192.168.1.98
MQTT_PORT=1883
MQTT_USER=your_user
MQTT_PASS=your_password
```

**Security:** Never commit `.env` files. Use `chmod 600` on credentials.

## Adding a New Buoy

1. Add to `BUOYS` dictionary in all scripts that reference it:
   - `buoy_to_influx_sqlite.py` (if EC source)
   - `fetch_noaa_buoy.py` (if NOAA source)
   - `sqlite_to_json.py`
   - `export_24hr_timeseries.py`
   - `influx_to_mqtt.py`

2. If new field mappings are needed, update `FIELD_MAP_*` dictionaries in relevant scripts

3. Update frontend JavaScript (not in this repo):
   - `~/site/assets/js/main.js` - buoy display order
   - `~/site/assets/js/charts.js` - chart configurations

4. Test the pipeline:
   ```bash
   python3 fetch_noaa_buoy.py  # or buoy_to_influx_sqlite.py
   sqlite3 ~/.local/share/buoy_data.sqlite "SELECT * FROM buoy_observation WHERE buoy_id='NEW_ID' LIMIT 5;"
   python3 sqlite_to_json.py
   cat ~/site/data/latest_buoy_v2.json | jq .NEW_ID
   ```

## Script Responsibilities

### buoy_to_influx_sqlite.py
- Parses Environment Canada SWOB-ML XML files from `data/buoy/`
- Extracts wave and meteorological data
- Primary sink: SQLite; optional secondary: InfluxDB
- Runs every minute via cron

### fetch_noaa_buoy.py
- Fetches NOAA NDBC 5-day feeds (`.txt` for meteorological, `.spec` for spectral waves)
- Merges data by timestamp
- Handles missing data indicators (`MM`)
- Converts m/s → km/h for wind speed
- Parses cardinal directions ('WSW') and numeric degrees
- Runs every 20 minutes (NOAA updates hourly)

### sqlite_to_json.py
- Queries SQLite for latest non-null values per field (within 2-hour freshness window)
- Exports `~/site/data/latest_buoy_v2.json`
- Converts wind km/h → knots for display
- Adds cardinal directions for wind/wave direction
- Runs every minute

### export_24hr_timeseries.py
- Generates 24-hour rolling timeseries per buoy
- Outputs separate JSON files to `~/site/data/timeseries_*.json`
- Used by ECharts-based visualization on website

### influx_to_mqtt.py
- Queries InfluxDB (or SQLite) for latest readings
- Publishes to MQTT with Home Assistant auto-discovery
- Sends sensor configs + state updates
- Runs every minute

### fetch_storm_surge.py
- Fetches GeoMet GDSPS storm surge forecasts using OWSLib
- Processes GeoTIFF/WMS layers for specific locations
- Runs every 6 hours (aligned with GeoMet updates)

### tide_to_sqlite.py
- Fetches NOAA tide predictions for configured stations
- Stores in SQLite for local querying
- Runs every 30 minutes

## Important Data Handling Notes

### NOAA Pressure Field
- Valid pressure values can be around 999 hPa (e.g., low-pressure systems)
- Do NOT treat 999 as a missing data indicator
- Only `MM`, `M`, `NA`, empty strings are missing indicators

### Spectral Wave Data (NOAA)
- **Swell** (SwH/SwP/SwD): Long-period ocean waves from distant storms
- **Wind waves** (WWH/WWP/WWD): Short-period locally-generated waves
- Only available for stations with spectral buoys (46087, 46088)

### Timestamp Handling
- NOAA provides UTC timestamps without timezone info
- Always parse as `datetime(..., tzinfo=timezone.utc)`
- SQLite stores Unix epoch integers
- JSON exports use ISO 8601 format

## Testing Changes

When modifying data processing logic:

1. Run script manually and check logs for errors
2. Query SQLite to verify data was inserted correctly
3. Check JSON exports for expected format/values
4. Monitor MQTT topics if testing Home Assistant integration
5. Verify website displays data correctly (if applicable)

## Common Issues

### "Influx unavailable" warnings
Normal if running SQLite-only mode. Scripts gracefully degrade.

### Stale data warnings on website
Check if cron jobs are running: `crontab -l` and verify log files show recent activity.

### Missing spectral data for EC buoys
Expected - only NOAA stations provide swell/wind wave separation.

### Wind direction shows as null
NOAA may report `MM` (missing) for calm conditions or sensor failures. This is handled correctly.
