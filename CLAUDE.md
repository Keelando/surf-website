# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time marine weather monitoring system for the Salish Sea region (Strait of Georgia, English Bay, Neah Bay). Collects data from:
- **Wave Buoys**: Environment Canada and NOAA buoys for wave/wind/temperature data
- **Tide Stations**: DFO IWLS (Integrated Water Level System) for tide observations and predictions

All data stored in SQLite, published to MQTT/Home Assistant, and exported as JSON for static website rendering.

**Live site:** https://halibutbank.ca

## Station Registry (NEW - 2025-11-01)

**Master station metadata:** `~/envcan_wave/stations.json`

Unified registry containing all monitored stations with coordinates, data types, and metadata. This replaces hardcoded station lists scattered across multiple scripts.

**Key files:**
- `stations.json` - Master metadata (6 buoys + 8 tide stations)
- `stations.py` - Python module for accessing station data
- `validate_stations.py` - Validation script for data integrity

**Web integration:**
- `~/site/data/stations.json` - Web-accessible copy (must be chmod 644)
- `~/site/assets/js/stations-map.js` - Leaflet map displaying all stations
- Map appears on index.html between buoy cards and charts section

**Usage:**
```python
from stations import get_all_buoys, get_tide_station

BUOYS = get_all_buoys()
point_atk = get_tide_station("point_atkinson")
```

## Core Architecture

### Data Flow Pipeline

```
Environment Canada XML feeds → buoy_to_influx_sqlite.py → Buoy SQLite (buoy_data.sqlite)
NOAA text/spectral feeds     → fetch_noaa_buoy.py       →      ↓
                                                         ├→ sqlite_to_json.py → ~/site/data/latest_buoy_v2.json
                                                         ├→ export_24hr_timeseries.py → ~/site/data/timeseries_*.json
                                                         └→ influx_to_mqtt.py → Home Assistant (MQTT)

DFO IWLS Tide API           → tide_to_sqlite.py        → Tide SQLite (tide_data.sqlite)
                                                         └→ export_tide_json.py → ~/site/data/tide-*.json
```

### Key Design Principles

1. **Separate SQLite databases**:
   - `~/.local/share/buoy_data.sqlite` - Wave buoy data
   - `~/.local/share/tide_data.sqlite` - Tide station data (NEW)
2. **InfluxDB as optional sink**: Soft dependency (code works without it)
3. **Per-field freshness**: Each metric (wave height, wind speed, etc.) independently queries for most recent non-null value within 2-hour window
4. **Deduplication**: Primary keys on timestamps prevent duplicate records
5. **Unit conversions**: Wind stored as km/h internally, displayed as knots; NOAA m/s → km/h on ingest
6. **Tide data separation**: Observations (dynamic, every 30 min) vs predictions/high-low (static, once daily)

### Buoy Database Schema

**Database:** `~/.local/share/buoy_data.sqlite`

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
- `4600146` - Halibut Bank (49.337°N, 123.731°W)
- `4600303` - Southern Georgia Strait (49.03°N, 123.43°W)
- `4600304` - English Bay (49.3°N, 123.36°W)
- `4600131` - Sentry Shoal (49.917°N, 124.917°W)

**NOAA:**
- `46087` - Neah Bay (48.495°N, 124.728°W) - includes spectral wave separation: swell vs wind waves
- `46088` - New Dungeness / Hein Bank (48.333°N, 123.167°W)

**Note:** All station metadata maintained in `stations.json`. See Station Registry section above.

## Tide Stations (DFO IWLS)

**Monitored stations (8 total):**

**Permanent (with real-time observations):**
- `point_atkinson` - Point Atkinson (07795)
- `kitsilano` - Kitsilano (07707)
- `new_westminster` - New Westminster (07654)
- `campbell_river` - Campbell River (08074)

**Temporary (predictions only):**
- `tsawwassen` - Tsawwassen (07590)
- `whiterock` - White Rock (07577)
- `crescent_pile` - Crescent Beach (07579)
- `nanaimo` - Nanoose Bay (07930) - Nanaimo area

**Note:** All station metadata maintained in `stations.json`. See Station Registry section above.

**Data sources:**
- **Observations (wlo)**: Real-time water levels from DFO sensors (6-minute intervals)
- **Predictions (wlp)**: Astronomical tide forecasts based on harmonic constituents (1-minute intervals)
- **High/Low Events (wlp-hilo)**: Pre-calculated high and low tide times from DFO

### Tide Database Schema

**Database:** `~/.local/share/tide_data.sqlite` (separate from buoy data)

**Design rationale:** Tide data is separated into three tables based on update frequency:
- **Observations** change every 6 minutes → fetched every 30 minutes
- **Predictions** are static astronomical calculations → fetched once daily
- **High/Low events** are static extrema → fetched once daily

**Table:** `tide_observation`
```sql
CREATE TABLE tide_observation (
    station_id TEXT NOT NULL,           -- DFO station ID (e.g., '5cebf1de3d0f4a073c4bb94c')
    station_name TEXT NOT NULL,         -- Internal key (e.g., 'point_atkinson')
    observation_time INTEGER NOT NULL,  -- Unix timestamp (UTC)
    water_level REAL,                   -- meters
    quality TEXT,                       -- QC flag code from DFO
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, observation_time)
);
```

**Table:** `tide_prediction`
```sql
CREATE TABLE tide_prediction (
    station_id TEXT NOT NULL,           -- DFO station ID
    station_name TEXT NOT NULL,         -- Internal key
    prediction_time INTEGER NOT NULL,   -- Unix timestamp (UTC)
    water_level REAL,                   -- meters
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, prediction_time)
);
```

**Table:** `tide_highlow`
```sql
CREATE TABLE tide_highlow (
    station_id TEXT NOT NULL,           -- DFO station ID
    station_name TEXT NOT NULL,         -- Internal key
    event_time INTEGER NOT NULL,        -- Unix timestamp (UTC)
    water_level REAL,                   -- meters
    event_type TEXT,                    -- 'high' or 'low' (computed from wlp-hilo data)
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, event_time)
);
```

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

# Fetch tide data from DFO IWLS (supports separate flags)
python3 tide_to_sqlite.py --all              # All data types
python3 tide_to_sqlite.py --observations     # Just observations (wlo)
python3 tide_to_sqlite.py --predictions      # Just predictions (wlp)
python3 tide_to_sqlite.py --highlow          # Just high/low events (wlp-hilo)

# Export tide JSON files (latest, timeseries, high/low)
python3 export_tide_json.py
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

# Check tide database table counts
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT 'tide_observation' as table_name, COUNT(*) as count FROM tide_observation
  UNION ALL
  SELECT 'tide_prediction', COUNT(*) FROM tide_prediction
  UNION ALL
  SELECT 'tide_highlow', COUNT(*) FROM tide_highlow;"

# Check latest tide observations per station
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT station_name, datetime(observation_time, 'unixepoch') AS last_obs,
         water_level, quality,
         (strftime('%s','now') - observation_time)/60.0 AS minutes_ago
  FROM tide_observation
  WHERE observation_time IN (
    SELECT MAX(observation_time) FROM tide_observation GROUP BY station_id
  )
  ORDER BY station_name;"

# Check today's high/low tide events
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT station_name, datetime(event_time, 'unixepoch', 'localtime') AS event_time,
         event_type, water_level
  FROM tide_highlow
  WHERE event_time >= strftime('%s', 'now', 'start of day')
    AND event_time < strftime('%s', 'now', '+1 day', 'start of day')
  ORDER BY station_name, event_time;"
```

### Log Monitoring

```bash
tail -f ~/envcan_wave/*.log
```

## Cron Schedule

Production system runs on cron (see `cron.txt`):

**Buoy data:**
- **Every minute**: Parse EC XMLs, export buoy JSON, push MQTT
- **Every 5 min**: Export 24h buoy timeseries
- **Every 20 min**: Fetch NOAA buoy data (5,25,45 min)

**Tide data:**
- **Every minute**: Export tide JSON (latest, timeseries, high/low)
- **Every 30 min**: Fetch tide observations (real-time water levels)
- **Daily 12:10 AM**: Fetch tide predictions (48-hour astronomical forecasts)
- **Daily 12:15 AM**: Fetch tide high/low events (48-hour extrema)

**Maintenance:**
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

### Caddy Web Server Configuration

**Location:** `/etc/caddy/Caddyfile`

The website is served using Caddy on port 8090. Cache headers are configured to:
- **Cache images** (banner, etc.) for 1 month to improve load times
- **No cache** for HTML/CSS/JS/JSON to allow immediate updates during development

```caddy
:8090 {
    root * /home/keelando/site
    file_server

    # Cache images for 1 month (includes banner)
    @images {
        path *.jpg *.jpeg *.png *.gif *.webp *.svg
    }
    header @images Cache-Control "public, max-age=2592000, immutable"

    # No caching for everything else (HTML/CSS/JS/data)
    @nocache {
        not path *.jpg *.jpeg *.png *.gif *.webp *.svg
    }
    header @nocache Cache-Control "no-store, no-cache, must-revalidate"

    # Enable compression
    encode gzip zstd
}
```

**Reload Caddy after changes:**
```bash
sudo caddy reload --config /etc/caddy/Caddyfile
```

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
- Fetches DFO IWLS tide data via API for configured stations
- **Separate database**: Writes to `~/.local/share/tide_data.sqlite` (not buoy_data.sqlite)
- **Three separate tables** with distinct update schedules:
  - `tide_observation` (wlo series) - Real-time water levels, 2-hour window
  - `tide_prediction` (wlp series) - Astronomical forecasts, 48-hour window
  - `tide_highlow` (wlp-hilo series) - High/low events, 48-hour window
- **Command-line flags**:
  - `--observations` - Fetch only observations (runs every 30 min via cron)
  - `--predictions` - Fetch only predictions (runs daily at 12:10 AM via cron)
  - `--highlow` - Fetch only high/low events (runs daily at 12:15 AM via cron)
  - `--all` - Fetch all data types (for manual testing)
- **Event type detection**: Compares wlp-hilo values with neighbors to classify as 'high' or 'low'
- **Rate limiting**: 2.1 second delay between station requests

### export_tide_json.py
- **Source database**: Reads from `~/.local/share/tide_data.sqlite`
- Exports three JSON files for tide page:
  - `tide-latest.json` - Current observation and prediction per station
  - `tide-timeseries.json` - Calendar day ±2 hours (observations + predictions)
  - `tide-hi-low.json` - 26-hour window of high/low events (12h before to 14h after)
- **Downsampling**: Observations to 15-minute intervals (:00, :15, :30, :45)
- **Timezone conversion**: All exports include Pacific time for display
- **Atomic writes**: Uses temp files to prevent partial JSON writes
- Runs every minute via cron

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

### Tide Data Architecture
- **Separate database rationale**: Tide data has different update frequencies than buoy data
  - Observations: Dynamic, updated every 6 minutes by DFO sensors
  - Predictions: Static, astronomical calculations don't change
  - High/low events: Static, extrema calculated once per day
- **Fetch optimization**: Only observations fetched frequently (every 30 min), predictions/high-low fetched once daily
- **Primary keys**: Prevent duplicates on (station_id, timestamp) without needing IGNORE logic
- **DFO API series codes**:
  - `wlo` - Water Level Observations (real-time sensor data)
  - `wlp` - Water Level Predictions (1-minute interval astronomical forecasts)
  - `wlp-hilo` - Water Level Prediction High/Low (extrema only)
- **Station metadata**: Station names and display info stored in `tide_stations.json`

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

### Missing tide predictions or high/low events
- Check if tide database exists: `ls -lh ~/.local/share/tide_data.sqlite`
- Verify prediction fetch ran: `tail tide_pred.log` and `tail tide_highlow.log`
- Predictions/high-low only fetch once daily (12:10 AM / 12:15 AM), so missing data may indicate:
  - Script hasn't run yet today
  - API error during fetch (check logs)
  - Empty query window (predictions use 48-hour window centered on now)

### Tide export shows "0 stations" for high/low
- High/low events use a 26-hour query window (12h before to 14h after current time)
- If predictions are stale (fetched >12 hours ago), they fall outside export window
- Solution: Manually run `python3 tide_to_sqlite.py --highlow` to refresh data

## Frontend Structure

### Tides Page (~/site/tides.html)

The tide monitoring page displays real-time water levels and predictions for DFO stations.

**Key features:**
- **Station selector dropdown** - Lists all monitored stations alphabetically
- **Auto-loads Point Atkinson** - Default station loads automatically on page load
- **Three data displays:**
  1. Current observation (latest water level from DFO sensor)
  2. Current prediction (astronomical tide forecast for now)
  3. Today's high/low tides (table showing predicted highs and lows)
- **28-hour tide chart** - ECharts visualization showing:
  - Tide predictions as smooth blue line
  - Actual observations as green scatter points
  - Interactive tooltips with Pacific time formatting
  - Responsive grid layout (10% margins, containLabel: true)
- **Auto-refresh** - Reloads data every 5 minutes
- **Responsive design** - Works on mobile, tablet, and desktop

**JavaScript file:** `~/site/assets/js/tides.js`

Key functions:
- `loadTideData()` - Fetches all three JSON files (latest, timeseries, high/low)
- `populateStationDropdown()` - Builds station selector, sets Point Atkinson as default
- `displayStation()` - Renders all components for selected station
- `displayTideChart()` - Initializes and renders ECharts tide chart
- Chart shows section first, then renders chart to ensure proper width measurement

**Chart styling:** Chart container uses consistent 1200px max-width across site:
- Border, shadow, and overflow styling in `nav-tide-styles.css`
- Grid uses percentage-based margins (10% left/right) for responsive width
- hideOverlap prevents x-axis label crowding
- Responsive font sizes (9px mobile, 10px desktop)

**Data sources:**
- `~/site/data/tide-latest.json` - Current conditions
- `~/site/data/tide-timeseries.json` - 28-hour rolling window
- `~/site/data/tide-hi-low.json` - Today's high/low events

### Chart Max-Width Standards

All chart-containing sections use **1200px max-width** for consistency:

**index.html:**
- `#charts-section` - Buoy charts
- `#wave-height-table-section` - Wave summary table
- `#storm-surge-section` - Storm surge forecasts

**tides.html:**
- `.tide-main-content` - Tide page main container

**CSS implementation:**
```css
#charts-section,
#storm-surge-section,
#wave-height-table-section,
.tide-main-content {
  max-width: 1200px;
  margin: 2rem auto;
  padding: 0 1rem;
}
```

All inline styles have been moved to CSS files for maintainability.
