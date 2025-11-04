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
sr3 (Sarracenia) subscribe  → Environment Canada XMLs  → buoy_to_influx_sqlite.py → Buoy SQLite (buoy_data.sqlite)
NOAA text/spectral feeds    → fetch_noaa_buoy.py       →      ↓
                                                         ├→ sqlite_to_json.py → ~/site/data/latest_buoy_v2.json
                                                         ├→ export_24hr_timeseries.py → ~/site/data/timeseries_*.json
                                                         └→ influx_to_mqtt.py → Home Assistant (MQTT)

DFO IWLS Tide API           → tide_to_sqlite.py        → Tide SQLite (tide_data.sqlite)
                                                         └→ export_tide_json.py → ~/site/data/tide-*.json

sr3 (Marine Forecast)       → EC Marine Forecast XMLs  → parse_marine_forecast.py → ~/site/data/marine_forecast.json
```

### Sarracenia (sr3) - XML File Subscriber

**Critical infrastructure:** sr3 subscribes to Environment Canada's AMQP broker and automatically downloads XML files for buoy observations and marine forecasts. **Two separate subscriptions run continuously:**

1. **Buoy observations** (`bc_buoys.conf`) - Downloads SWOB-ML XMLs every hour for 4 buoys
2. **Marine forecasts** (`marine_forecast.conf`) - Downloads marine weather forecast XMLs 2-4 times daily

#### Buoy Observation Subscription

**Configuration:** `~/.config/sr3/subscribe/bc_buoys.conf`

```conf
broker amqps://dd.weather.gc.ca
topicPrefix v02.post

directory /home/keelando/envcan_wave/data/buoy

instances 1
batch 50
logLevel info

# Subscribe to specific buoy IDs
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600146.#  # Halibut Bank
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600303.#  # Southern Strait
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600304.#  # English Bay
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600131.#  # Sentry Shoal
```

**How it works:**
1. Connects to Environment Canada's public AMQP broker (no authentication required)
2. Subscribes to SWOB-ML (Standard Weather Observation - Markup Language) topics for specified buoys
3. Downloads XML files to `~/envcan_wave/data/buoy/` as they're published (approximately hourly)
4. Runs as a long-lived daemon process via `sr3 foreground subscribe/bc_buoys`

**Commands:**
```bash
# Check if sr3 is running
ps aux | grep sr3

# View recent downloads
ls -lth ~/envcan_wave/data/buoy/*.xml | head

# Monitor sr3 logs
tail -f ~/.cache/sr3/log/subscribe_bc_buoys_*.log

# Start sr3 manually (if not running)
source .venv/bin/activate
sr3 start subscribe/bc_buoys  # starts as daemon
# OR
sr3 foreground subscribe/bc_buoys  # runs in foreground

# Stop sr3
sr3 stop subscribe/bc_buoys

# Check sr3 status
sr3 status
```

**Log location:** `~/.cache/sr3/log/`

**Note:** Without sr3 running, no new XML files are downloaded and `buoy_to_influx_sqlite.py` will only parse existing files. The sr3 process typically runs continuously as a daemon and should be monitored for unexpected stops.

#### Marine Forecast Subscription

**Configuration:** `~/.config/sr3/subscribe/marine_forecast.conf`

```conf
broker amqps://dd.weather.gc.ca
topicPrefix v02.post

directory /home/keelando/envcan_wave/data/marine_forecast

instances 1
batch 50
logLevel info

# Marine weather forecasts for Strait of Georgia
# m0000028 covers BOTH north and south of Nanaimo zones
subtopic *.WXO-DD.marine_weather.*.*.m0000028.#
```

**What it downloads:**
- Marine weather forecast XMLs for Strait of Georgia (covers both north and south zones)
- Updates 2-4 times daily (typically at 05h, 11h, 18h UTC)
- Files include warnings, wind/weather forecasts, and extended outlook

**Zones covered in m0000028:**
- **Strait of Georgia - north of Nanaimo** (internal key: `strait_georgia_north`)
- **Strait of Georgia - south of Nanaimo** (internal key: `strait_georgia_south`)

**Commands:**
```bash
# Check marine forecast subscription status
sr3 status | grep marine_forecast

# View recent marine forecast downloads
ls -lth ~/envcan_wave/data/marine_forecast/*.xml | head

# Monitor marine forecast logs
tail -f ~/.cache/sr3/log/subscribe_marine_forecast_*.log

# Start/stop marine forecast subscription
sr3 start subscribe/marine_forecast
sr3 stop subscribe/marine_forecast
```

**Log location:** `~/.cache/sr3/log/subscribe_marine_forecast_*.log`

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

## Marine Weather Forecasts (NEW - 2025-11-04)

**Source:** Environment Canada Marine Weather Forecasts (via Sarracenia/AMQP)

Real-time marine weather forecasts for Strait of Georgia zones, including wind warnings, detailed wind/weather forecasts, and extended outlook.

### Monitored Zones

| Zone | Internal Key | Coverage | Nearby Buoys |
|------|--------------|----------|--------------|
| Strait of Georgia - north of Nanaimo | `strait_georgia_north` | North of Nanaimo to Campbell River | Sentry Shoal (4600131) |
| Strait of Georgia - south of Nanaimo | `strait_georgia_south` | Vancouver to Nanaimo | Halibut Bank (4600146)<br>English Bay (4600304)<br>Southern Strait (4600303) |

**Note:** Both zones are contained in a single XML file (`m0000028_en.xml`) from Environment Canada.

### Data Structure

**Output file:** `~/site/data/marine_forecast.json`

```json
{
  "file": "20251104T182841.418Z_MSC_MarineWeather_m0000028_en.xml",
  "generated_utc": "2025-11-04T18:25:00+00:00",
  "region": "Pacific Coast",
  "sub_region": "Georgia Basin",
  "area": "Strait of Georgia",
  "locations": {
    "strait_georgia_north": {
      "zone_name": "Strait of Georgia - north of Nanaimo",
      "warnings": [
        {
          "location": "...",
          "type": "Gale warning",
          "status": "IN EFFECT",
          "category": "marine",
          "issued_utc": "2025-11-04T18:30:00+00:00"
        }
      ],
      "issued_utc": "2025-11-04T18:30:00+00:00",
      "forecast": {
        "period": "Today Tonight and Wednesday.",
        "wind": "Wind southeast 10 to 15 knots...",
        "weather": "Showers beginning this evening..."
      }
    },
    "strait_georgia_south": { ... }
  },
  "extended_forecast": [
    {"period": "Thursday", "forecast": "Wind southeast 15 to 25 knots..."},
    {"period": "Friday", "forecast": "..."},
    {"period": "Saturday", "forecast": "..."}
  ]
}
```

### Warning Types

Marine warnings by severity (wind speed):

- **Strong Wind Warning**: 20-33 knots
- **Gale Warning**: 34-47 knots
- **Storm Warning**: 48+ knots

### Update Schedule

- **Frequency**: 2-4 times daily
- **Typical update times**: 05h, 11h, 18h UTC (09:00 PM, 03:00 AM, 10:00 AM PST)
- **More frequent updates** occur when warnings are issued or conditions change rapidly

### Parser Script

**File:** `parse_marine_forecast.py`

**Functions:**
- Parses latest XML from `~/envcan_wave/data/marine_forecast/`
- Extracts warnings, regular forecast, extended forecast
- Handles both zones (north and south) from single XML
- Writes JSON directly to `~/site/data/marine_forecast.json`

**Run manually:**
```bash
source .venv/bin/activate
python3 parse_marine_forecast.py
```

**Logs:** `~/envcan_wave/marine_forecast.log`

## Development Commands

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start sr3 to begin downloading Environment Canada XML files
sr3 start subscribe/bc_buoys
```

**Important:** Ensure sr3 is running to automatically download Environment Canada buoy XML files. Without it, `buoy_to_influx_sqlite.py` will only parse existing files in `data/buoy/`.

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
- **Every 5 min**: Export tide JSON (latest, timeseries, high/low)
- **Every 30 min**: Fetch tide observations (real-time water levels)
- **Daily 12:05 AM**: Fetch tide predictions (48-hour astronomical forecasts)
- **Twice daily (12:10 AM & 12:10 PM)**: Fetch tide high/low events (48-hour extrema, redundancy)

**Marine forecasts:**
- **Every 30 min**: Parse marine forecast XMLs (updated 2-4x daily by sr3)

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

### Browser Cache Busting (IMPORTANT!)

**Problem:** Even though Caddy is configured with `Cache-Control: no-store, no-cache`, browsers (especially Firefox) can still aggressively cache CSS and JS files, causing stale styles/behavior to persist after updates.

**Solution:** CSS files are versioned in their filenames (e.g., `style-v3.css`) to force browsers to treat them as completely new resources.

**Current Implementation (as of 2025-11-03):**

**CSS files:**
- `~/site/assets/css/style-v3.css` - Main site styles
- `~/site/assets/css/nav-tide-styles-v3.css` - Navigation and tide page styles
- `~/site/assets/css/stations-map-v3.css` - Map component styles

**HTML references:**
```html
<!-- index.html and tides.html -->
<link rel="stylesheet" href="/assets/css/style-v3.css" />
<link rel="stylesheet" href="/assets/css/nav-tide-styles-v3.css" />
<link rel="stylesheet" href="/assets/css/stations-map-v3.css" />
```

**When to increment versions:**
- After making CSS changes that don't appear in browser
- When users report visual inconsistencies vs expected behavior
- After any significant UI/UX changes

**How to bust cache:**
1. Rename CSS files: `mv style-v3.css style-v4.css` (increment version number)
2. Update HTML files (`index.html`, `tides.html`) to reference new filenames
3. Optional: Delete old versioned files once confirmed working

**Example workflow:**
```bash
cd ~/site/assets/css
mv style-v3.css style-v4.css
mv nav-tide-styles-v3.css nav-tide-styles-v4.css
mv stations-map-v3.css stations-map-v4.css

# Then update both HTML files to reference v4 instead of v3
```

**Why this matters:**
- Saves hours of debugging "phantom" issues caused by stale CSS
- Prevents user confusion when site appears broken due to mixed old/new assets
- Much more reliable than query parameters or asking users to hard-refresh (Ctrl+Shift+R)
- Browser sees a completely new file path = guaranteed cache bust

**Note:** JS files don't currently use versioning since they're loaded dynamically and cache less aggressively, but can be versioned the same way if needed.

## Adding a New Buoy

1. **If adding an Environment Canada buoy**, update sr3 subscription config:
   - Edit `~/.config/sr3/subscribe/bc_buoys.conf`
   - Add new `subtopic` line with the buoy ID:
     ```conf
     subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.BUOY_ID.#
     ```
   - Restart sr3: `sr3 stop subscribe/bc_buoys && sr3 start subscribe/bc_buoys`

2. Add to `BUOYS` dictionary in all scripts that reference it:
   - `buoy_to_influx_sqlite.py` (if EC source)
   - `fetch_noaa_buoy.py` (if NOAA source)
   - `sqlite_to_json.py`
   - `export_24hr_timeseries.py`
   - `influx_to_mqtt.py`

3. If new field mappings are needed, update `FIELD_MAP_*` dictionaries in relevant scripts

4. Update frontend JavaScript (not in this repo):
   - `~/site/assets/js/main.js` - buoy display order
   - `~/site/assets/js/charts.js` - chart configurations

5. Test the pipeline:
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

### parse_marine_forecast.py
- Parses Environment Canada marine forecast XMLs from `~/envcan_wave/data/marine_forecast/`
- Downloads provided by sr3 subscription (`subscribe/marine_forecast`)
- Extracts forecast data for both Strait of Georgia zones (north and south) from single XML
- **Data extracted**:
  - Warnings (Gale, Strong Wind, Storm) with status and issued times
  - Regular forecast (Today/Tonight/Tomorrow) - wind, weather, period
  - Extended forecast (named days: Thursday, Friday, Saturday)
  - Wave forecasts (if present in XML)
- Writes directly to `~/site/data/marine_forecast.json` for website consumption
- Uses zone mapping to normalize location names to internal keys
- Runs every 30 minutes via cron (forecasts update 2-4x daily)

## Important Data Handling Notes

### NOAA Pressure Field
- Valid pressure values can be around 999 hPa (e.g., low-pressure systems)
- Do NOT treat 999 as a missing data indicator
- Only `MM`, `M`, `NA`, empty strings are missing indicators

### Spectral Wave Data (NOAA)
- **Swell** (SwH/SwP/SwD): Long-period ocean waves from distant storms
- **Wind waves** (WWH/WWP/WWD): Short-period locally-generated waves
- Only available for stations with spectral buoys (46087, 46088)

### Meteorological Direction Convention
- **Wind and wave directions indicate WHERE they are COMING FROM, not where they're going**
- Example: "West wind" (270°) = wind blowing FROM the west TO the east
- Example: "Northwest waves" (315°) = waves coming FROM the northwest TO the southeast
- **Frontend display arrows** (`~/site/assets/js/main.js:getDirectionalArrow()`):
  - Wind arrow (↓): rotation = `degrees` (arrow points in the direction wind is blowing TO)
  - Wave arrow (➤): rotation = `degrees + 90` (compensates for arrow naturally pointing east)
- This convention applies to all directional data: wind_direction, wave_direction_peak, swell_direction, wind_wave_direction

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
- **Fetch optimization**: Only observations fetched frequently (every 30 min), predictions fetched once daily, high/low fetched twice daily for redundancy
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

### UI/UX Enhancements (2025-11-02)

**Directional Arrows on Buoy Cards:**
- Added visual directional arrows for all wind and wave directions
- Helper function `getDirectionalArrow(degrees, arrowType)` in `main.js`
- Wind uses `↓` arrow, waves use `➤` arrow
- CSS transforms rotate arrows to match actual direction (e.g., 270° = west)
- Styling in `style-v2.css` with `.direction-arrow` class

**Navigation Links (Card → Map/Charts):**
- Each buoy card now has two navigation buttons:
  - `📍 View Location` - Scrolls to map and centers on selected buoy
  - `📊 View Charts` - Scrolls to charts section and selects that buoy
- Functions in `main.js`:
  - `scrollToMap(buoyId)` - Smooth scroll + map centering + popup
  - `scrollToCharts(buoyId)` - Smooth scroll to buoy selector dropdown + auto-select
- Map integration in `stations-map.js`:
  - `centerMapOnBuoy(buoyId)` - Centers map with animation, opens marker popup
  - Global function accessible via `window.centerMapOnBuoy`
  - Stores buoy markers in `buoyMarkers{}` object for easy lookup
- Pulse animation provides visual feedback when scrolling
- Chart button disabled if no data available (grayed out)
- Button styling in `style-v2.css` with `.buoy-nav-links` classes

**Tide Page Improvements:**
- Reduced excessive padding by ~38-50% across all components:
  - Tide selector: 2rem → 1rem
  - Card padding: 2rem → 1.25rem
  - Data groups: 2rem → 1.25rem margins/padding
  - Tide values: 1rem → 0.5rem padding
- Added station metadata display below station name:
  - Color-coded badge: Green for permanent stations, orange for prediction-only
  - DFO station code (e.g., "07795")
  - Precise coordinates (e.g., "49.3375°N, 123.2536°W")
  - Descriptive location (e.g., "West Vancouver")
- Loads metadata from `stations.json` for consistency with map
- Styling in `nav-tide-styles.css` with `.station-metadata` classes
- Mobile responsive: metadata items stack vertically on small screens

**Wave Breaking Threshold Annotations:**
- Added explanatory note below wave comparison chart
- Explains the two reference lines:
  - 0.7m (orange) - Small wind-driven waves may begin to break on exposed sandy beaches
  - 1.2m (red) - Moderate waves begin breaking on exposed sandy beaches
- Styled info box in `index.html` matches site design language

**Files Modified:**
- `~/site/assets/js/main.js` - Arrows, navigation functions, chart scroll fix
- `~/site/assets/js/stations-map.js` - Map centering function, marker storage
- `~/site/assets/js/tides.js` - Metadata display, stations.json integration
- `~/site/assets/css/style-v2.css` - Arrow styles, nav buttons, pulse animation
- `~/site/assets/css/stations-map.css` - Enhanced marker styles (for future use)
- `~/site/assets/css/nav-tide-styles.css` - Reduced padding, metadata styles
- `~/site/tides.html` - Added metadata container div
- `~/site/index.html` - Wave threshold explanation, scroll alignment fix

### Marine Forecasts Page & Warning Banners (2025-11-04)

**Major new feature:** Integrated Environment Canada marine weather forecasts with dismissible warning banners across all pages.

#### Forecasts Page (~/site/forecasts.html)

**New dedicated page** displaying marine weather forecasts for Strait of Georgia zones.

**URL:** `/forecasts.html`

**Features:**
- Displays both forecast zones (north and south of Nanaimo)
- Warning cards with severity-based styling (Storm/Gale/Strong Wind)
- Current forecast (Today/Tonight/Tomorrow) with wind and weather details
- Extended forecast (Thursday, Friday, Saturday) in responsive grid
- Wave forecast (when present in data)
- Auto-refresh every 5 minutes
- Smooth scroll to zone sections via URL hash (`#strait_georgia_north`)
- Zone highlight effect (blue glow for 2 seconds) when navigating from warnings

**JavaScript:** `~/site/assets/js/forecasts.js` (7.0 KB)
- `loadForecasts()` - Fetches marine_forecast.json and renders all zones
- `displayForecasts()` - Builds zone cards with warnings, forecast, extended outlook
- `renderZoneForecast()` - Creates HTML for individual zone
- `renderExtendedForecast()` - Renders 3-day outlook in grid layout
- `scrollToZoneIfNeeded()` - Smooth scrolls to zone anchor on page load
- `formatTimestamp()` - Converts UTC to Pacific time for display

**Data source:** `~/site/data/marine_forecast.json` (updated every 30 min by backend)

#### Warning Banner System

**Dismissible warning banners** appear at top of all pages (Buoys, Tides, Forecasts) when marine warnings are active.

**Features:**
- ✅ Severity-based color coding (Storm=red, Gale=orange, Strong Wind=amber)
- ✅ Click X to dismiss for 24 hours
- ✅ Dismissal persists across all pages (localStorage)
- ✅ Auto-expires after 24 hours
- ✅ Smooth fade-out animation when dismissed
- ✅ "View Forecast →" link scrolls to relevant zone on forecasts page
- ✅ Mobile-optimized compact layout (50% smaller on mobile)
- ✅ Automatic sorting by severity

**JavaScript:** `~/site/assets/js/warning-banner.js` (4.3 KB)
- `displayWarningBanners()` - Fetches forecast data, filters dismissed, renders banners
- `dismissWarning(warningId)` - Saves to localStorage, fades out banner
- `isWarningDismissed(warningId)` - Checks localStorage, auto-cleans expired
- `getWarningId(warning)` - Generates unique ID: `{zone}_{type}_{issued_utc}`
- `collectActiveWarnings()` - Extracts warnings from forecast data, sorts by severity
- `createWarningBanner()` - Builds HTML with zone-specific forecast link

**CSS:** `~/site/assets/css/warning-banner-v3.css` (3.4 KB)
- Severity classes: `.warning-storm`, `.warning-gale`, `.warning-strong-wind`
- Gradient backgrounds with white text
- Dismiss button styling (absolute positioned on mobile)
- Mobile responsive: 50% height reduction on small screens
- Subtle pulse animation
- Smooth transitions

**State Management (localStorage):**
- **Key:** `dismissed_marine_warnings`
- **Value:** JSON object mapping warning IDs to dismiss timestamps
- **Expiry:** 24 hours (auto-deleted by code)
- **Scope:** Per-browser, per-device (not synced)
- **Privacy:** Client-side only, never sent to server

**Example localStorage data:**
```json
{
  "strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00": 1730747282341,
  "strait_georgia_south_Strong wind warning_2025-11-04T18:30:00+00:00": 1730750123456
}
```

#### Navigation Updates

**All pages now have 3-tab navigation:**
```
[Buoys] [Tides] [Forecasts]
```

**Updated files:**
- `~/site/index.html` - Added Forecasts link, warning banner container
- `~/site/tides.html` - Added Forecasts link, warning banner container
- `~/site/forecasts.html` - Full navigation with active state

#### Mobile UX Improvements

**Warning banners optimized for mobile:**
- Desktop: Full-size with clear spacing
- Tablet (≤768px): 33% less padding, smaller fonts
- Small Mobile (≤480px): 47% less padding, ultra-compact layout

**Space savings:**
- Before: ~80-90px height on mobile
- After: ~40-50px height (50% reduction)

**Responsive adjustments:**
- Padding: `0.75rem → 0.4rem` (mobile)
- Font: `0.95rem → 0.75rem` (mobile)
- Dismiss button: Positioned absolutely in corner
- Link text: Shortened on small screens
- Tight line-height for compact display

#### Scroll-to-Zone Functionality

**Smart navigation from warning banners to forecasts:**
- Warning links include zone anchor: `/forecasts.html#strait_georgia_north`
- Forecasts page auto-scrolls to zone on load
- Blue highlight effect (2 seconds) shows where you landed
- Smooth scroll behavior (`scroll-behavior: smooth`)

**User flow:**
1. User sees warning banner on any page
2. Clicks "View Forecast →"
3. Navigates to forecasts.html
4. Smoothly scrolls to relevant zone section
5. Section briefly highlighted with blue glow

#### Files Created (5)

**Frontend:**
- `~/site/forecasts.html` (5.2 KB) - Forecasts page
- `~/site/assets/js/forecasts.js` (7.0 KB) - Forecast rendering logic
- `~/site/assets/js/warning-banner.js` (4.3 KB) - Warning banner module
- `~/site/assets/css/warning-banner-v3.css` (3.4 KB) - Warning styles

**Documentation:**
- `~/site/FRAMEWORK_DISCUSSION.md` - Framework evaluation (Alpine.js, HTMX, SvelteKit)
- `~/site/BROWSER_STATE_EXPLAINED.md` - Complete localStorage guide
- `~/site/STATE_QUICK_REFERENCE.md` - Quick reference card
- `~/site/DISMISSIBLE_WARNINGS_SUMMARY.md` - Technical implementation details
- `~/site/MOBILE_UX_IMPROVEMENTS.md` - Mobile optimization summary

#### Framework Decision (2025-11-04)

**Evaluated frameworks for state management and component reuse:**
- **Alpine.js** - Lightweight reactivity (15 KB)
- **HTMX** - HTML-over-wire, solves duplication
- **SvelteKit/Astro** - Full component framework

**Decision:** Stay vanilla JavaScript + localStorage
- Current size: 3 pages (may grow to 5-7)
- Simple static site deployment
- No build step needed
- Easy to maintain
- Fast and lightweight

**Revisit when:**
- Site grows to 10+ pages
- Need complex user interactions
- Want component-based architecture

**See:** `~/site/FRAMEWORK_DISCUSSION.md` for complete evaluation

#### Data Flow: Warnings

```
Environment Canada Marine Forecast (2-4x daily)
    ↓ (via sr3 AMQP subscription)
XML download (m0000028_en.xml)
    ↓ (every 30 min)
parse_marine_forecast.py
    ↓
~/site/data/marine_forecast.json
    ↓ (page load)
warning-banner.js + forecasts.js
    ↓
User sees warnings/forecasts
    ↓ (user clicks X)
localStorage (24-hour expiry)
    ↓ (subsequent page loads)
Warnings stay dismissed across all pages
```

#### Key Technical Details

**Warning ID Format:**
```javascript
`${zone_key}_${warning_type}_${issued_utc}`
// Example: "strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00"
```

**Why this format?**
- Different zones = different warnings
- Same zone can have multiple types
- New warning issued = new ID (reappears even if old one dismissed)

**24-Hour Expiry Logic:**
```javascript
const DISMISS_DURATION_MS = 24 * 60 * 60 * 1000;
const elapsed = Date.now() - dismissedTime;
if (elapsed > DISMISS_DURATION_MS) {
  // Expired - show warning again
}
```

**Browser Compatibility:**
- localStorage API (all modern browsers)
- CSS flexbox, gradients, transitions
- ES6 JavaScript (template literals, arrow functions)
- Smooth scroll CSS property

#### Testing & Debugging

**View localStorage (Browser DevTools):**
1. F12 → Application tab (Chrome) or Storage tab (Firefox)
2. Local Storage → halibutbank.ca
3. See `dismissed_marine_warnings`

**Test dismissal:**
```javascript
// Browser console
localStorage.getItem('dismissed_marine_warnings')
localStorage.removeItem('dismissed_marine_warnings') // Clear
```

**Simulate 24-hour expiry:**
```javascript
let d = JSON.parse(localStorage.getItem('dismissed_marine_warnings'));
d[Object.keys(d)[0]] = Date.now() - (25 * 60 * 60 * 1000);
localStorage.setItem('dismissed_marine_warnings', JSON.stringify(d));
// Refresh - warning reappears
```
