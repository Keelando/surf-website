# Detailed Architecture

Technical documentation for database schemas, script responsibilities, and data flow.

## Database Schemas

### Buoy Database

**Location:** `~/.local/share/buoy_data.sqlite`

#### Table: `buoy_observation`

```sql
CREATE TABLE buoy_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buoy_id TEXT NOT NULL,
    observation_time INTEGER NOT NULL,  -- Unix timestamp (UTC)

    -- Wave metrics (Environment Canada + NOAA)
    wave_height_sig REAL,         -- Significant wave height (m)
    wave_height_peak REAL,        -- Peak wave height (m)
    wave_period_sig REAL,         -- Significant wave period (s)
    wave_period_avg REAL,         -- Average wave period (s)
    wave_period_peak REAL,        -- Peak wave period (s)
    wave_direction_avg REAL,      -- Average wave direction (degrees, coming FROM)
    wave_direction_peak REAL,     -- Peak wave direction (degrees, coming FROM)

    -- NOAA spectral wave data (Neah Bay, New Dungeness only)
    swell_height REAL,            -- Swell height (m)
    swell_period REAL,            -- Swell period (s)
    swell_direction REAL,         -- Swell direction (degrees, coming FROM)
    wind_wave_height REAL,        -- Wind wave height (m)
    wind_wave_period REAL,        -- Wind wave period (s)
    wind_wave_direction REAL,     -- Wind wave direction (degrees, coming FROM)

    -- Meteorological data
    wind_speed REAL,              -- Wind speed (km/h, stored internally)
    wind_gust REAL,               -- Wind gust (km/h)
    wind_direction REAL,          -- Wind direction (degrees, coming FROM)
    air_temp REAL,                -- Air temperature (°C)
    sea_temp REAL,                -- Sea surface temperature (°C)
    pressure REAL,                -- Atmospheric pressure (hPa)

    -- Metadata
    source_file TEXT,             -- Source XML filename
    recorded_at TEXT DEFAULT (datetime('now'))  -- When record was inserted
);

-- Index for uniqueness: one observation per buoy per timestamp
CREATE UNIQUE INDEX uniq_buoy_ts ON buoy_observation(buoy_id, observation_time);

-- Indexes for efficient queries
CREATE INDEX idx_buoy_time ON buoy_observation(buoy_id, observation_time DESC);
CREATE INDEX idx_observation_time ON buoy_observation(observation_time DESC);
```

**Design notes:**
- All wave heights in meters
- All temperatures in Celsius
- Wind speed stored as km/h internally, converted to knots on export
- Direction values: 0-360°, where value = direction wind/waves are coming FROM
- NULL values indicate missing or unavailable data

---

### Wind Database

**Location:** `~/.local/share/wind_data.sqlite`

#### Table: `wind_observation`

Real-time wind and weather observations from Environment Canada land-based weather stations.

```sql
CREATE TABLE wind_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,           -- ICAO/TC code (e.g., 'CWSB', 'CYVR')
    station_name TEXT,                  -- Friendly name (e.g., 'Point Atkinson')
    observation_time INTEGER NOT NULL,  -- Unix timestamp (UTC)

    -- Wind metrics (10-minute averages)
    wind_speed_kmh REAL,               -- Average wind speed (km/h)
    wind_gust_kmh REAL,                -- Maximum gust (km/h)
    wind_direction_deg INTEGER,        -- Wind direction (degrees, coming FROM)

    -- Atmospheric conditions
    air_temp_c REAL,                   -- Air temperature (°C)
    pressure_hpa REAL,                 -- Station pressure (hPa)
    pressure_mslp_hpa REAL,            -- Mean sea level pressure (hPa)

    -- Additional meteorology
    humidity_percent REAL,             -- Relative humidity (%)
    dewpoint_c REAL,                   -- Dewpoint temperature (°C)
    visibility_km REAL,                -- Visibility (km)
    rainfall_1hr_mm REAL,              -- Rainfall past 1 hour (mm)
    rainfall_6hr_mm REAL,              -- Rainfall past 6 hours (mm)

    -- Metadata
    source_file TEXT,                  -- Source SWOB-ML filename
    recorded_at TEXT DEFAULT (datetime('now'))
);

-- Index for efficient "latest by station" queries
CREATE INDEX idx_wind_station_time ON wind_observation(station_id, observation_time DESC);

-- De-duplication: one observation per station per timestamp
CREATE UNIQUE INDEX uniq_wind_station_ts ON wind_observation(station_id, observation_time);
```

**Data source:** Environment Canada SWOB-ML XMLs via sr3 Sarracenia

**Update frequency:** Every 10 minutes by Environment Canada, parsed every minute by system

**Design notes:**
- Wind speed stored as km/h internally, converted to knots on export
- Direction values: 0-360°, where value = direction wind is coming FROM
- MSNG (missing) values in XMLs → NULL in database
- Schema auto-expands to add new columns as needed (future-proof)

**Monitored EC stations** — see the `wind` section of `config/stations.json`
for the authoritative list (it also carries the non-EC stations: Jericho,
White Rock, the NOAA land sites, and Colebrook):
- CWGT (Sisters Island)
- CWGB (Ballenas)
- CWEL (Entrance Island)
- CWSB (Point Atkinson)
- CVTF (Tsawwassen)
- CWVF (Sand Heads)
- CWEZ (Saturna Island)
- CWQK (Race Rocks)
- CWAS (Pam Rocks)
- CYVR (YVR Airport)
- CZBB (Boundary Bay Airport)
- CYAZ (Tofino Airport)
- CWZO (Kelp Reefs — wind only)
- CWDR (Discovery Island)
- CWLM (Victoria Gonzales — 65 m hilltop)

---

### Tide Database

**Location:** `~/.local/share/tide_data.sqlite`

#### Table: `tide_observation`

Real-time water level observations from DFO sensors.

```sql
CREATE TABLE tide_observation (
    station_id TEXT NOT NULL,           -- DFO API station ID (e.g., '5cebf1de3d0f4a073c4bb94c')
    station_name TEXT NOT NULL,         -- Internal key (e.g., 'point_atkinson')
    observation_time INTEGER NOT NULL,  -- Unix timestamp (UTC)
    water_level REAL,                   -- Water level (meters relative to chart datum)
    quality TEXT,                       -- QC flag code from DFO (e.g., '1' = good)
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, observation_time)
);

-- Index for efficient time-range queries
CREATE INDEX idx_tide_obs_time ON tide_observation(station_name, observation_time DESC);
```

**Update frequency:** Every 6 minutes by DFO, fetched every 30 min by system

#### Table: `tide_prediction`

Astronomical tide predictions (harmonic analysis).

```sql
CREATE TABLE tide_prediction (
    station_id TEXT NOT NULL,           -- DFO API station ID
    station_name TEXT NOT NULL,         -- Internal key
    prediction_time INTEGER NOT NULL,   -- Unix timestamp (UTC)
    water_level REAL,                   -- Predicted water level (meters)
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, prediction_time)
);

-- Index for efficient time-range queries
CREATE INDEX idx_tide_pred_time ON tide_prediction(station_name, prediction_time DESC);
```

**Update frequency:** Static calculations, fetched once daily (48-hour rolling window)

**Data interval:** 1-minute predictions from DFO API

#### Table: `tide_highlow`

High and low tide events (extrema).

```sql
CREATE TABLE tide_highlow (
    station_id TEXT NOT NULL,           -- DFO API station ID
    station_name TEXT NOT NULL,         -- Internal key
    event_time INTEGER NOT NULL,        -- Unix timestamp (UTC) of high/low
    water_level REAL,                   -- Water level at extremum (meters)
    event_type TEXT,                    -- 'high' or 'low'
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, event_time)
);

-- Index for event type queries
CREATE INDEX idx_tide_highlow_type ON tide_highlow(station_name, event_time DESC, event_type);
```

**Update frequency:** Static calculations, fetched twice daily (redundancy)

**Event type detection:** Script compares wlp-hilo values with neighboring predictions to classify as high vs low

---

## Script Responsibilities

### buoy_to_influx_sqlite.py

**Purpose:** Parse Environment Canada SWOB-ML XML files

**Data source:** XMLs in `~/envcan_wave/data/buoy/` (downloaded by sr3)

**Processing:**
1. Find all `.xml` files in data directory
2. Parse each XML using `ElementTree`
3. Extract wave and meteorological observations
4. Map EC field names to database columns
5. Insert into SQLite (with deduplication via unique index)
6. Optionally write to InfluxDB (soft dependency)
7. Log processing statistics

**Field mappings (EC → Database):**
- `avg_wave_hgt_10min` → `wave_height_sig`
- `max_wave_hgt_10min` → `wave_height_peak`
- `avg_wave_per_10min` → `wave_period_avg`
- `dom_wave_per_10min` → `wave_period_peak`
- `wind_spd_avg_10min` → `wind_speed`
- `air_temp` → `air_temp`
- `sea_surface_temp` → `sea_temp`
- `stn_pres` → `pressure`

**Frequency:** Every minute (cron)

**Output:** Records in `buoy_observation` table

**Logs:** `~/envcan_wave/buoy_sqlite.log`

---

### fetch_noaa_buoy.py

**Purpose:** Fetch NOAA NDBC 5-day historical data

**Data sources:**
- `https://www.ndbc.noaa.gov/data/realtime2/[BUOY].txt` - Meteorological data
- `https://www.ndbc.noaa.gov/data/realtime2/[BUOY].spec` - Spectral wave data

**Processing:**
1. For each NOAA buoy (46087, 46088):
2. Download `.txt` file (met data)
3. Download `.spec` file (spectral data)
4. Parse both files (space-delimited)
5. Merge by timestamp
6. Handle missing data indicators (`MM`, `M`, `NA`)
7. Convert units: m/s → km/h for wind
8. Parse cardinal directions ('WSW') → degrees
9. Insert into SQLite with deduplication

**Unit conversions:**
- Wind speed: m/s × 3.6 = km/h
- Wave heights: Already in meters
- Pressure: Already in hPa

**Missing data handling:**
- `MM`, `M`, `NA`, empty string → NULL
- 999 for pressure is VALID (low-pressure systems)

**Frequency:** Every 20 minutes (at :05, :25, :45)

**Output:** Records in `buoy_observation` table

**Logs:** `~/envcan_wave/noaa.log`

---

### wind_to_sqlite.py

**Purpose:** Parse Environment Canada SWOB-ML XML files for wind stations

**Data source:** XMLs in `~/envcan_wave/data/wind/` (downloaded by sr3)

**Processing:**
1. Find all `.xml` files in wind data directory
2. Parse each XML using `ElementTree`
3. Extract station ID from XML content (not filename)
4. Extract wind and meteorological observations
5. Map EC SWOB-ML field names to database columns
6. Insert into SQLite (with deduplication via unique index)
7. Optionally write to InfluxDB (soft dependency)
8. Log processing statistics

**Field mappings (EC SWOB-ML → Database):**
- `avg_wnd_spd_pst10mts` → `wind_speed_kmh`
- `max_avg_wnd_spd_pst10mts` → `wind_gust_kmh`
- `avg_wnd_dir_pst10mts` → `wind_direction_deg`
- `avg_air_temp_pst10mts` → `air_temp_c`
- `avg_stn_pres_pst10mts` → `pressure_hpa`
- `avg_mslp_pst10mts` → `pressure_mslp_hpa`
- `avg_rel_hum_pst10mts` → `humidity_percent`
- `avg_dwpt_temp_pst10mts` → `dewpoint_c`
- `avg_vis_pst10mts` → `visibility_km`
- `pcpn_amt_pst1hr` → `rainfall_1hr_mm`
- `pcpn_amt_pst6hrs` → `rainfall_6hr_mm`

**Frequency:** Every minute (cron)

**Output:** Records in `wind_observation` table

**Logs:** `~/envcan_wave/wind_parser.log`

**Note:** Station ID parsing extracts from `tc_id` element in XML, not filename (filenames contain timestamps/locations that don't match station IDs)

---

### export_wind_json.py

**Purpose:** Export latest wind station snapshot for website

**Processing:**
1. For each wind station:
2. Query latest non-null value for each field (within 2-hour freshness window)
3. Convert wind speed: km/h → knots (÷ 1.852)
4. Add cardinal direction labels for wind directions
5. Mark data as stale if > 2 hours old
6. Format timestamps as ISO 8601
7. Build JSON object with all stations
8. Write atomically to avoid partial writes

**Freshness window:** 2 hours
- Each field independently queries for most recent non-null value
- Data marked as stale if observation_time > 2 hours ago

**Query pattern per field:**
```sql
SELECT field_name
FROM wind_observation
WHERE station_id = ?
  AND field_name IS NOT NULL
  AND observation_time >= strftime('%s', 'now', '-2 hours')
ORDER BY observation_time DESC
LIMIT 1
```

**Frequency:** Every 5 minutes (cron)

**Output:** `site/data/latest_wind.json`

**JSON structure:**
```json
{
  "CWSB": {
    "station_id": "CWSB",
    "station_name": "Point Atkinson",
    "observation_time": "2025-11-20T19:00:00Z",
    "wind_speed_kt": 12.5,
    "wind_gust_kt": 18.2,
    "wind_direction_deg": 270,
    "wind_direction_cardinal": "W",
    "air_temp_c": 10.5,
    "pressure_hpa": 1013.2,
    "stale": false,
    ...
  },
  ...
}
```

**Logs:** `~/envcan_wave/wind_export.log`

---

### export_wind_24hr_timeseries.py

**Purpose:** Generate 24-hour rolling wind timeseries for charts

**Processing:**
1. For each wind station:
2. Query all observations from last 24 hours
3. Extract fields: time, wind speed, wind gust, wind direction, temperature, pressure
4. Convert wind speeds to knots
5. Format as array of objects suitable for ECharts
6. Write separate JSON file with all stations' data

**Frequency:** Every 10 minutes (cron)

**Output:** `site/data/wind_timeseries_24hr.json`

**JSON structure:**
```json
{
  "CWSB": {
    "station_id": "CWSB",
    "station_name": "Point Atkinson",
    "data": {
      "wind_speed_kt": [
        {"time": "2025-11-20T19:00:00Z", "value": 12.5},
        ...
      ],
      "wind_gust_kt": [...],
      "wind_direction_deg": [...],
      "air_temp_c": [...],
      "pressure_hpa": [...]
    }
  },
  ...
}
```

**Logs:** `~/envcan_wave/wind_timeseries_export.log`

---

### sqlite_to_json.py

**Purpose:** Export latest buoy snapshot for website

**Processing:**
1. For each buoy:
2. Query latest non-null value for each field (within 2-hour freshness window)
3. Convert wind speed: km/h → knots (÷ 1.852)
4. Add cardinal direction labels for wind/wave directions
5. Format timestamps as ISO 8601
6. Build JSON object with all buoys
7. Write atomically to avoid partial writes

**Freshness window:** 2 hours
- Each field independently queries for most recent non-null value
- Example: Wave height from 1h ago + wind speed from 30min ago = both valid

**Query pattern per field:**
```sql
SELECT field_name
FROM buoy_observation
WHERE buoy_id = ?
  AND field_name IS NOT NULL
  AND observation_time >= strftime('%s', 'now', '-2 hours')
ORDER BY observation_time DESC
LIMIT 1
```

**Frequency:** Every minute (cron)

**Output:** `site/data/latest_buoy_v2.json`

**JSON structure:**
```json
{
  "4600146": {
    "buoy_id": "4600146",
    "buoy_name": "Halibut Bank",
    "observation_time": "2025-11-05T19:00:00Z",
    "wave_height_sig": 1.2,
    "wind_speed": 15.4,
    "wind_speed_knots": 8.3,
    "wind_direction": 270,
    "wind_direction_cardinal": "W",
    ...
  },
  ...
}
```

**Logs:** `~/envcan_wave/buoy_sqlite.log` (shared with buoy_to_influx_sqlite.py)

---

### export_24hr_timeseries.py

**Purpose:** Generate 24-hour rolling timeseries for charts

**Processing:**
1. For each buoy:
2. Query all observations from last 24 hours
3. Extract fields: time, wave height, wind speed, wave period, direction, temps
4. Format as array of objects suitable for ECharts
5. Write separate JSON file per buoy

**Frequency:** Every 5 minutes (cron)

**Output:** `site/data/timeseries_[BUOY_ID].json`

**JSON structure:**
```json
{
  "buoy_id": "46087",
  "buoy_name": "Neah Bay",
  "data": [
    {
      "time": "2025-11-05T19:00:00Z",
      "wave_height_sig": 1.2,
      "wind_speed_knots": 8.3,
      "wave_period_avg": 6.5,
      "air_temp": 12.3,
      "sea_temp": 11.8
    },
    ...
  ]
}
```

**Logs:** `~/envcan_wave/timeseries.log`

---

### influx_to_mqtt.py

**Purpose:** Publish buoy data to MQTT for Home Assistant

**Processing:**
1. Query InfluxDB (or fall back to SQLite) for latest readings
2. For each buoy and each metric:
3. Send Home Assistant MQTT discovery message
4. Send state update message
5. Handle connection errors gracefully

**MQTT topics:**
- Discovery: `homeassistant/sensor/buoy_[BUOY_ID]_[METRIC]/config`
- State: `homeassistant/sensor/buoy_[BUOY_ID]_[METRIC]/state`

**Frequency:** Every minute (cron)

**Output:** MQTT messages

**Logs:** `~/envcan_wave/mqtt.log`

---

### tide_to_sqlite.py

**Purpose:** Fetch tide data from DFO IWLS API

**Data source:** DFO API at `https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/`

**Command-line flags:**
- `--observations` - Fetch wlo (real-time water levels)
- `--predictions` - Fetch wlp (astronomical forecasts)
- `--highlow` - Fetch wlp-hilo (high/low events)
- `--all` - Fetch all three types

**Processing:**

**For observations (wlo):**
1. Query DFO API for 2-hour window (now - 1h to now + 1h)
2. Parse JSON response
3. Extract time, water level, quality flag
4. Insert into `tide_observation` table

**For predictions (wlp):**
1. Query DFO API for 48-hour window (now - 24h to now + 24h)
2. Parse JSON response (1-minute interval predictions)
3. Extract time, predicted water level
4. Insert into `tide_prediction` table

**For high/low (wlp-hilo):**
1. Query DFO API for 48-hour window
2. Parse JSON response (extrema only)
3. Determine event type by comparing with neighbors:
   - If value > neighbors → 'high'
   - If value < neighbors → 'low'
4. Insert into `tide_highlow` table

**Rate limiting:** 2.1 second delay between station requests (DFO API requirement)

**Frequency:**
- Observations: Every 30 minutes
- Predictions: Daily at 12:05 AM
- High/low: Twice daily at 12:10 AM & PM

**Output:** Records in tide database tables

**Logs:**
- `~/envcan_wave/tide_obs.log` (observations)
- `~/envcan_wave/tide_pred.log` (predictions)
- `~/envcan_wave/tide_highlow.log` (high/low)

---

### export_tide_json.py

**Purpose:** Export tide data to JSON for website

**Processing:**

**For tide-latest.json:**
1. For each station:
2. Query latest observation (if station has observations)
3. Query prediction closest to now
4. Combine into single object per station

**For tide-timeseries.json:**
1. For each station:
2. Query observations for calendar day ± 2 hours
3. Downsample observations to 15-minute intervals (:00, :15, :30, :45)
4. Query predictions for same time range
5. Convert timestamps to Pacific time
6. Export as separate arrays (observations, predictions)

**For tide-hi-low.json:**
1. For each station:
2. Query high/low events in 26-hour window (12h before to 14h after current time)
3. Sort by event time
4. Convert timestamps to Pacific time

**Atomic writes:** Uses temporary files, then renames to prevent partial writes

**Frequency:** Every 5 minutes (cron)

**Output:**
- `site/data/tide-latest.json`
- `site/data/tide-timeseries.json`
- `site/data/tide-hi-low.json`

**Logs:** `~/envcan_wave/tide_export.log`

---

### parse_marine_forecast.py

**Purpose:** Parse Environment Canada marine forecast XMLs

**Data source:** XMLs in `~/envcan_wave/data/marine_forecast/` (downloaded by sr3)

**Processing:**
1. Find latest `.xml` file in marine_forecast directory
2. Parse XML using `ElementTree`
3. Extract metadata (generated time, region, area)
4. Extract warnings for each location
5. Map location names to internal zone keys
6. Extract regular forecast (Today/Tonight/Tomorrow)
7. Extract extended forecast (named days)
8. Extract wave forecast (if present)
9. Write JSON directly to website data directory

**Zone mapping:**
```python
ZONE_MAP = {
    'Strait of Georgia - north of Nanaimo': 'strait_georgia_north',
    'Strait of Georgia - south of Nanaimo': 'strait_georgia_south'
}
```

**Warning extraction:**
- Parse `<warning>` elements
- Extract type, status, category, issued time
- Filter to "IN EFFECT" warnings only

**Frequency:** Every 30 minutes (cron)

**Output:** `site/data/marine_forecast.json`

**Logs:** `~/envcan_wave/marine_forecast.log`

---

### fetch_storm_surge.py

**Purpose:** Fetch GeoMet GDSPS storm surge forecasts

**Data source:** Environment Canada GeoMet WMS/WCS service

**Processing:**
1. Connect to GeoMet using OWSLib
2. Query storm surge layers for specific locations
3. Extract forecast data (surge height, timing)
4. Export to JSON

**Frequency:** Every 6 hours (at 1, 7, 13, 19h) - aligned with GeoMet updates

**Output:** `site/data/storm_surge.json` (if implemented)

**Logs:** `~/envcan_wave/storm_surge.log`

---

## Data Flow Diagrams

### Buoy Data Flow

```
Environment Canada AMQP Broker
         ↓ (sr3 subscription)
data/buoy/*.xml (SWOB-ML XMLs)
         ↓ (buoy_to_influx_sqlite.py, every minute)
buoy_data.sqlite (buoy_observation table)
         ↓ (sqlite_to_json.py, every minute)
latest_buoy_v2.json (latest snapshot)

NOAA NDBC HTTP Server
         ↓ (fetch_noaa_buoy.py, every 20 min)
buoy_data.sqlite (buoy_observation table)
         ↓ (export_24hr_timeseries.py, every 5 min)
timeseries_*.json (24h rolling)

buoy_data.sqlite
         ↓ (influx_to_mqtt.py, every minute)
MQTT → Home Assistant
```

### Wind Data Flow

```
Environment Canada AMQP Broker
         ↓ (sr3 subscription: bc_wind_stations)
data/wind/*.xml (SWOB-ML XMLs, every 10 min)
         ↓ (wind_to_sqlite.py, every minute)
wind_data.sqlite (wind_observation table)
         ↓ (export_wind_json.py, every 5 min)
latest_wind.json (latest snapshot)

wind_data.sqlite
         ↓ (export_wind_24hr_timeseries.py, every 10 min)
wind_timeseries_24hr.json (24h rolling)
         ↓ (browser, page load)
         ├→ winds.html (sortable table + map)
         └→ winds-charts.js (ECharts 24h timeseries)
```

### Tide Data Flow

```
DFO IWLS API (https://api-iwls.dfo-mpo.gc.ca)
         ↓ (tide_to_sqlite.py)
         ├→ --observations (every 30 min)
         ├→ --predictions (daily 12:05 AM)
         └→ --highlow (twice daily)
tide_data.sqlite (3 tables)
         ↓ (export_tide_json.py, every 5 min)
         ├→ tide-latest.json
         ├→ tide-timeseries.json
         └→ tide-hi-low.json
```

### Marine Forecast Data Flow

```
Environment Canada AMQP Broker
         ↓ (sr3 subscription)
data/marine_forecast/*.xml (Marine forecast XMLs, 2-4x daily)
         ↓ (parse_marine_forecast.py, every 30 min)
marine_forecast.json
         ↓ (browser, page load)
         ├→ warning-banner.js (dismissible warnings)
         └→ forecasts.js (forecast page)
```

### Webcam Data Flow

```
YouTube Livestreams / Direct URLs / Yawcam Servers
         ↓ (fetch_webcam.py, various intervals)
         ├→ yt-dlp + Deno (YouTube)
         ├→ HTTP GET (Direct URLs)
         └→ Yawcam API (Yawcam servers)
/mnt/storage/[webcam]_cam/[PREFIX]_[TIMESTAMP].jpg (archive)
         ↓ (atomic copy)
site/data/[webcam]/latest.jpg (website)
         ↓ (slideshow management)
site/data/[webcam]/slideshow/img_[TIMESTAMP].jpg (last 7)
         ↓
site/data/[webcam]/slideshow_manifest.json
         ↓ (browser, page load)
webcams.html (latest image + slideshow carousel)
```

**Webcam sources (5 total):**
- White Rock Pier (YouTube, 10 min, 24/7)
- White Rock East Beach (YouTube, 10 min, DISABLED)
- Cox Bay (YouTube, 15 min, daylight only)
- Mud Bay HD (Direct URL, 30 min, daylight only)
- Ambleside - Hollyburn Sailing Club (20 min, daylight only, permission-gated)

**Storage:**
- Archive: `/mnt/storage/` (external USB SATA, 223.6GB)
- Website: `site/data/[webcam]/`
- Cleanup: Automatic when disk usage > 80%

**Complete documentation:** See [WEBCAM_PIPELINE.md](WEBCAM_PIPELINE.md)

---

## Important Data Handling Notes

### Direction Convention

**Meteorological convention:** Direction indicates WHERE wind/waves are COMING FROM

- **270° (West wind):** Wind blowing FROM west TO east
- **315° (NW waves):** Waves coming FROM northwest TO southeast

**Frontend display:**
- Wind arrow (↓): `rotation = degrees` (points where wind is blowing TO)
- Wave arrow (➤): `rotation = degrees + 90` (compensates for arrow naturally pointing east)

### Unit Conversions

| Metric | Storage (DB) | Display (JSON/Frontend) |
|--------|--------------|-------------------------|
| Wind Speed | km/h | knots (÷ 1.852) |
| Wave Height | meters | meters |
| Temperature | Celsius | Celsius |
| Pressure | hPa | hPa |

**NOAA ingest conversion:** m/s × 3.6 = km/h

### Timestamp Formats

| Context | Format | Example |
|---------|--------|---------|
| SQLite storage | Unix epoch (INTEGER) | 1699200000 |
| JSON exports | ISO 8601 UTC | "2025-11-05T19:00:00Z" |
| Frontend display | Pacific time (formatted) | "Nov 5, 2025 11:00 AM PST" |

### Missing Data Indicators

**NOAA source:**
- `MM` = Missing
- `M` = Missing
- `NA` = Not Available
- Empty string = Missing
- **999 for pressure = VALID** (low-pressure systems)

**Database:** NULL = missing or unavailable

### Per-Field Freshness

Each metric independently queries for most recent non-null value within 2-hour window.

**Rationale:**
- Buoys don't always report all fields in every observation
- Different sensors may fail at different times
- Allows displaying most recent available data for each metric

**Example query:**
```sql
SELECT wave_height_sig
FROM buoy_observation
WHERE buoy_id = '46087'
  AND wave_height_sig IS NOT NULL
  AND observation_time >= strftime('%s', 'now', '-2 hours')
ORDER BY observation_time DESC
LIMIT 1;
```

### Deduplication Strategy

**Primary keys prevent duplicates:**
- Buoys: `(buoy_id, observation_time)`
- Tides: `(station_id, observation_time)` or `(station_id, prediction_time)`

**Behavior:** If duplicate timestamp arrives, INSERT fails silently (expected)

---

## Performance Considerations

### SQLite Optimizations

1. **Indexes:** Created on frequently queried columns (buoy_id, observation_time)
2. **Journal mode:** Default DELETE, consider WAL for concurrent access
3. **Vacuum:** Run periodically to reclaim space
4. **Unique indexes:** Provide both deduplication and fast lookups

### Query Patterns

**Efficient:**
```sql
-- Uses index: idx_buoy_time
SELECT * FROM buoy_observation
WHERE buoy_id = '46087'
  AND observation_time > 1699200000
ORDER BY observation_time DESC;
```

**Inefficient:**
```sql
-- Full table scan (no WHERE clause)
SELECT * FROM buoy_observation
ORDER BY observation_time DESC;
```

### Cron Job Scheduling

**Staggered execution** to avoid resource contention:
- Buoy processing: Every minute (:00)
- NOAA fetch: Every 20 min (:05, :25, :45)
- Tide observations: Every 30 min (:00, :30)
- Tide predictions: Daily (12:05 AM)

---

For command examples, see `COMMANDS.md`.
For deployment configuration, see `DEPLOYMENT.md`.
For troubleshooting, see `TROUBLESHOOTING.md`.
