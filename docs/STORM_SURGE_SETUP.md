# Storm Surge Forecast Setup Guide

Complete guide for setting up Environment Canada's GDSPS (Global Deterministic Surge and Precipitation System) storm surge forecasts.

---

## Table of Contents

1. [Overview](#overview)
2. [What is Storm Surge?](#what-is-storm-surge)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Running the Pipeline](#running-the-pipeline)
7. [Understanding the Output](#understanding-the-output)
8. [Automation](#automation)
9. [Verification & Testing](#verification--testing)
10. [Troubleshooting](#troubleshooting)
11. [Advanced Topics](#advanced-topics)

---

## Overview

This system fetches storm surge forecasts from Environment Canada's GeoMet WMS service and:
- Downloads hourly surge predictions for specific locations
- Stores historical forecasts for accuracy analysis (hindcasting)
- Exports data as JSON for frontend visualization
- Runs automatically via cron twice daily (after 00Z and 12Z model runs)

**Stations monitored:**
- Point Atkinson (49.338°N, -123.254°W) - Inner Strait, tide observations
- Crescent Beach Channel (49.054°N, -122.897°W) - Boundary Bay, tide predictions
- Campbell River (50.042°N, -125.247°W) - Discovery Passage, tide observations
- Neah Bay (48.495°N, -124.728°W) - Pacific entrance, NOAA buoy location
- New Dungeness (48.333°N, -123.167°W) - Strait of Juan de Fuca, NOAA buoy location
- Tofino (49.154°N, -125.913°W) - Open Pacific coast

**Data source:** GDSPS 15km resolution model via Environment Canada GeoMet

---

## What is Storm Surge?

**Storm surge** is the abnormal rise in water level caused by severe weather conditions, primarily:
- Strong sustained winds pushing water toward shore
- Low atmospheric pressure (inverse barometer effect)
- Storm systems (hurricanes, cyclones, intense low-pressure systems)

**Why it matters:**
- Can cause coastal flooding when combined with high tides
- Critical for marine safety and coastal infrastructure
- Predictions help forecast total water levels (tide + surge)

**GDSPS Model:**
- Global Deterministic Surge and Prediction System
- 15km horizontal resolution
- Updates 2 times daily (00Z, 12Z)
- Provides 10-day forecasts
- Combines ocean circulation model with atmospheric forcing

---

## Prerequisites

### System Requirements

- **OS:** Linux (tested on Ubuntu/Debian)
- **Python:** 3.8 or higher
- **Disk space:** ~100 MB for database and JSON exports
- **Network:** Stable internet connection (WMS queries are rate-limited)

### Required Python Packages

```bash
pip install owslib
```

**OWSLib** is the only external dependency - it handles WMS (Web Map Service) communication with GeoMet.

### Environment Canada GeoMet Access

**No API key required!** Environment Canada's GeoMet is open access.

**Service endpoint:**
```
https://geo.weather.gc.ca/geomet
```

**Documentation:**
- Main docs: https://eccc-msc.github.io/open-data/msc-geomet/readme_en/
- GDSPS layer info: https://eccc-msc.github.io/open-data/msc-data/nwp_gdsps/readme_gdsps_en/

**Important note:** Environment Canada's documentation can be incomplete or outdated. This guide provides practical, tested instructions.

---

## Installation

### Step 1: Clone or Navigate to Repository

```bash
cd ~/surf-website  # Or your installation directory
```

### Step 2: Activate Virtual Environment

```bash
source .venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install owslib
```

Verify installation:
```bash
python3 -c "import owslib; print(owslib.__version__)"
```

### Step 4: Verify Scripts Exist

```bash
ls -lh fetch_storm_surge.py export_hindcast_json.py
```

You should see:
- `fetch_storm_surge.py` - Main fetcher (downloads forecasts from GeoMet)
- `export_hindcast_json.py` - Hindcast exporter (creates +48h accuracy charts)

---

## Configuration

### Script Configuration (Optional)

Open `fetch_storm_surge.py` to customize:

```python
# Enable verbose debugging output
TESTING = False  # Set to True for detailed progress

# Stations to monitor
STATIONS = {
    "Point_Atkinson": {"lat": 49.3375, "lon": -123.253583, "name": "Point Atkinson"},
    "Crescent_Beach_Channel": {"lat": 49.0536, "lon": -122.8969, "name": "Crescent Beach Channel"},
    "Campbell_River": {"lat": 50.042, "lon": -125.247, "name": "Campbell River"},
    "Neah_Bay": {"lat": 48.495, "lon": -124.728, "name": "Neah Bay"},
    "New_Dungeness": {"lat": 48.333, "lon": -123.167, "name": "New Dungeness"},
    "Tofino": {"lat": 49.154, "lon": -125.913, "name": "Tofino"}
}

# Output directory for JSON files
OUTPUT_DIR = Path("site/data/storm_surge").expanduser()

# Database for hindcast storage
DB_PATH = Path("~/.local/share/storm_surge_forecast.sqlite").expanduser()
DB_RETENTION_DAYS = 11  # Keep 11 days of forecast history

# Rate limiting (be respectful to GeoMet!)
FETCH_DELAY = 0.5  # seconds between requests
```

### Adding New Stations

To monitor additional locations:

1. Find coordinates (lat/lon in decimal degrees)
2. Add to `STATIONS` dict in `fetch_storm_surge.py`:
   ```python
   "My_Station": {"lat": 49.123, "lon": -123.456, "name": "My Station Name"}
   ```
3. Update `STATIONS` dict in `export_hindcast_json.py` (same format)

**Important:** Use underscores in station IDs, not spaces.

---

## Running the Pipeline

### First-Time Manual Run

**Test mode (verbose output):**
```bash
# Edit fetch_storm_surge.py and set TESTING = True
python3 fetch_storm_surge.py
```

You'll see detailed progress:
```
🌊 Storm Surge Forecast Fetcher
==================================================
🔌 Connecting to Environment Canada GeoMet...
📅 Forecast period: 2025-11-06 12:00 to 2025-11-16 12:00 UTC
⏱️  Interval: 1 hours
📊 Total timesteps: 241

📍 Fetching Point Atkinson...
    Total timesteps to fetch: 241
    Estimated time: ~120 seconds (2.0 minutes)
    Progress: 10/241 (4.1%) - Success: 10, Failed: 0
    ...
    ✅ Retrieved 241/241 forecasts (Failed: 0)
    💾 Saved to /home/user/site/data/storm_surge/Point_Atkinson.json

📍 Fetching Crescent Beach Channel...
    ...

✅ Created combined forecast: /home/user/site/data/storm_surge/combined_forecast.json
📊 Contains 2 stations

🎯 This is the 12Z run - storing to database for hindcast...
💾 Stored 482 forecast points to database
```

**Production mode (minimal output):**
```bash
# Set TESTING = False in fetch_storm_surge.py
python3 fetch_storm_surge.py
```

### Understanding Runtime

**Typical execution time:** 3-5 minutes for 2 stations

**Why so slow?**
- 240+ timesteps per station (hourly forecasts for 10 days)
- 0.5 second delay between requests (rate limiting)
- WMS query overhead

**Math:** 240 timesteps × 2 stations × 0.5s = 240 seconds (4 minutes)

### Export Hindcast Data

After collecting several days of forecasts (requires 12Z runs):

```bash
python3 export_hindcast_json.py
```

Output:
```
🌊 Storm Surge Hindcast Export (+48h)
==================================================
📊 Found 7 days of forecasts (2025-11-01 to 2025-11-07)

📍 Processing Point Atkinson...
   ✅ 168 predictions
   📅 Range: 2025-11-03 to 2025-11-09

📍 Processing Crescent Beach Channel...
   ✅ 168 predictions
   📅 Range: 2025-11-03 to 2025-11-09

💾 Wrote hindcast data to /home/user/site/data/storm_surge/hindcast.json
📊 Total stations: 2
```

**Note:** Hindcast export requires 2+ days of data (12Z runs only).

---

## Understanding the Output

### JSON File Structure

**Individual station files:** `site/data/storm_surge/Point_Atkinson.json`

```json
{
  "station_id": "Point_Atkinson",
  "station_name": "Point Atkinson",
  "location": {
    "lat": 49.337,
    "lon": -123.253
  },
  "generated_utc": "2025-11-06T13:15:42.123456+00:00",
  "forecast": {
    "2025-11-06T12:00:00Z": -0.052,
    "2025-11-06T13:00:00Z": -0.058,
    "2025-11-06T14:00:00Z": -0.062,
    ...
  },
  "unit": "meters"
}
```

**Combined forecast:** `site/data/storm_surge/combined_forecast.json`

```json
{
  "generated_utc": "2025-11-06T13:15:42.123456+00:00",
  "stations": {
    "Point_Atkinson": { /* full station data */ },
    "Crescent_Beach_Channel": { /* full station data */ }
  }
}
```

**Hindcast data:** `site/data/storm_surge/hindcast.json`

```json
{
  "generated_utc": "2025-11-06T13:20:15.987654+00:00",
  "description": "Storm surge predictions made 48 hours in advance",
  "forecast_horizon_hours": 48,
  "max_days_back": 10,
  "actual_days_available": 7,
  "stations": {
    "Point_Atkinson": {
      "station_id": "Point_Atkinson",
      "station_name": "Point Atkinson",
      "location": { "lat": 49.337, "lon": -123.253 },
      "hindcast": [
        {
          "time": "2025-11-03T12:00:00Z",
          "value": -0.045,
          "forecast_date": "2025-11-01",
          "hours_ahead": 48.0
        },
        ...
      ]
    }
  }
}
```

### Interpreting Surge Values

**Units:** Meters above/below predicted astronomical tide

**Typical ranges for Salish Sea:**
- **Normal conditions:** -0.1 to +0.1 m
- **Moderate weather:** -0.2 to +0.2 m
- **Strong winds:** +0.3 to +0.5 m
- **Storm surge event:** +0.5 m or higher

**Negative values:** Storm surge can be negative (water pushed away from shore by offshore winds)

**Total water level = Astronomical tide + Storm surge**

Example:
- Predicted tide: 4.5 m
- Storm surge: +0.3 m
- **Total water level: 4.8 m** (higher than tide table!)

### Database Structure

**Database:** `~/.local/share/storm_surge_forecast.sqlite`

**Schema:**
```sql
CREATE TABLE forecast_archive (
    station_id TEXT NOT NULL,
    forecast_run_time TEXT NOT NULL,  -- Date of 12Z run (YYYY-MM-DD)
    valid_time TEXT NOT NULL,         -- ISO timestamp
    surge_value REAL NOT NULL,        -- Meters
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (station_id, forecast_run_time, valid_time)
);
```

**Purpose:** Store one forecast per day (12Z run) for hindcast analysis

**Retention:** Automatically purges data older than 11 days

---

## Automation

### Cron Schedule (Recommended)

Add to crontab (`crontab -e`):

```bash
# Storm surge: Fetch every 6 hours (aligned with GeoMet updates)
0 1,7,13,19 * * * cd /home/user/surf-website && source .venv/bin/activate && python3 fetch_storm_surge.py >> ~/envcan_wave/storm_surge.log 2>&1

# Hindcast export: Daily at 14:00 UTC (after 13:00 fetch)
0 14 * * * cd /home/user/surf-website && source .venv/bin/activate && python3 export_hindcast_json.py >> ~/envcan_wave/hindcast_export.log 2>&1
```

**Why these times?**
- GeoMet updates at 01Z, 07Z, 13Z, 19Z (approximately)
- Fetching at these hours ensures fresh forecasts
- 12Z run (13:00 UTC fetch) is stored for hindcast analysis

### Log File Management

**Create log directory:**
```bash
mkdir -p ~/envcan_wave
```

**Monitor logs:**
```bash
# Watch real-time (during scheduled run)
tail -f ~/envcan_wave/storm_surge.log

# Check recent errors
grep -i error ~/envcan_wave/storm_surge.log | tail -20

# View last complete run
tail -100 ~/envcan_wave/storm_surge.log
```

**Log rotation (optional):**
```bash
# Add to crontab to prevent logs from growing indefinitely
0 0 * * 0 find ~/envcan_wave -name "*.log" -size +50M -exec truncate -s 10M {} \;
```

---

## Verification & Testing

### Check JSON Output

```bash
# Verify files exist
ls -lh site/data/storm_surge/

# Check file age (should be recent if cron is working)
ls -lht site/data/storm_surge/ | head

# View latest forecast (first 20 lines)
head -20 site/data/storm_surge/Point_Atkinson.json

# Count timesteps
cat site/data/storm_surge/Point_Atkinson.json | jq '.forecast | length'

# Check latest value
cat site/data/storm_surge/Point_Atkinson.json | jq '.forecast | to_entries | last'

# View combined forecast structure
cat site/data/storm_surge/combined_forecast.json | jq 'keys'
```

### Check Database

```bash
# Connect to database
sqlite3 ~/.local/share/storm_surge_forecast.sqlite

# Check record count
SELECT COUNT(*) FROM forecast_archive;

# View recent forecasts
SELECT
    station_id,
    forecast_run_time,
    COUNT(*) as timesteps,
    MIN(valid_time) as first_time,
    MAX(valid_time) as last_time
FROM forecast_archive
GROUP BY station_id, forecast_run_time
ORDER BY forecast_run_time DESC
LIMIT 5;

# Check data freshness
SELECT
    station_id,
    MAX(forecast_run_time) as latest_run
FROM forecast_archive
GROUP BY station_id;

# Exit
.quit
```

Expected output after 3 days:
```
station_id              latest_run
Point_Atkinson          2025-11-06
Crescent_Beach_Channel  2025-11-06
```

### Test Hindcast Export

```bash
# Requires 2+ days of data
python3 export_hindcast_json.py

# Verify hindcast JSON
cat site/data/storm_surge/hindcast.json | jq '.actual_days_available'
cat site/data/storm_surge/hindcast.json | jq '.stations | keys'
```

### Manual Testing Checklist

- [ ] `fetch_storm_surge.py` runs without errors
- [ ] JSON files created in `site/data/storm_surge/`
- [ ] Individual station files exist (e.g., `Point_Atkinson.json`)
- [ ] `combined_forecast.json` exists and contains all stations
- [ ] Database created at `~/.local/share/storm_surge_forecast.sqlite`
- [ ] Database contains records (after 13:00 UTC run)
- [ ] `export_hindcast_json.py` runs (after 2+ days of data)
- [ ] `hindcast.json` created (after hindcast export succeeds)
- [ ] Log files created and contain expected output
- [ ] Cron jobs scheduled and executing

---

## Troubleshooting

### "No data retrieved" or All Failures

**Possible causes:**
1. **Network connectivity issue**
   ```bash
   # Test GeoMet connectivity
   curl -I "https://geo.weather.gc.ca/geomet"
   ```

2. **Coordinates outside model domain**
   - GDSPS covers oceans/coasts but not deep inland
   - Verify coordinates are in marine areas
   - Try Point Atkinson (known working location)

3. **GeoMet service disruption**
   - Check Environment Canada status: https://weather.gc.ca/
   - Try again in 30 minutes

4. **WMS layer name changed**
   - Verify layer name: https://geo.weather.gc.ca/geomet?service=WMS&request=GetCapabilities
   - Search for "GDSPS" or "surge"

### "Database locked" Error

**Cause:** Another instance of script running simultaneously

**Solution:**
```bash
# Check for lock file
ls -l /tmp/storm_surge_fetch.lock

# If stale (>5 minutes old), remove
rm /tmp/storm_surge_fetch.lock

# Verify no other instances running
ps aux | grep fetch_storm_surge.py
```

### Slow Performance / Timeouts

**Symptoms:** Script takes >10 minutes or times out

**Solutions:**
1. **Increase timeout in script:**
   ```python
   wms = WebMapService(WMS_URL, version="1.3.0", timeout=600)  # 10 minutes
   ```

2. **Check network speed:**
   ```bash
   # Time a single request
   time curl "https://geo.weather.gc.ca/geomet?service=WMS&request=GetCapabilities" > /dev/null
   ```

3. **Reduce FETCH_DELAY (not recommended):**
   - Default: 0.5 seconds
   - Minimum: 0.2 seconds (risk of rate limiting)

### Missing Hindcast Data

**Symptoms:** `export_hindcast_json.py` shows "No forecast data in database"

**Causes:**
1. **12Z run hasn't occurred yet**
   - Hindcast only stores data from 13:00 UTC cron run
   - Wait until after first 13:00 UTC execution

2. **Database doesn't exist**
   ```bash
   ls -lh ~/.local/share/storm_surge_forecast.sqlite
   ```
   - If missing, run `fetch_storm_surge.py` at 13:00 UTC

3. **Not enough days collected**
   - Hindcast requires 2+ days
   - Check database:
     ```bash
     sqlite3 ~/.local/share/storm_surge_forecast.sqlite \
       "SELECT COUNT(DISTINCT forecast_run_time) FROM forecast_archive;"
     ```

### JSON Files Not Updating

**Check cron execution:**
```bash
# View cron log (Ubuntu/Debian)
grep CRON /var/log/syslog | grep storm_surge | tail -20

# Check if cron ran recently
ls -lt ~/envcan_wave/storm_surge.log
```

**Check for errors in log:**
```bash
tail -50 ~/envcan_wave/storm_surge.log
```

**Common issues:**
- Virtual environment not activated (path issues)
- Permissions (cron runs as different user)
- Lock file not cleaned up

**Test cron command manually:**
```bash
cd /home/user/surf-website && source .venv/bin/activate && python3 fetch_storm_surge.py
```

### "Value extraction failed" Warnings

**Symptoms:** Many "⚠️ Error fetching data" messages, partial data retrieved

**Causes:**
1. **Coordinates slightly outside coverage area**
   - Try adjusting lat/lon by 0.01-0.05 degrees
   - Verify using GeoMet WMS viewer

2. **Temporal coverage gap**
   - Some forecast times may be unavailable
   - Normal to have occasional missing values

**Not an issue if:**
- >90% of timesteps succeed
- Critical times (next 48h) have data

---

## Advanced Topics

### Custom Bounding Box

The script queries a small area around each station (±0.25°). To adjust:

```python
def get_bounding_box(lat, lon, offset=0.25):  # Change offset
    return (lon - offset, lat - offset, lon + offset, lat + offset)
```

**Smaller offset:** Faster queries, more precise location
**Larger offset:** More robust to coordinate uncertainties

### WMS Query Details

Under the hood, `fetch_storm_surge.py` uses OWSLib to query:

```python
response = wms.getfeatureinfo(
    layers=["GDSPS_15km_StormSurge"],
    srs="EPSG:4326",              # WGS84 lat/lon
    bbox=(lon-0.25, lat-0.25, lon+0.25, lat+0.25),
    size=(100, 100),               # Image resolution
    format="image/jpeg",
    query_layers=["GDSPS_15km_StormSurge"],
    info_format="text/plain",      # Response format
    xy=(50, 50),                   # Center pixel
    feature_count=1,
    time="2025-11-06T12:00:00Z"   # ISO timestamp
)
```

**Response format:**
```
value_0 = '0.123'
```

### Hindcast Analysis

The hindcast feature answers: **"How accurate were predictions made 48 hours in advance?"**

**Use cases:**
- Validate model performance
- Understand forecast uncertainty
- Build confidence intervals for operational use

**Implementation:**
1. Store 12Z run daily to database
2. Query for predictions where `valid_time ≈ forecast_run_time + 48h`
3. Export as time series

**Example query:**
```sql
SELECT
    forecast_run_time,
    valid_time,
    surge_value,
    ROUND((julianday(valid_time) - julianday(forecast_run_time)) * 24, 1) as hours_ahead
FROM forecast_archive
WHERE station_id = 'Point_Atkinson'
  AND hours_ahead BETWEEN 46 AND 50  -- ~48h window
ORDER BY valid_time ASC;
```

### Adding Spectral Parameters

GDSPS provides additional layers (wind stress, pressure, currents). To fetch:

1. List available layers:
   ```bash
   python3 -c "
   from owslib.wms import WebMapService
   wms = WebMapService('https://geo.weather.gc.ca/geomet?SERVICE=WMS&REQUEST=GetCapabilities', version='1.3.0')
   for layer in wms.contents:
       if 'GDSPS' in layer:
           print(layer)
   "
   ```

2. Add to fetch loop in `fetch_storm_surge.py`

3. Modify JSON export structure

### Integrating with Tide Predictions

**Total water level = Tide + Surge**

To combine with DFO tide predictions:

```python
# Pseudocode
tide_prediction = get_tide_for_time(station, timestamp)  # From tide_data.sqlite
surge_forecast = get_surge_for_time(station, timestamp)  # From storm_surge_forecast.sqlite
total_water_level = tide_prediction + surge_forecast
```

**Important:** Match station coordinates carefully - tide station vs surge grid point

---

## Related Documentation

- **Main project guide:** `CLAUDE.md`
- **All commands:** `docs/COMMANDS.md`
- **Deployment & cron:** `docs/DEPLOYMENT.md`
- **Detailed architecture:** `docs/ARCHITECTURE_DETAILED.md`
- **Troubleshooting:** `docs/TROUBLESHOOTING.md`

---

## Quick Reference

### Essential Commands

```bash
# Fetch latest forecast
python3 fetch_storm_surge.py

# Export hindcast
python3 export_hindcast_json.py

# Check database
sqlite3 ~/.local/share/storm_surge_forecast.sqlite "SELECT COUNT(*) FROM forecast_archive;"

# View latest JSON
cat site/data/storm_surge/combined_forecast.json | jq '.generated_utc'

# Check cron log
tail -50 ~/envcan_wave/storm_surge.log
```

### File Locations

| File/Directory | Purpose |
|----------------|---------|
| `fetch_storm_surge.py` | Main fetcher script |
| `export_hindcast_json.py` | Hindcast exporter |
| `site/data/storm_surge/` | JSON output directory |
| `~/.local/share/storm_surge_forecast.sqlite` | Forecast database |
| `~/envcan_wave/storm_surge.log` | Fetch log |
| `~/envcan_wave/hindcast_export.log` | Hindcast export log |
| `/tmp/storm_surge_fetch.lock` | Lock file (prevents concurrent runs) |

### Default Schedule

| Time (UTC) | Script | Purpose |
|------------|--------|---------|
| 01:00 | `fetch_storm_surge.py` | Fetch 00Z run |
| 07:00 | `fetch_storm_surge.py` | Fetch 06Z run |
| 13:00 | `fetch_storm_surge.py` | Fetch 12Z run (stored to DB) |
| 14:00 | `export_hindcast_json.py` | Export +48h hindcast |
| 19:00 | `fetch_storm_surge.py` | Fetch 18Z run |

---

**Questions or issues?** Check the troubleshooting section above or review Environment Canada's GeoMet documentation.

**Happy forecasting! 🌊**
