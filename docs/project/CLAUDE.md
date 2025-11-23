# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time marine weather monitoring system for the Salish Sea region (Strait of Georgia, English Bay, Neah Bay). Collects data from:
- **Wave Buoys**: Environment Canada and NOAA buoys for wave/wind/temperature data
- **Tide Stations**: DFO IWLS (Integrated Water Level System) for tide observations and predictions
- **Marine Forecasts**: Environment Canada marine weather forecasts with warnings

All data stored in SQLite, published to MQTT/Home Assistant, and exported as JSON for static website rendering.

**Live site:** https://halibutbank.ca

---

## Architecture (High-Level)

### Data Flow Pipeline

```
sr3 (Buoys)   → EC Buoy XMLs → buoy_to_influx_sqlite.py → SQLite (buoy_data.sqlite)
NOAA feeds    → fetch_noaa_buoy.py ↗
                                    ├→ sqlite_to_json.py → JSON exports
                                    ├→ export_24hr_timeseries.py
                                    └→ influx_to_mqtt.py → Home Assistant

sr3 (Wind)    → EC Wind XMLs → wind_to_sqlite.py → SQLite (wind_data.sqlite)
                                                    ├→ export_wind_json.py → JSON exports
                                                    └→ export_wind_24hr_timeseries.py

DFO IWLS API  → tide_to_sqlite.py → SQLite (tide_data.sqlite)
                                    └→ export_tide_json.py → JSON exports

sr3 (Marine)  → EC Marine XMLs → parse_marine_forecast.py → marine_forecast.json
```

### Databases

**Three separate SQLite databases:**
- `~/.local/share/buoy_data.sqlite` - Wave/wind/temperature data from buoys
- `~/.local/share/wind_data.sqlite` - Wind/weather data from land stations
- `~/.local/share/tide_data.sqlite` - Tide observations/predictions/high-low events

**Rationale:** Different update frequencies, data structures, and sources

### Key Scripts

| Script | Purpose | Frequency |
|--------|---------|-----------|
| `buoy_to_influx_sqlite.py` | Parse EC buoy XMLs | Every minute |
| `fetch_noaa_buoy.py` | Fetch NOAA 5-day feeds | Every 20 min |
| `wind_to_sqlite.py` | Parse EC wind station XMLs | Every minute |
| `tide_to_sqlite.py` | Fetch DFO tide data | Obs: 30min, Pred: daily |
| `sqlite_to_json.py` | Export latest buoy snapshot | Every minute |
| `export_wind_json.py` | Export latest wind snapshot | Every 5 min |
| `export_24hr_timeseries.py` | Export buoy timeseries | Every 5 min |
| `export_wind_24hr_timeseries.py` | Export wind timeseries | Every 10 min |
| `export_tide_json.py` | Export tide JSONs | Every 5 min |
| `parse_marine_forecast.py` | Parse marine forecast XMLs | Every 30 min |
| `fetch_storm_surge.py` | Fetch GDSPS surge forecasts | Every 6 hours |
| `export_hindcast_json.py` | Export hindcast data (38-61h PST) | Daily |

**See `docs/COMMANDS.md` for detailed usage.**

---

## Critical Conventions

### 1. Meteorological Direction = Coming FROM

**Wind and wave directions indicate WHERE they are COMING FROM, not where they're going**
- "West wind" (270°) = blowing FROM west TO east
- "Northwest waves" (315°) = coming FROM northwest TO southeast

**Frontend arrows:**
- Wind arrow (↓): rotation = `degrees` (points where wind is blowing TO)
- Wave arrow (➤): rotation = `degrees + 90` (compensates for arrow pointing east)

### 2. Unit Conversions

- **Storage:** km/h (internal database)
- **Display:** knots (frontend/JSON exports)
- **NOAA ingest:** Convert m/s → km/h on fetch

### 3. Per-Field Freshness

Each metric independently queries for most recent non-null value within **2-hour freshness window**.

Example: If wave height was last reported 30 min ago but wind speed 1 hour ago, both are valid within the window.

### 4. Timestamp Handling

- **SQLite:** Unix epoch (INTEGER)
- **JSON exports:** ISO 8601 UTC format
- **NOAA data:** Always parse as `datetime(..., tzinfo=timezone.utc)`

### 5. NOAA Pressure Field

**IMPORTANT:** Valid pressure values can be ~999 hPa (low-pressure systems)
- Do NOT treat 999 as missing data indicator
- Only `MM`, `M`, `NA`, empty strings = missing

### 6. Spectral Wave Data (NOAA Only)

- **Swell** (SwH/SwP/SwD): Long-period ocean waves from distant storms
- **Wind waves** (WWH/WWP/WWD): Short-period locally-generated waves
- Only available for NOAA stations 46087 (Neah Bay), 46088 (New Dungeness)
- EC buoys do NOT provide spectral data (expected)

---

## Station Registry

**Master metadata:** `~/envcan_wave/stations.json`

Unified registry containing all monitored stations with coordinates, data types, and metadata.

**Key files:**
- `stations.json` - Master metadata (6 buoys + 8 tide stations)
- `stations.py` - Python module for accessing station data
- `validate_stations.py` - Validation script

**Usage:**
```python
from stations import get_all_buoys, get_tide_station

BUOYS = get_all_buoys()
point_atk = get_tide_station("point_atkinson")
```

---

## Monitored Stations

### Buoys (6)

**Environment Canada:**
- `4600146` - Halibut Bank
- `4600303` - Southern Georgia Strait
- `4600304` - English Bay
- `4600131` - Sentry Shoal

**NOAA:**
- `46087` - Neah Bay (includes spectral: swell vs wind waves)
- `46088` - New Dungeness / Hein Bank

### Wind Stations (10)

**Environment Canada SWOB-ML:**
- `CWGT` - Sisters Island
- `CWGB` - Ballenas
- `CWEL` - Entrance Island
- `CWSB` - Point Atkinson
- `CVTF` - Tsawwassen
- `CWVF` - Sand Heads
- `CWEZ` - Saturna Island
- `CWQK` - Race Rocks
- `CYVR` - YVR Airport
- `CZBB` - Boundary Bay Airport

**Database:** `wind_data.sqlite` (separate from buoys)
**Update frequency:** Every 10 minutes (parsed every minute)
**Data fields:** Wind speed/gust/direction, temperature, pressure, humidity, dewpoint, visibility, rainfall

### Tide Stations (12)

**Permanent (with real-time observations):**
- `point_atkinson` - Point Atkinson (07795)
- `kitsilano` - Kitsilano (07707)
- `new_westminster` - New Westminster (07654)
- `campbell_river` - Campbell River (08074)

**Temporary (observations + predictions):**
- `tofino` - Tofino (08615)
- `ucluelet` - Ucluelet (08595)
- `port_renfrew` - Port Renfrew (08525)
- `victoria_harbor` - Victoria Harbor (07120)

**Temporary (predictions only):**
- `tsawwassen` - Tsawwassen (07590)
- `whiterock` - White Rock (07577)
- `crescent_pile` - Crescent Beach (07579)
- `nanaimo` - Nanoose Bay (07930)

**All metadata in `config/stations.json`**

---

## Sarracenia (sr3) - Critical Infrastructure

**Purpose:** Subscribes to Environment Canada's AMQP broker and automatically downloads XML files

**Three subscriptions run continuously:**
1. **Buoy observations** (`bc_buoys.conf`) - SWOB-ML XMLs hourly
2. **Wind stations** (`bc_wind_stations.conf`) - SWOB-ML XMLs every 10 minutes
3. **Marine forecasts** (`marine_forecast.conf`) - Forecast XMLs 2-4x daily

**Key commands:**
```bash
sr3 status                               # Check if all running
sr3 start subscribe/bc_buoys             # Start buoy subscription
sr3 start subscribe/bc_wind_stations     # Start wind station subscription
sr3 start subscribe/marine_forecast      # Start forecast subscription
ps aux | grep sr3                        # Check process status
```

**Without sr3 running:** No new XML files downloaded, parsers only process existing files

**See `docs/DEPLOYMENT.md` for sr3 config details.**

---

## Marine Weather Forecasts

**Source:** Environment Canada Marine Weather via sr3

**Zones monitored:**
- Strait of Georgia - north of Nanaimo (`strait_georgia_north`)
- Strait of Georgia - south of Nanaimo (`strait_georgia_south`)

**Warning types by severity:**
- **Strong Wind Warning**: 20-33 knots
- **Gale Warning**: 34-47 knots
- **Storm Warning**: 48+ knots

**Output:** `~/site/data/marine_forecast.json`

**Frontend:** Dismissible warning banners on all pages + dedicated forecasts page

---

## Storm Surge Forecasts

**Source:** Environment Canada GDSPS (Global Deterministic Surge and Prediction System) via GeoMet WMS

**Stations monitored:**
- Point Atkinson (49.338°N, -123.254°W) - Tide observations available
- Crescent Beach Channel (49.054°N, -122.897°W) - Tide predictions available
- Campbell River (50.042°N, -125.247°W) - Tide observations available
- Neah Bay (48.495°N, -124.728°W) - NOAA buoy 46087 location
- New Dungeness (48.333°N, -123.167°W) - NOAA buoy 46088 location
- Tofino (49.154°N, -125.913°W) - Open Pacific coast

**Model details:**
- 15 km horizontal resolution
- Updates 4x daily (00Z, 06Z, 12Z, 18Z)
- 10-day hourly forecasts
- Storm surge = water level anomaly above astronomical tide

**Data pipeline:**
1. `fetch_storm_surge.py` - Fetch forecasts from GeoMet WMS (every 6 hours)
2. Store 18Z run to `~/.local/share/storm_surge_forecast.sqlite` for hindcast analysis (closest to noon Pacific)
3. `export_hindcast_json.py` - Export 38-61h predictions (full Pacific calendar day 2 days ahead, daily)

**Outputs:**
- `~/site/data/storm_surge/<station_id>.json` - Individual station forecasts (6 files)
- `~/site/data/storm_surge/combined_forecast.json` - All stations combined
- `~/site/data/storm_surge/hindcast.json` - Historical +48h predictions

**Units:** Meters (above/below predicted tide)

**Typical ranges:**
- Normal: -0.1 to +0.1 m
- Moderate weather: -0.2 to +0.2 m
- Storm surge event: +0.5 m or higher

**Total water level = Astronomical tide + Storm surge**

**See `docs/STORM_SURGE_SETUP.md` for complete setup guide.**

---

## Getting Started

```bash
# Setup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start sr3 (critical!)
sr3 start subscribe/bc_buoys
sr3 start subscribe/bc_wind_stations
sr3 start subscribe/marine_forecast

# Run data pipeline manually
python3 buoy_to_influx_sqlite.py
python3 wind_to_sqlite.py
python3 fetch_noaa_buoy.py
python3 sqlite_to_json.py
python3 export_wind_json.py

# Check if data arrived
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT COUNT(*) FROM buoy_observation;"
sqlite3 ~/.local/share/wind_data.sqlite "SELECT COUNT(*) FROM wind_observation;"
cat ~/site/data/latest_buoy_v2.json | jq '.["4600146"]'
cat ~/site/data/latest_wind.json | jq '.CWSB'
```

**See `docs/COMMANDS.md` for more examples.**

---

## Adding a New Buoy

1. **If EC buoy:** Update `~/.config/sr3/subscribe/bc_buoys.conf` with new subtopic, restart sr3
2. **Add to `stations.json`** - Master registry
3. **Update scripts:** `buoy_to_influx_sqlite.py`, `fetch_noaa_buoy.py`, `sqlite_to_json.py`, `export_24hr_timeseries.py`, `influx_to_mqtt.py`
4. **Update frontend:** `~/site/assets/js/main.js`, `charts.js`
5. **Test pipeline:**
   ```bash
   python3 buoy_to_influx_sqlite.py  # or fetch_noaa_buoy.py
   sqlite3 ~/.local/share/buoy_data.sqlite "SELECT * FROM buoy_observation WHERE buoy_id='NEW_ID' LIMIT 5;"
   python3 sqlite_to_json.py
   cat ~/site/data/latest_buoy_v2.json | jq .NEW_ID
   ```

---

## Common Issues

### sr3 not running
**Symptom:** No new XML files, stale buoy data

**Fix:**
```bash
sr3 status
sr3 start subscribe/bc_buoys
tail -f ~/.cache/sr3/log/subscribe_bc_buoys_*.log
```

### Stale data warnings on website
**Check:** Cron jobs running? Log files show recent activity?

```bash
crontab -l
tail -f ~/envcan_wave/*.log
```

### Missing spectral data for EC buoys
**Expected** - Only NOAA stations provide swell/wind wave separation

### Wind direction shows as null
**Expected** - NOAA reports `MM` (missing) for calm conditions or sensor failures

### Missing tide predictions/high-low events
**Check:**
```bash
ls -lh ~/.local/share/tide_data.sqlite
tail ~/envcan_wave/tide_pred.log
tail ~/envcan_wave/tide_highlow.log
```

Predictions/high-low fetch **once daily** (12:05-12:15 AM). If missing, may not have run yet today.

### "Influx unavailable" warnings
**Normal** if running SQLite-only mode. Scripts gracefully degrade.

**See `docs/TROUBLESHOOTING.md` for detailed debugging.**

---

## Testing Changes

When modifying data processing logic:

1. Run script manually, check logs for errors
2. Query SQLite to verify data inserted correctly
3. Check JSON exports for expected format/values
4. Monitor MQTT topics if testing Home Assistant integration
5. Verify website displays data correctly

---

## Documentation Structure

**Core docs (this repo):**
- `CLAUDE.md` - This file (high-level reference for Claude Code)
- `README.md` - Project overview for humans
- `docs/COMMANDS.md` - Detailed command examples and database queries
- `docs/DEPLOYMENT.md` - Cron schedules, config files, server setup
- `docs/TROUBLESHOOTING.md` - Debugging guide, common issues
- `docs/ARCHITECTURE_DETAILED.md` - Full database schemas, script details
- `docs/STORM_SURGE_SETUP.md` - Storm surge forecast setup guide (GDSPS/GeoMet)

**Frontend docs (`~/site/`):**
- `docs/FRONTEND_CHANGELOG.md` - UI/UX changes, feature history
- `docs/FRAMEWORK_DISCUSSION.md` - Framework evaluation notes
- `docs/BROWSER_STATE_EXPLAINED.md` - localStorage usage guide
- `docs/WARNING_BANNER_UPGRADE_SUMMARY.md` - Warning banner improvements

---

## Key Design Principles

1. **Separate databases** for buoys vs tides (different update frequencies)
2. **InfluxDB optional** - Code works without it (soft dependency)
3. **Per-field freshness** - Each metric independently queries recent non-null value
4. **Deduplication** - Primary keys on timestamps prevent duplicates
5. **Unit conversions** - Store km/h, display knots
6. **Tide separation** - Observations (dynamic) vs predictions/high-low (static)

---

**For detailed schemas, script responsibilities, cron schedules, and configuration files, see the documentation files in the `docs/` directory.**
