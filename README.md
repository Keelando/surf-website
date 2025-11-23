---

# 🌊 Salish Sea Wave Conditions Monitor

A real-time, open-source wave and weather monitoring system for the **Salish Sea** region — combining data from Environment Canada and NOAA buoys.

📍 **Live demo:** [halibutbank.ca](https://halibutbank.ca)  
🧭 **Region:** Strait of Georgia, English Bay, Neah Bay, and surrounding waters  
⚙️ **Stack:** Python · SQLite · MQTT · Home Assistant · ECharts

![Halibut Bank Dashboard Screenshot](assets/screenshot.png)

---

## Overview

This system collects, processes, and displays marine weather data from:
- **6 Wave Buoys** – Halibut Bank, English Bay, Southern Georgia Strait, Sentry Shoal (EC), Neah Bay & New Dungeness (NOAA)
- **10 Wind Stations** – Point Atkinson, Sisters Islets, Entrance Island, Ballenas, Sand Heads, Tsawwassen, Saturna, Race Rocks, YVR, Boundary Bay (Environment Canada)
- **12 Tide Stations** – Point Atkinson, Kitsilano, Tsawwassen, White Rock, Crescent Beach, New Westminster, Campbell River, Nanaimo, Tofino, and more (DFO IWLS)

### Key Features
- 🔁 Automated XML + text feed collection
- 💾 SQLite database for local persistence (auto schema management)
- 📡 MQTT integration with Home Assistant (auto-discovery)
- 🧩 JSON outputs for static website rendering
- 📊 24-hour interactive charts (ECharts)
- 🌊 Real-time tide predictions and observations
- ⚙️ Smart deduplication and update scheduling
- 🌊 NOAA "swell vs wind wave" separation  

---

## 📍 Monitored Stations

### Wave Buoys (6)

**Environment Canada:**
- `4600146` – Halibut Bank (off Vancouver)
- `4600303` – Southern Georgia Strait
- `4600304` – English Bay (Vancouver Harbor)
- `4600131` – Sentry Shoal (Northern Strait of Georgia)

**NOAA NDBC:**
- `46087` – Neah Bay (includes spectral wave data: swell vs wind waves)
- `46088` – New Dungeness / Hein Bank

**Additional monitored (Surrey FlowWorks):**
- `CRPILE` – Crescent Beach Ocean (pile-mounted wave station)
- `CRCHAN` – Crescent Channel (radar-based wave measurement)

### Wind Stations (10)

**Environment Canada SWOB-ML weather stations:**
- `CWGT` – Sisters Islets (Strait of Georgia)
- `CWGB` – Ballenas (Strait of Georgia)
- `CWEL` – Entrance Island (Nanaimo area)
- `CWSB` – Point Atkinson (West Vancouver)
- `CVTF` – Tsawwassen (Delta)
- `CWVF` – Sand Heads (Fraser River mouth)
- `CWEZ` – Saturna (Gulf Islands)
- `CWQK` – Race Rocks (Juan de Fuca Strait)
- `CYVR` – YVR Airport (Richmond)
- `CZBB` – Boundary Bay Airport (Delta)

**Data provided:** Wind speed/gust/direction, temperature, pressure, humidity, dewpoint, visibility, rainfall

**Update frequency:** Environment Canada updates every 10 minutes, parsed every minute

### Tide Stations (12)

**DFO IWLS stations** providing real-time observations and astronomical predictions:
- `point_atkinson` – Point Atkinson (07795)
- `kitsilano` – Kitsilano (07707)
- `new_westminster` – New Westminster (07654)
- `campbell_river` – Campbell River (08074)
- `tsawwassen` – Tsawwassen (07590, predictions only)
- `whiterock` – White Rock (07577, predictions only)
- `crescent_pile` – Crescent Beach (07579, predictions only)
- `nanaimo` – Nanoose Bay (07930, predictions only)
- `tofino` – Tofino (08615)
- `ucluelet` – Ucluelet (08595)
- `port_renfrew` – Port Renfrew (08525)
- `victoria_harbor` – Victoria Harbor (07120)

**All station metadata:** See `config/stations.json`

---

## 🏗️ System Architecture

```
Environment Canada XML → buoy_to_influx_sqlite.py → SQLite Database (buoy_data.sqlite)
NOAA 5-day feeds       → fetch_noaa_buoy.py       →      ↓
Environment Canada XML → wind_to_sqlite.py        → SQLite Database (wind_data.sqlite)
DFO IWLS Tides         → tide_to_sqlite.py        → SQLite Database (tide_data.sqlite)
                                                            ↓
                                                   ├→ sqlite_to_json.py → ~/site/data/latest_buoy_v2.json
                                                   ├→ export_24hr_timeseries.py → timeseries_*.json
                                                   ├→ export_wind_json.py → latest_wind.json
                                                   ├→ export_wind_24hr_timeseries.py → wind_timeseries_24hr.json
                                                   ├→ export_tide_json.py → tide-*.json
                                                   └→ influx_to_mqtt.py → Home Assistant (MQTT)
```

### Hardware Setup
- **Home Assistant Server** (Lenovo M715Q): Runs InfluxDB + MQTT broker  
- **Surf Server** (Lenovo M910Q, Ubuntu): Runs Python scripts + hosts static website  

---

## ⚙️ Installation

### Prerequisites
```bash
sudo apt install python3 python3-venv sqlite3

Setup

cd ~/envcan_wave
python3 -m venv .venv
source .venv/bin/activate
pip install requests influxdb paho-mqtt owslib

Optional: InfluxDB Integration

Create ~/.config/buoy_influx_1.env:

INFLUX_HOST=192.168.1.98
INFLUX_PORT=8086
INFLUX_USER=your_user
INFLUX_PASS=your_password
INFLUX_DB=buoy_data

> 🔒 Security note:

Never commit real .env or .sqlite files to GitHub.

Use chmod 600 ~/.config/buoy_influx_1.env to protect credentials locally.

A sample .env.example is included for structure only.





---

🗂️ Recommended Repository Layout

envcan_wave/
├── buoy_to_influx_sqlite.py
├── fetch_noaa_buoy.py
├── sqlite_to_json.py
├── export_24hr_timeseries.py
├── influx_to_mqtt.py
├── fetch_storm_surge.py
├── .gitignore
├── README.md
└── examples/
    ├── example_data.json
    └── example_crontab.txt

site/
├── index.html
├── charts.html
├── assets/
│   ├── js/
│   └── css/
└── data/  ← (ignored by .gitignore)


---

🔄 Automated Data Collection (Cron)

Add to crontab (crontab -e):

# Parse Environment Canada XMLs every minute
* * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/buoy_to_influx_sqlite.py >> $HOME/envcan_wave/parser.log 2>&1

# Fetch NOAA data every 5 minutes (NOAA updates hourly)
5,25,45 * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/fetch_noaa_buoy.py >> $HOME/envcan_wave/noaa.log 2>&1

# Export latest snapshot every minute
* * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/sqlite_to_json.py >> $HOME/envcan_wave/json_export.log 2>&1

# Export 24h timeseries every 5 minutes
*/5 * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/export_24hr_timeseries.py >> $HOME/envcan_wave/timeseries_export.log 2>&1

# Push to Home Assistant via MQTT every minute
* * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/influx_to_mqtt.py >> $HOME/envcan_wave/mqtt.log 2>&1

# Fetch storm surge forecast every 6 hours (GeoMet updates every 6h)
30 1,7,13,19 * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/fetch_storm_surge.py >> $HOME/envcan_wave/storm_surge.log 2>&1

# Fetch tide data every 30 minutes (DFO IWLS)
*/30 * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/tide_to_sqlite.py >> $HOME/envcan_wave/tide.log 2>&1

# Export tide JSON every minute
* * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/export_tide_json.py >> $HOME/envcan_wave/tide_export.log 2>&1

# Parse Environment Canada wind station XMLs every minute
* * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/wind_to_sqlite.py >> $HOME/envcan_wave/wind_parser.log 2>&1

# Export wind JSON every 5 minutes
*/5 * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/export_wind_json.py >> $HOME/envcan_wave/wind_export.log 2>&1

# Export wind 24h timeseries every 10 minutes
*/10 * * * * $HOME/envcan_wave/.venv/bin/python3 $HOME/envcan_wave/export_wind_24hr_timeseries.py >> $HOME/envcan_wave/wind_timeseries_export.log 2>&1

# Cleanup old XML files (keep 2 days)
0 * * * * find $HOME/envcan_wave/data/buoy -name "*.xml" -mtime +2 -delete


---

🗄️ Database Schemas

### Buoy Database (`buoy_data.sqlite`)

Table: buoy_observation

CREATE TABLE buoy_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buoy_id TEXT NOT NULL,
    observation_time INTEGER NOT NULL,  -- Unix timestamp

    -- Wave metrics
    wave_height_sig REAL,
    wave_height_peak REAL,
    wave_period_sig REAL,
    wave_period_avg REAL,
    wave_period_peak REAL,
    wave_direction_avg REAL,
    wave_direction_peak REAL,

    -- NOAA spectral data (Neah Bay only)
    swell_height REAL,
    swell_period REAL,
    swell_direction REAL,
    wind_wave_height REAL,
    wind_wave_period REAL,
    wind_wave_direction REAL,

    -- Meteorological
    wind_speed REAL,          -- km/h (converted to knots for display)
    wind_gust REAL,           -- km/h
    wind_direction REAL,      -- degrees
    air_temp REAL,            -- °C
    sea_temp REAL,            -- °C
    pressure REAL,            -- hPa

    -- Metadata
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX uniq_buoy_ts ON buoy_observation(buoy_id, observation_time);
CREATE INDEX idx_buoy_time ON buoy_observation(buoy_id, observation_time DESC);

### Wind Database (`wind_data.sqlite`)

Table: wind_observation

CREATE TABLE wind_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,           -- ICAO/TC code (e.g., 'CWSB', 'CYVR')
    station_name TEXT,                  -- Friendly name
    observation_time INTEGER NOT NULL,  -- Unix timestamp

    -- Wind metrics (10-minute averages)
    wind_speed_kmh REAL,               -- km/h (converted to knots for display)
    wind_gust_kmh REAL,
    wind_direction_deg INTEGER,        -- degrees (coming FROM)

    -- Atmospheric conditions
    air_temp_c REAL,
    pressure_hpa REAL,
    pressure_mslp_hpa REAL,            -- Mean sea level pressure

    -- Additional meteorology
    humidity_percent REAL,
    dewpoint_c REAL,
    visibility_km REAL,
    rainfall_1hr_mm REAL,
    rainfall_6hr_mm REAL,

    -- Metadata
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_wind_station_time ON wind_observation(station_id, observation_time DESC);
CREATE UNIQUE INDEX uniq_wind_station_ts ON wind_observation(station_id, observation_time);

### Tide Database (`tide_data.sqlite`)

See `docs/ARCHITECTURE_DETAILED.md` for full tide database schemas (observations, predictions, high/low events)


---

📊 Web Interface

### Buoys Page (index.html)

Displays current conditions for all buoys with:
- Real-time wind speed/direction
- Wave height/period/direction
- Air/sea temperature
- Atmospheric pressure
- Data staleness warnings (>2 hours old)
- Source badges (Environment Canada vs NOAA)

**Neah Bay Special Features:**
- Displays swell (ocean waves) prominently
- Collapsible detailed view with wind waves + combined metrics

**Interactive Charts:**
- 24-hour wave height comparison (all buoys)
- Per-buoy wave, wind, and temperature timeseries
- Storm surge forecasts (GeoMet GDSPS)
- Responsive design for mobile and desktop

### Winds Page (winds.html)

Real-time wind conditions from 10 Environment Canada weather stations:
- **Sortable table** - Current wind speed/gust/direction, temperature, pressure
- **Interactive map** - Leaflet map showing all wind stations with live data popups
- **24-hour charts** - ECharts visualization with:
  - Wind speed and gust timeseries (knots)
  - Wind direction arrows (10-minute averages)
  - Station selector dropdown with search
- **Data staleness indicators** - Visual warnings for stale data (>2 hours)
- **Regional coverage** - Strait of Georgia, Gulf Islands, Juan de Fuca

### Tides Page (tides.html)

Real-time tide monitoring for 12 DFO stations:
- **Current observation** - Latest water level measurement
- **Current prediction** - Astronomical tide forecast (now)
- **High/Low table** - Today's predicted high and low tides
- **Combined water level chart** - ECharts visualization showing:
  - Astronomical tide predictions (blue line)
  - Actual observations (green dots)
  - Storm surge forecast (orange dashed line)
  - Total water level = tide + surge (purple bold line)
  - Interactive tooltips with Pacific time
  - Day navigation (today, tomorrow, +2 days)
- **Auto-loads Point Atkinson** as default station
- Station selector dropdown for all monitored locations
- Auto-refreshes every 5 minutes

### Forecasts Page (forecasts.html)

Marine weather forecasts and warnings from Environment Canada:
- **Warning banners** - Dismissible banners on all pages (Storm/Gale/Strong Wind)
- **Zone-specific forecasts** - Strait of Georgia (north/south of Nanaimo)
- **Extended forecast** - Today, Tonight, Tomorrow, and named days
- **Wave forecast** - Predicted wave heights and conditions
- **Smooth navigation** - Scroll-to-zone links



---

🧠 Key Scripts

| Script | Purpose |
|--------|---------|
| `buoy_to_influx_sqlite.py` | Parse EC buoy XMLs → SQLite; optional InfluxDB sync |
| `fetch_noaa_buoy.py` | Download + merge NOAA data (met + spectral) |
| `wind_to_sqlite.py` | Parse EC wind station XMLs → SQLite |
| `tide_to_sqlite.py` | Fetch DFO IWLS tide data (observations + predictions) |
| `sqlite_to_json.py` | Export latest buoy readings for website display |
| `export_wind_json.py` | Export latest wind station readings |
| `export_24hr_timeseries.py` | Export rolling 24-hour buoy timeseries |
| `export_wind_24hr_timeseries.py` | Export rolling 24-hour wind timeseries |
| `export_tide_json.py` | Export tide data (latest, timeseries, high/low) |
| `influx_to_mqtt.py` | Publish MQTT topics for Home Assistant |
| `fetch_storm_surge.py` | Fetch GeoMet GDSPS storm surge forecasts |
| `parse_marine_forecast.py` | Parse EC marine forecast XMLs |



---

🛠️ Maintenance

Check recent observations:

sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT buoy_id, datetime(observation_time, 'unixepoch') AS last_obs,
         (strftime('%s','now') - observation_time)/3600.0 AS hours_ago
  FROM buoy_observation
  WHERE observation_time IN (
    SELECT MAX(observation_time) FROM buoy_observation GROUP BY buoy_id
  );
"

Tail logs:

tail -f ~/envcan_wave/*.log


---

🔧 Adding a New Buoy

1. Add buoy ID to the BUOYS dictionary in all relevant scripts


2. Add new field mappings if required


3. Update frontend order arrays (main.js, charts.js)


4. Verify:

python3 fetch_noaa_buoy.py
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT * FROM buoy_observation WHERE buoy_id='NEW_ID' LIMIT 5;"
python3 sqlite_to_json.py
cat ~/site/data/latest_buoy_v2.json | jq .NEW_ID




---

🧩 Design Notes

Wind: stored as km/h, displayed as knots

Wave direction: degrees → cardinal direction

Timestamps: Unix epoch → ISO 8601 in JSON

High-frequency buoys: 10–15 min updates

Low-frequency buoys: 30–60 min updates



---

🧱 Why SQLite?

No service dependency

Simple schema migrations

Easy inspection & backup

Great performance for local datasets



---

🔐 Security

MQTT credentials are defined in influx_to_mqtt.py (use env vars or config file)

InfluxDB credentials live in ~/.config/buoy_influx_1.env (private)

SQLite database is local-only (no network exposure)

Website is static-only — no dynamic backend required



---

🧾 License

MIT License — free for personal and educational use.
See LICENSE file for details.


---

🙏 Acknowledgments

Environment Canada – SWOB-ML buoy data

NOAA NDBC – Spectral and meteorological feeds

Home Assistant Community – MQTT discovery patterns

ECharts – Beautiful visualization library



---

📞 Contact

Website: halibutbank.ca
Maintainer: Keelan W.
Last updated: October 2025