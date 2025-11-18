# TODO List

## Upcoming Tasks

### Wind Stations (NEW FEATURE)

**Stations to implement (9 total):**
1. Sisters Islets - `CWGT`
2. Ballenas - `CWGB`
3. Entrance Island - `CWEL`
4. Point Atkinson - `CWSB`
5. Tsawwassen - `CVTF`
6. Sand Heads - `CWVF`
7. Saturna - `CWEZ`
8. Race Rocks - `CWQK`
9. YVR Airport - `CYVR`

**Data fields to capture:**
- Wind speed: `avg_wnd_spd_pst10mts` (km/h → display as knots)
- Wind gust: `max_avg_wnd_spd_pst10mts` (km/h → display as knots)
- Wind direction: `avg_wnd_dir_pst10mts` (degrees + cardinal)
- Air temperature: `avg_air_temp_pst10mts` (°C)
- Atmospheric pressure: `avg_stn_pres_pst10mts` (hPa)
- Rainfall: `pcpn_amt_pst1hr` or `pcpn_amt_pst6hrs` (mm)

**Implementation plan:**

1. **Data Pipeline Setup**
   - [x] Confirm Environment Canada station IDs
   - [ ] Create sr3 subscription config (`~/.config/sr3/subscribe/bc_wind_stations.conf`)
     - broker: amqps://dd.weather.gc.ca
     - topicPrefix: v02.post
     - subtopic pattern: `*.WXO-DD.observations.swob-ml.*.STATION_ID.#` (need to confirm path)
     - directory: `/home/keelando/envcan_wave/data/wind`
   - [ ] Research correct SWOB-ML subtopic pattern for land-based weather stations
   - [ ] Test sr3 subscription with 1-2 stations first
   - [ ] Start full sr3 subscription after confirming data arrives

2. **Database Schema**
   - [ ] Decision: Create new `wind_data.sqlite` (separate from buoys for clarity)
   - [ ] Create `wind_observation` table:
     ```sql
     CREATE TABLE IF NOT EXISTS wind_observation (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       station_id TEXT NOT NULL,
       observation_time INTEGER NOT NULL,
       wind_speed_kmh REAL,
       wind_gust_kmh REAL,
       wind_direction_deg INTEGER,
       air_temp_c REAL,
       pressure_hpa REAL,
       rainfall_1hr_mm REAL,
       rainfall_6hr_mm REAL,
       source_file TEXT,
       recorded_at TEXT DEFAULT (datetime('now'))
     );
     ```
   - [ ] Add indexes:
     - `CREATE INDEX idx_wind_station_time ON wind_observation(station_id, observation_time DESC);`
     - `CREATE UNIQUE INDEX uniq_wind_station_ts ON wind_observation(station_id, observation_time);`

3. **Backend Scripts**
   - [ ] Create `wind_to_sqlite.py` - Parse SWOB-ML XMLs and insert to database
     - Mirror structure of `buoy_to_influx_sqlite.py`
     - SWOB-ML field mapping (see Data fields above)
     - Handle missing values ("MSNG")
     - Deduplication via unique index
     - Optional InfluxDB support (soft dependency)
   - [ ] Create `export_wind_json.py` - Export latest readings
     - Output: `~/site/data/latest_wind.json`
     - Format: `{ "CWGT": { "station_name": "Sisters Islets", "wind_speed_kt": 15.2, ... }, ... }`
     - Per-field freshness (2-hour window like buoys)
   - [ ] Create `export_wind_24hr_timeseries.py` - Export 24hr data for charts
     - Output: `~/site/data/wind_timeseries_24hr.json`
     - Hourly data points for past 24 hours
   - [ ] Add wind stations to `stations.json` registry
     - Include: station_id, name, lat/lon, type: "wind"

4. **Frontend**
   - [ ] Create `~/site/winds.html` - New dedicated wind page
     - Header: "Wind Conditions"
     - Tagline: "Real-time observations from coastal weather stations"
     - Navigation bar with "Winds" link
   - [ ] Create wind station cards (compact design like buoys)
     - Current: Wind speed/gust (knots), direction (cardinal + arrow), temp (°C)
     - Expandable details: Pressure, rainfall, timestamps
     - Regional grouping (Strait of Georgia, Juan de Fuca, etc.)
   - [ ] Create JavaScript module (`~/site/assets/js/winds_page.js`)
     - Load latest_wind.json
     - Render wind cards
     - Handle missing data gracefully
   - [ ] Implement 24hr wind charts
     - Chart 1: Wind speed + gusts (knots)
     - Chart 2: Wind direction (polar chart or line)
     - Use ECharts like buoy/tide pages
   - [ ] Add wind stations to Leaflet map on index.html
     - New marker type/color for wind stations
     - Popup shows latest wind data
   - [ ] Update navigation bar on all pages
     - Add "Winds" link between "Tides" and "Forecasts"
   - [ ] Create CSS styles for wind-specific elements
     - Wind rose/arrow styling
     - Card layout optimizations

5. **Automation**
   - [ ] Add cron jobs for wind data processing
     ```
     */1 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/wind_to_sqlite.py
     */1 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/export_wind_json.py
     */5 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/export_wind_24hr_timeseries.py
     ```
   - [ ] Configure sr3 to auto-start on reboot
     - Add systemd service or update existing sr3 startup

6. **Documentation**
   - [ ] Update README.md with wind station info
   - [ ] Update CLAUDE.md with wind pipeline details
   - [ ] Update ARCHITECTURE_DETAILED.md with wind database schema
   - [ ] Document wind station IDs and metadata in stations.json

**Additional Data Sources (Future Enhancement):**

- **Jericho Wind Station** (Jericho Sailing Centre)
  - URL: https://jsca.bc.ca/main/downld02.txt
  - Format: Custom text file (non-SWOB-ML)
  - Would need dedicated fetch script similar to Surrey FlowWorks integration

- **US Wind Stations** (NOAA) - Southern Salish Sea / Puget Sound coverage
  - Cherry Point, WA
  - Sandy Point Shores, WA
  - Orcas Island Airport (KORS) - METAR format
  - Libbey Beach, WA
  - Data sources to investigate:
    - NOAA CO-OPS API (coastal stations with meteorological data)
    - NOAA METAR (airports): `https://tgftp.nws.noaa.gov/data/observations/metar/stations/KORS.TXT`
    - NOAA NWS API: `https://api.weather.gov/`
  - Would integrate into existing `wind_data.sqlite` database
  - Need to identify specific station IDs and optimal API endpoints

### Known Issues (Not Currently Affecting Operation)

- **ECharts "connect nulls" behavior**
  - Charts may connect lines across data gaps in some scenarios
  - Not currently observed as a problem in production
  - Would require injecting explicit null values at gap timestamps if needed

---

## Completed (2025-11-18)

✅ **Buoy Card Refinements**
  - Wave height significant figures (1 sig fig, except Crescent Beach keeps 2 decimals)
  - Show latest available wave data with timestamp
  - History table date column cleanup
  - Add Crescent Beach Ocean (CRPILE) to wave comparison chart
  - "Hide Details" now collapses both details AND history sections

✅ **Tofino tide station added**
  - Added DFO IWLS station near Tofino/Ucluelet
  - Corresponding GDSPS storm surge location added
  - Enables hindcast validation for west coast exposure

✅ **Storm surge hindcast plot improvements**
  - Simplified legend to highlight observed surge (black line)
  - Added subtitle explaining colored lines are historical forecast runs
  - Reduced legend clutter

---

## Completed (2025-11-11)

✅ **Condensed buoy cards refactor**
  - Reduced default card height by 60% (~150px vs ~300-400px)
  - Compact single-line wind/wave display
  - Expandable "Show Details" button for full metrics
  - Expandable "Show History (24h)" with 12 hourly observations
  - Added `degreesToCardinal()` for wind direction in history table
  - History table sorted newest-first
  - Collapsible region groups (click header to collapse/expand)
  - Default: Strait of Georgia expanded, others collapsed
  - Fixed Surrey attribution: "Surrey (FlowWorks)"
  - Added max-width: 450px constraint on cards

✅ **Peak storm surge display on storm surge page**
  - Added prominent "Peak Today" card above forecast chart
  - Shows absolute peak (positive or negative) for current Pacific day
  - Displays value (±X.XX m) and time (24h format)
  - Updates when switching stations
  - Hidden if no data available

---

## Completed (2025-11-09)

✅ **Combined water level forecasts (tide + storm surge)**
  - Created `export_combined_water_level.py` to merge astronomical tide predictions with storm surge forecasts
  - Exports to `~/site/data/combined-water-level.json` (1.5MB covering next 2 days)
  - Added multi-series ECharts visualization on tides.html:
    - Astronomical Tide (blue solid line)
    - Observations (green dots, today only)
    - Storm Surge (orange dashed line)
    - Total Water Level (purple bold line - tide + surge)
  - Implemented day navigation (today, tomorrow, +2 days) with arrow buttons
  - Automated via cron (every 5 min + after storm surge updates)
  - Stations: Point Atkinson, Campbell River, Crescent Beach

---

## Completed (2025-11-06)

✅ **Storm Surge Forecast Setup Guide**
  - Created comprehensive documentation (`docs/STORM_SURGE_SETUP.md`)
  - Improves upon Environment Canada's incomplete documentation
  - Includes prerequisites, installation, configuration, automation
  - Detailed troubleshooting and verification procedures
  - Explains GDSPS model, WMS queries, and hindcast analysis

---

## Completed (2025-11-05)

✅ **Warning banner improvements**
  - Variable dismiss durations by severity (Storm: 12h, Gale: 12h, Strong Wind: 6h)
  - Dismissal feedback toast messages
  - Enhanced visual hierarchy (border thickness by severity)
  - Mobile sticky positioning
  - Improved accessibility (ARIA labels, roles, live regions)

✅ **Documentation reorganization**
  - Split 1135-line CLAUDE.md into focused docs (71% reduction)
  - Created `docs/` subdirectories following industry standards
  - Created COMMANDS.md, DEPLOYMENT.md, TROUBLESHOOTING.md, ARCHITECTURE_DETAILED.md
  - Created FRONTEND_CHANGELOG.md
  - Updated all cross-references

---

## Completed (2025-11-04)

✅ **Marine forecasts & warning banners**
  - Created dedicated forecasts page (`~/site/forecasts.html`)
  - Dismissible warning banners across all pages
  - localStorage-based state management (24h expiry)
  - Severity-based color coding (Storm/Gale/Strong Wind)
  - Mobile-optimized compact layout (50% height reduction)
  - Smooth scroll-to-zone navigation
  - Framework evaluation (decided to stay vanilla JS)

---

## Completed (2025-11-02)

✅ **UI/UX enhancements**
  - Directional arrows on buoy cards (wind/wave directions)
  - Navigation links (card → map/charts)
  - Tide page padding reduction (38-50%)
  - Station metadata display (badges, coordinates, DFO codes)
  - Wave breaking threshold annotations

---

## Completed (2025-11-01)

✅ **Station registry system**
  - Created master `stations.json` with all station metadata
  - Unified buoy and tide station data
  - Interactive Leaflet map embedded on index.html
  - Color-coded markers (EC buoys, NOAA buoys, DFO tides)
  - Click markers to center map and view details

---

## Completed (2025-10-31)

✅ Implemented tide monitoring page (`~/site/tides.html`)
  - Station selector dropdown
  - Auto-loads Point Atkinson by default
  - Current observation and prediction display
  - High/low tide table
  - 28-hour ECharts tide prediction chart
  - Responsive design (mobile, tablet, desktop)

✅ Fixed chart horizontal compression issues
  - Standardized 1200px max-width across all chart sections
  - Moved inline styles to CSS files
  - Fixed grid layout with percentage-based margins

✅ Updated documentation (README.md and CLAUDE.md)
  - Added tide system architecture
  - Documented new scripts and cron jobs
  - Added frontend structure documentation
