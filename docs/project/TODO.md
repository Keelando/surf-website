# TODO List

## Upcoming Tasks

### Home Assistant / InfluxDB Removal (Future Consideration - 2025-11-18)

**Current state:**
- InfluxDB is only used for Home Assistant integration
- `influx_to_mqtt.py` runs every minute, publishes EC buoy data to HA
- Parsers (`buoy_to_influx_sqlite.py`, `wind_to_sqlite.py`) have soft dependency on InfluxDB
- SQLite is primary storage for everything else (website, exports)

**Decision:** No more data needs to be sent to Home Assistant

**Future cleanup options:**
- [ ] Stop `influx_to_mqtt.py` cron job
- [ ] Remove InfluxDB soft dependencies from parsers
- [ ] Remove InfluxSink classes from `buoy_to_influx_sqlite.py` and `wind_to_sqlite.py`
- [ ] Uninstall InfluxDB client library
- [ ] Remove `~/.config/buoy_influx_1.env` config file
- [ ] Update documentation to remove InfluxDB references

**Benefits:** Simplified architecture, one less dependency, cleaner code

**Note:** Not urgent - current setup works fine, InfluxDB gracefully degrades if unavailable

---

### API Polling Frequency Review (NEW - 2025-11-18)

**Goal:** Be gentle on external APIs - reduce unnecessary polling

**Current frequencies:**
- NOAA buoys: Every 20 min (72x/day)
- Surrey FlowWorks: Every 20 min (72x/day)
- Tide observations: Every 30 min (48x/day)
- Storm surge: 4x/day (reasonable)

**Recommendations:**
- [ ] NOAA: Reduce to 30-60 min intervals (24-48x/day)
  - Stations update 10-60 min depending on type
  - Current 20min is conservative but excessive
  - Suggested: */30 or 5,35 pattern

- [ ] Surrey: Reduce to 30 min intervals (48x/day)
  - FlowWorks updates every 10 min
  - Current 20min is reasonable but could be gentler
  - Suggested: */30 pattern

- [ ] Tides: Keep at 30 min (48x/day) ✓
  - DFO updates every 6 min
  - 30 min is already gentle

**Action items:**
- [ ] Update crontab with new schedules
- [ ] Monitor for any stale data issues
- [ ] Document changes in DEPLOYMENT.md

---

### Wind Stations ✅ COMPLETED (2025-11-18)

**All 10 stations now operational via SR3:**
1. ✅ Sisters Islets - `CWGT`
2. ✅ Ballenas - `CWGB`
3. ✅ Entrance Island - `CWEL`
4. ✅ Point Atkinson - `CWSB`
5. ✅ Tsawwassen - `CVTF`
6. ✅ Sand Heads - `CWVF`
7. ✅ Saturna - `CWEZ`
8. ✅ Race Rocks - `CWQK`
9. ✅ YVR Airport - `CYVR`
10. ✅ Boundary Bay Airport - `CZBB` (added 2025-11-18)

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

   **📊 Quick Start - Available Data:**

   Backend already provides:
   - `~/site/data/latest_wind.json` - Current conditions (all units pre-converted)
     - `wind_speed_kt` / `wind_gust_kt` (already in knots, not km/h)
     - `wind_direction_deg` + `wind_direction_cardinal` (e.g., "W", "NE")
     - `air_temp_c`, `pressure_hpa`, `rainfall_1hr_mm`, `rainfall_6hr_mm`
     - `stale: true/false` (auto-calculated, >2hr = stale)
     - `observation_time` (ISO 8601 timestamp)

   - `~/site/data/wind_timeseries_24hr.json` - 24hr historical (hourly samples)
     - Same fields as above, in `{"time": "...", "value": ...}` format
     - Ready for ECharts (just map to series data)

   - Station metadata in `config/stations.json` under `"wind"` key
     - IDs: CWGT, CWGB, CWEL, CWSB, CVTF, CWVF, CWEZ, CWQK, CYVR
     - Includes lat/lon for map integration

   **Copy patterns from:**
   - `~/site/index.html` - Buoy cards, staleness indicators, wind arrows
   - `~/site/charts.html` - ECharts 24hr timeseries
   - `~/site/tides.html` - Station selector dropdown

   **Tasks:**
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

**BC Stations (Custom/Non-SWOB-ML):**
- **Jericho Wind Station** (Jericho Sailing Centre)
  - URL: https://jsca.bc.ca/main/downld02.txt
  - Format: Custom text file (non-SWOB-ML)
  - Would need dedicated fetch script similar to Surrey FlowWorks integration
  - Priority: High (excellent English Bay coverage for sailors)

- **YVR Airport - NavCanada AeroView** (Alternative/Supplement to SWOB-ML)
  - URL: https://spaces.navcanada.ca/workspace/aeroview/CYVR
  - Update frequency: Every minute (vs hourly SWOB-ML)
  - Format: Unknown - need to investigate API/data feed availability
  - Would provide higher resolution wind data for YVR
  - Priority: Medium (enhancement to existing CYVR station)
  - Note: Investigate if NavCanada provides a public data API or parseable feed

- **Ambleside** (West Vancouver)
  - Location: Near Ambleside Park
  - Data source: TBD (possibly municipal or private weather station)
  - Need to identify data feed and format
  - Priority: Medium (complements Point Atkinson coverage)

**US Stations (NOAA) - Southern Salish Sea / Puget Sound:**
- **Orcas Island Airport (KORS)** - San Juan Islands
  - Format: METAR (aviation weather)
  - URL: `https://tgftp.nws.noaa.gov/data/observations/metar/stations/KORS.TXT`
  - Priority: High (fills coverage gap in San Juans)

- **Cherry Point, WA** - Northern Puget Sound
  - Likely source: NOAA CO-OPS or military weather station
  - Need to identify specific station ID
  - Priority: High (industrial/ferry terminal area)

- **Sandy Point Shores, WA** - Near Canadian border
  - Likely source: NOAA or local weather network
  - Need to identify station ID and data feed
  - Priority: Medium

- **Libbey Beach, WA** - Whidbey Island area
  - Need to identify data source
  - Priority: Medium

**Implementation Notes:**
- All US/custom stations would integrate into existing `wind_data.sqlite` database
- Data sources to investigate:
  - NOAA CO-OPS API: `https://api.tidesandcurrents.noaa.gov/` (coastal meteorological)
  - NOAA METAR: `https://tgftp.nws.noaa.gov/data/observations/metar/stations/`
  - NOAA NWS API: `https://api.weather.gov/`
- Parser scripts would follow same pattern as `wind_to_sqlite.py`
- Export scripts already handle any station IDs added to WIND_STATIONS dict

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
