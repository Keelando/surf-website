# TODO List

## Upcoming Tasks

### Map Enhancements - Direction Arrows & Data Labels (Medium Urgent)

**Goal:** Add visual direction indicators and data labels to map markers

**Implementation:**
- [ ] **Wind stations map (winds.html)**
  - Add rotated arrow icons showing wind direction at each station
  - Display current wind speed (e.g., "25 kt") near marker
  - Consider color-coding by wind speed intensity

- [ ] **Buoy map (if exists, or future enhancement)**
  - Add wave direction arrows
  - Display key metrics: wave height (e.g., "0.7 m"), swell height, etc.
  - Consider separate arrows for swell vs wind waves (NOAA buoys)

**Technical approach:**
- Use Leaflet's DivIcon for custom HTML markers with rotation
- Update markers when data refreshes
- Ensure readable on both light/dark backgrounds
- Mobile-friendly sizing

**Benefits:**
- Immediate visual understanding of conditions at a glance
- No need to click markers to see basic data
- Better spatial awareness of wind/wave patterns across region

**Priority:** Medium-High (improves usability significantly)

---

### Lightstation Map Labels - Zoom-Dependent Display

**Goal:** Show lightstation names as labels above map icons, but only at appropriate zoom levels to prevent clutter

**Implementation:**
- [ ] Add station name labels above lightstation markers
- [ ] Implement zoom-level threshold (e.g., only show labels at zoom ≥ 8 or 9)
- [ ] Position labels above icons with clean typography
- [ ] Consider subtle background or text shadow for readability
- [ ] Ensure labels don't overlap at higher zoom levels

**Technical approach:**
- Listen to Leaflet map `zoomend` event
- Toggle label visibility based on `map.getZoom()` threshold
- Use CSS transitions for smooth fade in/out
- Test optimal zoom level where labels don't overlap

**Benefits:**
- Quick identification of lightstations when zoomed in
- No visual clutter at overview zoom levels
- Better user experience for exploring specific regions

**Priority:** Low-Medium (nice enhancement for lightstation page)

---

### Lighthouse Performance Reports (Next Session)

Add Lighthouse performance auditing to monitor frontend performance and accessibility.

**Implementation ideas:**
- Automated Lighthouse CI reports for key pages (index, winds, tides, forecasts)
- Performance budgets and alerts
- Track metrics over time (performance, accessibility, best practices, SEO)
- Identify optimization opportunities

**Priority:** Medium (site works well, but good to monitor)

---

### Backend Data Audit (Future Task - Rainy Day Project)

**Goal:** Audit the backend data pipeline for errors and missed opportunities

**Tasks:**
- [ ] **Environment Canada XML audit**
  - Parse sample SWOB-ML XMLs to identify all available fields
  - Compare against fields currently being captured in `buoy_to_influx_sqlite.py` and `wind_to_sqlite.py`
  - Document fields we're currently ignoring/throwing away
  - Evaluate which additional fields would be useful (e.g., humidity, dewpoint, visibility for buoys)
  - Check if EC provides wave spectral data (period bands, directional spectra) that we're missing

- [ ] **NOAA data audit**
  - Review NOAA .txt and .spec file formats for additional fields
  - Check if we're missing any useful meteorological data
  - Verify all spectral wave components are being captured correctly

- [ ] **Error logging audit**
  - Review all parser logs for recurring errors or warnings
  - Check for data validation issues (malformed XMLs, unexpected values)
  - Identify stations with frequent data gaps or stale data
  - Look for silent failures in data processing

- [ ] **Database schema optimization**
  - Check for unused columns that could be removed
  - Identify missing indexes that would improve query performance
  - Consider adding data quality flags (e.g., sensor status, QC codes)

- [ ] **Data completeness report**
  - Generate statistics on data availability per station
  - Identify time gaps in historical data
  - Check if all expected fields are being populated

**Benefits:**
- Capture more useful data from existing sources
- Improve data quality and reliability
- Better understanding of system health

**Priority:** Low (system works well, but could be optimized)

---

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

### Wind Stations 🚧 IN PROGRESS (2025-11-19)

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

5. **Automation** ✅ COMPLETED (2025-11-19)
   - [x] Add cron jobs for wind data processing
     ```
     */1 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/wind_to_sqlite.py
     */5 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/export_wind_json.py
     */10 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/export_wind_24hr_timeseries.py
     ```
   - [x] Configure sr3 to auto-start on reboot (already configured)

6. **Documentation**
   - [ ] Update README.md with wind station info
   - [ ] Update CLAUDE.md with wind pipeline details
   - [ ] Update ARCHITECTURE_DETAILED.md with wind database schema
   - [ ] Document wind station IDs and metadata in stations.json

**COMPLETED (2025-11-19):**
- [x] Created winds.html page with sortable table
- [x] Data pipeline fully operational (parser, exports, cron)
- [x] Fixed station ID parsing bug (was matching year "2025" instead of station code)
- [x] 10 EnvCan wind stations + buoy wind data integrated
- [x] Removed ESTEVAN RCS test station
- [x] Added "Under Development" banner

**NEXT STEPS (Winds Page Enhancement):**
- [x] Add wind station coordinates to `stations.json` (10 stations) ✅ COMPLETED (2025-11-20)
  - CWGT (Sisters Island), CWGB (Ballenas), CWEL (Entrance Island)
  - CWSB (Point Atkinson), CVTF (Tsawwassen), CWVF (Sand Heads)
  - CWEZ (Saturna Island), CWQK (Race Rocks), CYVR (YVR), CZBB (Boundary Bay)
- [x] Implement interactive Leaflet map on winds.html ✅ COMPLETED (2025-11-20)
  - Created `winds-map.js` - wind-focused map showing only stations with wind data
  - Wind station markers (💨) show current wind speed, gust, direction, temp
  - Buoy markers (🌊) show wind data for buoys that report wind
  - Popups display real-time conditions with staleness indicators
- [x] Implement 24-hour wind trend charts ✅ COMPLETED (2025-11-19)
  - Station selector dropdown with search functionality
  - Wind speed/gust chart (knots)
  - Wind direction arrows (rotated symbols like buoy page)
  - Uses `wind_timeseries_24hr.json` + buoy timeseries data
- [x] Remove "Under Development" banner ✅ COMPLETED (2025-11-20)

**Additional Data Sources (Future Enhancement):**

**BC Lightstation Reports (Environment Canada):**
- **Source:** https://dd.weather.gc.ca/today/bulletins/alphanumeric/YYYYMMDD/FP/CWVR/HH/FPCN61_CWVR_DDHHMM___52863
- **Format:** Text-based reports (FPCN61)
- **Update frequency:** Every 3 hours
- **Data includes:**
  - Wind speed/direction (knots) - some stations estimated
  - Sea state (height, conditions: chop/moderate/rippled)
  - Swell direction and intensity (low/moderate/heavy)
  - Visibility (miles, fog/rain conditions)
- **Regions covered:**
  - Strait of Georgia (Cape Mudge, Chrome Island, Merry Island)
  - Juan de Fuca Strait
  - West Coast Vancouver Island (Cape Scott, Quatsino, Nootka, Estevan Point, Lennard Island, Cape Beale)
  - Central Coast (Chatham Point, Pulteney Point, Scarlett Point, Addenbroke Island, Dryad Point, Ivory Island, McInnes Island, Boat Bluff, Bonilla Island)
  - Hecate Strait (Langara Island, Green Island)
- **Implementation plan:**
  1. Create parser for text-based report format (period-delimited fields)
  2. Determine URL pattern for fetching latest report (date/hour path structure)
  3. Store parsed data in `wind_data.sqlite` or new `lightstation_data.sqlite`
  4. Export to JSON for website integration
  5. Add lightstation markers to winds map
  6. Schedule fetching every 3 hours via cron
- **Priority:** Medium (excellent coastal coverage, complements existing wind stations)
- **Note:** Text parsing will be simpler than XML/SWOB-ML but requires handling variations in format

**BC Stations (Custom/Non-SWOB-ML):**
- **Jericho Wind Station** (Jericho Sailing Centre) ⭐ CONFIRMED DATA SOURCE
  - URL: https://jsca.bc.ca/main/downld02.txt
  - Coordinates: 49.28°N, 123.2°W
  - Format: Fixed-width text table with header row
  - Update frequency: 30-minute intervals
  - Data fields to capture (standard set):
    - Wind speed (mph) → convert to knots
    - Wind gust (hi wind speed) → convert to knots
    - Wind direction (degrees)
    - Air temperature (°F) → convert to °C
    - Barometric pressure (mb) → convert to hPa
    - Rain amount (inches) → convert to mm
  - Fields available but NOT capturing: humidity, dew point, wind chill, heat index, indoor readings
  - Unit conversions needed: mph → knots (× 0.868976), °F → °C ((x-32)×5/9), mb → hPa (1:1), inches → mm (× 25.4)
  - Implementation:
    - Create `fetch_jericho_wind.py` similar to Surrey FlowWorks integration
    - Parse fixed-width text format (not CSV)
    - Insert into `wind_data.sqlite` with station_id 'JERICHO'
    - Add to `config/stations.json` under wind section
    - Integration with existing export scripts (already handle any station ID)
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

**US Stations (NOAA/NWS) - Southern Salish Sea / Puget Sound:**

- **Orcas Island Airport (KORS)** - San Juan Islands
  - **WANTED** - User confirmed priority for wind data
  - Format: METAR text OR NWS API (JSON)
  - URLs:
    - METAR: `https://tgftp.nws.noaa.gov/data/observations/metar/stations/KORS.TXT`
    - NWS API: `https://api.weather.gov/stations/KORS/observations/latest`
  - Priority: High (fills coverage gap in San Juans)
  - Implementation: Evaluate easiest approach (METAR vs NWS API)

- **Bellingham International Airport (KBLI)** - Bellingham, WA
  - **WANTED** - User confirmed priority for wind data
  - Format: NWS API (JSON - confirmed working, see sample data)
  - URL: `https://api.weather.gov/stations/KBLI/observations/latest`
  - Data available: wind speed/dir/gust, temp, dewpoint, pressure, humidity, visibility
  - Priority: High (northern Puget Sound coverage)
  - Coordinates: 48.8°N, -122.53°W
  - Implementation: If accumulating multiple NWS API stations, consider unified parser

- **Cherry Point, WA** - Northern Puget Sound
  - Likely source: NOAA CO-OPS or military weather station
  - Need to identify specific station ID
  - Priority: Medium (may be covered by KBLI)

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
  - **NWS API (RECOMMENDED)**: `https://api.weather.gov/stations/{STATION_ID}/observations/latest`
    - JSON format (easy parsing)
    - Rich data: wind, temp, dewpoint, pressure, humidity, visibility
    - Confirmed working: KBLI, KORS available
    - No API key required
    - If multiple NWS stations are added, create unified `fetch_nws_weather.py` parser
  - NOAA METAR (TEXT): `https://tgftp.nws.noaa.gov/data/observations/metar/stations/`
    - Alternative if NWS API unavailable
    - Requires METAR parsing (more complex)
  - NOAA CO-OPS API: `https://api.tidesandcurrents.noaa.gov/` (coastal meteorological)
- Parser scripts would follow same pattern as `wind_to_sqlite.py`
- Export scripts already handle any station IDs added to WIND_STATIONS dict
- Add KORS and KBLI to `config/stations.json` wind section when implementing

### White Rock Pier Webcam Integration (Needs Reimplementation)

**Old script (broken):**
```bash
#!/bin/sh
# Fetched latest frame from WR YouTube stream, saved with timestamp
rm /home/ubuntu/new_surf_604/604-surf-website/web/react-windswell/public/wrcam/*.jpg
filename="WR$(date +%s)"
cd /home/ubuntu/new_surf_604/604-surf-website/web/
ffmpeg -hide_banner -loglevel error -i "$(/usr/bin/pipenv run yt-dlp -g https://www.youtube.com/watch?v=4MK3E9EWDSY)" -frames 1 /home/ubuntu/new_surf_604/604-surf-website/web/react-windswell/public/wrcam/${filename}.jpg
echo {\"filename\":\"$filename\"}>/home/ubuntu/new_surf_604/604-surf-website/web/react-windswell/public/wrcam/latestimagefilename.json
/usr/local/bin/aws s3 cp /home/ubuntu/new_surf_604/604-surf-website/web/react-windswell/public/wrcam/${filename}.jpg s3://wr-pier-cam-images/
```

**Issues:**
- YouTube link likely broken or changed
- Needs ffmpeg installation
- Should be reimplemented for current infrastructure

**Implementation plan:**
- [ ] Verify White Rock Pier webcam source (find current YouTube stream or alternative)
- [ ] Install ffmpeg on server
- [ ] Create `fetch_wr_webcam.py` or bash script
- [ ] Output to `~/site/data/wrcam/` directory
- [ ] Add cron job (frequency TBD - every 5-10 min?)
- [ ] Add webcam image display to website (possibly on index or dedicated cam page)
- [ ] Decide on S3 upload requirement (archive old images?)

**Priority:** Medium-Low (nice-to-have, visual enhancement)

---

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
