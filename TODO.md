# TODO List

## Upcoming Tasks

### 🗺️ Interactive Map of Stations & Buoys

**Goal:** Add a Leaflet.js embedded map showing all monitored stations and buoys

**Implementation plan:**
- Add new page: `~/site/map.html` or embed on index/tides page
- Use Leaflet.js for interactive map
- Plot markers for:
  - **Wave Buoys** (5 total):
    - Environment Canada: Halibut Bank, English Bay, Southern Georgia Strait, Sentry Shoal
    - NOAA: Neah Bay (46087), New Dungeness (46088)
  - **Tide Stations** (8+ total) - coordinates already available in `/home/keelando/envcan_wave/tide_stations.json`
    - Point Atkinson (49.3375, -123.253583)
    - Kitsilano (49.276583, -123.13936)
    - Tsawwassen (49.00677, -123.12933)
    - White Rock (49.016667, -122.8)
    - Crescent Beach (49.033333, -122.883333)
    - New Westminster (49.2, -122.91)
    - Campbell River (50.042, -125.247)
    - Rose Harbour (52.1552, -131.0909)

**Data sources:**
- Tide stations: `~/envcan_wave/tide_stations.json` (has lat/lon already)
- Buoy coordinates: Need to gather from EC/NOAA sources
  - Could scrape from NOAA NDBC station pages
  - Or add to a `buoy_stations.json` file

**Features to consider:**
- Color-coded markers (EC buoys, NOAA buoys, DFO tide stations)
- Popups showing station name and current conditions
- Click to navigate to relevant data on site
- Zoom to Salish Sea region by default
- Responsive design for mobile

**Priority:** Medium (nice-to-have enhancement)

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
