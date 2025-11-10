# TODO List

## Upcoming Tasks

### Storm Surge & Tides

- **Find tide measurement station near Tofino**
  - Research DFO IWLS stations in Tofino/Ucluelet area
  - Add tide station to `tide_stations.json`
  - Add corresponding GDSPS storm surge location
  - Enable hindcast validation for west coast exposure
  - Priority: Compare exposed ocean coast vs protected inland waters

### Tides Page Enhancements

- **Fix ECharts "connect nulls" issue in tide charts**
  - Chart is connecting lines across data gaps when it shouldn't
  - ECharts needs explicit null values at gap timestamps to avoid connecting
  - Need to inject null entries in time series where data is missing
  - Affects all chart pages (buoys, tides, storm surge)
  - Reference: ECharts connectNulls option + data array structure

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
