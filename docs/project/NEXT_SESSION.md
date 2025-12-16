# Next Session Plan

**Date created:** 2025-12-16

## Quick Wins (1-2 hours)

### 1. Documentation Updates
- [ ] Update `CLAUDE.md` with:
  - Webcams section (3 webcams: White Rock Pier, White Rock East Beach, Cox Bay)
  - Lightstations section (23 stations)
  - NOAA land stations (CPMW1, SISW1)
  - Updated station counts throughout
  - Note about field name differences (wind_direction vs wind_direction_deg)

### 2. Script Organization
- [ ] Create subdirectories:
  - `scripts/fetch/` - All fetch_* scripts (12 scripts)
  - `scripts/export/` - All export_* scripts (9 scripts)
  - `scripts/parse/` - Parser scripts (3 scripts)
  - `lib/` or `utils/` - Shared modules (config.py, units.py, directions.py, logging_config.py, stations.py)
- [ ] Update import paths across codebase
- [ ] Update cron jobs with new paths
- [ ] Test all scripts after move

### 3. Documentation Cleanup
- [ ] Review and consolidate duplicate/outdated docs
- [ ] Ensure consistency across README, CLAUDE.md, and other docs
- [ ] Archive obsolete documentation

## Technical Debt Items (Future Sessions)

### High Priority
1. **Field Name Unification** (2-4 hours)
   - Problem: Buoys use `wind_direction`, wind stations use `wind_direction_deg`
   - Caused the map bug we fixed today (stations-map.js:370)
   - Solution: Standardize on one field name across both databases
   - Impact: Eliminates entire class of frontend bugs

2. **Database Schema Audit** (4-6 hours)
   - `buoy_observation` has 58+ columns, many unused
   - Review what's actually used vs what's cruft
   - Opportunity to clean up and optimize

### Medium Priority
3. **Export Script Consolidation** (3-5 hours)
   - Many export scripts share similar patterns
   - Create base exporter class with common logic
   - Reduce code duplication

4. **Station Registry Enforcement** (2-3 hours)
   - Some scripts hardcode station lists
   - Enforce using `stations.py` everywhere
   - Single source of truth

### Low Priority
5. **Type Hints & Testing**
   - Add type annotations for better IDE support
   - Create pytest suite for critical functions

## Bugs Fixed This Session

1. ✅ **Wind direction arrows missing** (stations-map.js)
   - Root cause: Only checked `wind_direction_deg` (wind stations), not `wind_direction` (buoys)
   - Fixed: Line 370 now checks both with fallback
   - Commit: 7b49f5c

2. ✅ **Webcam markers disappeared**
   - Root cause: Webcams added to frontend `stations.json` but not backend config
   - `export_stations_json.py` overwrote frontend file
   - Fixed: Added webcams to backend `config/stations.json`
   - Commits: 83ece2e, c0f6358

## Files Modified This Session

### Backend (envcan_wave)
- `config/stations.json` - Added webcams section
- `export_stations_json.py` - Improved validation for all station types
- `export_wind_json.py` - Added CPMW1, SISW1, COLEB
- `fetch_surrey_wave_v2.py` - Route COLEB to wind database
- `fetch_noaa_land.py` - New script for NOAA land stations

### Frontend (site)
- `assets/js/stations-map.js` - Fixed wind direction field name handling

## Notes

- All changes committed and pushed
- No breaking changes
- Webcams will now persist through `export_stations_json.py` runs
- Field name inconsistency (wind_direction vs wind_direction_deg) should be addressed in future refactor
