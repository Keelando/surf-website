# Next Session Plan

**Last updated:** 2026-01-15
**Status:** Maintenance mode - major features complete

---

## 🎯 POTENTIAL NEXT PRIORITIES

### Priority 1: Health Monitoring System (Phase 1)

**Effort:** 2-3 hours
**Impact:** HIGH - Makes future refactors painless

#### Create Health Check Script

**File:** `scripts/monitoring/health_check.py`

**Checks to implement:**
1. **Data Freshness** - Flag stations >2hrs old (warning), >4hrs old (error)
2. **Cron Job Monitoring** - Verify critical jobs ran recently via syslog
3. **Database Integrity** - Check size, recent writes, WAL mode
4. **Export File Freshness** - Verify JSON exports are recent and parseable

**Output:** `/home/keelando/site/data/system_health.json`

```json
{
  "generated_utc": "2025-12-19T12:00:00Z",
  "overall_status": "warning",
  "checks": {
    "data_freshness": {
      "status": "warning",
      "stale_stations": [
        {"id": "CPMW1", "name": "Cherry Point", "age_hours": 4.5}
      ]
    },
    "cron_jobs": {"status": "ok", ...},
    "database_integrity": {"status": "ok", ...},
    "export_files": {"status": "ok", ...}
  }
}
```

**Add to crontab:**
```bash
0 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/monitoring/health_check.py >> /home/keelando/envcan_wave/logs/health_check.log 2>&1
```

**Success criteria:**
- [ ] Script runs successfully in <5 seconds
- [ ] Detects stale data (e.g., Cherry Point at 4+ hours)
- [ ] Identifies missing/overdue cron jobs
- [ ] JSON validates against expected schema
- [ ] Running hourly via cron

**Documentation:**
- See `docs/project/HEALTH_MONITORING_PLAN.md` for full implementation details

---

## ✅ COMPLETED (2025-12-19)

### Staleness Threshold Unification + Lightstation Date Bug ✅
**Completed:** 2025-12-19 (afternoon session)

**Issues resolved:**
1. ✅ **Unified staleness thresholds across all data types**
   - Buoys/Wind/Tides: 3 hours (appropriate for frequent updates)
   - Lightstations: 12 hours (appropriate for 3-hour update cycles)
   - All displays now use backend `stale` flag consistently
   - Frontend warnings updated to match backend thresholds

2. ✅ **Lightstation date bug (MAJOR FIX)**
   - Issue: Dates showing 6 days in the future (Dec 25 instead of Dec 19)
   - Root cause: Parser threshold `> 5 days` didn't trigger when difference was exactly 5 days
   - Fix: Changed to `> now + 1 hour` - never accept future dates for current observations
   - Parser now correctly handles month boundaries

3. ✅ **Transparent stale markers on maps**
   - Stale directional arrows now show at 35% opacity
   - Direction and values still visible but clearly faded
   - Applied to all map popups (winds-map.js, stations-map.js, lightstation-map.js)

4. ✅ **Prominent STALE indicators on all pages**
   - Buoys page: "⚠️ STALE (>3h old)"
   - Lightstations page: "⚠️ STALE DATA (>12h old)"
   - Map popups: Red backgrounds and "STALE" in headers
   - Lightstation popups unified across both maps

5. ✅ **Display consistency fixes**
   - Buoys page was calculating staleness client-side (could mismatch)
   - Now all pages use backend `stale` flag from JSON exports
   - Eliminates timing-based inconsistencies

**Files modified:**
- Backend: `sqlite_to_json.py`, `export_wind_json.py`, `export_tide_json.py`, `export_lightstation_json.py`, `parse_lightstation.py`
- Frontend: `main.js`, `lightstations.html`, `lightstation-map.js`, `stations-map.js`, `winds-map.js`

---

### Cherry Point Investigation + Logging Fixes ✅
**Completed:** 2025-12-19 (morning session)

**Issues resolved:**
1. ✅ Cherry Point "feed down" - Root cause: NOAA station hardware outage (not our code)
   - Data pipeline working correctly, station hasn't reported since 2025-12-18 22:54 UTC
   - Cron job running every 20 min but finding no new observations (all duplicates)

2. ✅ Log file location mismatch - Fixed duplicate log directories
   - Was: Scripts logging to `lib/logs/`, cron redirecting to `logs/`
   - Now: Everything consolidated to `/home/keelando/envcan_wave/logs/`
   - File: `lib/logging_config.py:28` changed from `Path(__file__).parent / "logs"` to `Path(__file__).parent.parent / "logs"`

3. ✅ Stale station links added to warning card
   - Users can now click station names in stale data warning to check source feeds
   - Reuses sourceLinks from main table for consistency
   - File: `/home/keelando/site/assets/js/wind-stations.js`

4. ✅ Wind direction field unification complete
   - Fixed last reference in `export_wind_24hr_timeseries.py`
   - All scripts now use correct field names (wind_direction_deg for wind stations, wind_direction for buoys)

**Commits:** 8848620, 20484ae, 35fabf5

---

### Wind Direction Field Unification ✅
**Completed in:** 2025-12-18 - commits 0a09976 (backend), c1f9375 (frontend)

**Backend:**
- ✅ Updated `export_wind_json.py` to query `wind_direction_deg` field
- ✅ Fixed cardinal direction calculation
- ✅ Moved Colebrook from buoys to wind stations (it's a land wind station)
- ✅ Added White Rock East Beach (`whiterock_pier`) to wind stations registry

**Frontend:**
- ✅ Updated `stations-map.js` to use `wind_direction_deg || wind_direction`
- ✅ Updated `winds-map.js` to use unified field (4 locations)
- ✅ Updated `wind-stations.js` to use unified field (4 locations)
- ✅ Updated `main.js` to use unified field (buoy cards)

**Webcam Coordinates Fixed:**
- ✅ All 3 webcams updated to accurate GPS coordinates
- ✅ Created `docs/WEBCAM_COORDINATES.md` as authoritative reference
- ✅ Hard-coded in 4 places so they never get lost again
- ✅ White Rock Pier: 49.021719°N, 122.807111°W
- ✅ White Rock East Beach: 49.01647°N, 122.79082°W
- ✅ Cox Bay: 49.106802°N, 125.872949°W

**Result:**
- All wind direction arrows working on both maps
- White Rock East Beach now on winds map
- Colebrook shows as land wind station (not wave)
- Backend/frontend fully unified on `wind_direction_deg`

---

## ✅ Quick Wins - COMPLETED (2025-12-17)

### 1. Documentation Updates ✅
- [x] Update `CLAUDE.md` with:
  - Webcams section (3 webcams: White Rock Pier, White Rock East Beach, Cox Bay)
  - Lightstations section (23 stations)
  - NOAA land stations (CPMW1, SISW1)
  - Updated station counts throughout
  - Note about field name differences (wind_direction vs wind_direction_deg)
- **Completed in:** commit 374d589

### 2. Script Organization ✅
- [x] Create subdirectories:
  - `scripts/fetch/` - All fetch_* scripts (11 scripts)
  - `scripts/export/` - All export_* scripts (13 scripts)
  - `scripts/parse/` - Parser scripts (5 scripts)
  - `lib/` - Shared modules (config.py, units.py, directions.py, logging_config.py, stations.py)
- [x] Update import paths across codebase
- [x] Update cron jobs with new paths
- [x] Test all scripts after move
- **Completed in:** commit 374d589

### 3. Documentation Cleanup ✅
- [x] Review and consolidate duplicate/outdated docs
- [x] Ensure consistency across README, CLAUDE.md, and other docs
- [x] Archive obsolete documentation (moved to archive/docs/refactoring_plans/completed/)
- **Completed in:** commits 374d589, today's session

---

## 🎯 CURRENT PRIORITY: Database Schema Cleanup

**Effort:** 4-6 hours
**Impact:** MEDIUM-HIGH - Reduces bloat, improves performance
**Status:** Ready to execute

### Next Up: Database Schema Audit

**Why now:** With field unification complete, we can safely audit and clean up unused columns.

**Goals:**
1. Review `buoy_observation` table (58+ columns, many unused)
2. Review `wind_observation` table - identify deprecated fields
3. Document which columns are actually used vs cruft
4. Create migration plan to drop unused columns
5. Update any scripts that might reference old fields

**Approach:**
1. Query actual column usage in export scripts
2. Check frontend JavaScript for field references
3. Identify safe-to-remove columns (never used, deprecated)
4. Create SQL migration script with DROP COLUMN statements
5. Test thoroughly before applying

---

## 📋 Technical Debt Items (Future Sessions)

### High Priority
1. **Database Schema Audit** (4-6 hours)
   - `buoy_observation` has 58+ columns, many unused
   - Review what's actually used vs what's cruft
   - Opportunity to clean up and optimize

### Low Priority
2. **Export Script Consolidation** (3-5 hours) - LOW VALUE
   - Export scripts already use shared `lib/` utilities (units, directions, config, stations, logging_config)
   - Remaining duplication is SQLite connection boilerplate and staleness logic
   - Scripts have legitimate differences (per-field freshness, different databases)
   - Moderate effort for modest benefit - not urgent

3. **Type Hints & Testing**
   - Add type annotations for better IDE support
   - Create pytest suite for critical functions

### ✅ Completed
- **Station Registry Enforcement** - All scripts now use `lib/stations.py`

## Bugs Fixed This Session (2025-12-18)

1. ✅ **Wind direction arrows broken on all maps**
   - Root cause: Backend switched to `wind_direction_deg` but frontend still used `wind_direction`
   - Fixed: Updated all frontend JS files to use `wind_direction_deg || wind_direction`
   - Affected files: stations-map.js, winds-map.js, wind-stations.js, main.js

2. ✅ **White Rock East Beach missing from winds map**
   - Root cause: `whiterock_pier` not in wind stations registry
   - Fixed: Added to `stations.json` wind section with coordinates

3. ✅ **Colebrook showing as wave station**
   - Root cause: Located in buoys section instead of wind section
   - Fixed: Moved from buoys to wind stations in `stations.json`

4. ✅ **Webcam coordinates inaccurate (3rd-4th time)**
   - Root cause: Coordinates only stored in one place, kept getting lost
   - Fixed: Hard-coded in 4 documentation files as authoritative source
   - Created `docs/WEBCAM_COORDINATES.md` quick reference

## Bugs Fixed Previous Session (2025-12-16)

1. ✅ **Wind direction arrows missing** (stations-map.js)
   - Root cause: Only checked `wind_direction_deg` (wind stations), not `wind_direction` (buoys)
   - Fixed: Line 370 now checks both with fallback
   - Commit: 7b49f5c

2. ✅ **Webcam markers disappeared**
   - Root cause: Webcams added to frontend `stations.json` but not backend config
   - `export_stations_json.py` overwrote frontend file
   - Fixed: Added webcams to backend `config/stations.json`
   - Commits: 83ece2e, c0f6358

## Bugs Fixed This Session (2025-12-17)

1. ✅ **Lightstation popup links not working**
   - Root cause: querystring/ID conversion mismatch between map popup and dropdown
   - Fixed: lightstation-map.js:349 - simplified station name conversion
   - Also added check for 24hr data availability before navigating

2. ✅ **Webcam coordinates incorrect**
   - Root cause: Coordinates added to frontend but overwritten by backend export
   - Fixed: Updated backend config/stations.json with correct coordinates
   - Now persists through export runs

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
