# Database Schema Cleanup Analysis

**Date:** 2025-12-19
**Status:** Analysis Complete - Ready for Migration

---

## Executive Summary

Analyzed 56 columns in `buoy_observation` and 20 columns in `wind_observation` tables. Identified **2 columns safe to drop immediately** and documented **several opportunities for future cleanup** after field unification completes.

### Quick Stats
- **buoy_observation**: 56 columns → Can drop 2 (water_level fields)
- **wind_observation**: 20 columns → Can drop 1 (wind_direction) after backfill

---

## buoy_observation Table Analysis (56 columns)

### ✅ Columns ACTIVELY USED (46 columns)

These fields are in the `ALL_FIELDS` export list and are actively queried/displayed:

**Wave Heights (6):**
- wave_height_sig, wave_height_peak, wave_height_max, wave_height_avg
- wave_height_spectral, wave_crest_height_max

**Wave Periods (9):**
- wave_period_sig, wave_period_avg, wave_period_peak, wave_period_max_wave
- wave_period_spectral, wave_period_energy_spectral, wave_period_sig_basic
- wave_height_max_avg, wave_period_max_avg

**Wave Directions (4):**
- wave_direction_avg, wave_direction_peak
- wave_direction_spread_avg, wave_direction_spread_peak

**NOAA Spectral Data (6):**
- swell_height, swell_period, swell_direction
- wind_wave_height, wind_wave_period, wind_wave_direction

**Wind - Primary Sensor (4):**
- wind_speed, wind_gust, wind_direction, wind_sensor_height

**Wind - Secondary Sensor (5):**
- wind_speed_sensor_2, wind_gust_sensor_2, wind_direction_sensor_2
- wind_samples_bad_1, wind_samples_bad_2

**Temperature (2):**
- air_temp, sea_temp

**Pressure (5):**
- pressure, pressure_msl, pressure_sensor_2
- pressure_trend_char, pressure_trend_amount

**Position (2):**
- buoy_lat_current, buoy_lon_current

**Solar (1):**
- solar_current

**System Health (3):**
- battery_voltage, watchman_boot_count, obstruction_lamp_current

**Orientation (2):**
- compass_heading_1, compass_heading_2

### 🗑️ SAFE TO DELETE (2 columns)

**water_level_predicted** (262 rows out of 75,638 = 0.3%)
- **History**: Added during Surrey wave integration, removed Dec 11-12, 2024
- **Issue**: Mixed tide predictions with wave observations (see KNOWN_ISSUES.md)
- **Current state**: No longer populated by any script
- **Frontend**: Not used
- **Decision**: **DELETE** ✅

**water_level_observed** (423 rows out of 75,638 = 0.6%)
- **History**: Same as above - leftover from Surrey integration bug
- **Current state**: No longer populated (removed from `fetch_surrey_wave_v2.py`)
- **Frontend**: Not used
- **Decision**: **DELETE** ✅

### 📊 Metadata Columns (6 columns)

**KEEP** - Required for database operation:
- id (PRIMARY KEY)
- buoy_id (station identifier)
- observation_time (timestamp)
- source_file (provenance)
- recorded_at (insert timestamp)

---

## wind_observation Table Analysis (20 columns)

### ✅ Columns ACTIVELY USED (11 columns)

**Core Wind Fields (7):**
- wind_speed_kmh ✅ Exported
- wind_gust_kmh ✅ Exported
- wind_direction_deg ✅ Exported (NEW unified field)
- air_temp_c ✅ Exported
- pressure_hpa ✅ Exported
- rainfall_1hr_mm ✅ Exported
- rainfall_6hr_mm ✅ Exported

**Additional Fields - STORED but NOT EXPORTED (4):**
- humidity_percent (82,376 rows = 66.6%) ⚠️
- dewpoint_c (82,376 rows = 66.6%) ⚠️
- pressure_mslp_hpa (sparse data) ⚠️
- visibility_km (1,431 rows = 1.2%) ⚠️

**Status**: These 4 fields were added Nov 18, 2025 but NEVER added to export script!
**Frontend**: None of these are displayed in UI
**Decision**: Either add to export OR drop (recommend adding to export - useful data!)

### 🔄 FIELD UNIFICATION IN PROGRESS (2 columns)

**wind_direction** (INTEGER) - OLD field
- **Data**: 110,050 rows (89% of database) - mostly historical
- **Status**: Being phased out in favor of wind_direction_deg
- **Decision**: **Keep for now**, drop after backfilling wind_direction_deg

**wind_direction_deg** (REAL) - NEW unified field
- **Data**: 7,891 rows (6.4%) - only recent data
- **Status**: Active field, all new ingestion uses this
- **Decision**: **KEEP** ✅

### 📝 Calculated Fields from 3rd Party Sources (2 columns)

**wind_chill_c** (1,033 rows = 0.8%)
- **Source**: Jericho, NWS weather fetch scripts
- **Status**: Populated for specific stations (JERICHO, KBLI, KORS)
- **Frontend**: NOT displayed
- **Decision**: **KEEP** (might be useful for future features)

**heat_index_c** (559 rows = 0.5%)
- **Same as wind_chill_c**
- **Decision**: **KEEP** (might be useful for future features)

### 📊 Metadata Columns (5 columns)

**KEEP** - Required for database operation:
- id (PRIMARY KEY)
- station_id
- observation_time
- source_file
- recorded_at
- station_name (metadata)

---

## Recommended Actions

### Immediate (Safe & Simple)

**1. Drop unused buoy water_level columns:**
```sql
-- buoy_data.sqlite
ALTER TABLE buoy_observation DROP COLUMN water_level_predicted;
ALTER TABLE buoy_observation DROP COLUMN water_level_observed;
```
**Impact**: Removes 2 unused columns, reclaims minimal space
**Risk**: NONE - these fields are not populated or used

### Near-term (After Field Unification)

**2. Add missing fields to wind export:**

Edit `scripts/export/export_wind_json.py`:
```python
ALL_FIELDS = [
    "wind_speed_kmh",
    "wind_gust_kmh",
    "wind_direction_deg",
    "air_temp_c",
    "pressure_hpa",
    "rainfall_1hr_mm",
    "rainfall_6hr_mm",
    # ADD THESE:
    "humidity_percent",      # 66% populated!
    "dewpoint_c",            # 66% populated!
    "pressure_mslp_hpa",     # Sparse but useful
    "visibility_km",         # 1% populated
]
```

**3. Backfill wind_direction_deg from wind_direction:**
```sql
-- Populate wind_direction_deg for all rows that only have wind_direction
UPDATE wind_observation
SET wind_direction_deg = CAST(wind_direction AS REAL)
WHERE wind_direction IS NOT NULL
  AND wind_direction_deg IS NULL;
```

**4. Drop old wind_direction column:**
```sql
ALTER TABLE wind_observation DROP COLUMN wind_direction;
```

---

## Future Considerations

### Frontend Display Opportunities

These fields are **stored and exported** but **not displayed** in UI:

**Buoy Fields:**
- System health: battery_voltage, watchman_boot_count, obstruction_lamp_current
- Orientation: compass_heading_1, compass_heading_2
- Secondary sensors: wind_speed_sensor_2, wind_gust_sensor_2, etc.

**Wind Fields:**
- humidity_percent (once added to export)
- dewpoint_c (once added to export)
- wind_chill_c
- heat_index_c

**Recommendation**: Consider adding a "Station Details" or "Advanced Metrics" section to website.

### Database Size Impact

Current database sizes:
- buoy_data.sqlite: 23.0 MB (75,638 rows)
- wind_data.sqlite: ~30 MB est. (123,645 rows)

Dropping 2-3 columns will have **minimal impact** on database size (< 1% reduction). The real benefit is:
- Cleaner schema
- Less confusion for developers
- Faster schema introspection
- Reduced backup size (marginal)

---

## Migration Plan

### Phase 1: Immediate Cleanup (LOW RISK)
1. Create backup of buoy_data.sqlite
2. Drop water_level_predicted column
3. Drop water_level_observed column
4. Verify exports still work
5. Monitor for 24 hours

### Phase 2: Wind Export Enhancement (MEDIUM EFFORT)
1. Update export_wind_json.py to include 4 missing fields
2. Test JSON output format
3. (Optional) Update frontend to display new fields
4. Deploy

### Phase 3: Field Unification Completion (NEEDS TESTING)
1. Create backup of wind_data.sqlite
2. Run backfill script (UPDATE wind_direction_deg)
3. Verify all rows now have wind_direction_deg
4. Update any remaining scripts that reference wind_direction
5. Drop wind_direction column
6. Deploy

---

## Testing Checklist

Before applying migrations:

- [ ] Create full database backups
- [ ] Test export scripts produce valid JSON
- [ ] Check website displays data correctly
- [ ] Verify no error logs after 24 hours
- [ ] Confirm station status debug script shows all active

After migration:

- [ ] Run `python3 scripts/debug_station_status.py`
- [ ] Check ~/site/data/latest_buoy_v2.json
- [ ] Check ~/site/data/latest_wind.json
- [ ] Visit website and verify all stations visible
- [ ] Check for JavaScript console errors

---

## Files to Modify

### Phase 1 (Drop water_level columns):
- None (SQL only)

### Phase 2 (Wind export enhancement):
- `scripts/export/export_wind_json.py` (add 4 fields to ALL_FIELDS)
- `~/site/assets/js/wind-stations.js` (optional - display new fields)

### Phase 3 (Drop old wind_direction):
- `scripts/parse/wind_to_sqlite.py` (remove wind_direction from CREATE TABLE if still there)
- Any other scripts that might reference the old field (grep first)

---

## Rollback Plan

If something breaks:

### Phase 1 Rollback:
```bash
# Restore from backup
cp ~/.local/share/buoy_data.sqlite.backup ~/.local/share/buoy_data.sqlite
```

### Phase 2 Rollback:
```bash
# Git revert the export script changes
git checkout HEAD -- scripts/export/export_wind_json.py
python3 scripts/export/export_wind_json.py
```

### Phase 3 Rollback:
```bash
# Restore from backup (column drops are irreversible without backup!)
cp ~/.local/share/wind_data.sqlite.backup ~/.local/share/wind_data.sqlite
```

---

## Monitoring

After each phase, monitor these metrics for 24-48 hours:

```bash
# Check for errors in logs
tail -f ~/envcan_wave/*.log

# Verify station counts remain stable
python3 scripts/debug_station_status.py

# Check database sizes
ls -lh ~/.local/share/*.sqlite

# Verify cron jobs succeed
grep "ERROR\|WARN" ~/envcan_wave/*.log | tail -20
```

---

## Conclusion

**Recommended immediate action:**
- Drop 2 water_level columns from buoy_observation (safe, low impact)

**Recommended follow-up:**
- Add 4 missing fields to wind export (unlock stored data!)
- Backfill and drop old wind_direction field (complete unification)

**Total effort:** 2-3 hours for all phases
**Risk level:** LOW (with proper backups and testing)
**Benefit:** Cleaner schema, unlocks hidden data, completes field unification
