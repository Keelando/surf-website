# Known Issues

## Surrey Water Level Integration Bug (Dec 11, 2024)

### Issue
When adding `water_level_predicted` and `water_level_observed` channels to Surrey stations, the wave/wind data stopped appearing in exports and on the map.

### Root Cause
Different channels update at different timestamps, creating separate database rows:
- Water level data: timestamps at :00, :20
- Wave/wind data: timestamps at :10, :30

When water_level rows became the "latest" observation, they had NULL wave/wind fields. The export script skips stations where the latest row has only NULL values.

### Solution
Deleted NULL-heavy rows from database to restore wave/wind data as "latest".

### Prevention
1. **Test before deploying**: After adding channels, manually run fetch and check database for NULL rows
2. **Monitor exports**: Watch for "Skipped" messages in json_export.log
3. **Sanity checks**: Add automated detection for consecutive NULL-heavy rows

### Future Fix
- Refactor fetch to collect all channels in single transaction
- OR: Separate tide data into different table/database
- OR: Improve upsert logic to merge data from different timestamps

### Files Affected
- `fetch_surrey_wave_v2.py` - Channel definitions
- `sqlite_to_json.py` - Export logic (looks at latest observation_time)
- `buoy_data.sqlite` - Database rows with mixed NULL/valid data
