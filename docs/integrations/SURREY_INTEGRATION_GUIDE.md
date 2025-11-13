# Surrey FlowWorks Integration Guide

## Overview
Integrate Surrey's FlowWorks wave and wind stations into your existing pipeline using the v2 API with JWT authentication.

## What You're Getting

### 3 New Stations:
1. **Crescent Beach Ocean (CRPILE)** - Full wave + wind + temp
   - Location: 49.0122°N, -122.9411°W (500m offshore)
   - Data: Wave height/period, wind, air/sea temp
   - Update: Every 10 minutes

2. **Crescent Channel (CRCHAN)** - Wind + radar wave
   - Location: 49.0536°N, -122.8969°W (channel marker)
   - Data: Wind, wave height (radar-based)
   - Update: Every 10 minutes

3. **Colebrook (COLEB)** - Wind only
   - Location: 49.0858°N, -122.845°W (pump house)
   - Data: Wind speed/direction/gust, air temp
   - Update: Every 10 minutes

---

## Files Created

### 1. `fetch_surrey_wave_v2.py`
Production-ready fetcher that:
- Authenticates with FlowWorks API v2
- Fetches last 2 hours of data
- Converts m/s → km/h for winds
- Filters to 10-minute intervals (matching v1)
- Stores in your existing SQLite database
- Uses same schema as EnvCan/NOAA buoys

### 2. `stations_updated.json`
Updated station metadata including:
- All 3 Surrey stations
- FlowWorks site IDs
- Channel ID mappings
- Coordinates and data types

### 3. `update_exports_for_surrey.py`
Helper script to automatically add Surrey stations to your export scripts.

---

## Installation Steps

### Step 1: Test the Fetcher
```bash
cd ~/envcan_wave
python fetch_surrey_wave_v2.py
```

Expected output:
```
🌊 Surrey FlowWorks Data Fetcher (API v2)
======================================================================
✅ Authenticated - expires 2025-02-04 15:30:00+00:00

📡 Fetching Crescent Beach Ocean...
  ✅ wind_speed: 12 points
  ✅ wind_direction: 12 points
  ✅ wind_gust: 12 points
  ✅ wave_height_sig: 12 points
  ...
```

### Step 2: Verify Data in SQLite
```bash
sqlite3 ~/.local/share/buoy_data.sqlite
```
```sql
SELECT buoy_id, COUNT(*) as points, MAX(observation_time) as latest
FROM buoy_observation 
WHERE buoy_id IN ('CRPILE', 'CRCHAN', 'COLEB')
GROUP BY buoy_id;
```

Expected result:
```
CRPILE|144|1738689600
CRCHAN|60|1738689600
COLEB|48|1738689600
```

### Step 3: Update Export Scripts
```bash
python update_exports_for_surrey.py
```

This adds Surrey stations to:
- `sqlite_to_json.py` (latest snapshot)
- `export_24hr_timeseries.py` (charts)

Review the `.bak` files to ensure changes look correct.

### Step 4: Update stations.json
```bash
cp stations_updated.json ~/envcan_wave/stations.json
```

### Step 5: Test Exports
```bash
# Test latest snapshot
python sqlite_to_json.py

# Test 24h timeseries
python export_24hr_timeseries.py
```

Check output files:
```bash
cat ~/site/data/latest_buoy_v2.json | jq '.CRPILE'
cat ~/site/data/buoy_timeseries_24h.json | jq '.CRPILE'
```

### Step 6: Add to Cron
```bash
crontab -e
```

Add:
```cron
# Fetch Surrey FlowWorks data every 10 minutes
*/10 * * * * ~/envcan_wave/.venv/bin/python3 ~/envcan_wave/fetch_surrey_wave_v2.py >> ~/envcan_wave/surrey.log 2>&1
```

---

## Frontend Integration (Optional)

### Add to main.js display order:
```javascript
const order = [
  "4600146", // Halibut Bank
  "4600304", // English Bay
  "CRPILE",  // Crescent Beach Ocean (NEW!)
  "CRCHAN",  // Crescent Channel (NEW!)
  "4600303", // Southern Georgia Strait
  "4600131", // Sentry Shoal
  "46087",   // Neah Bay
];
```

### Add to charts.js buoy selector:
```html
<option value="CRPILE">Crescent Beach Ocean</option>
<option value="CRCHAN">Crescent Channel</option>
<option value="COLEB">Colebrook</option>
```

---

## Differences from V1

### API Changes:
| Aspect | V1 | V2 |
|--------|----|----|
| Auth | Token in URL | JWT Bearer header |
| Endpoint | `/site/{id}/channel/{ch}/data/intervaltype/HH/intervalnum/{n}` | `/sites/{id}/channels/{ch}/data?startDateFilter=...` |
| Date format | Interval-based | ISO 8601 timestamps |
| Response | Direct array | Wrapped in `{"Resources": [...]}` |

### Database Changes:
- V1: Separate `flowworks_crescent_ocean` table with long format (datetime, station, datatype, value)
- V2: Same `buoy_observation` table as other buoys with wide format (one row per timestamp)

### Benefits of V2:
✅ Unified database schema  
✅ Auto-appears in all exports  
✅ No frontend changes needed  
✅ Standard JWT auth  
✅ Better API documentation  

---

## Troubleshooting

### "Authentication failed"
- Check credentials: `surreyrain / surreyrain`
- Verify API endpoint: https://developers.flowworks.com/fwapi/v2/authenticate
- Check your contact if credentials changed

### "No data returned"
- FlowWorks stations have ~30min delay
- Try fetching more hours: Change `hours=2` to `hours=4`
- Check station status on Surrey's dashboard

### "Column doesn't exist"
- The script auto-adds columns
- But you can manually add: `ALTER TABLE buoy_observation ADD COLUMN {field} REAL;`

### "Rate limit exceeded"
- Default: ~1 req/sec per user
- Add more `time.sleep()` between channels
- Contact FlowWorks support to increase limit

---

## Monitoring

### Check logs:
```bash
tail -f ~/envcan_wave/surrey.log
```

### Check data freshness:
```bash
sqlite3 ~/.local/share/buoy_data.sqlite \
  "SELECT buoy_id, datetime(observation_time, 'unixepoch') as latest 
   FROM buoy_observation 
   WHERE buoy_id IN ('CRPILE', 'CRCHAN', 'COLEB')
   ORDER BY observation_time DESC 
   LIMIT 3;"
```

### Check website:
```
https://halibutbank.ca/
```
Surrey stations should appear in the buoy list and charts!

---

## Migration from V1 (Optional)

If you want to migrate your old FlowWorks data:

1. **Export v1 data:**
```python
# In your old system
data = get_crescent_data(station='crescentpile', hours_of_data=168)  # 1 week
data.to_csv('crescentpile_v1_export.csv')
```

2. **Import to v2 schema:**
```python
import pandas as pd
import sqlite3

df = pd.read_csv('crescentpile_v1_export.csv')
# Convert to wide format and insert...
# (Manual process - probably not worth it for old data)
```

---

## Next Steps

1. ✅ Test fetch script
2. ✅ Update exports
3. ✅ Add to cron
4. 🔜 Add to frontend (optional)
5. 🔜 Set up monitoring alerts
6. 🔜 Consider adding to comparison charts

---

## Support

**Surrey Contact:** coastal@surrey.ca  
**FlowWorks Support:** http://www.flowworks.com/contact  
**Your old v1 code:** Keep for reference but deprecate

---

## Summary

You now have:
- ✅ Clean v2 API integration
- ✅ Unified database schema
- ✅ Auto-export to website
- ✅ Same cron pattern as other buoys
- ✅ 3 new data sources for Boundary Bay

The Surrey stations will show up automatically once you run the export scripts! 🌊
