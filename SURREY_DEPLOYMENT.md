# Surrey FlowWorks Integration - Deployment Guide

## Overview
This guide covers deploying the Surrey FlowWorks v2 integration to your production environment.

## Files Included

### Core Integration Files
- `fetch_surrey_wave_v2.py` - FlowWorks API v2 data fetcher
- `stations.json` - Updated station metadata (includes Surrey stations)
- `sqlite_to_json.py` - Updated latest snapshot exporter
- `export_24hr_timeseries.py` - Updated 24h timeseries exporter

### New Surrey Stations
1. **CRPILE** (Crescent Pile) - Full wave + wind + temp
2. **CRCHAN** (Crescent Channel) - Wind + radar wave
3. **COLEB** (Colebrook) - Wind only

---

## Deployment Steps

### 1. Copy Files to Production

```bash
# Copy fetcher script
cp fetch_surrey_wave_v2.py ~/envcan_wave/
chmod +x ~/envcan_wave/fetch_surrey_wave_v2.py

# Update stations.json
cp stations.json ~/envcan_wave/stations.json

# Update export scripts (BACKUP FIRST!)
cp ~/envcan_wave/sqlite_to_json.py ~/envcan_wave/sqlite_to_json.py.backup
cp ~/envcan_wave/export_24hr_timeseries.py ~/envcan_wave/export_24hr_timeseries.py.backup

cp sqlite_to_json.py ~/envcan_wave/
cp export_24hr_timeseries.py ~/envcan_wave/
```

### 2. Test FlowWorks API Authentication

```bash
cd ~/envcan_wave
python3 fetch_surrey_wave_v2.py
```

**Expected output:**
```
🌊 Surrey FlowWorks Data Fetcher (API v2)
======================================================================
✅ Authenticated - expires 2025-XX-XX XX:XX:XX+00:00

📡 Fetching Crescent Pile...
  ✅ wind_speed: 12 points
  ✅ wind_direction: 12 points
  ✅ wind_gust: 12 points
  ✅ wave_height_sig: 12 points
  ...
```

**If authentication fails:**
- Check that you're in production environment (not behind proxy)
- Verify credentials are still valid: `surreyrain / surreyrain`
- Contact coastal@surrey.ca if credentials changed

### 3. Verify Data in SQLite

```bash
sqlite3 ~/.local/share/buoy_data.sqlite
```

```sql
-- Check for Surrey data
SELECT buoy_id, COUNT(*) as points,
       datetime(MAX(observation_time), 'unixepoch') as latest_utc
FROM buoy_observation
WHERE buoy_id IN ('CRPILE', 'CRCHAN', 'COLEB')
GROUP BY buoy_id;

-- Check sample data for Crescent Pile
SELECT datetime(observation_time, 'unixepoch') as time,
       wave_height_sig, wind_speed, wind_direction, air_temp
FROM buoy_observation
WHERE buoy_id = 'CRPILE'
ORDER BY observation_time DESC
LIMIT 5;
```

**Expected result:**
- CRPILE should have wave + wind + temp data
- CRCHAN should have wave + wind data
- COLEB should have wind + temp data (no waves)

### 4. Test Export Scripts

```bash
# Test latest snapshot
python3 ~/envcan_wave/sqlite_to_json.py

# Verify Surrey stations in output
cat ~/site/data/latest_buoy_v2.json | jq 'keys'
cat ~/site/data/latest_buoy_v2.json | jq '.CRPILE'

# Test 24h timeseries
python3 ~/envcan_wave/export_24hr_timeseries.py

# Verify Surrey stations
cat ~/site/data/buoy_timeseries_24h.json | jq 'keys'
cat ~/site/data/buoy_timeseries_24h.json | jq '.CRPILE.wave_height_sig | length'
```

### 5. Add to Crontab

**IMPORTANT:** Fetch every **20 minutes** (not 10) to reduce API load.

```bash
crontab -e
```

Add this line:
```cron
# Fetch Surrey FlowWorks data every 20 minutes
*/20 * * * * ~/envcan_wave/.venv/bin/python3 ~/envcan_wave/fetch_surrey_wave_v2.py >> ~/envcan_wave/surrey.log 2>&1
```

**Verify cron job is added:**
```bash
crontab -l | grep surrey
```

### 6. Update Frontend (Optional)

If you want Surrey stations to appear on the website in a specific order, update:

**File: `~/site/assets/js/main.js`**
```javascript
// Add Surrey stations to display order
const order = [
  "4600146", // Halibut Bank
  "4600304", // English Bay
  "CRPILE",  // Crescent Pile ← NEW!
  "CRCHAN",  // Crescent Channel ← NEW!
  "4600303", // Southern Georgia Strait
  "4600131", // Sentry Shoal
  "46087",   // Neah Bay
  "46088",   // New Dungeness
];
```

**File: `~/site/assets/js/charts.js`**
```html
<!-- Add to buoy selector dropdown -->
<option value="CRPILE">Crescent Pile</option>
<option value="CRCHAN">Crescent Channel</option>
<option value="COLEB">Colebrook</option>
```

**Note:** If you don't update the frontend, Surrey stations will still appear automatically - they just won't be in a specific position.

---

## Monitoring

### Check Logs
```bash
# Surrey fetcher logs
tail -f ~/envcan_wave/surrey.log

# Look for authentication issues or API errors
grep -i "error\|fail" ~/envcan_wave/surrey.log
```

### Check Data Freshness
```bash
sqlite3 ~/.local/share/buoy_data.sqlite \
  "SELECT buoy_id,
          datetime(MAX(observation_time), 'unixepoch') as latest_utc,
          CAST((strftime('%s', 'now') - MAX(observation_time)) / 60 AS INT) as age_minutes
   FROM buoy_observation
   WHERE buoy_id IN ('CRPILE', 'CRCHAN', 'COLEB')
   GROUP BY buoy_id;"
```

**Expected:** Age should be < 30 minutes if cron is running properly.

### Check Website
Visit https://halibutbank.ca and verify:
- Surrey stations appear in buoy list
- Data is updating every 20 minutes
- Charts work for Surrey stations

---

## Troubleshooting

### "Authentication failed" or 403 Error

**Cause:** Blocked by firewall, API credentials changed, or IP restriction

**Fix:**
1. Check network connectivity: `curl -I https://developers.flowworks.com/fwapi/v2/`
2. Verify credentials still valid (contact coastal@surrey.ca)
3. Check if your server IP is whitelisted with FlowWorks

### "No data returned" or Empty Results

**Cause:** FlowWorks stations often have ~30min reporting delay

**Fix:**
```python
# In fetch_surrey_wave_v2.py, increase hours parameter
count = fetch_and_store(api, station_key, station_config, conn, hours=4)  # Changed from 2 to 4
```

### Surrey Stations Not Appearing on Website

**Cause:** Export scripts not running or JSON files not being served

**Check:**
```bash
# Verify export cron jobs are running
tail ~/envcan_wave/export_latest.log
tail ~/envcan_wave/export_timeseries.log

# Manually run exports
python3 ~/envcan_wave/sqlite_to_json.py
python3 ~/envcan_wave/export_24hr_timeseries.py

# Check JSON files exist and have Surrey data
ls -lh ~/site/data/*.json
jq 'keys' ~/site/data/latest_buoy_v2.json
```

### "Column doesn't exist" Database Error

**Cause:** Script tries to query a field that doesn't exist in table

**Fix:** The fetcher auto-adds columns, but you can manually add:
```bash
sqlite3 ~/.local/share/buoy_data.sqlite
```
```sql
ALTER TABLE buoy_observation ADD COLUMN wave_height_sig REAL;
ALTER TABLE buoy_observation ADD COLUMN wave_period_avg REAL;
-- etc. for any missing columns
```

### API Rate Limit Exceeded

**Cause:** Too many requests to FlowWorks API

**Fix:**
1. Verify cron is set to 20 minutes (not 10)
2. Increase sleep time in fetch_surrey_wave_v2.py:
   ```python
   time.sleep(1.0)  # Changed from 0.5 to 1.0
   ```

---

## API v2 vs v1 Differences

| Aspect | V1 | V2 |
|--------|----|----|
| **Auth** | Token in URL | JWT Bearer header |
| **Endpoint** | `/site/{id}/channel/{ch}/data/...` | `/sites/{id}/channels/{ch}/data?startDateFilter=...` |
| **Response** | Direct array | `{"Resources": [...]}` |
| **Database** | Separate table | Same `buoy_observation` table |
| **Integration** | Manual exports | Auto-export with other buoys |

**Benefits of V2:**
- ✅ Unified schema - Surrey stations work like any other buoy
- ✅ Auto-export - No frontend changes needed
- ✅ Standard auth - JWT tokens instead of API keys
- ✅ Better API docs - Official v2 documentation available

---

## Support

**Surrey Contact:** coastal@surrey.ca
**FlowWorks API Support:** http://www.flowworks.com/contact
**API Documentation:** https://developers.flowworks.com/

**Credentials (as of 2025-11-05):**
- Username: `surreyrain`
- Password: `surreyrain`

---

## Rollback Plan

If something goes wrong, restore from backups:

```bash
# Restore export scripts
cp ~/envcan_wave/sqlite_to_json.py.backup ~/envcan_wave/sqlite_to_json.py
cp ~/envcan_wave/export_24hr_timeseries.py.backup ~/envcan_wave/export_24hr_timeseries.py

# Remove Surrey cron job
crontab -e
# Delete the Surrey FlowWorks line

# Remove Surrey data from database (optional)
sqlite3 ~/.local/share/buoy_data.sqlite \
  "DELETE FROM buoy_observation WHERE buoy_id IN ('CRPILE', 'CRCHAN', 'COLEB');"
```

---

## Next Steps After Deployment

1. ✅ Monitor logs for first 24 hours
2. ✅ Verify data freshness on website
3. ✅ Check if all three stations reporting correctly
4. 🔜 Consider adding Surrey stations to comparison charts
5. 🔜 Set up monitoring alerts if data goes stale
6. 🔜 Add Surrey locations to map view (if you have one)

---

## Summary

Once deployed, you'll have:
- ✅ 3 new data sources covering Boundary Bay / Crescent Beach area
- ✅ 10-minute update frequency (fetched every 20min, data updates every 10min)
- ✅ Full wave, wind, and temperature data for Crescent Pile
- ✅ Unified integration with existing buoy pipeline
- ✅ Auto-export to website with no frontend changes required

The Surrey stations will appear automatically on your website once the cron job runs! 🌊
