# Next Steps - Offline Testing & Duplicate Data Fix

## ✅ What We've Confirmed

1. **Timestamp bug is FIXED** (commits a40665c + d239748 on Nov 6)
2. **Duplicate data exists BEFORE Nov 7** (expected - old junk data)
3. **Unique data exists AFTER Nov 7** (working correctly!)
4. **Offline testing environment is ready**

## 🎯 Action Items

### 1. Run Diagnostic on Production Data

If you want to analyze the **real** production hindcast data:

```bash
# Download the production hindcast.json
curl https://halibutbank.ca/data/storm_surge/hindcast.json > /tmp/prod_hindcast.json

# Analyze it
python3 tests/diagnose_hindcast_duplicates.py /tmp/prod_hindcast.json
```

This will show you:
- How many duplicate timestamps exist before Nov 7
- Which stations are affected
- When unique data starts for each station

### 2. Filter Out Junk Data in Frontend

**Recommended fix:** Add a date filter in your frontend JavaScript to hide pre-Nov-7 data.

**File:** `~/site/assets/js/storm_surge_charts.js` (or similar)

```javascript
// When loading hindcast data, filter out junk data before Nov 7, 2024
function loadHindcastData(stationId) {
    fetch('/data/storm_surge/hindcast.json')
        .then(response => response.json())
        .then(data => {
            const stationData = data.stations[stationId];
            const cutoffDate = new Date('2024-11-07T00:00:00Z');

            // Filter out duplicate data from before the timestamp bug fix
            const filteredHindcast = stationData.hindcast.filter(item => {
                const itemDate = new Date(item.time);
                return itemDate >= cutoffDate;
            });

            // Use filteredHindcast for charting
            renderHindcastChart(filteredHindcast);
        });
}
```

**Alternative:** Add a UI toggle to show/hide pre-Nov-7 data

```javascript
const showHistoricalData = localStorage.getItem('show_pre_nov7_data') === 'true';

if (!showHistoricalData) {
    hindcastData = hindcastData.filter(item =>
        new Date(item.time) >= new Date('2024-11-07T00:00:00Z')
    );
}
```

### 3. Optional: Clean Up Database

If you want to **permanently remove** the junk data from the database:

```bash
# Backup first!
cp ~/.local/share/storm_surge_forecast.sqlite ~/.local/share/storm_surge_forecast.sqlite.backup

# Remove records before Nov 7, 2024
sqlite3 ~/.local/share/storm_surge_forecast.sqlite <<EOF
DELETE FROM forecast_archive
WHERE valid_time < '2024-11-07T00:00:00Z';

VACUUM;
EOF

# Re-export hindcast
python3 export_hindcast_json.py
```

**Warning:** This is permanent! Make sure you have a backup.

### 4. Test Frontend Changes

After adding the date filter:

```bash
# 1. Copy test fixture with duplicates
cp tests/fixtures/storm_surge/hindcast_with_duplicates.json ~/site/data/storm_surge/hindcast.json

# 2. Open frontend in browser
cd ~/site && python3 -m http.server 8000
# Open http://localhost:8000

# 3. Navigate to hindcast chart

# 4. Verify:
#    - Only data from Nov 7+ is displayed
#    - Each station has unique values
#    - No duplicate/overlapping lines
```

### 5. Validate Test Data

```bash
# Validate timestamp format
python3 tests/validate_hindcast_timestamps.py

# Diagnose duplicates
python3 tests/diagnose_hindcast_duplicates.py
```

## 📋 Quick Reference

### Test Fixtures Available

**Clean data (no duplicates):**
- `tests/fixtures/storm_surge/hindcast.json` - Perfect test data

**Data with duplicates (mimics production):**
- `tests/fixtures/storm_surge/hindcast_with_duplicates.json` - Test filtering logic

### Scripts

| Script | Purpose |
|--------|---------|
| `tests/setup_offline_test.sh` | Copy fixtures to data directory |
| `tests/validate_hindcast_timestamps.py` | Check timestamp format |
| `tests/diagnose_hindcast_duplicates.py` | Find duplicate data |

### Key Dates

- **Nov 6, 2024:** Timestamp bug fixed (commits a40665c + d239748)
- **Nov 7, 2024:** First day with correct, unique data per station
- **Before Nov 7:** Data may be duplicated across stations (junk)

## 💡 Why This Happened

**The timestamp bug caused:**
1. `fetch_storm_surge.py` stored dates without times (e.g., "2024-11-06")
2. Hours ahead calculations were off by 12 hours
3. Database queries may have returned wrong data
4. Stations likely got Point Atkinson's data instead of their own

**After the fix:**
1. Full ISO timestamps stored (e.g., "2024-11-06T18:00:00Z")
2. Hours ahead calculations correct
3. Each station gets its own unique data

## 🎉 You're Ready!

1. ✅ Backend test fixtures are in place
2. ✅ Offline testing environment works
3. ✅ Diagnostic tools available
4. ✅ Frontend filtering approach documented
5. ✅ No duplication needed in frontend repo

**Your instinct was right:** The old data is junk, and filtering it out in the frontend is the cleanest solution! 🎯
