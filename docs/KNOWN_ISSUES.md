# Known Issues

## 🔧 Storm Surge Hindcast Missing Tide Observations (Feb 8, 2026) - OPEN

### Issue
Storm surge hindcast page (`/storm_surge.html`) only shows predicted water levels, not actual observed tide data. This prevents comparison of forecast accuracy vs reality.

**What works:**
- ✅ Tide observations being fetched every 30 minutes (tide_to_sqlite.py)
- ✅ Observations stored in tide_data.sqlite
- ✅ Observations displayed on tide charts (fixed 2026-02-08)

**What's missing:**
- ❌ Observations not included in `combined-water-level.json` export
- ❌ Storm surge page can't show "predicted vs actual" comparison
- ❌ Can't validate forecast accuracy for past dates

### Root Cause
`export_combined_water_level.py` only exports:
- Astronomical tide predictions (from SQLite)
- Storm surge forecasts (from GeoMet)
- Combined total water level (prediction + forecast)

It does NOT load or export tide observations, even though they're available in the database.

### Solution
Add tide observation loading to `export_combined_water_level.py`:
1. Query `tide_observation` table for recent data
2. Include observations in JSON output
3. Update chart renderer to display as scatter points

**See:** `docs/project/NEXT_SESSION.md` for implementation details

---

## ✅ YouTube Webcam Capture 403 Forbidden Errors (Jan 16, 2026) - RESOLVED

### Issue
YouTube webcam captures (White Rock Pier, White Rock East Beach, Cox Bay) stopped working around Jan 16, 2026. All captures failed with HTTP 403 Forbidden errors when trying to download HLS video segments.

**Symptoms:**
- yt-dlp successfully retrieves stream URL and metadata
- ffmpeg/yt-dlp fails with "HTTP error 403 Forbidden" on video segment downloads
- Thumbnails and stream info work fine, only video segments blocked
- Affects all quality levels initially, but lowest quality (144p) works with workaround

**Error example:**
```
[hls @ 0x...] Opening 'https://rr3---sn-uxa0n-t8ge7.googlevideo.com/videoplayback/...' for reading
[https @ 0x...] HTTP error 403 Forbidden
[hls @ 0x...] Failed to open segment 17341888 of playlist 0
```

### Root Cause
YouTube changed their HLS segment delivery to block automated access. This is a recurring issue tracked in yt-dlp GitHub:
- [Issue #15212](https://github.com/yt-dlp/yt-dlp/issues/15212) - Dec 2025
- [Issue #14680](https://github.com/yt-dlp/yt-dlp/issues/14680) - Oct 2025
- [Issue #14456](https://github.com/yt-dlp/yt-dlp/issues/14456) - Sep 2025

Two factors contributed to the failure:
1. **Old yt-dlp version** - System had 2024.04.09, needed 2025.12.08+
2. **YouTube ad-request handling** - New yt-dlp versions default to ad-request behavior that YouTube blocks

### Solution (Jan 16, 2026)

**1. Updated yt-dlp in venv:**
```bash
.venv/bin/pip install -U yt-dlp  # Updated to 2025.12.08
```

**2. Added workaround flag to disable ad-request:**
```python
["yt-dlp", "--extractor-args", "youtube:request-no-ads=false", ...]
```

**3. Changed capture approach - download segment first, then extract frame:**
```python
# OLD (broken): Get URL, pass to ffmpeg
stream_url = subprocess.run(["yt-dlp", "-g", url])
subprocess.run(["ffmpeg", "-i", stream_url, "-frames:v", "1", output])

# NEW (working): Download short segment, then extract frame
subprocess.run(["yt-dlp", "--extractor-args", "youtube:request-no-ads=false",
                "-f", "worst", "--downloader-args", "ffmpeg:-t 2",
                "-o", temp_video, url])
subprocess.run(["ffmpeg", "-i", temp_video, "-frames:v", "1", output])
```

**4. Fixed venv path resolution:**
```python
# Script now finds yt-dlp in venv instead of using outdated system version
_venv_bin = Path(sys.executable).parent
YT_DLP_PATH = str(_venv_bin / "yt-dlp") if (_venv_bin / "yt-dlp").exists() else "yt-dlp"
```

### Current Limitations

**144p quality only:** Higher quality formats (360p, 720p, 1080p) still get 403 blocked. Using `-f worst` (144p, 256x144) as workaround.

**Frame sizes:**
- White Rock Pier: ~6 KB (192x144 after crop)
- East Beach: ~0.6 KB when camera content is dark

### Potential Future Improvements

1. **Install deno JS runtime** - yt-dlp warns about missing JS runtime:
   ```
   WARNING: [youtube] No supported JavaScript runtime could be found.
   YouTube extraction without a JS runtime has been deprecated.
   See https://github.com/yt-dlp/yt-dlp/wiki/EJS
   ```
   Installing deno may enable better format extraction.

2. **Test higher quality formats** - Periodically test if 360p/480p work:
   ```bash
   .venv/bin/yt-dlp --extractor-args "youtube:request-no-ads=false" -f 93 ...
   ```
   Format IDs: 91=144p, 92=240p, 93=360p, 94=480p, 95=720p

3. **Monitor yt-dlp releases** - YouTube frequently changes, yt-dlp releases fixes. Keep venv yt-dlp updated:
   ```bash
   .venv/bin/pip install -U yt-dlp
   ```

### Files Changed
- `scripts/fetch/fetch_webcam.py`:
  - Added `YT_DLP_PATH` detection for venv
  - Replaced `get_stream_url()` + `capture_frame()` with unified `capture_youtube_frame()`
  - Added `--extractor-args "youtube:request-no-ads=false"`
  - Changed format from `best[height<=720]` to `worst`

### Testing
```bash
# Test single capture
.venv/bin/python3 scripts/fetch/fetch_webcam.py whiterock

# Test yt-dlp directly
.venv/bin/yt-dlp --extractor-args "youtube:request-no-ads=false" -f worst \
  --downloader-args "ffmpeg:-t 2" -o /tmp/test.mp4 "https://www.youtube.com/watch?v=4MK3E9EWDSY"
```

---

## ✅ Colebrook Windy Push Using Wrong Database (Jan 11, 2026) - RESOLVED

### Issue
Colebrook station was successfully reporting "3/3 stations updated" to Windy API, but was pushing **month-old data** (Dec 15, 2025) instead of current data.

### Root Cause
**Colebrook is a LAND-BASED wind station, NOT a buoy!**

The data was being collected correctly into the `wind_observation` table, but the Windy push code was querying the `buoy_observation` table which had stale data.

```python
# WRONG - Always queried buoy_observation table for ALL stations
data = get_latest_station_data(buoy_conn, buoy_id)  # Returns Dec 15, 2025 data for COLEB
```

### Solution (Jan 11, 2026)
1. Created `get_latest_wind_station_data()` function to query `wind_observation` table
2. Updated Windy push logic to route to correct database based on station type:
```python
# CORRECT - Route to appropriate database
is_wind_only = (station_key == "colebrook")
if is_wind_only:
    data = get_latest_wind_station_data(wind_conn, station_id)  # Current data from wind table
else:
    data = get_latest_station_data(buoy_conn, station_id)
```

3. Updated `config/stations.json` to make Colebrook's land-based nature crystal clear:
   - Changed type: `"land_based_wind_station"` (not `"wind_monitoring_station"`)
   - Added explicit flag: `"is_buoy": false`
   - Location: `"Boundary Bay (LAND-BASED)"` (all caps)
   - Note: `"LAND-BASED wind monitoring station at pump house - NOT a buoy, NO wave data"`

4. Added comment in `fetch_surrey_wave_v2.py` configuration:
```python
"buoy_id": "COLEB",  # Station ID (NOT a buoy - land-based wind station, uses wind_observation table)
```

### Files Changed
- `scripts/fetch/fetch_surrey_wave_v2.py` (lines 417-565)
- `config/stations.json` (lines 812-836)

### Result
- ✅ Colebrook now pushes current data to Windy (was Dec 15, now Jan 11)
- ✅ Clear documentation prevents future "buoy" confusion
- ✅ Architecture properly separates land stations from marine buoys

---

## 📍 Database Locations (Reference)

**IMPORTANT:** All databases are in `/home/keelando/.local/share/`, NOT in the project directory!

```bash
/home/keelando/.local/share/tide_data.sqlite         # 40.2 MB - DFO + Surrey tides
/home/keelando/.local/share/buoy_data.sqlite         # 23.0 MB - Wave/wind observations
/home/keelando/.local/share/storm_surge_forecast.sqlite  # 7.8 MB - ECCC GDSPS forecasts
```

The empty file at `/home/keelando/envcan_wave/data/tide_data.sqlite` is a historical artifact and should be ignored.

---

## ✅ Surrey API Timezone Issue - Using UTC Instead of Pacific (Dec 12, 2025) - RESOLVED

### Issue
Surrey geodetic stations were showing stale observation data (2+ hours old) even though the FlowWorks API had recent data available.

### Root Cause
**The Surrey FlowWorks API expects Pacific time in request parameters, not UTC!**

Our `fetch_surrey_tides.py` script was sending UTC timestamps:
```python
# WRONG - Surrey API returns no data
now = datetime.now(timezone.utc)
params = {
    'startDateFilter': '2025-12-12T23:17:01',  # UTC time
    'endDateFilter': '2025-12-12T17:17:01'      # UTC time
}
# Result: 0 points
```

When we send Pacific time:
```python
# CORRECT - Surrey API returns data
now = datetime.now(ZoneInfo('America/Vancouver'))
params = {
    'startDateFilter': '2025-12-12T15:17:01',  # Pacific time (no TZ indicator)
    'endDateFilter': '2025-12-12T09:17:01'      # Pacific time
}
# Result: 32 points
```

### Solution (Dec 12, 2025)
Changed `get_channel_data()` to use Pacific time:
```python
# Surrey API expects Pacific time (no TZ indicator)
now = datetime.now(ZoneInfo('America/Vancouver'))
```

### Result
- ✅ Observations now fetch successfully (82 points)
- ✅ Data shows up-to-date within 10-20 minutes
- ✅ No more "stale data" warnings for Surrey stations

### Files Modified
- `fetch_surrey_tides.py` (line 130) - Changed from `timezone.utc` to `ZoneInfo('America/Vancouver')`

### Important Note
**The Surrey API is undocumented regarding timezone expectations.** This was discovered through trial and error testing different request formats. Keep this in mind for future API integrations.

---

## ⚠️ Geodetic Tide Chart Date Picker Breaks All Graphs (Dec 12, 2025) - RESOLVED

### Issue
When changing the date on geodetic tide plots (Surrey stations), the graphs disappear and NO graphs show up on any page until the browser is refreshed.

### Impact
- **Severity:** High (breaks user experience)
- **Affected:** Crescent Beach Ocean, Crescent Channel Ocean tide charts
- **Workaround:** Refresh the page

### Files to Investigate
- `/home/keelando/site/assets/js/tides.js` - Date picker event handlers
- Frontend chart rendering logic for geodetic stations

---

## ⏰ ECCC Storm Surge Forecast Missing Early Morning Hours (Dec 12, 2025)

### Issue
DFO tide stations show "No storm surge data available" for the first 4 hours of each day (00:00-04:00 Pacific).

### Root Cause
ECCC GDSPS (Global Deterministic Storm Surge Prediction System) forecast starts at **12:00Z (04:00 Pacific)** each day, not from midnight.

**Evidence:**
```json
Point_Atkinson forecast:
- First entry: "2025-12-12T12:00:00Z" (04:00 AM Pacific)
- Missing: 00:00-04:00 Pacific
```

### Impact
- **Severity:** Low (middle of night hours)
- **Affected stations:** Point Atkinson, Campbell River, Tofino, Port Renfrew, Crescent Pile
- **User experience:** Gaps in storm surge display during overnight hours

### Potential Solutions
1. **Check API availability:** Does ECCC provide earlier forecast data that we're not fetching?
2. **Interpolate from previous day:** Use tail end of previous day's forecast to fill gap
3. **Extrapolate:** Extend forecast backwards from 04:00 using recent trend
4. **Accept limitation:** Document that ECCC forecast starts at 04:00

**Files to investigate:**
- `fetch_storm_surge.py` - fetching logic from ECCC API
- `export_combined_water_level.py` - data processing and time ranges

---

## ✅ Surrey Tide Predictions Fetching Wrong Time Range (Dec 12, 2025) - RESOLVED

### Issue
Surrey tide predictions were fetching PAST data instead of FUTURE forecasts.

### Root Cause
`fetch_surrey_tides.py` (formerly `sync_surrey_to_tide_db.py`) used the same time range logic for both predictions and observations:
```python
# WRONG - fetched backward in time for predictions
end = datetime.now(timezone.utc)
start = end - timedelta(hours=48)  # Goes BACKWARD
```

This meant:
- ❌ **Predictions**: Fetching past 48 hours (useless - we need future forecasts!)
- ✅ **Observations**: Fetching past 48 hours (correct)

### Solution (Dec 12, 2025)
Updated `get_channel_data()` to support both forward and backward time ranges:

```python
def get_channel_data(self, site_id, channel_id, hours_past=0, hours_future=0):
    now = datetime.now(timezone.utc)

    if hours_future > 0:
        # Fetch FUTURE data (predictions)
        start = now
        end = now + timedelta(hours=hours_future)
    else:
        # Fetch PAST data (observations)
        start = now - timedelta(hours=hours_past)
        end = now
```

**New behavior:**
- ✅ **Predictions**: Fetch 4 days forward (96 hours) - matches DFO pattern
- ✅ **Observations**: Fetch 48 hours backward
- ✅ Covers: present calendar day + 2 full days + timezone margin

### Files Modified
- `fetch_surrey_tides.py` (formerly `sync_surrey_to_tide_db.py`) (lines 115-139, 183-199, 235-249, 310-315)

### Database Location
All databases are stored in `/home/keelando/.local/share/`:
- `tide_data.sqlite` (40.2 MB) - DFO + Surrey tides
- `buoy_data.sqlite` (23.0 MB) - Wave/wind observations
- `storm_surge_forecast.sqlite` (7.8 MB) - ECCC GDSPS forecasts

---

## ✅ Surrey Water Level Integration Bug (Dec 11-12, 2024) - RESOLVED

### Issue
When adding `water_level_predicted` and `water_level_observed` channels to Surrey stations, the wave/wind data stopped appearing in exports and on the map.

### Root Cause
**Mixing observations and predictions in the same database:**
- Tide predictions: future timestamps (e.g., 18:00 PST when current time is 10:00 PST)
- Wave/wind observations: current/past timestamps (e.g., 10:20 PST)
- Export script found "latest" observation_time → got future time from tide predictions
- Searched for wave/wind data before that future time → found nothing → skipped stations

### Final Solution (Dec 12, 2024)
**Separated tide data from buoy database:**

1. **Removed water_level channels from `fetch_surrey_wave_v2.py`**
   - Tide predictions don't belong with observations
   - Buoy database now contains only observations (wave/wind/temp)

2. **Refactored `fetch_surrey_tides.py` (formerly `sync_surrey_to_tide_db.py`) to fetch directly from Surrey API**
   - No longer reads from buoy database
   - Fetches water_level data directly to tide database
   - Runs independently via cron every 20 minutes

3. **Added future timestamp filter to `sqlite_to_json.py`**
   - Export only considers timestamps <= NOW when finding "latest"
   - Prevents future predictions from being used as reference point

### Result
- ✅ Wave/wind stations back on map (CRPILE, CRCHAN, COLEB)
- ✅ Tide data flowing correctly to tide database
- ✅ Clean separation: observations (buoy DB) vs predictions (tide DB)
- ✅ Both pipelines working independently

### Files Modified
- `fetch_surrey_wave_v2.py` - Removed water_level channels (lines 68-89)
- `fetch_surrey_tides.py` (formerly `sync_surrey_to_tide_db.py`) - Refactored to fetch from API instead of buoy DB
- `sqlite_to_json.py` - Added `observation_time <= NOW` filter (line 84)

---

## Storm Surge Card "Invalid Date" Display (Dec 12, 2024)

### Issue
Storm surge card on Tides page shows "Invalid time date" for the calculation timestamp.

### Root Cause
Frontend JavaScript (`tides.js:464`) expects `station.tide_offset.observation_time` field, but backend (`export_tide_json.py:171-174`) doesn't include it in the JSON output.

```javascript
// tides.js line 464 - tries to access undefined field
const calcTime = new Date(station.tide_offset.observation_time);  // undefined!
```

### Current Calculation Method
The tide offset (observed - predicted) currently compares:
- Most recent observation (up to 2 hours old)
- Closest prediction to "now" (within ±30 minutes)

**Problem:** Observation and prediction can be at different times (up to 30 min apart), which can introduce error.

### Proposed Fixes

#### 1. Add observation_time to tide_offset (Quick Fix)
```python
# export_tide_json.py:171-174
station_data["tide_offset"] = {
    "value": round(offset, 2),  # Also: change from 3 to 2 decimal places
    "observation_time": station_data["observation"]["time"],  # Add this
    "description": "Observed minus predicted (storm surge + forecast error)"
}
```

#### 2. Better Time Matching (Enhancement)
Match prediction to the EXACT observation time instead of "now":
- Query prediction at same timestamp as observation
- Interpolate if needed for 5-minute intervals
- Ensures apples-to-apples comparison

#### 3. Reduce Decimal Places
Change tide offset from 3 decimals (0.057 m) to 2 decimals (0.06 m) for cleaner display.

### Files Affected
- `export_tide_json.py` (lines 165-174) - Backend calculation
- `/home/keelando/site/assets/js/tides.js` (line 464) - Frontend display
- `/home/keelando/site/data/tide-latest.json` - Output JSON

---

## Tide Residuals Not Showing on All Station Charts (Dec 12, 2024)

### Issue
Residual line (observed - predicted) doesn't appear on tide charts for some stations or some days.

### Root Cause
The residual plotting code exists (`tides.js:792-826`, `tides.js:1003-1020`) but has restrictive conditions:

```javascript
// Only shows residuals if ALL conditions met:
if (isGeodetic && observations.length > 0 && predictions.length > 0) {
    // Calculate residuals...
}

// Later...
if (residuals.length > 0 && dayOffset === 0) {  // Only on "Today" tab!
    // Add to chart
}
```

### Current Restrictions
1. **Station type:** Only `SURREY_FLOWWORKS` stations (Crescent Beach Ocean, Crescent Channel Ocean)
2. **Day tab:** Only shows on "Today" (dayOffset=0), not Yesterday/Tomorrow
3. **Data availability:** Requires both observations AND predictions

### Proposed Fixes

#### 1. Show Residuals on All Days
```javascript
// tides.js:1003 - Remove dayOffset restriction
if (residuals.length > 0) {  // Was: && dayOffset === 0
```

#### 2. Extend to All Stations (Optional)
```javascript
// tides.js:795-797 - Show for ALL stations with observations
if (observations.length > 0 && predictions.length > 0) {  // Remove isGeodetic check
```

This would display helpful residual lines for Point Atkinson, Campbell River, Tofino, etc.

### Files Affected
- `/home/keelando/site/assets/js/tides.js` (lines 792-826, 1003-1020) - Frontend chart rendering

---

## Tide Residual Calculation Strategy Question (Dec 12, 2024)

### Question
Should tide residuals be calculated on-the-fly during JSON export (current) or pre-calculated and saved to database?

### Current Approach: On-the-fly (Recommended ✓)
**Pros:**
- Simple implementation
- Always uses latest data
- No database bloat
- Easy to change calculation logic

**Cons:**
- Slight computational overhead (negligible)
- Can't query historical residuals easily

### Alternative: Save to `tide_offset` Table
The `tide_offset` table exists in the schema but is NOT currently used.

**Pros:**
- Queryable historical residual data
- Useful for trend analysis
- Could speed up exports (minimal benefit)

**Cons:**
- Database bloat (2,016 points/week × stations)
- Data duplication (redundant with obs + pred tables)
- Need backfill logic for historical data
- More complex to maintain

### Recommendation
**Keep calculating on-the-fly** because:
- Tide data changes slowly (5-min intervals)
- Export scripts run frequently anyway
- Can calculate historical residuals when needed by querying observations + predictions
- Simpler architecture

### Files Referenced
- `tide_to_sqlite.py` (lines 77-95) - Unused `tide_offset` table schema
- `export_tide_json.py` (lines 165-174) - Current on-the-fly calculation

---

## Add Surrey Geodetic Stations to Hindcast Plot (Dec 12, 2024)

### Enhancement Request
Add the two Surrey geodetic stations to the storm surge hindcast comparison plot:
- Crescent Beach Ocean (`crescent_beach_ocean`)
- Crescent Channel Ocean (`crescent_channel_ocean`)

### Current Status
The hindcast plot currently shows 4 stations with DFO IWLS data:
- Point Atkinson
- Campbell River
- Crescent Beach Channel (uses DFO station `crescent_pile`)
- Tofino

The Surrey stations have:
- ✅ Tide observations in database (synced from Surrey FlowWorks buoy DB)
- ✅ Tide predictions in database (synced from Surrey FlowWorks buoy DB)
- ✅ Already calculating observed surge via `export_observed_storm_surge.py`
- ❌ NOT included in hindcast export (no GDSPS forecast points for these locations)

### Implementation Options

#### Option 1: Observed Surge Only (Recommended)
Add Surrey stations to hindcast plot showing **only** the observed surge line (no GDSPS forecast):
- Shows actual tide residuals (obs - pred) as black line
- Useful for seeing real storm surge conditions at these locations
- No forecast lines since GDSPS doesn't have these coordinates

**Files to modify:**
- `export_observed_storm_surge.py` - Add Surrey stations to TIDE_TO_SURGE_MAP
- `storm_surge_page.js` - Handle stations with observed surge but no hindcast data

#### Option 2: Interpolate GDSPS Forecast
Interpolate GDSPS storm surge forecast from nearby station (Crescent Beach Channel):
- More complex implementation
- May not be accurate due to local bathymetry differences
- Probably not worth the effort

### Recommended Approach
**Option 1** - Show observed surge only for Surrey stations:

```python
# export_observed_storm_surge.py - Update TIDE_TO_SURGE_MAP
TIDE_TO_SURGE_MAP = {
    "point_atkinson": "Point_Atkinson",
    "campbell_river": "Campbell_River",
    "crescent_pile": "Crescent_Beach_Channel",
    "tofino": "Tofino",
    "crescent_beach_ocean": "Crescent_Beach_Ocean",      # NEW
    "crescent_channel_ocean": "Crescent_Channel_Ocean"   # NEW
}
```

Frontend will need to handle stations that have `observedSurgeData` but no `hindcastData`.

### Files Affected
- `export_observed_storm_surge.py` (line 29-34) - Add Surrey stations to mapping
- `storm_surge_page.js` (lines 544-573, 575-590) - Handle stations with obs but no hindcast
- `export_hindcast_json.py` - No changes needed (GDSPS doesn't cover these locations)
