# Surrey Tide Data Simplification Plan

**Date:** 2026-01-12
**Status:** Planning complete, ready for implementation
**Backup Branch:** `archive/geodetic-tide-corrections-2026-01-12`

---

## Goal

Simplify Surrey tide handling by using FlowWorks' pre-calculated tidal residuals instead of computing our own geodetic corrections. This eliminates complex datum conversion logic while improving data accuracy.

---

## Current State (Complex)

### Data Flow
1. **Fetch from FlowWorks** (`fetch_surrey_tides.py`):
   - Water level observed (Channel 2296/2279)
   - Water level predicted (Channel 2620/2621)
   - Tidal residual (Channel 2414/3660) ← **Already fetched but not used!**

2. **Current Exports** (`export_tide_json.py`, `scripts/export/water_level_export.py`):
   - Calculate residual ourselves: `observed - predicted`
   - Apply geodetic datum corrections
   - Complex timestamp matching logic

### Problems
- Duplicating Surrey's own tidal residual calculations
- Complex geodetic datum handling (CGVD28 ↔ Chart Datum)
- Uncertainty about correction accuracy
- Extra computational overhead

---

## New Approach (Simplified)

### Use Surrey's Pre-Calculated Data

**Crescent Beach Ocean (surrey_crescent_ocean):**
- **Water Level Chart:**
  - Observed: Channel 2296 (Anderra - "gold standard")
  - Predicted: Channel 2620 (Tidal prediction)
- **Storm Surge Comparison:**
  - Observed residual: Channel 2414 (Surrey's tidal residual)
  - Forecast: GDSPS storm surge model

**Crescent Channel Ocean (surrey_crescent_channel):**
- **Storm Surge Comparison Only:**
  - Observed residual: Channel 3660 (Surrey's tidal residual)
  - Forecast: GDSPS storm surge model

### Benefits
- ✅ Trust Surrey's "gold standard" Anderra sensor
- ✅ Use Surrey's validated tidal residual calculations
- ✅ No geodetic datum conversions needed
- ✅ Simpler code, easier to maintain
- ✅ More accurate (Surrey knows their instruments best)

---

## Implementation Plan

### 1. Modify `export_tide_json.py`

**Current behavior:**
- Exports observed/predicted water levels
- Calculates `tide_offset = observed - predicted`

**New behavior for Surrey stations:**
```python
# For Surrey stations, export tidal residual from surrey_geodetic_data table
if station_id.startswith("surrey_"):
    cur.execute("""
        SELECT observation_time, tidal_residual
        FROM surrey_geodetic_data
        WHERE station_id = ?
        ORDER BY observation_time DESC
        LIMIT 1
    """, (station_id,))

    residual_row = cur.fetchone()
    if residual_row:
        station_data["tide_offset"] = {
            "value": round(residual_row[1], 3),
            "observation_time": datetime.fromtimestamp(residual_row[0], tz=timezone.utc).isoformat(),
            "source": "surrey_calculated",
            "description": "Tidal residual (Surrey FlowWorks calculation)"
        }
```

### 2. Modify the observed-surge path in `scripts/export/water_level_export.py`

**Current behavior:**
- Calculates `observed_surge = tide_observation - tide_prediction`
- Matches timestamps within 5-minute window

**New behavior for Surrey stations:**
```python
def fetch_surrey_tidal_residual(conn, station_id, start_time):
    """Fetch Surrey's pre-calculated tidal residual."""
    cur = conn.cursor()
    cur.execute("""
        SELECT observation_time, tidal_residual
        FROM surrey_geodetic_data
        WHERE station_id = ?
        AND observation_time >= ?
        AND tidal_residual IS NOT NULL
        ORDER BY observation_time ASC
    """, (station_id, start_time))

    return cur.fetchall()

# In main export loop, add Surrey stations:
TIDE_TO_SURGE_MAP = {
    "point_atkinson": "Point_Atkinson",
    "campbell_river": "Campbell_River",
    "crescent_pile": "Crescent_Beach_Channel",
    "tofino": "Tofino",
    # Add Surrey stations:
    "crescent_beach_ocean": "Crescent_Beach_Ocean",
    "crescent_channel_ocean": "Crescent_Channel_Ocean"
}

# For Surrey stations, fetch tidal residual directly:
if station_id.startswith("surrey_"):
    residuals = fetch_surrey_tidal_residual(conn, station_id, start_time)
    surge_data = [
        {
            "time": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "observed_surge_m": round(residual, 4),
            "source": "surrey_calculated"
        }
        for ts, residual in residuals
        if datetime.fromtimestamp(ts, tz=timezone.utc).minute % 15 == 0  # Downsample to 15-min
    ]
```

### 3. Update Frontend

**Files to modify:**
- `/home/keelando/envcan_wave/site/assets/js/storm_surge_page.js`

**Changes:**
- Handle Surrey stations in hindcast plot
- Show "Surrey calculated" vs "GDSPS forecast" labels
- Handle stations with observed residual but no GDSPS hindcast data

### 4. Database Schema

**No changes needed!** Already have:
- `tide_observation` table (for water level charts)
- `tide_prediction` table (for water level charts)
- `surrey_geodetic_data` table (for tidal residual)

---

## Code to Remove (Future Cleanup)

After validation, can safely remove:
- Geodetic datum conversion logic in `export_tide_json.py` (lines ~180-210)
- Complex Surrey timestamp matching in the observed-surge path of `scripts/export/water_level_export.py`
- Any CGVD28 ↔ Chart Datum offset calculations

**Note:** Keep backup branch available for reference: `archive/geodetic-tide-corrections-2026-01-12`

---

## Testing Checklist

- [ ] Crescent Beach Ocean water level chart shows observed vs predicted
- [ ] Crescent Beach Ocean storm surge shows Surrey residual vs GDSPS
- [ ] Crescent Channel Ocean storm surge shows Surrey residual vs GDSPS
- [ ] Tidal residual values match FlowWorks GUI
- [ ] No "Invalid Date" or timestamp errors
- [ ] Storm surge page loads without console errors
- [ ] Hindcast comparison includes both Surrey stations

---

## Files to Modify

### Backend
1. `scripts/export/export_tide_json.py` - Add Surrey residual export
2. `scripts/export/water_level_export.py` (observed-surge path) - Use Surrey residuals directly
3. `scripts/fetch/fetch_surrey_tides.py` - ✓ Already updated (channel 3660 added)

### Frontend
1. `/home/keelando/envcan_wave/site/assets/js/storm_surge_page.js` - Handle Surrey stations

### Documentation
1. `docs/SURREY.md` - Update with simplified approach
2. `docs/project/NEXT_SESSION.md` - ✓ Already updated with branch backup

---

## Summary

**Before:** Fetch observed/predicted → Calculate residual ourselves → Apply geodetic corrections → Hope it's right
**After:** Fetch Surrey's pre-calculated tidal residual → Trust their validation → Display directly

Much simpler, more accurate, easier to maintain!
