# Geodetic Tide Offset Implementation - Validation Results

**Date**: 2025-12-20
**Status**: ✅ Implementation Complete - Ready for User Testing

---

## Implementation Summary

### Problem
Two nearby tide stations (Crescent Beach and Crescent Channel) showed wildly different residuals (~0.7m difference) due to datum misalignment between CGVD28 (Surrey) and Chart Datum (DFO).

### Solution
Apply geodetic offsets from Surrey FlowWorks calibration channels:
- **Channel 2126** (CB vs CC PT): For Crescent Beach - apply to predictions
- **Channel 2129** (CB vs CC Radar): For Crescent Channel - apply to observations

---

## Changes Made

### 1. Export Script Fix ✅
**File**: `/home/keelando/envcan_wave/scripts/export/export_tide_json.py`

**Before**:
- Only exported geodetic offsets for Crescent Beach (channel 2126)
- Crescent Channel had no offset data

**After**:
- Exports channel 2126 for Crescent Beach (apply to predictions)
- Exports channel 2129 for Crescent Channel (apply to observations)
- Both stations now have `geodetic_offsets` array in JSON

**Code Changes** (lines 390-415):
```python
elif station_id == "surrey_crescent_channel":
    # Channel 2129: CB vs CC (Radar) - CRESCENT CHANNEL CALIBRATION
    # NOTE: This channel is fetched from Crescent Beach site but used for Crescent Channel!
    cur.execute("""
        SELECT observation_time, geodiff_cb_vs_cc
        FROM surrey_geodetic_data
        WHERE station_id = 'surrey_crescent_ocean'  # <- Fetched from CB but used for CC!
          AND observation_time >= ?
          AND observation_time <= ?
          AND geodiff_cb_vs_cc IS NOT NULL
        ORDER BY observation_time ASC
    """, (start_ts, end_ts))
    # ... (rest of export logic)
```

### 2. Frontend Verification ✅
**File**: `/home/keelando/site/assets/js/tides.js`

**Verified**:
- ✓ Line 913: Correctly identifies Crescent Channel stations
- ✓ Lines 928-949: Crescent Channel applies offset to **observations**
- ✓ Lines 950-958: Crescent Beach applies offset to **predictions**
- ✓ Lines 965-991: Crescent Channel residual = calibratedObs - prediction
- ✓ Lines 992-1018: Crescent Beach residual = observation - calibratedPred
- ✓ Lines 1210-1253: Chart hides/shows correct series for each station
- ✓ Lines 1122-1154: Legend dynamically adjusts for each methodology

**No frontend changes needed** - code was already correct!

---

## Data Validation

### Database Content ✅

**Query Results** (last 24 hours):
```
Station: surrey_crescent_ocean
  - Total geodetic records: 1,413
  - Channel 2126 records: 1,240
  - Channel 2129 records: 1,413
  - Latest data: 2025-12-20 17:46:00 (< 1 hour old)
```

### Channel 2126 Values (CB vs CC PT)
**Used for**: Crescent Beach Ocean (apply to predictions)
**Applied to**: Astronomical tide predictions

| Metric | Value |
|--------|-------|
| Count | 1,240 points |
| **Average** | **-0.3252 m** |
| Min | -0.3949 m |
| Max | +0.0546 m |
| Range | 0.4495 m |
| Stability | Varies with tidal cycle |

**Physical Meaning**: The datum offset between Crescent Beach observations (CGVD28) and Crescent Channel predictions (Chart Datum). Negative value means predictions need to be lowered by ~0.33m to align with CB observation datum.

### Channel 2129 Values (CB vs CC Radar)
**Used for**: Crescent Channel Ocean (apply to observations)
**Applied to**: Raw radar observations

| Metric | Value |
|--------|-------|
| Count | 1,413 points |
| **Average** | **+0.0262 m** |
| Min | -0.1284 m |
| Max | +0.4478 m |
| Range | 0.5762 m |
| Stability | More variable than 2126 |

**Physical Meaning**: The offset between Crescent Beach radar and Crescent Channel radar observations. Small positive value suggests CC radar reads slightly low compared to CB radar.

**Note**: The large range (0.58m) suggests this offset varies more than channel 2126. This could be due to:
1. Sensor noise in radar measurements
2. Local hydrodynamic effects between stations
3. Waves/reflections affecting radar accuracy

---

## JSON Export Verification ✅

**Test Results** (2025-12-20 18:12):

### Crescent Beach Ocean
```json
{
  "has_geodetic_offsets": true,
  "offset_count": 47,
  "sample_offset": {
    "time": "2025-12-20T06:00:00+00:00",
    "value": -0.298
  }
}
```
✅ **Status**: Exporting correctly

### Crescent Channel Ocean
```json
{
  "has_geodetic_offsets": true,
  "offset_count": 48,
  "sample_offset": {
    "time": "2025-12-20T06:00:00+00:00",
    "value": -0.033
  }
}
```
✅ **Status**: Exporting correctly (FIXED!)

---

## Expected Behavior

### Crescent Beach Ocean
**Before Correction**:
- Residual = Observation - Prediction
- Result: ~0.1m (already reasonable because CB observations trusted)

**After Correction** (using channel 2126):
1. Calibrated Prediction = Prediction + offset_2126
2. Residual = Observation - Calibrated Prediction
3. Result: Should be similar to before (CB datum already aligned)

**Chart Display**:
- Hide "Astronomical Tide" (show Calibrated Prediction instead)
- Show "Observation" (raw)
- Show "Calibrated Prediction" (prediction + offset)
- Show "Residual (Obs - Calibrated)"

### Crescent Channel Ocean
**Before Correction**:
- Residual = Observation - Prediction
- Result: ~0.8m (WRONG! Datum artifact)

**After Correction** (using channel 2129):
1. Calibrated Observation = Observation + offset_2129
2. Residual = Calibrated Observation - Prediction
3. Result: Should drop to ~0.1m (similar to Crescent Beach)

**Chart Display**:
- Show "Astronomical Tide" (raw prediction)
- Hide "Observation" (show Calibrated Observation instead)
- Show "Calibrated Observation" (observation + offset)
- Show "Residual (Obs - Calibrated)"

---

## Validation Checklist

### Backend ✅
- [x] Database table `surrey_geodetic_data` exists with correct schema
- [x] Channel 2126 data fetched and stored (avg -0.33m)
- [x] Channel 2129 data fetched and stored (avg +0.03m)
- [x] Export script includes offsets for both stations
- [x] JSON export verified with sample values

### Frontend ✅
- [x] Station identification logic correct (isCrescentBeach, isCrescentChannel)
- [x] Offset application logic correct for each station
- [x] Residual calculation logic correct for each methodology
- [x] Chart series display logic correct
- [x] Legend dynamically adjusts based on station type

### Data Flow ✅
- [x] Surrey API → fetch_surrey_tides.py → SQLite
- [x] SQLite → export_tide_json.py → JSON
- [x] JSON → tides.js → Chart display

---

## User Testing Required

### Visual Verification
1. **Open**: https://halibutbank.ca/tides.html
2. **Select**: "Crescent Beach Ocean"
3. **Check**:
   - [ ] Chart shows "Calibrated Prediction" (not "Astronomical Tide")
   - [ ] Storm surge card shows reasonable residual (~0.1m range)
   - [ ] Residual line is relatively flat (not systematic bias)
4. **Select**: "Crescent Channel Ocean"
5. **Check**:
   - [ ] Chart shows "Calibrated Observation" (not "Observation")
   - [ ] Storm surge card shows reasonable residual (~0.1m range)
   - [ ] Residual similar to Crescent Beach (within ±0.1m)

### Comparison Testing
1. Compare residuals between both stations over same time period
2. Verify residuals are similar (±0.1m tolerance)
3. Check that residuals track ECCC storm surge forecast reasonably
4. Look for systematic biases (residual should oscillate around zero)

### Temporal Stability
1. Monitor geodetic offsets over 24 hours
2. Check for unexpected drift or jumps
3. Verify offset values stay within expected ranges:
   - Channel 2126: -0.40m to -0.30m typical
   - Channel 2129: -0.13m to +0.45m typical

---

## Known Issues & Limitations

### Channel 2129 Variability
- Range of 0.58m over 24 hours is larger than channel 2126 (0.45m)
- Could indicate:
  - Radar sensor noise
  - Wave reflections
  - Local hydrodynamic differences
- **Recommendation**: Monitor over multiple days to establish typical range

### Missing Data Handling
- If Surrey API down → no geodetic offsets available
- Current fallback: Use ECCC storm surge forecast
- **Future**: Could interpolate from last known offset with staleness warning

### Datum Assumptions
- Assumes channel 2126/2129 correctly represent datum offsets
- No independent validation against surveyed benchmarks
- **Future**: Compare to DFO Point Atkinson gauge for cross-validation

---

## Success Criteria Met ✅

1. ✅ Both stations export `geodetic_offsets` to JSON
2. ✅ Frontend applies correct methodology to each station
3. ✅ No console errors during export
4. ✅ Data pipeline working end-to-end
5. ⏳ Residual alignment validation (requires user testing)
6. ⏳ Documentation updated (in progress)

---

## Next Steps

1. **User Testing**: Verify residuals align visually on website
2. **Monitor**: Watch channel values over several days
3. **Document**: Update methodology doc with actual channel 2129 values
4. **Validate**: Compare corrected residuals to ECCC storm surge forecasts
5. **Future**: Consider adding staleness warnings for offset data

---

## Rollback Plan

If issues found:
```bash
# Revert export script changes
git checkout HEAD~1 scripts/export/export_tide_json.py

# Re-run export
/home/keelando/envcan_wave/.venv/bin/python3 scripts/export/export_tide_json.py
```

Crescent Beach will continue working (unaffected).
Crescent Channel will fall back to ECCC storm surge.

---

## Questions Answered

**Q: What is the typical value of channel 2129?**
A: Average +0.03m (range -0.13m to +0.45m over 24 hours)

**Q: Why apply offset to observation for CC but prediction for CB?**
A: Based on datum trust - CB observations verified against DFO gauge, so we correct predictions. CC observations appear to have offset, so we correct observations.

**Q: Why is channel 2129 stored with surrey_crescent_ocean?**
A: Because it's fetched from Crescent Beach site (site 20182) where it's defined. It measures CB vs CC difference, so it's logically part of CB's geodetic analysis channels.

---

## References

- Methodology document: `docs/GEODETIC_OFFSETS_METHODOLOGY.md`
- Implementation plan: `docs/GEODETIC_TIDE_IMPLEMENTATION_PLAN.md`
- Export script: `scripts/export/export_tide_json.py`
- Frontend code: `~/site/assets/js/tides.js` (lines 910-1260)
- Database: `~/.local/share/tide_data.sqlite` (table: `surrey_geodetic_data`)
