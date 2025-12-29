# Geodetic Tide Offset Implementation Plan

**Created**: 2025-12-20
**Status**: Ready for implementation
**Goal**: Fix geodetic tide offset calculation and display for Surrey FlowWorks stations

---

## Current Status

### What's Working ✓
1. Database table `surrey_geodetic_data` exists with correct schema
2. Data is being fetched from Surrey API (channels 2126 and 2129)
3. Crescent Beach Ocean exports geodetic offsets (channel 2126: ~-0.34m)
4. Frontend has dual methodology code ready for both stations
5. Comprehensive methodology documentation exists

### What's Broken ✗
1. **Crescent Channel Ocean missing geodetic offsets in JSON export**
   - Export script only handles `surrey_crescent_ocean`
   - Missing export for `surrey_crescent_channel` using channel 2129
2. **Frontend can't apply Crescent Channel methodology without offset data**
3. **Validation incomplete** - need to verify residuals align after correction

---

## Problem Summary

Two nearby tide stations show wildly different residuals due to datum misalignment:
- **Crescent Beach Ocean**: residual ~0.1m (reasonable)
- **Crescent Channel Ocean**: residual ~0.8m (datum artifact!)

**Solution**: Apply geodetic offsets from Surrey's calibration channels:
- Channel 2126 (CB vs CC PT): ~-0.34m → Apply to **Crescent Beach predictions**
- Channel 2129 (CB vs CC Radar): TBD → Apply to **Crescent Channel observations**

---

## Implementation Tasks

### Task 1: Fix Export Script
**File**: `/home/keelando/envcan_wave/scripts/export/export_tide_json.py`

**Current code** (lines 364-387):
```python
if station_id == "surrey_crescent_ocean":
    # Only exports for Crescent Beach!
    # Channel 2126: CB vs CC (PT)
    ...
```

**Required changes**:
1. Add elif block for `station_id == "surrey_crescent_channel"`
2. Query `geodiff_cb_vs_cc` column instead of `geodiff_cbvscc_pt`
3. Export as `geodetic_offsets` array (same format)

**Expected behavior**:
- Both stations export `geodetic_offsets` array
- Crescent Beach: uses channel 2126 (geodiff_cbvscc_pt)
- Crescent Channel: uses channel 2129 (geodiff_cb_vs_cc)

---

### Task 2: Verify Data Fetching
**File**: `/home/keelando/envcan_wave/scripts/fetch/fetch_surrey_tides.py`

**Check**:
- [ ] Channel 2129 is being fetched (line 321: geodiff_cb_vs_cc)
- [ ] Data is stored in database correctly
- [ ] Both stations have recent data

**Test query**:
```sql
SELECT station_id, COUNT(*) as count,
       MAX(datetime(observation_time, 'unixepoch')) as latest
FROM surrey_geodetic_data
WHERE geodiff_cb_vs_cc IS NOT NULL
   OR geodiff_cbvscc_pt IS NOT NULL
GROUP BY station_id;
```

**Expected result**:
- Both `surrey_crescent_ocean` and `surrey_crescent_channel` should have data
- Timestamps should be recent (< 1 hour old)

---

### Task 3: Frontend Verification
**File**: `/home/keelando/site/assets/js/tides.js`

**Verify logic** (lines 913-958):
- [ ] Line 913: `isCrescentChannel` correctly identifies station
- [ ] Line 914: `isCrescentBeach` correctly identifies station
- [ ] Lines 928-949: Crescent Channel applies offset to **observations**
- [ ] Lines 950-958: Crescent Beach applies offset to **predictions**
- [ ] Lines 964-1020: Residual calculations use correct methodology

**Expected behavior**:
- If `stationKey === 'crescent_channel_ocean'` AND `geodetic_offsets` exists:
  → Create `calibratedObservation` array (obs + offset)
  → Calculate residual = calibratedObs - prediction
- If `stationKey === 'crescent_beach_ocean'` AND `geodetic_offsets` exists:
  → Create `calibratedPrediction` array (pred + offset)
  → Calculate residual = observation - calibratedPred

---

### Task 4: End-to-End Testing

#### Step 1: Verify Database Content
```bash
# Check channel 2126 data (Crescent Beach)
sqlite3 ~/.local/share/tide_data.sqlite \
  "SELECT COUNT(*), AVG(geodiff_cbvscc_pt)
   FROM surrey_geodetic_data
   WHERE geodiff_cbvscc_pt IS NOT NULL
     AND observation_time > strftime('%s', 'now', '-2 hours')"

# Expected: COUNT > 0, AVG ≈ -0.34
```

```bash
# Check channel 2129 data (Crescent Channel)
sqlite3 ~/.local/share/tide_data.sqlite \
  "SELECT COUNT(*), AVG(geodiff_cb_vs_cc)
   FROM surrey_geodetic_data
   WHERE geodiff_cb_vs_cc IS NOT NULL
     AND observation_time > strftime('%s', 'now', '-2 hours')"

# Expected: COUNT > 0, AVG = TBD (document this!)
```

#### Step 2: Verify JSON Export
```bash
# Run export
/home/keelando/envcan_wave/.venv/bin/python3 \
  /home/keelando/envcan_wave/scripts/export/export_tide_json.py

# Check Crescent Beach has geodetic_offsets
jq '.stations.crescent_beach_ocean | has("geodetic_offsets")' \
  ~/site/data/tide-timeseries.json
# Expected: true

# Check Crescent Channel has geodetic_offsets
jq '.stations.crescent_channel_ocean | has("geodetic_offsets")' \
  ~/site/data/tide-timeseries.json
# Expected: true (after fix!)

# Check offset values
jq '.stations.crescent_beach_ocean.geodetic_offsets[0]' \
  ~/site/data/tide-timeseries.json
# Expected: {"time": "...", "value": -0.34...}

jq '.stations.crescent_channel_ocean.geodetic_offsets[0]' \
  ~/site/data/tide-timeseries.json
# Expected: {"time": "...", "value": ???}
```

#### Step 3: Frontend Testing
1. Open `https://halibutbank.ca/tides.html`
2. Select "Crescent Beach Ocean" from dropdown
3. Open browser DevTools → Console
4. Look for residual calculations in chart data
5. Verify residuals are ~0.1m, not ~0.8m
6. Repeat for "Crescent Channel Ocean"
7. Compare residuals - should be similar after correction

---

### Task 5: Validation & Documentation

#### Validation Criteria
1. **Data Availability**:
   - [ ] Both stations have geodetic_offsets in JSON
   - [ ] Offsets update every 15 minutes (match observation frequency)
   - [ ] No missing data gaps > 1 hour

2. **Residual Alignment**:
   - [ ] Crescent Beach residuals: -0.2m to +0.3m range (after correction)
   - [ ] Crescent Channel residuals: -0.2m to +0.3m range (after correction)
   - [ ] Residual patterns match between stations (±0.1m tolerance)

3. **Temporal Stability**:
   - [ ] Geodetic offsets stable over 24 hours (< ±0.05m drift)
   - [ ] No systematic bias in residuals (average ≈ 0m)

#### Documentation Updates
1. **GEODETIC_OFFSETS_METHODOLOGY.md**:
   - [ ] Document actual channel 2129 values (TBD currently)
   - [ ] Fill in "Why Different Approaches?" section
   - [ ] Document validation results
   - [ ] Add screenshots showing corrected residuals

2. **README or CHANGELOG**:
   - [ ] Document the fix for Crescent Channel
   - [ ] Explain why two methodologies are needed
   - [ ] Link to methodology document

---

## Edge Cases to Handle

### Missing Geodetic Data
**Scenario**: Surrey API down, no offset data available
**Current behavior**: Frontend falls back to ECCC storm surge
**Action**: Verify fallback works gracefully

### Stale Offset Data
**Scenario**: Last offset > 2 hours old
**Current behavior**: Uses last known offset
**Action**: Consider adding staleness warning in UI

### Large Deviations
**Scenario**: Corrected residual differs from ECCC forecast by >0.5m
**Current behavior**: Both displayed for comparison
**Action**: Document this is expected during extreme events

---

## Success Criteria

✅ **Implementation complete when**:
1. Both stations export geodetic_offsets
2. Frontend applies correct methodology to each station
3. Residuals from both stations align (±0.1m)
4. No console errors or missing data warnings
5. Validation criteria met over 24-hour test period
6. Documentation updated with actual values

---

## Rollback Plan

If issues arise:
1. Revert export script changes
2. Frontend will fall back to ECCC storm surge for Crescent Channel
3. Crescent Beach will continue using channel 2126 (unaffected)

---

## Questions to Resolve

1. **What is the typical value of channel 2129?**
   Need to document this in methodology after observing real data

2. **Why apply offset to observation for CC but prediction for CB?**
   Need to verify this is the correct approach (test both ways?)

3. **Should we validate against independent measurements?**
   DFO gauge at Point Atkinson for comparison?

---

## Next Steps

1. ⏭️ Implement export script fix (Task 1)
2. ⏭️ Verify data fetching (Task 2)
3. ⏭️ Test frontend (Task 3)
4. ⏭️ Run validation tests (Task 4)
5. ⏭️ Update documentation (Task 5)
