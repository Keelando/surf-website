# Geodetic Tide Offset Methodology

**Last Updated**: 2025-12-20
**Status**: Under validation
**Author**: Generated from implementation experience

## Table of Contents
1. [The Physical Puzzle](#the-physical-puzzle)
2. [Background](#background)
3. [Channel Selection Rationale](#channel-selection-rationale)
4. [Mathematical Formulas](#mathematical-formulas)
5. [Code Implementation](#code-implementation)
6. [Data Flow](#data-flow)
7. [Testing & Channel Selection Journey](#testing--channel-selection-journey)
8. [Validation Criteria](#validation-criteria)
9. [Future Considerations](#future-considerations)

---

## The Physical Puzzle

### Problem Discovery

Two nearby geodetic tide stations at Crescent Beach showed **wildly different residuals** when comparing raw observations to astronomical predictions:

| Station | Raw Residual (obs - pred) | Expected? |
|---------|---------------------------|-----------|
| **Crescent Beach Ocean** | ~0.1m | ✓ Reasonable |
| **Crescent Channel Ocean** | ~0.8m | ✗ Puzzling |

**Key Insight**: These stations are geographically very close (~500m apart). Such different residuals suggest **datum alignment issues**, not actual physical differences in water level.

### The Question

Why would two nearby tide stations show such dramatically different apparent residuals when measuring the same ocean?

### The Answer

The observation systems use different **vertical datums** (reference levels):
- **DFO astronomical predictions**: Chart Datum (height above lowest astronomical tide)
- **Surrey FlowWorks observations**: CGVD28 (Canadian Geodetic Vertical Datum 1928, referenced to mean sea level)

The ~0.7m discrepancy between raw residuals is primarily a **datum artifact**, not real storm surge.

---

## Background

### Geodetic Datums Explained

**Chart Datum** (used by DFO/CHS):
- Defined as the lowest astronomical tide (LAT)
- Used for nautical charts and navigation
- Heights are always positive (water is above LAT)
- Local reference - varies by location

**CGVD28** (Canadian Geodetic Vertical Datum 1928):
- National geodetic datum based on mean sea level
- Used for surveying and engineering
- Can have negative values (below mean sea level)
- Consistent reference across Canada

**The Offset**: The difference between these two datums varies by location. At Crescent Beach, Chart Datum is approximately 2-3m below CGVD28.

### Surrey FlowWorks Integration

**What is Surrey FlowWorks?**
- Municipal water monitoring system operated by City of Surrey
- Provides radar-based water level measurements at coastal locations
- Co-located with wave buoy stations (CRPILE, CRCHAN)
- Measures in CGVD28 datum

**Available Geodetic Analysis Channels**:
| Channel | Description | Typical Value | Usage |
|---------|-------------|---------------|-------|
| **2126** | CB vs CC (Predicted Tide) | ~-0.34m | Crescent Beach calibration |
| **2129** | CB vs CC (Radar) | TBD | Crescent Channel calibration |
| 2414 | Tidal Residual | TBD | Not currently used |
| 2454 | CB PT vs Radar | TBD | Not currently used |

---

## Channel Selection Rationale

### Solving the Residual Puzzle

**Channel 2126: Crescent Beach vs Crescent Channel (Predicted Tide)**
- **Measures**: Difference between Crescent Beach observation and Crescent Channel predicted tide
- **Typical value**: ~-0.34m
- **Physical meaning**: The datum offset needed to align CB observations with CC predictions
- **Used for**: **Crescent Beach Ocean** station calibration

**Application**:
```
calibrated_prediction = CC_astronomical_prediction + offset_2126
residual = CB_observation - calibrated_prediction
```

**Rationale**:
- We trust the Crescent Beach observation datum (verified against DFO gauge)
- DFO predictions are in Chart Datum
- Apply offset to prediction to bring it into CB observation datum
- Result: Realistic residual representing actual storm surge and local effects

---

**Channel 2129: Crescent Beach vs Crescent Channel (Radar)**
- **Measures**: Offset between Crescent Beach radar and Crescent Channel radar observations
- **Typical value**: ~+0.03m (range: -0.13m to +0.45m)
- **Physical meaning**: The datum offset between the two Surrey radar stations
- **Used for**: **Crescent Channel Ocean** station calibration

**Application**:
```
calibrated_observation = CC_raw_observation + offset_2129
residual = calibrated_observation - CC_astronomical_prediction
```

**Rationale**:
- The Crescent Channel observation appears to have datum offset issues
- Correct the observation to align with the prediction datum
- Result: Realistic residual comparable to Crescent Beach

---

### Why Different Approaches?

The decision to apply offsets to different variables (prediction vs observation) is based on:

1. **Data Trust**: For Crescent Beach, we trust the observation datum more, so we correct predictions. For Crescent Channel, the observation datum appears less reliable.

2. **Testing Consideration**: *[TODO: Document reasoning - was this based on data quality analysis, stability testing, or experimental comparison?]*

3. **Validation Goal**: Both approaches should produce similar residual patterns after correction, confirming the methodology removes datum artifacts.

---

## Mathematical Formulas

### Crescent Beach Ocean (`crescent_beach_ocean`)

**Methodology**: Apply geodetic offset to **prediction**

```
# Step 1: Get geodetic offset from Channel 2126
offset_2126(t) = CB_observation(t) - CC_prediction(t)

# Step 2: Apply offset to prediction
calibrated_prediction(t) = CC_prediction(t) + offset_2126(t)

# Step 3: Calculate residual
residual(t) = CB_observation(t) - calibrated_prediction(t)
```

**What the residual represents**:
- After datum correction, residual = actual storm surge + local hydrodynamic effects + prediction error
- No longer contaminated by ~0.34m datum artifact

---

### Crescent Channel Ocean (`crescent_channel_ocean`)

**Methodology**: Apply geodetic offset to **observation**

```
# Step 1: Get geodetic offset from Channel 2129
offset_2129(t) = CB_radar(t) - CC_radar(t)

# Step 2: Apply offset to observation
calibrated_observation(t) = CC_observation(t) + offset_2129(t)

# Step 3: Calculate residual
residual(t) = calibrated_observation(t) - CC_prediction(t)
```

**What the residual represents**:
- Datum-corrected observation compared to astronomical prediction
- Should be comparable to Crescent Beach residuals
- Represents storm surge + local effects after alignment

---

## Code Implementation

### Frontend

**File**: `/home/keelando/site/assets/js/tides.js`

**Key Sections**:

| Lines | Purpose |
|-------|---------|
| 911-958 | Dual methodology implementation - applies offsets to prediction OR observation based on station |
| 960-1018 | Residual calculation for each methodology with time-matched data points |
| 466-553 | Storm surge card calculation showing real-time residual |
| 1204-1248 | Conditional chart series display (hides astronomical tide or raw observation) |
| 1116-1149 | Dynamic legend based on station type |

**Time Matching Logic**:
- Observations and predictions may not share exact timestamps
- Code finds closest match within 5 minutes (300,000 ms)
- Ensures residual calculation uses temporally-aligned data

---

### Backend

**Fetch**: `/home/keelando/envcan_wave/scripts/fetch/fetch_surrey_tides.py`
- Queries Surrey FlowWorks API for geodetic offset channels (2126, 2129)
- Fetches 48-hour time window
- Stores in SQLite database

**Store**: `/home/keelando/envcan_wave/scripts/parse/tide_to_sqlite.py`
- Table: `surrey_geodetic_data`
- Schema: `timestamp`, `station_id`, `channel`, `offset_value`

**Export**: `/home/keelando/envcan_wave/scripts/export/export_tide_json.py`
- Reads geodetic offsets from SQLite
- Exports to `/data/tide-timeseries.json` as `geodetic_offsets` array
- Frontend consumes this JSON for calibration

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. DATA ACQUISITION                                             │
└─────────────────────────────────────────────────────────────────┘
   Surrey FlowWorks API (channels 2126, 2129)
            ↓
   fetch_surrey_tides.py (every 30 min via cron)
            ↓
   SQLite: surrey_geodetic_data table

┌─────────────────────────────────────────────────────────────────┐
│ 2. DATA EXPORT                                                  │
└─────────────────────────────────────────────────────────────────┘
   SQLite (surrey_geodetic_data + tide_predictions + observations)
            ↓
   export_tide_json.py (every 30 min via cron)
            ↓
   /home/keelando/site/data/tide-timeseries.json
   {
     "stations": {
       "crescent_beach_ocean": {
         "predictions": [...],
         "observations": [...],
         "geodetic_offsets": [
           {"time": "2025-12-20T10:00:00Z", "value": -0.34},
           ...
         ]
       },
       "crescent_channel_ocean": {
         "predictions": [...],
         "observations": [...],
         "geodetic_offsets": [...]
       }
     }
   }

┌─────────────────────────────────────────────────────────────────┐
│ 3. FRONTEND PROCESSING                                          │
└─────────────────────────────────────────────────────────────────┘
   Browser loads https://halibutbank.ca/tides.html
            ↓
   tides.js fetches tide-timeseries.json
            ↓
   Applies geodetic offset methodology:
     - Crescent Beach: offset applied to prediction
     - Crescent Channel: offset applied to observation
            ↓
   Calculates residuals using time-matched data
            ↓
   Renders ECharts visualization + storm surge card
```

---

## Testing & Channel Selection Journey

### Initial Problem Discovery (December 2025)

**Observation**:
- Crescent Beach Ocean: `residual = obs - pred ≈ 0.1m`
- Crescent Channel Ocean: `residual = obs - pred ≈ 0.8m`

**Hypothesis**:
The 0.7m difference is too large to be real storm surge between two stations only 500m apart. Likely caused by datum mismatch.

---

### Available Channels Evaluated

| Channel | Description | Evaluation | Decision |
|---------|-------------|------------|----------|
| 2126 | CB vs CC (Predicted Tide) | ~-0.34m offset, stable | ✓ **Used for CB** |
| 2129 | CB vs CC (Radar) | *[TODO: Document value]* | ✓ **Used for CC** |
| 2414 | Tidal Residual | *[TODO: Why not used?]* | ✗ Not used |
| 2454 | CB PT vs Radar | *[TODO: Why not used?]* | ✗ Not used |

**Selection Criteria**:
- Channel 2126 chosen for Crescent Beach: Direct comparison of CB observation vs CC prediction (PT-based)
- Channel 2129 chosen for Crescent Channel: Radar-to-radar comparison between stations
- Channels 2414 and 2454 not used: These are alternative analyses that could be used for validation
- Selection based on: Direct applicability to each station's methodology (obs vs pred)

---

### Methodology Selection

**Why apply offset to prediction for Crescent Beach?**
- CB observation datum verified against DFO tide gauge (trusted reference)
- Predictions are in Chart Datum, observations in CGVD28
- Apply offset to prediction to bring it into observation datum
- Result: Residual calculated in trusted CB observation datum

**Why apply offset to observation for Crescent Channel?**
- CC observation datum appears to have larger offset from Chart Datum
- Use channel 2129 to align CC observations with CB observations
- Apply offset to observation to correct for datum difference
- Result: Residual comparable to Crescent Beach after correction

---

### Current Validation Status

**As of 2025-12-20**:
- ✓ Methodology implemented in code (frontend and backend)
- ✓ Export script fixed to include both geodetic channels
- ✓ Data flow verified end-to-end (database → JSON → frontend)
- ✓ Channel values documented (2126: -0.33m, 2129: +0.03m)
- ⏳ User visual validation in progress
- ⏳ Awaiting comparison with independent measurements
- ⏳ Testing over full tidal cycle for systematic biases

**Next Steps**:
1. Monitor residuals from both stations over multiple tidal cycles
2. Compare corrected residuals to ECCC storm surge forecasts
3. Check for temporal stability of geodetic offsets
4. Validate that both stations show similar residual patterns

---

## Validation Criteria

### Success Criteria

✓ **Residual Alignment**:
- Post-correction residuals from both stations should be similar (within ±0.1m)
- Patterns should match over tidal cycle

✓ **Physical Plausibility**:
- Residuals should track ECCC storm surge forecasts reasonably
- No systematic biases (residual should oscillate around zero)
- Residuals should correlate with wind/pressure events

✓ **Temporal Stability**:
- Geodetic offsets should be relatively stable over time
- Any drift in offsets should be slow and explainable

---

### Known Limitations

⚠ **Surrey Radar Accuracy**:
- Radar water level sensors may have accuracy limitations (~±5cm)
- Reflections from waves can introduce noise
- Heavy rain may affect radar readings

⚠ **Geodetic Offset Uncertainty**:
- Surrey's geodetic analysis channels have their own uncertainty
- Offset values may drift due to sensor recalibration or datum shifts

⚠ **Local Hydrodynamics**:
- Even after datum correction, some residual differences may remain
- Crescent Beach vs Crescent Channel may have real local effects (currents, bathymetry)
- These are valid physical differences, not errors

---

### Edge Cases

**Missing Geodetic Offset Data**:
- If Surrey API fails or offset data is unavailable
- **Current behavior**: Fall back to ECCC storm surge forecast
- **Future consideration**: Interpolate from last known offset? Alert user?

**Large Deviations from ECCC Forecast**:
- If corrected residual differs significantly from ECCC forecast (>0.5m)
- Could indicate: (1) actual extreme event, (2) datum issue, or (3) sensor malfunction
- **Current behavior**: Display both for comparison
- **Future consideration**: Flag unusual deviations? Automated quality check?

**Stale or Suspicious Observation Data**:
- Surrey sensors occasionally report stale or erroneous values
- **Current behavior**: Timestamps shown, user can judge freshness
- **Future consideration**: Automated staleness detection? Quality flags?

---

## Future Considerations

### Additional Geodetic Stations

Surrey FlowWorks expands to other coastal locations → apply similar methodology:
- Evaluate datum offset at each new station
- Select appropriate geodetic channel
- Determine whether to correct prediction or observation

---

### Other Channel Applications

**Channel 2414 (Tidal Residual)**:
- Direct tidal residual calculation by Surrey
- Potential to compare with our calculated residuals
- Could validate our methodology

**Channel 2454 (CB PT vs Radar)**:
- Pressure transducer vs radar comparison
- Could indicate sensor drift or data quality issues
- Useful for quality assurance

---

### Automation of Channel Selection

**Current**: Manual selection of channels 2126 and 2129 hardcoded per station

**Future**: Automated selection based on:
- Real-time data quality metrics
- Correlation with known reference stations
- Temporal stability of offsets
- Cross-validation between multiple channels

---

## References

- DFO Tide Tables: https://www.tides.gc.ca/
- CGVD28 Documentation: https://www.nrcan.gc.ca/
- Surrey FlowWorks: https://surrey.flowworks.com/
- Project Documentation: `/home/keelando/envcan_wave/docs/`

---

## Contact & Feedback

If you have questions about this methodology or notice unusual residuals, please:
- Open an issue: https://github.com/keelananderson/envcan-wave/issues
- Email: *[TODO: Add contact email]*

---

**Document History**:
- 2025-12-20: Initial documentation created
- *[Future updates will be logged here]*
