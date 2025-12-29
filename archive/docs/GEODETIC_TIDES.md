# Geodetic Tide Monitoring

Complete reference for geodetic datum tide stations (Surrey FlowWorks) integration.

**Quick Links:**
- [Overview](#overview)
- [Implementation](#implementation)
- [Offset Methodology](#offset-methodology)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)

---

## Overview

### What are Geodetic Tides?

**Geodetic Datum (CGVD28):** Fixed reference to mean sea level
- 0.0m = geodetic reference point (surveyed)
- Water level can be positive or negative
- Used for land surveying, construction

**Chart Datum:** Maritime reference for navigation
- 0.0m = lowest astronomical tide (LAT)
- Water level always positive in practice
- Used for nautical charts, shipping

### Surrey FlowWorks Stations

**Geodetic stations (CGVD28):**
- Crescent Beach Ocean
- Crescent Channel Ocean

**Chart datum station (for comparison):**
- Crescent Beach Channel (DFO IWLS #07579)

---

## Implementation

### Database Schema

**Table:** `tide_observation`

```sql
CREATE TABLE tide_observation (
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    observation_time INTEGER NOT NULL,
    water_level REAL,  -- In meters, CGVD28 or Chart Datum depending on station
    quality TEXT,
    recorded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (station_id, observation_time)
);
```

### Data Sources

**Surrey API → tide_data.sqlite**

```python
# fetch_surrey_tides.py
def fetch_observations():
    # Fetches water_level_observed channel
    # Returns: water level in meters CGVD28
    # Update frequency: Every 5 minutes
```

**DFO IWLS API → tide_data.sqlite**

```python
# tide_to_sqlite.py
def fetch_observations():
    # Series: wlo (water level observed)
    # Returns: water level in meters Chart Datum
    # Update frequency: Every hour
```

### Frontend Handling

**Tide Charts (tides.js):**

```javascript
// Detect geodetic stations
const isGeodetic = station.source === 'SURREY_FLOWWORKS';

if (isGeodetic) {
  // Display: "Water Level (CGVD28 Geodetic Datum)"
  // Y-axis can show negative values
  // No conversion to chart datum
}
```

**Storm Surge Calculation:**

For geodetic stations:
```javascript
tideOffset = observedWaterLevel - predictedWaterLevel
// Both in CGVD28, so direct subtraction works
```

For chart datum stations:
```javascript
tideOffset = observedWaterLevel - predictedWaterLevel
// Both in Chart Datum, direct subtraction works
```

---

## Offset Methodology

### Measured Offsets Between Stations

Surrey operates multiple stations at Crescent Beach. Measured offsets allow cross-validation.

**Known Offsets (from Surrey FlowWorks analysis):**

| From Station | To Station | Offset (m) | Purpose | Status |
|--------------|------------|------------|---------|--------|
| Crescent Beach (CB) | Crescent Channel (CC) | *[See source data]* | Channel comparison | ✓ Active |
| CB Pressure Transducer | CB Radar | *[Validation]* | Sensor comparison | ✓ Used for CC |
| Tidal Residual | - | *[Research]* | Storm surge study | ✗ Not production |

**Purpose of offsets:**
1. **Cross-validation:** Ensure sensors agree
2. **Backup:** If one station fails, estimate from another
3. **Datum conversion:** Future chart datum support

### Calculating Tide Offset (Storm Surge)

**Formula:**
```
tide_offset = observed_water_level - predicted_water_level
```

**For Surrey stations:**
- Observed: Real-time measurement (CGVD28)
- Predicted: Astronomical tide prediction (CGVD28)
- Offset: Residual (storm surge + forecast error)

**Important:** Both must be in same datum!

**Current Implementation:**

```python
# export_tide_json.py (lines 165-174)
if observation and prediction:
    offset = observation_value - prediction_value
    station_data["tide_offset"] = {
        "value": round(offset, 2),
        "observation_time": observation["time"],
        "description": "Observed minus predicted (storm surge + forecast error)"
    }
```

### Validation Method

**Approach:** Compare Surrey geodetic offset to DFO chart datum offset

1. Get Surrey Ocean observed (CGVD28)
2. Get Surrey Ocean predicted (CGVD28)
3. Calculate offset_A = obs - pred

4. Get DFO Channel observed (Chart Datum)
5. Get DFO Channel predicted (Chart Datum)
6. Calculate offset_B = obs - pred

7. Compare: offset_A ≈ offset_B?

**Expected:** Offsets should be similar (both measure storm surge)
**Difference:** Due to local bathymetry, distance between stations

---

## Validation

### Test Results (Dec 2024)

**Stations tested:**
- Crescent Beach Ocean (Surrey geodetic)
- Crescent Beach Channel (DFO chart datum)

**Methodology:**
1. Simultaneous observations from both stations
2. Calculate tide residuals independently
3. Compare residuals (should match within tolerance)

**Results:**
- ✅ Geodetic calculations producing valid offsets
- ✅ Frontend displaying correctly with datum labels
- ✅ Storm surge card showing calculation timestamp
- ⚠️  Minor discrepancies due to station separation (expected)

### Validation Checklist

- [x] Surrey API fetching correctly (Pacific timezone fix)
- [x] Data stored in tide_data.sqlite
- [x] Predictions and observations both in CGVD28
- [x] Tide offset calculation correct
- [x] Frontend displays geodetic label
- [x] Storm surge comparison includes Surrey stations
- [ ] Cross-station offset validation (future)
- [ ] Long-term accuracy monitoring (future)

---

## Troubleshooting

### Negative Water Levels

**Expected for geodetic datum!**

CGVD28 reference is above LAT, so during low tides:
```
Water level = -0.5m CGVD28  (perfectly normal)
Water level = +2.0m Chart Datum (same water height)
```

### Tide Offset Shows "Invalid Date"

**Issue:** Frontend expects `observation_time` field

**Fix:** Already implemented in export_tide_json.py:174

```python
station_data["tide_offset"] = {
    "value": round(offset, 2),
    "observation_time": station_data["observation"]["time"],  # ← Added
    "description": "..."
}
```

### Surrey Stations Missing from Hindcast

**Issue:** GDSPS doesn't have storm surge forecasts for Surrey locations

**Solution:** Show observed surge only (no forecast comparison)

Update `export_observed_storm_surge.py`:
```python
TIDE_TO_SURGE_MAP = {
    "crescent_pile": "Crescent_Beach_Channel",
    "crescent_beach_ocean": "Crescent_Beach_Ocean",      # Add
    "crescent_channel_ocean": "Crescent_Channel_Ocean"   # Add
}
```

### Stale Tide Data

See [SURREY.md Troubleshooting](#troubleshooting) section

---

## Future Enhancements

### Datum Conversion

**Goal:** Display Surrey data in Chart Datum for user familiarity

**Requirements:**
1. Measure precise offset: CGVD28 → Chart Datum at Crescent Beach
2. Apply conversion: `chart_datum = cgvd28 + offset`
3. Update frontend with toggle: "View in Chart Datum"

**Challenge:** Offset varies slightly with location
- Need survey-grade measurement
- Or calculate from known benchmark

### Additional Geodetic Stations

**Candidates:**
- White Rock (if FlowWorks adds station)
- Boundary Bay (future expansion)

**Integration:**
- Same pipeline as Crescent Beach
- Update stations.json configuration
- Add to frontend geodetic station list

---

## References

**Surrey FlowWorks:**
- API Documentation: Internal/undocumented
- Timezone: America/Vancouver (critical!)
- Update frequency: 5-10 minutes

**DFO IWLS:**
- Station 07579: Crescent Beach (Chart Datum)
- Series: wlo, wlp, wlp-hilo
- Update frequency: 1 hour (observations)

**Geodetic Datums:**
- CGVD28: Canadian Geodetic Vertical Datum 1928
- Chart Datum: Lowest Astronomical Tide (LAT)

---

**Document Status:** Active (Dec 2024)
**Last Updated:** 2024-12-29
**Maintained By:** See docs/project/CLAUDE.md
