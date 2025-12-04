# Environment Canada Buoy Data Guide

Comprehensive guide to understanding EC buoy data collection, field availability, and reporting patterns.

---

## Buoy Types & Capabilities

Environment Canada operates different buoy types with varying sensor configurations:

### Wave Buoys (Full Suite)
**Buoys:** Halibut Bank (4600146), Sentry Shoal (4600131)

**Sensors:**
- Wave height/period/direction sensors (Datawell or similar)
- Dual wind sensors (redundancy)
- Air & sea temperature
- Barometric pressure (dual sensors)
- GPS positioning
- Solar panel current monitor

**Reporting frequency:**
- **Every 10 minutes:** Wind, temp, pressure, position, solar
- **Every hour:** Full wave suite + all other metrics

**Total fields:** ~55 (35 we capture after filtering diagnostics)

### Met Buoys (Limited Wave Data)
**Buoys:** English Bay (4600304), Southern Georgia Strait (4600303)

**Sensors:**
- **Limited wave capability** - basic height/period only
- Dual wind sensors (redundancy)
- Air & sea temperature
- Barometric pressure (dual sensors)
- GPS positioning
- Solar panel current monitor

**Reporting frequency:**
- **Every 10 minutes:** Wind, temp, pressure, position, solar (wave fields = MSNG)
- **Every hour:** Basic wave data + all other metrics

**Total fields:** ~42 (17-25 we capture depending on time)

**Key limitation:** Wave fields report "MSNG" (missing) except on the hour

---

## Reporting Patterns

### 10-Minute Observations (XX:00, XX:10, XX:20, etc.)

**Wave Buoys (Halibut, Sentry):**
```
✓ Full wave suite (15 fields)
✓ Wind speed/gust/direction (both sensors)
✓ Air & sea temperature
✓ Pressure (station, MSL, trend)
✓ GPS position
✓ Solar current
```

**Met Buoys (English Bay, Southern Strait):**
```
✗ Wave fields = MSNG (missing)
✓ Wind speed/gust/direction (both sensors)
✓ Air & sea temperature
✓ Pressure (station, MSL, trend)
✓ GPS position
✓ Solar current
```

### Hourly Observations (XX:00 only)

**All Buoys:**
```
✓ All available sensors report data
✓ Wave buoys: Full 15-field wave suite
✓ Met buoys: Basic 8-field wave suite
```

---

## Field Name Variations

Environment Canada uses different field names across buoy types. Our parser handles all variants.

### Wind Gust Field Names

**Wave Buoys (Halibut, Sentry):**
- `max_wnd_spd_pst10mts` = Maximum wind speed past 10 minutes
- `max_wnd_spd_pst10mts_1` = Sensor 1
- `max_wnd_spd_pst10mts_2` = Sensor 2

**Met Buoys (English Bay, Southern Strait):**
- `max_avg_wnd_spd_pst10mts` = Maximum average wind speed past 10 minutes
- `max_avg_wnd_spd_pst10mts_1` = Sensor 1
- `max_avg_wnd_spd_pst10mts_2` = Sensor 2

**Our mapping:** Both map to `wind_gust` in the database

### Wave Field Names

**Wave Buoys (Full Suite):**
- `avg_sig_wave_hgt_pst20mts` - Significant wave height
- `avg_pk_wave_dir_pst20mts` - Peak wave direction
- `avg_wave_dir_sprd_pst20mts` - Direction spread (sea state quality)
- `spetrl_sig_wave_hgt_pst20mts` - Spectral significant height
- `max_wave_hgt_pst20mts` - Maximum wave height
- Plus 10 more wave fields...

**Met Buoys (Limited Suite - hourly only):**
- `sig_wave_hgt_pst20mts` - Significant wave height (simpler name)
- `pk_wave_hgt_pst20mts` - Peak wave height
- `avg_max_wave_hgt_pst20mts` - Average max wave height
- `avg_wave_hgt_pst20mts` - Average wave height
- `pk_wave_pd_pst20mts` - Peak wave period
- 3 more basic wave fields...

**Note:** Met buoys lack directional data and spectral analysis

---

## Database Schema

All captured fields (35 total after filtering diagnostics):

### Wave Metrics (15 fields)
```
wave_height_sig              - Significant wave height (m)
wave_height_peak            - Peak wave height (m)
wave_height_max             - Maximum wave height in 20min (m)
wave_height_avg             - Average wave height (m)
wave_height_spectral        - Spectral significant height (m)
wave_crest_height_max       - Max crest above water level (m)

wave_period_sig             - Significant wave period (s)
wave_period_avg             - Average wave period (s)
wave_period_peak            - Peak wave period (s)
wave_period_max_wave        - Period of max wave (s)
wave_period_spectral        - Spectral period (s)
wave_period_energy_spectral - Spectral energy period (s)

wave_direction_avg          - Average wave direction (°)
wave_direction_peak         - Peak wave direction (°)
wave_direction_spread_avg   - Direction spread - sea state chaos (°)
wave_direction_spread_peak  - Peak direction spread (°)
```

### Wind Metrics (8 fields)
```
wind_speed                  - Primary sensor wind speed (km/h → knots)
wind_gust                   - Primary sensor wind gust (km/h → knots)
wind_direction              - Primary sensor direction (°)
wind_sensor_height          - Anemometer height above sea level (m)

wind_speed_sensor_2         - Redundant sensor wind speed (km/h → knots)
wind_gust_sensor_2          - Redundant sensor wind gust (km/h → knots)
wind_direction_sensor_2     - Redundant sensor direction (°)

wind_samples_bad_1          - QC: Bad samples sensor 1 (count)
wind_samples_bad_2          - QC: Bad samples sensor 2 (count)
```

### Temperature (2 fields)
```
air_temp                    - Air temperature (°C)
sea_temp                    - Sea surface temperature (°C)
```

### Pressure (5 fields)
```
pressure                    - Station pressure (hPa)
pressure_msl                - Mean sea level pressure (hPa) - better for forecasting
pressure_sensor_2           - Redundant sensor (hPa)
pressure_trend_char         - 3hr tendency code (0=steady, 1=rising, 8=falling)
pressure_trend_amount       - 3hr pressure change (hPa)
```

### Position (2 fields)
```
buoy_lat_current            - Current GPS latitude (°) - tracks drift
buoy_lon_current            - Current GPS longitude (°) - tracks drift
```

### Environmental (1 field)
```
solar_current               - Solar panel current (A) - cloudiness indicator!
```

---

## Data Freshness & Export Logic

### Per-Field Freshness Window (2 hours)

Each metric independently queries for the most recent **non-null** value within a 2-hour window.

**Why this matters:**
- Wave buoys report waves every 10 min → always fresh
- Met buoys report waves only hourly → can be up to 1 hour old
- All buoys report wind/temp every 10 min → always fresh

**Example (English Bay at 19:30):**
```json
{
  "wind_speed": 17.3,              // From 19:20 observation (10 min old)
  "wave_height_sig": 0.2,          // From 19:00 observation (30 min old)
  "observation_time": "19:20:00Z"  // Latest observation timestamp
  "field_times": {
    "wave_height_sig": "19:00:00Z" // Individual field timestamp if different
  }
}
```

### Export Behavior

**JSON exports** (`latest_buoy_v2.json`):
- Only includes fields with non-null values within 2-hour window
- Met buoy wave data disappears after 2 hours if no hourly update
- Wind/temp data always present (10-minute updates)

**Timeseries exports** (`buoy_timeseries_24h.json`):
- Downsampled to hourly intervals
- Met buoys show wave data gaps (null) between hourly points
- Wave buoys show continuous wave data

---

## Field Comparison Matrix

| Field Category | Halibut Bank (Wave) | English Bay (Met) | Update Frequency |
|----------------|-------------------|------------------|------------------|
| Wave height (basic) | ✓ 6 fields | ✓ 4 fields | HB: 10min, EB: hourly |
| Wave period (basic) | ✓ 6 fields | ✓ 3 fields | HB: 10min, EB: hourly |
| Wave direction | ✓ 4 fields | ✗ None | HB: 10min |
| Wind speed/gust | ✓ Dual sensors | ✓ Dual sensors | Both: 10min |
| Wind direction | ✓ Dual sensors | ✓ Dual sensors | Both: 10min |
| Air/sea temp | ✓ | ✓ | Both: 10min |
| Pressure (station/MSL) | ✓ Dual sensors | ✓ Dual sensors | Both: 10min |
| Pressure trend | ✓ 3hr change | ✓ 3hr change | Both: 10min |
| GPS position | ✓ | ✓ | Both: 10min |
| Solar current | ✓ | ✓ | Both: 10min |

---

## NOAA Buoys (Comparison)

NOAA buoys (Neah Bay, New Dungeness) have different capabilities:

**Additional NOAA fields:**
- Spectral wave separation: Swell vs wind waves
- 6 extra fields: swell_height, swell_period, swell_direction, wind_wave_height, wind_wave_period, wind_wave_direction

**NOAA reporting:**
- Varies by station (10min to hourly)
- Consistent field names across all NOAA stations
- No "MSNG" pattern - fields either present or absent

---

## Diagnostic Fields (Available but Not Captured)

The following fields exist in EC buoy XMLs but are **not currently captured** (per 2025-12-04 decision):

```
avg_batry_volt_pst10mts           - Battery voltage (V)
wtchmn_boot_cnt_pst1hr            - Watchman boot count (system health)
avg_cmpss_hdng_pst10mts_1/2       - Compass heading dual sensors (°)
avg_wtr_lvl_snsr_volt_pst10mts    - Water level sensor voltage (V)
avg_obstrn_lamp_crnt_pst10mts     - Obstruction lamp current (A)
```

**Rationale:** Technical/diagnostic data not relevant for marine weather users.

---

## Troubleshooting

### "Why are wave fields null for English Bay?"

**Check observation time:**
- If timestamp ends in :10, :20, :30, :40, :50 → Wave fields expected to be null
- If timestamp ends in :00 → Wave fields should have data

**Expected behavior:**
```python
# English Bay at 19:20
wave_height_sig = None  # ✓ Correct (off-hour)

# English Bay at 19:00
wave_height_sig = 0.2   # ✓ Correct (hourly)
```

### "Why do Halibut and English Bay have different wind gust field names?"

Different buoy hardware/firmware versions use different XML field names. Our parser handles all variants via the `FIELD_MAP` dictionary in `buoy_to_influx_sqlite.py`.

### "Are wave direction spread values useful?"

**Yes!** Direction spread indicates sea state quality:
- **Low spread (10-20°):** Organized swell, clean conditions
- **High spread (30-40°):** Choppy, disorganized sea state, multiple directions

---

## References

- **EC SWOB-ML Format:** https://dd.weather.gc.ca/observations/doc/
- **Buoy Metadata:** `config/stations.json`
- **Parser Implementation:** `buoy_to_influx_sqlite.py`
- **Export Logic:** `sqlite_to_json.py`, `export_24hr_timeseries.py`

---

**Last Updated:** 2025-12-04
**Applies to:** Environment Canada SWOB-ML buoy observations
