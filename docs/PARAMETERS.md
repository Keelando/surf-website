# Data Parameters Reference

Complete reference for all data parameters collected and displayed across the system.

**Quick Links:**
- [Buoy Parameters](#buoy-parameters)
- [Wind Station Parameters](#wind-station-parameters)
- [Tide Parameters](#tide-parameters)
- [Website Display Parameters](#website-display-parameters)
- [Quick Reference Table](#quick-reference-table)

---

## Buoy Parameters

### Wave Metrics

| Parameter | Unit | Source | Description |
|-----------|------|--------|-------------|
| `wave_height_sig` | meters | EC, NOAA | Significant wave height (Hs) - average of highest 1/3 of waves |
| `wave_height_peak` | meters | EC | Maximum wave height in sample period |
| `wave_period_avg` | seconds | EC, NOAA | Average wave period |
| `wave_period_peak` | seconds | EC, NOAA | Peak wave period (dominant swell) |
| `wave_direction_avg` | degrees | EC | Average wave direction (meteorological - FROM direction) |
| `wave_direction_peak` | seconds | EC | Peak wave direction |
| `wave_direction_spread_avg` | degrees | EC | Average directional spread |
| `wave_direction_spread_peak` | degrees | EC | Peak directional spread |

### Spectral Wave Data (NOAA Only)

| Parameter | Unit | Source | Description |
|-----------|------|--------|-------------|
| `swell_height` | meters | NOAA | Swell component height (SwH) |
| `swell_period` | seconds | NOAA | Swell period (SwP) |
| `swell_direction` | degrees | NOAA | Swell direction (SwD) |
| `wind_wave_height` | meters | NOAA | Wind wave height (WWH) - locally generated |
| `wind_wave_period` | seconds | NOAA | Wind wave period (WWP) |
| `wind_wave_direction` | degrees | NOAA | Wind wave direction (WWD) |

**Note:** EC buoys do NOT provide spectral data. Only NOAA stations 46087, 46088 have this.

### Wind & Meteorological

| Parameter | Unit | Source | Description |
|-----------|------|--------|-------------|
| `wind_speed` | km/h (stored), knots (display) | EC, NOAA | Sustained wind speed |
| `wind_gust` | km/h (stored), knots (display) | EC, NOAA | Wind gust speed |
| `wind_direction` | degrees | EC, NOAA | Wind direction (meteorological - FROM direction) |
| `air_temp` | °C | EC, NOAA | Air temperature |
| `sea_temp` | °C | EC, NOAA | Sea surface temperature |
| `pressure` | hPa | EC, NOAA | Atmospheric pressure |

**Important:** Wind direction = WHERE wind is COMING FROM (not going to)

---

## Wind Station Parameters

### Wind Measurements

| Parameter | Unit | Description |
|-----------|------|-------------|
| `wind_speed_kmh` | km/h | Sustained wind speed (10-min average) |
| `wind_gust_kmh` | km/h | Peak gust speed |
| `wind_direction_deg` | degrees | Wind direction (FROM) |
| `wind_chill_c` | °C | Wind chill temperature |

### Atmospheric

| Parameter | Unit | Description |
|-----------|------|-------------|
| `air_temp_c` | °C | Air temperature |
| `pressure_hpa` | hPa | Station pressure |
| `pressure_mslp_hpa` | hPa | Mean sea level pressure |
| `humidity_percent` | % | Relative humidity |
| `dewpoint_c` | °C | Dew point temperature |
| `visibility_km` | km | Visibility distance |

### Precipitation

| Parameter | Unit | Description |
|-----------|------|-------------|
| `rainfall_1hr_mm` | mm | Rainfall in past 1 hour |
| `rainfall_6hr_mm` | mm | Rainfall in past 6 hours |

---

## Tide Parameters

### Water Level

| Parameter | Unit | Description |
|-----------|------|-------------|
| `water_level` | meters | Water level above datum |
| `quality` | text | QC flag (good, provisional, etc.) |

**Datums:**
- **Chart Datum (DFO):** 0.0m = Lowest Astronomical Tide (LAT)
- **Geodetic (Surrey):** 0.0m = CGVD28 reference (can be negative)

### High/Low Events

| Parameter | Unit | Description |
|-----------|------|-------------|
| `event_time` | timestamp | Time of high/low tide |
| `water_level` | meters | Predicted height |
| `event_type` | text | "high" or "low" |

### Tide Offset (Storm Surge)

| Parameter | Unit | Description |
|-----------|------|-------------|
| `tide_offset` | meters | Observed - Predicted = Storm surge + forecast error |

---

## Website Display Parameters

### Buoy Cards (`latest_buoy_v2.json`)

```json
{
  "4600146": {
    "name": "Halibut Bank",
    "observation_time": "2024-12-29T22:00:00+00:00",
    "stale": false,
    "wave_height_m": 1.2,
    "wave_period_s": 8.5,
    "wave_direction_deg": 270,
    "wave_direction_cardinal": "W",
    "wind_speed_kt": 15.2,
    "wind_gust_kt": 18.5,
    "wind_direction_deg": 285,
    "wind_direction_cardinal": "WNW",
    "air_temp_c": 8.5,
    "sea_temp_c": 9.2,
    "pressure_hpa": 1013.2
  }
}
```

### Wind Stations (`latest_wind.json`)

```json
{
  "CWSB": {
    "name": "Point Atkinson",
    "observation_time": "2024-12-29T22:00:00+00:00",
    "stale": false,
    "wind_speed_kt": 12.0,
    "wind_gust_kt": 15.5,
    "wind_direction_deg": 300,
    "wind_direction_cardinal": "NW",
    "air_temp_c": 7.8,
    "pressure_hpa": 1015.3,
    "humidity": 72.0,
    "dew_point_c": 3.2
  }
}
```

### Tide Stations (`tide-latest.json`)

```json
{
  "stations": {
    "point_atkinson": {
      "name": "Point Atkinson",
      "observation": {
        "time": "2024-12-29T22:00:00+00:00",
        "value": 2.45,
        "stale": false
      },
      "prediction": {
        "time": "2024-12-29T22:00:00+00:00",
        "value": 2.38
      },
      "tide_offset": {
        "value": 0.07,
        "observation_time": "2024-12-29T22:00:00+00:00",
        "description": "Observed minus predicted"
      }
    }
  }
}
```

---

## Quick Reference Table

### Unit Conversions

| Parameter | Storage | Display | Conversion |
|-----------|---------|---------|------------|
| Wind speed | km/h | knots | kt = km/h × 0.539957 |
| Wave height | meters | meters | 1:1 |
| Temperature | °C | °C | 1:1 |
| Pressure | hPa | hPa | 1:1 |

### Data Freshness Thresholds

| Type | Warning | Error | Typical Update |
|------|---------|-------|----------------|
| Buoys | 2 hours | 4 hours | 1 hour |
| Wind | 2 hours | 4 hours | 10 minutes |
| Tides | 2 hours | 4 hours | 30 minutes |
| Lightstations | 6 hours | 12 hours | 3 hours |

### Direction Convention

**Meteorological (all wind/wave):**
- 0° / 360° = North
- 90° = East
- 180° = South
- 270° = West

**Direction = WHERE IT'S COMING FROM**
- "West wind" (270°) = blowing FROM west TO east
- Arrow points where wind/waves are GOING

### Missing Data Indicators

| Value | Meaning | Display |
|-------|---------|---------|
| `null` | Not available | — |
| `MM` | NOAA missing marker | — |
| `-999` | Invalid/error | — |
| Empty string | No data | — |

**Exception:** Pressure ~999 hPa is VALID (low pressure system)

---

## Data Sources

### Environment Canada (EC)
- **Buoys:** SWOB-ML XML via Sarracenia (sr3)
- **Wind Stations:** SWOB-ML XML via Sarracenia
- **Update:** Every 10 minutes (wind), hourly (buoys)

### NOAA NDBC
- **Buoys:** Text files (.txt) + Spectral (.spec)
- **Update:** Every hour
- **Stations:** 46087, 46088, 46267, CPMW1, SISW1

### DFO IWLS
- **Tide Data:** JSON API
- **Series:** wlo (obs), wlp (pred), wlp-hilo (events)
- **Update:** Hourly (obs), daily (pred)

### Surrey FlowWorks
- **Wave + Tide:** Private API
- **Datum:** CGVD28 (geodetic)
- **Timezone:** Pacific (NOT UTC!)
- **Update:** Every 5-10 minutes

---

**Document Status:** Active (Dec 2024)
**Last Updated:** 2024-12-29
