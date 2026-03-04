# Daylight Detection for Webcam Captures

This document describes the daylight detection features added to the webcam capture system.

## Overview

The system now includes:
1. **Daylight detection utility** - Calculates sunrise/sunset times and determines if it's daylight
2. **Configurable webcam capture** - Each webcam can be configured to capture 24/7 or only during daylight
3. **Sunlight times export** - Exports detailed sun/twilight times for display on the website

## Components

### 1. Daylight Detection Module (`scripts/utils/daylight.py`)

A utility module that uses the `astral` library to calculate accurate sunrise/sunset times.

**Key functions:**
- `is_daylight(lat, lon, margin_minutes=30)` - Returns True if it's currently daylight
- `calculate_sunrise_sunset(lat, lon, date=None)` - Returns sunrise and sunset times
- `get_daylight_info(lat, lon, date=None)` - Returns detailed daylight information

**Usage:**
```python
from scripts.utils.daylight import is_daylight

# Check if it's daylight in White Rock, BC with 30-minute margin
if is_daylight(49.0253, -122.8031, margin_minutes=30):
    # Capture webcam image
    pass
```

**Command-line testing:**
```bash
python3 scripts/utils/daylight.py whiterock
python3 scripts/utils/daylight.py coxbay
```

### 2. Webcam Capture with Daylight Check (`scripts/fetch/fetch_webcam.py`)

The webcam capture script now supports per-webcam daylight checking.

**Configuration options:**
- `check_daylight` (bool) - Enable/disable daylight checking (default: False)
- `daylight_margin_minutes` (int) - Minutes before sunrise and after sunset to extend capture window (default: 30)

**Current configuration:**
- **White Rock Pier Cam**: Capture 24/7 (`check_daylight: False`)
- **Boundary Bay Cam**: Capture 24/7 (`check_daylight: False`)
- **Cox Bay Cam**: Only during daylight with 60-minute margin (`check_daylight: True`, `daylight_margin_minutes: 60`)

**Behavior:**
- When `check_daylight: False` - Captures images 24/7 regardless of daylight
- When `check_daylight: True` - Only captures between (sunrise - margin) and (sunset + margin)
- If outside daylight window, script exits with code 0 and logs the skip reason

### 3. Sunlight Times Export (`scripts/export/export_sunlight_times.py`)

Exports comprehensive sunlight information for display on the website.

**Output files:**
- `site/data/wrcam/sunlight.json` - White Rock sunlight times
- `site/data/bbcam/sunlight.json` - Boundary Bay sunlight times
- `site/data/coxbay/sunlight.json` - Cox Bay sunlight times
- `site/data/sunlight_times.json` - Combined data for all locations

**Data included:**
- Sunrise and sunset times
- Solar noon
- Dawn times (astronomical, nautical, civil)
- Dusk times (civil, nautical, astronomical)
- Golden hour and blue hour times
- Current daylight status
- Current phase (night, twilight, daylight)
- Daylight duration

**Run manually:**
```bash
python3 scripts/export/export_sunlight_times.py
```

**JSON structure:**
```json
{
  "date": "2025-12-22",
  "generated_at": "2025-12-22T22:16:39Z",
  "sunrise": "2025-12-22T16:03:44Z",
  "sunset": "2025-12-23T00:15:53Z",
  "solar_noon": "2025-12-22T20:09:39Z",
  "dawn_astronomical": "2025-12-22T14:06:30Z",
  "dawn_nautical": "2025-12-22T14:45:06Z",
  "dawn_civil": "2025-12-22T15:25:32Z",
  "dusk_civil": "2025-12-22T00:54:06Z",
  "dusk_nautical": "2025-12-22T01:34:32Z",
  "dusk_astronomical": "2025-12-22T02:13:08Z",
  "golden_hour": {
    "start": "2025-12-22T15:39:30Z",
    "end": "2025-12-22T16:57:54Z"
  },
  "blue_hour": {
    "start": "2025-12-22T15:25:32Z",
    "end": "2025-12-22T15:39:30Z"
  },
  "daylight_duration_seconds": 29529,
  "is_daylight_now": true,
  "current_phase": "daylight"
}
```

## Dependencies

- `astral` - Sunrise/sunset calculations (added to venv)
- `pytz` - Timezone handling

Install with:
```bash
/home/keelando/envcan_wave/.venv/bin/pip install astral pytz
```

## Scheduling

You can schedule the sunlight times export to run daily:

```bash
# Add to crontab to run daily at midnight
0 0 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/export/export_sunlight_times.py
```

## Timezone Handling

All times are calculated in UTC but the system properly handles the Pacific timezone crossing midnight. When sunset occurs after midnight UTC (which happens for Pacific locations), the date is automatically adjusted.

Example for Dec 22, 2025 in White Rock, BC:
- Sunrise: 08:03 PST = 16:03 UTC (same day)
- Sunset: 16:15 PST = 00:15 UTC **next day** (Dec 23)

## Future Enhancements

Potential improvements:
- Add twilight detection (civil, nautical, astronomical) to webcam capture decisions
- Export moon phase and moonrise/moonset times
- Add seasonal daylight statistics (longest/shortest days)
- Create visual sunlight timeline for website display
