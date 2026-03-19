# Jericho Wind Station Integration - Planning Document

**Created:** 2025-12-08
**Status:** ✅ COMPLETE - Fully implemented and operational

## Overview

Integrate Jericho Sailing Centre wind station data into the marine monitoring system.

## Data Source

- **Organization:** Jericho Sailing Centre Association (JSCA)
- **URL:** https://jsca.bc.ca/main/downld02.txt
- **Location:** Jericho Beach, Vancouver (49.28°N, 123.2°W)
- **Format:** Fixed-width text table with header row
- **Update Frequency:** 30-minute intervals
- **Significance:** Excellent English Bay coverage for sailors

## Data Fields

### Fields to Capture (Standard Set)
| Field | Source Unit | Target Unit | Conversion |
|-------|-------------|-------------|------------|
| Wind Speed | mph | knots | × 0.868976 |
| Wind Gust (hi wind speed) | mph | knots | × 0.868976 |
| Wind Direction | degrees | degrees | 1:1 |
| Air Temperature | °F | °C | (x-32)×5/9 |
| Barometric Pressure | mb | hPa | 1:1 |
| Rain Amount | inches | mm | × 25.4 |

### Fields Available But NOT Capturing
- Humidity
- Dew point
- Wind chill
- Heat index
- Indoor readings

## Implementation Plan

### 1. Database Schema
Station will use existing `wind_data.sqlite` structure:
- **Station ID:** `JERICHO`
- **Source:** Jericho Sailing Centre

### 2. Fetch Script
**File:** `fetch_jericho_wind.py`

**Pattern:** Similar to `fetch_surrey_wave_v2.py`
- HTTP GET to https://jsca.bc.ca/main/downld02.txt
- Parse fixed-width text format (NOT CSV)
- Apply unit conversions
- Insert into `wind_data.sqlite`
- Handle errors gracefully (network, parsing)

### 3. Configuration
Add to `config/stations.json`:
```json
"wind": {
  "JERICHO": {
    "id": "JERICHO",
    "name": "Jericho Sailing Centre",
    "location": "English Bay, Vancouver",
    "lat": 49.28,
    "lon": -123.2,
    "source": "Jericho Sailing Centre Association",
    "type": "weather_station",
    "update_frequency_minutes": 30,
    "data_types": [
      "wind_speed",
      "wind_gust",
      "wind_direction",
      "air_temp",
      "pressure",
      "rainfall"
    ],
    "url": "https://jsca.bc.ca/main/downld02.txt"
  }
}
```

### 4. Cron Schedule
```bash
# Fetch Jericho wind data every 30 minutes (matches update frequency)
*/30 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/fetch_jericho_wind.py >> /home/keelando/envcan_wave/logs/jericho_wind.log 2>&1
```

### 5. Export Integration
**No changes needed** - existing export scripts already handle any station ID:
- `export_wind_json.py` ✅
- `export_wind_24hr_timeseries.py` ✅

## Reference Implementation

**Surrey FlowWorks Integration** - Similar pattern:
- See: `fetch_surrey_wave_v2.py`
- See: `docs/integrations/SURREY_INTEGRATION_GUIDE.md`

## Testing Plan

1. **Manual fetch test:**
   ```bash
   curl https://jsca.bc.ca/main/downld02.txt
   # Verify data format
   ```

2. **Parser test:**
   - Create sample data file for offline testing
   - Test all unit conversions
   - Verify database insertions

3. **Integration test:**
   - Run fetch script manually
   - Verify data appears in `wind_data.sqlite`
   - Check JSON exports include JERICHO station
   - Verify frontend displays new station

## Questions to Answer During Implementation

1. **Fixed-width format details:**
   - What are the exact column positions?
   - How are headers structured?
   - How is missing data represented?

2. **Data quality:**
   - How often does the feed update?
   - Are there gaps in the data?
   - What's the historical reliability?

3. **Error handling:**
   - What happens if the feed is down?
   - How to handle malformed data?
   - Retry strategy?

## Priority

**High** - Excellent English Bay coverage for sailors, fills gap in current wind station network

## Next Steps

1. Examine current data feed format
2. Create parser for fixed-width text
3. Implement unit conversions
4. Test with real data
5. Add to cron schedule
6. Update documentation
