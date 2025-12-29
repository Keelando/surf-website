# Website Display Parameters Documentation

This document details which parameters are currently displayed on each page of the halibutbank.ca website, and suggests additional parameters that could be added.

---

## Buoys Page (index.html)

### Currently Displayed Parameters

#### Main Buoy Cards (Compact View - Always Visible)
- **Wind**:
  - Wind speed (rounded to integer, kn)
  - Wind gust (rounded to integer, kn)
  - Wind direction (cardinal + degrees)
  - Wind direction arrow (visual indicator)

- **Wave** (varies by buoy type):
  - **EC Buoys** (Halibut Bank, English Bay, Sentry Shoal, Southern Georgia):
    - Significant wave height (`wave_height_sig`, 1 decimal for most, 2 decimals for Boundary Bay)
    - Average or peak wave period (`wave_period_avg` or `wave_period_peak`)
    - Peak wave direction cardinal + degrees
    - Wave direction arrow

  - **NOAA - Neah Bay** (46087):
    - Swell height (`swell_height`, 1 decimal)
    - Swell period (`swell_period`)
    - Swell direction cardinal + degrees
    - Swell direction arrow

  - **NOAA - New Dungeness** (46088):
    - Significant wave height (`wave_height_sig`)
    - Average wave period (`wave_period_avg`)
    - Peak wave direction cardinal + degrees
    - Wave direction arrow

  - **Surrey Buoys** (CRPILE, CRCHAN):
    - Significant wave height (`wave_height_sig`, 2 decimals)
    - Average or peak wave period
    - Wave direction (when available)

#### Expandable Details Section (Show Details Button)

**NOAA Buoys** (46087, 46088, 46267):
- Significant wave (combined): height, avg period
- Wind waves (local chop): height, period, direction (cardinal + degrees + arrow)
- Ocean swell (long period): height, period, direction (cardinal + degrees + arrow)
- Peak metrics: peak period, peak direction (cardinal + degrees + arrow)
- Sea temperature (°C)
- Air temperature (°C)
- Atmospheric pressure (hPa)

**EC Buoys** (4600146, 4600304, 4600303, 4600131):
- Peak wave height (`wave_height_peak`, when available)
- Peak wave period (`wave_period_peak`, when available)
- Sea temperature (°C)
- Air temperature (°C)
- Atmospheric pressure (hPa)

**Surrey Buoys** (CRPILE, CRCHAN):
- Sea temperature (°C, 1 decimal)
- Air temperature (°C, 1 decimal)
- Pressure (hPa)

#### Expandable History Section (12-hour history table)
- Time (Pacific time, weekday-day format + hour)
- Wind speed + direction (cardinal) + gust
- Wave height (or swell height for Neah Bay)
- Wave period (or swell period for Neah Bay)
- Sea temperature
- Air temperature

#### Charts Section
- **Wave Comparison Chart**: Compares wave heights across multiple buoys (24h)
- **Individual Buoy Charts** (24h trends):
  - Wave height chart
  - Wave period chart (for New Dungeness only)
  - Wind speed & gust chart
  - Temperature chart (sea + air)

#### Wave Height Summary Table (24h)
Shows min/max/avg wave heights for all buoys over 24 hours

---

### Available But NOT Currently Displayed on Buoys Page

Based on the buoy data fields available in `buoy_to_influx_sqlite.py` and `latest_buoy_v2.json`:

#### Wave Metrics
- `wave_height_max` - Maximum wave height (EC buoys)
- `wave_height_avg` - Average wave height (EC buoys)
- `wave_height_spectral` - Spectral significant wave height (EC buoys)
- `wave_crest_height_max` - Maximum wave crest height above average water level
- `wave_period_sig` - Significant wave period (different from average)
- `wave_period_max_wave` - Period of maximum wave
- `wave_period_spectral` - Average spectral wave period
- `wave_period_energy_spectral` - Spectral wave energy period
- `wave_direction_avg` - Average wave direction (degrees)
- `wave_direction_spread_avg` - Average wave direction spread
- `wave_direction_spread_peak` - Peak wave direction spread
- `wave_period_sig_basic` - Significant wave period (basic method)
- `wave_height_max_avg` - Average of maximum wave heights
- `wave_period_max_avg` - Average period of maximum waves

#### Wind Metrics (Sensor 2 data)
- `wind_speed_sensor_2` - Wind speed from secondary sensor (anemometer at different height)
- `wind_gust_sensor_2` - Wind gust from secondary sensor
- `wind_direction_sensor_2` - Wind direction from secondary sensor
- `wind_sensor_height` - Height of wind sensor above water (typically 5m)
- `wind_samples_bad_1` - Quality indicator: bad samples from sensor 1
- `wind_samples_bad_2` - Quality indicator: bad samples from sensor 2

#### Pressure Metrics
- `pressure_msl` - Mean sea level pressure
- `pressure_sensor_2` - Pressure from secondary sensor
- `pressure_trend_char` - Pressure tendency characteristic (code)
- `pressure_trend_amount` - Amount of pressure change over past 3 hours

#### Position & Environmental
- `buoy_lat_current` - Current buoy latitude (GPS position)
- `buoy_lon_current` - Current buoy longitude (GPS position)
- `solar_current` - Solar panel current (cloudiness indicator!)

#### System Health & Diagnostics
- `battery_voltage` - Battery voltage
- `watchman_boot_count` - System boot counter (reliability indicator)
- `obstruction_lamp_current` - Current draw from obstruction lamp
- `compass_heading_1` - Compass heading from sensor 1
- `compass_heading_2` - Compass heading from sensor 2

#### NOAA-specific (46087, 46088, 46267)
- `wave_height_peak` - Peak wave height (0-30% wave height)
- `field_times` - Individual timestamps for each parameter

---

### Suggested Additions for Buoys Page

#### High Priority (Useful for mariners)
1. **Pressure Trend** (`pressure_trend_amount`):
   - Show in details section as "+1.2 hPa (last 3h)" or "-0.8 hPa (last 3h)"
   - Very useful for predicting weather changes
   - Color code: green for rising, red for falling rapidly

2. **Solar Current** (cloudiness indicator):
   - Display in details as "☀️ Solar: 2.8 mA (partly cloudy)"
   - Can infer cloud cover: >4mA = sunny, 1-4mA = partly cloudy, <1mA = overcast
   - Useful real-time sky conditions

3. **Wave Direction Spread**:
   - Shows how "confused" or organized the waves are
   - Low spread = organized swell, high spread = choppy/confused seas
   - Display as "Wave spread: 36° (moderate)"

4. **Maximum Wave Height** (`wave_height_max`):
   - The actual maximum wave in the measurement period
   - More relevant than significant height for safety
   - Display as "Max wave: 0.7m (sig: 0.4m)" ratio

5. **Buoy Position Drift**:
   - Compare current position to nominal position
   - Alert if buoy has dragged anchor significantly
   - Show distance drifted

#### Medium Priority (Technical/Advanced users)
6. **Wind Sensor Height**: Show that wind is measured at 5m above water (helps with conversions)
7. **Secondary Wind Sensor Data**: Show both sensor readings for comparison
8. **Spectral Wave Metrics**: For technical users interested in wave modeling
9. **Battery Voltage**: Health indicator for data reliability
10. **Compass Headings**: Show buoy orientation (useful for understanding sensor alignment)

#### Low Priority (Diagnostic)
11. **Bad Sample Counts**: Data quality indicators
12. **Boot Count**: System reliability metric

---

## Winds Page (winds.html)

### Currently Displayed Parameters

#### Wind Conditions Table (sortable)
- Station name
- Wind speed (kn)
- Wind gust (kn)
- Wind direction (cardinal + degrees)
- Air temperature (°C)
- Atmospheric pressure (hPa)
- Last updated time (Pacific)

#### 24-Hour Wind Trend Chart (per station)
- Wind speed over time
- Wind gust over time
- Wind direction

#### 24-Hour Wind Data Table
- Time (Pacific)
- Wind speed (kn)
- Wind gust (kn)
- Direction (cardinal)

#### Map
- Station locations with wind direction indicators

### Available But NOT Currently Displayed
Same as buoy sensors:
- Secondary wind sensor data
- Wind sensor height
- Bad sample counts
- Additional meteorological data at weather stations

### Suggested Additions
1. **Pressure trend** - predict weather changes
2. **Visibility** (if available from weather stations)
3. **Humidity** (if available)
4. **Wind speed in m/s or km/h** (in addition to knots) for non-mariners

---

## Tides Page (tides.html)

### Currently Displayed Parameters

#### Current Conditions
- Current observed water level (m)
- Current predicted tide (m)
- Storm surge (difference between observed and predicted, m)

#### Today's High & Low Tides Table
- Time of high/low tide (Pacific)
- Height (m)
- Type (High/Low)

#### Tide & Water Level Forecast Chart
- Predicted tide (blue line)
- Observed water level (when available)
- Storm surge visualization

#### Station Metadata
- Station name
- Location
- Data source (DFO)

### Available But NOT Currently Displayed
Most tide data is already well-displayed. Potential additions:
- **Tidal range** (difference between high and low)
- **Slack tide times** (times of minimal current)
- **Moon phase** (affects tides)
- **King tide warnings** (exceptionally high tides)

### Suggested Additions
1. **Moon phase indicator** - Shows why tides are particularly high/low
2. **Tidal range** - Useful for beach access, shellfish harvesting
3. **Next 7 days high/low summary** - Planning ahead

---

## Storm Surge Page (storm_surge.html)

### Currently Displayed Parameters

#### 10-Day Forecast
- Storm surge forecast (m, relative to predicted tide)
- Peak surge predictions for:
  - 0-24 hours
  - 24-72 hours
  - 72-168 hours (7 days)
- Time series chart showing forecast evolution

#### Hindcast (Forecast Accuracy)
- 48-hour advance predictions
- Comparison to actual/recent forecasts
- Model performance evaluation

#### Model Information
- Model name (GDSPS)
- Resolution (15 km)
- Update frequency (4x daily)
- Forecast horizon (10 days/240h)

### Available But NOT Currently Displayed
The storm surge data is well-represented. Possible additions:
- **Total water level forecast** (predicted tide + surge) rather than just surge
- **Threshold exceedance probability** (chance of flooding)
- **Historical surge events** for comparison

### Suggested Additions
1. **Combined water level forecast** - Show predicted tide + surge as "total water level"
2. **Flood risk thresholds** - Mark levels that cause flooding at specific locations
3. **Wind/pressure that causes surge** - Help understand the forcing

---

## Lightstations Page (lightstations.html)

### Currently Displayed Parameters

#### Lightstation Cards (by region)
Based on the HTML structure, typically shows:
- Station name
- Wind speed, gust, direction
- Sea state description (textual)
- Swell description (textual)
- Visibility
- Air temperature
- Atmospheric pressure
- Last report time

### Available But NOT Currently Displayed
These are human observations, so data is limited to what observers report. The reports are textual rather than numerical.

### Suggested Additions
1. **Historical trends** - Show past 24h of reports if available
2. **Photo/webcam integration** - Some lightstations have cameras
3. **Sea surface temperature** - If observers record it

---

## Forecasts Page (forecasts.html)

### Currently Displayed Parameters

#### Marine Forecast Zones
- Zone name (e.g., "Strait of Georgia")
- Active warnings (gale, storm, etc.)
- Wind forecast by period (today, tonight, tomorrow)
- Sea state forecast
- Weather conditions
- Extended forecast (multi-day)

#### Metadata
- Forecast issue time
- Next update time

### Available But NOT Currently Displayed
Forecasts are textual bulletins from Environment Canada, so display is already comprehensive.

### Suggested Additions
1. **Wind/wave graphs from forecast** - Visualize the textual forecasts
2. **Comparison to current conditions** - Show how conditions match/differ from forecast
3. **Forecast accuracy history** - Track how accurate recent forecasts have been

---

## Webcams Page (webcams.html)

### Currently Displayed Webcams

#### White Rock Pier Cam
- **ID**: `whiterock`
- **Name**: White Rock Pier Cam
- **Location**: White Rock, BC
- **Coordinates**: 49.021719°N, 122.807111°W
- **Decimal Format**: `lat: 49.021719, lon: -122.807111`
- **Source**: YouTube Livestream (https://www.youtube.com/watch?v=4MK3E9EWDSY)
- **Update Frequency**: 10 minutes
- **Stream Delay**: ~6 minutes
- **View**: White Rock Pier and Semiahmoo Bay
- **Cropping**: Left 25% removed (street), right 75% kept (pier/sea)
- **Archive**: `/mnt/storage/whiterock_cam/`
- **Website Path**: `~/site/data/wrcam/latest.jpg`

#### White Rock East Beach
- **ID**: `boundarybay`
- **Name**: White Rock East Beach
- **Location**: White Rock, BC
- **Coordinates**: 49.01647°N, 122.79082°W
- **Decimal Format**: `lat: 49.01647, lon: -122.79082`
- **Source**: YouTube Livestream (https://www.youtube.com/watch?v=O8RsAq9RUlA)
- **Update Frequency**: 10 minutes
- **Stream Delay**: ~20 minutes
- **View**: East Beach looking toward Boundary Bay
- **Cropping**: Full frame (no cropping)
- **Archive**: `/mnt/storage/boundarybay_cam/`
- **Website Path**: `~/site/data/bbcam/latest.jpg`

#### Cox Bay
- **ID**: `coxbay`
- **Name**: Cox Bay
- **Location**: Tofino, BC (West Coast Vancouver Island)
- **Coordinates**: 49.106802°N, 125.872949°W
- **Source**: Pacific Sands Beach Resort Livestream (https://www.youtube.com/watch?v=LqaP8m2OIqM)
- **Update Frequency**: 10 minutes
- **Stream Delay**: ~20 minutes
- **View**: Cox Bay surf zone, Pacific Ocean conditions
- **Cropping**: Full frame (no cropping)
- **Archive**: `/mnt/storage/coxbay_cam/`
- **Website Path**: `~/site/data/coxbay/latest.jpg`

### Currently Displayed Parameters

#### Main Webcam Display
- Latest captured image (JPEG)
- Image timestamp (ISO format + Unix timestamp)
- Source information
- Link to live YouTube stream
- Slideshow of last 7 images

#### Metadata (`latest.json`)
```json
{
  "filename": "PREFIX_YYYYMMDD_HHMMSS_UnixTimestamp.jpg",
  "timestamp": "ISO 8601 timestamp",
  "timestamp_unix": "Unix timestamp (seconds)",
  "source": "Source description",
  "url": "YouTube URL"
}
```

#### Slideshow Manifest (`slideshow_manifest.json`)
```json
[
  {
    "filename": "img_TIMESTAMP.jpg",
    "timestamp": "ISO 8601 timestamp",
    "path": "slideshow/img_TIMESTAMP.jpg"
  }
]
```

### Technical Details

#### Image Capture Process
1. **YouTube Stream Resolution**: Uses yt-dlp to fetch best available stream
2. **Frame Extraction**: ffmpeg captures single frame with optional cropping
3. **Quality**: JPEG quality level 3 (very high, ~200-350 KB per image)
4. **Frequency**: Every 10 minutes via cron
5. **Atomic Updates**: Temp file written then atomically renamed to avoid partial images
6. **Slideshow**: Last 7 images kept in `slideshow/` directory
7. **Archive**: All images saved with timestamp to archive directory

#### Storage Management
- **Archive Directory**: `/mnt/storage/{webcam}_cam/`
- **Website Directory**: `~/site/data/{webcam}/`
- **Disk Cleanup**: Automatic when usage exceeds 80%
- **Minimum Retention**: 24 hours of images
- **Target After Cleanup**: 75% disk usage
- **Estimated Daily Storage**: ~47 MB per webcam (144 images × ~330 KB)
- **Estimated Annual Storage**: ~17 GB per webcam

#### Coordinate Reference System
- **Datum**: WGS84
- **Format**: Decimal degrees
- **Precision**: 4 decimal places (~11 meters)

### Available But NOT Currently Displayed
- Historical image archive browser
- Time-lapse videos
- Comparison view (side-by-side webcams)
- Synchronized timestamp comparison
- Wave height annotations
- Tide level indicators
- Weather overlay (wind direction, conditions)

### Suggested Additions
1. **Archive Browser**: Calendar view to select historical dates
2. **Time-Lapse Generator**: Daily or weekly time-lapse from slideshow images
3. **Multi-Cam Comparison**: Side-by-side view of all webcams
4. **Condition Annotations**:
   - Overlay current wave height (from nearby buoy)
   - Overlay wind speed/direction arrow
   - Overlay tide level indicator
   - Color-coded surf rating
5. **Metadata Overlay**:
   - Display capture time on image
   - Display conditions summary
6. **Download Options**: Allow download of current or historical images
7. **Integration with Buoys**: Link to nearby buoy data (e.g., Cox Bay ↔ La Perouse Bank)

---

## Additional Data Sources to Consider

### Not Currently Integrated
1. **Wave buoy spectral data** - Full wave spectrum analysis
2. **Current/flow data** - Tidal currents (from DFO?)
3. **Salinity** - From some oceanographic buoys
4. **Sea surface temperature trends** - Daily/seasonal changes
5. **Ice conditions** - Relevant for winter in some areas
6. **Satellite imagery** - SST, chlorophyll, etc.
7. **Rainfall** - From weather stations
8. **Lightning detection** - Thunderstorm proximity
9. **UV Index** - Sun exposure for mariners

### Derived/Calculated Parameters
1. **Beaufort Scale** - Convert wind to Beaufort number
2. **Sea state description** - Convert wave height to Douglas scale
3. **Wind chill** - For winter conditions
4. **Heat index** - For summer conditions
5. **Wave steepness** - Wave height / wavelength (breaking criterion)
6. **Significant wave height trend** - Rate of increase/decrease
7. **Wind speed trend** - Accelerating/decelerating

---

## Data Quality Indicators to Add

1. **Freshness warnings**: Already implemented (shows age of data)
2. **Sensor health**: Battery voltage, boot count
3. **Data gaps**: Show periods of missing data on charts
4. **Accuracy estimates**: Forecast confidence intervals
5. **Source reliability**: Flag stations with frequent outages

---

## Summary of Top Recommendations

### For Buoys Page (Highest Impact)
1. ✅ **Pressure trend** (3-hour change) - Predict approaching storms
2. ✅ **Solar current as cloud indicator** - Quick sky conditions
3. ✅ **Maximum wave height** - Safety (larger than significant)
4. ✅ **Wave direction spread** - Sea state organization
5. ✅ **Buoy position drift** - Equipment health

### For All Pages
1. **Moon phase** (tides page) - Explains tidal range
2. **Combined water level** (storm surge page) - Easier to interpret
3. **Beaufort/Douglas scales** - Standardized descriptions
4. **Data quality indicators** - Build user trust
5. **Trend arrows** - Quick visual for increasing/decreasing parameters
