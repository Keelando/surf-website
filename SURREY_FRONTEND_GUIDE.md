# Surrey Stations - Frontend Integration

## Quick Start

Surrey stations will **automatically appear** in your buoy data exports once the fetcher runs. No frontend changes are required for basic functionality.

However, to control display order and add to selectors, make these optional updates:

---

## Optional Frontend Updates

### 1. Control Display Order

**File:** `~/site/assets/js/main.js`

Find the buoy display order array and add Surrey stations:

```javascript
const buoyOrder = [
  "4600146",  // Halibut Bank
  "4600304",  // English Bay
  "CRPILE",   // ← Crescent Pile (NEW)
  "CRCHAN",   // ← Crescent Channel (NEW)
  "4600303",  // Southern Georgia Strait
  "4600131",  // Sentry Shoal
  "46087",    // Neah Bay
  "46088",    // New Dungeness
  "COLEB",    // ← Colebrook (wind only, NEW)
];
```

**Location suggestion:** Place Surrey stations near other Boundary Bay / southern stations.

### 2. Add to Chart Selector

**File:** `~/site/assets/js/charts.js` or `~/site/index.html` (wherever your buoy selector dropdown is)

```html
<select id="buoy-selector" class="buoy-select">
  <option value="4600146">Halibut Bank</option>
  <option value="4600304">English Bay</option>
  <option value="CRPILE">Crescent Pile</option>  <!-- NEW -->
  <option value="CRCHAN">Crescent Channel</option>  <!-- NEW -->
  <option value="4600303">Southern Georgia Strait</option>
  <option value="4600131">Sentry Shoal</option>
  <option value="46087">Neah Bay</option>
  <option value="46088">New Dungeness</option>
  <option value="COLEB">Colebrook (Wind Only)</option>  <!-- NEW -->
</select>
```

### 3. Station Metadata

Surrey stations are in `stations.json` with full metadata:

```javascript
// Available metadata for each Surrey station:
{
  "id": "CRPILE",
  "name": "Crescent Pile",
  "location": "Crescent Beach, Surrey",
  "lat": 49.0122,
  "lon": -122.9411,
  "source": "Surrey FlowWorks",
  "data_types": ["wave_height_sig", "wave_period_avg", "wind_speed", "wind_gust", "wind_direction", "air_temp", "sea_temp"]
}
```

Use this for:
- Map markers
- Station info cards
- Tooltips

### 4. Data Fields

**CRPILE (Crescent Pile) - Full suite:**
- `wave_height_sig` - Significant wave height (m)
- `wave_height_peak` - Peak wave height (m)
- `wave_period_avg` - Average wave period (s)
- `wave_period_peak` - Peak wave period (s)
- `wind_speed` - Wind speed (knots, already converted)
- `wind_gust` - Wind gust (knots)
- `wind_direction` - Wind direction (degrees)
- `air_temp` - Air temperature (°C)
- `sea_temp` - Sea temperature (°C)

**CRCHAN (Crescent Channel) - Radar waves:**
- `wave_height_sig` - Wave height from radar (m)
- `wind_speed`, `wind_gust`, `wind_direction`
- `air_temp`

**COLEB (Colebrook) - Wind only:**
- `wind_speed`, `wind_gust`, `wind_direction`
- `air_temp`

### 5. Add Map Markers (Optional)

If you have a map view, add Surrey station markers:

```javascript
const surreyStations = [
  {
    id: "CRPILE",
    name: "Crescent Pile",
    lat: 49.0122,
    lon: -122.9411,
    type: "wave_buoy"
  },
  {
    id: "CRCHAN",
    name: "Crescent Channel",
    lat: 49.0536,
    lon: -122.8969,
    type: "wave_buoy"
  },
  {
    id: "COLEB",
    name: "Colebrook",
    lat: 49.0858,
    lon: -122.845,
    type: "weather_station"  // No waves
  }
];
```

---

## JSON Data Format

Surrey stations appear in the same format as other buoys:

### latest_buoy_v2.json
```json
{
  "CRPILE": {
    "name": "Crescent Pile",
    "observation_time": "2025-11-05T23:30:00+00:00",
    "wave_height_sig": 0.8,
    "wave_period_avg": 3.2,
    "wind_speed": 12.5,
    "wind_direction": 270,
    "wind_direction_cardinal": "W",
    "air_temp": 8.5,
    "sea_temp": 9.2,
    "stale": false
  },
  "CRCHAN": { ... },
  "COLEB": { ... }
}
```

### buoy_timeseries_24h.json
```json
{
  "CRPILE": {
    "wave_height_sig": {
      "data": [
        {"time": "2025-11-05T00:00:00Z", "value": 0.7},
        {"time": "2025-11-05T00:10:00Z", "value": 0.8},
        ...
      ]
    },
    "wind_speed": { ... }
  }
}
```

---

## Testing

After deploying the backend integration:

1. **Check JSON exports:**
   ```bash
   curl https://halibutbank.ca/data/latest_buoy_v2.json | jq '.CRPILE'
   ```

2. **Verify your site can access the data:**
   - Open browser console
   - Check that Surrey stations load with other buoys
   - Verify charts render for Surrey stations

3. **Test responsiveness:**
   - Surrey stations should update every 10-20 minutes
   - Check "stale" flag works if data is old

---

## Display Considerations

**Colebrook (COLEB) - Wind Only:**
- No wave data available
- Hide wave charts/widgets for this station
- Display as "Weather Station" not "Wave Buoy"

**Wave Height Differences:**
- CRPILE uses `wave_height_sig` (significant wave height)
- If you display "Wave Height" generically, Surrey stations use this field
- Environment Canada buoys may use `wave_height_max` - standardize naming

**Update Frequency:**
- Surrey: 10-minute intervals
- Environment Canada: 60-minute intervals
- NOAA: 60-minute intervals

Consider showing update frequency in the UI.

---

## That's It!

The Surrey stations are just like any other buoy in your data. They'll appear automatically once the backend fetcher runs.

**Optional updates above are only for:**
- Custom display order
- Dropdown selectors
- Map markers
- Special handling for wind-only stations

Otherwise, your existing buoy display logic should work as-is! 🌊
