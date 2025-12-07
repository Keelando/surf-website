# Feature Plan: Buoy Position Drift Tracking & Visualization

## Overview
Track and visualize buoy position drift over time by plotting GPS positions on the map. Shows how buoys move from their nominal anchor positions due to currents, winds, and weather.

---

## Current Data Available

### EC Buoys (Already Collected!)
From `buoy_to_influx_sqlite.py`, we're already collecting:
- ✅ `buoy_lat_current` - Current GPS latitude
- ✅ `buoy_lon_current` - Current GPS longitude

### Stations Metadata
From `stations.json`:
- ✅ `lat` - Nominal (anchor) position latitude
- ✅ `lon` - Nominal (anchor) position longitude

### Which Buoys Have Position Data?
**Environment Canada buoys** (C-MAN stations) report GPS positions:
- Halibut Bank (4600146)
- English Bay (4600304)
- Southern Georgia Strait (4600303)
- Sentry Shoal (4600131)
- La Perouse Bank (4600206)

**NOAA buoys** - May have position data in their feeds (to verify)

**Surrey FlowWorks buoys** - Likely fixed positions (no drift)

---

## Feature Components

### 1. Data Collection & Storage

#### Option A: Store in SQLite Database (Recommended)
Add position history table:
```sql
CREATE TABLE buoy_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buoy_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    distance_from_nominal REAL,  -- meters
    bearing_from_nominal REAL,   -- degrees
    UNIQUE(buoy_id, timestamp)
);

CREATE INDEX idx_buoy_positions_buoy ON buoy_positions(buoy_id, timestamp DESC);
```

**Pros:**
- Efficient querying for time ranges
- Can calculate drift statistics
- Easy to prune old data

**Cons:**
- Requires database maintenance

#### Option B: Store in JSON Files
Daily position logs: `/data/buoy_positions/YYYY-MM-DD.json`
```json
{
  "4600146": [
    {
      "time": "2025-12-07T14:00:00Z",
      "lat": 49.3372,
      "lon": -123.7315,
      "drift_m": 12.5,
      "bearing": 85
    }
  ]
}
```

**Pros:**
- Simple, no database needed
- Easy to inspect/debug

**Cons:**
- Slower for long time ranges
- More files to manage

#### Recommended: Hybrid Approach
- Store positions in SQLite for fast querying
- Export daily JSON summaries for web display
- Keep last 30 days of detailed positions
- Archive older data as monthly summaries

### 2. Position Tracking Script

Create `track_buoy_positions.py`:
```python
#!/usr/bin/env python3
"""
Track buoy GPS positions and calculate drift from nominal position.
Run every time buoy data is collected (hourly for EC buoys).
"""

import sqlite3
from datetime import datetime
from math import radians, cos, sin, asin, sqrt, atan2, degrees

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate distance between two GPS coordinates in meters.
    """
    R = 6371000  # Earth radius in meters

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))

    return R * c

def calculate_bearing(lat1, lon1, lat2, lon2):
    """
    Calculate bearing from point 1 to point 2 in degrees (0-360).
    """
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    y = sin(dlon) * cos(lat2)
    x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)

    bearing = atan2(y, x)
    bearing = degrees(bearing)
    bearing = (bearing + 360) % 360

    return bearing

def track_position(buoy_id, current_lat, current_lon, nominal_lat, nominal_lon, timestamp):
    """
    Record buoy position and calculate drift.
    """
    # Calculate distance and bearing from nominal position
    distance = haversine_distance(nominal_lat, nominal_lon, current_lat, current_lon)
    bearing = calculate_bearing(nominal_lat, nominal_lon, current_lat, current_lon)

    # Store in database
    db = sqlite3.connect('data/buoy_positions.db')
    db.execute('''
        INSERT OR REPLACE INTO buoy_positions
        (buoy_id, timestamp, lat, lon, distance_from_nominal, bearing_from_nominal)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (buoy_id, timestamp, current_lat, current_lon, distance, bearing))
    db.commit()
    db.close()

    return distance, bearing
```

**Integration Point:**
Modify `buoy_to_influx_sqlite.py` to call `track_position()` when processing buoy data.

### 3. Export Script for Web Display

Create `export_buoy_positions.py`:
```python
#!/usr/bin/env python3
"""
Export buoy position data for web display.
Generates JSON files with position tracks.
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path

def export_position_tracks(days=7):
    """
    Export position tracks for the last N days.
    """
    db = sqlite3.connect('data/buoy_positions.db')
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    # Get positions for each buoy
    cursor = db.execute('''
        SELECT buoy_id, timestamp, lat, lon, distance_from_nominal, bearing_from_nominal
        FROM buoy_positions
        WHERE timestamp > ?
        ORDER BY buoy_id, timestamp
    ''', (cutoff,))

    tracks = {}
    for row in cursor:
        buoy_id, timestamp, lat, lon, distance, bearing = row

        if buoy_id not in tracks:
            tracks[buoy_id] = []

        tracks[buoy_id].append({
            'time': timestamp,
            'lat': lat,
            'lon': lon,
            'drift_m': round(distance, 1),
            'bearing': round(bearing, 1)
        })

    # Export to JSON
    output_file = Path('site/data/buoy_position_tracks.json')
    with open(output_file, 'w') as f:
        json.dump({
            'generated': datetime.now().isoformat(),
            'tracks': tracks,
            'days': days
        }, f, indent=2)

    db.close()
    return tracks
```

### 4. Map Visualization

#### Update `stations-map.js` to show position tracks:

**Visual Elements:**
1. **Nominal Position Marker** (anchor icon)
   - Gray anchor symbol at fixed position
   - Label: "Halibut Bank (Anchor)"

2. **Current Position Marker** (buoy icon)
   - Standard buoy marker at current GPS position
   - Label: "Halibut Bank (Current)"

3. **Position Track** (polyline)
   - Colored line connecting historical positions
   - Color: gradient from old (light gray) to recent (blue)
   - Width: 2-3px

4. **Drift Distance Indicator**
   - Dashed line between nominal and current
   - Distance label: "12.5m drift"

5. **Drift Circle** (optional)
   - Circle around nominal position showing typical drift range
   - Radius: 95th percentile of historical drift
   - Color: semi-transparent blue

#### Implementation:

```javascript
// Load position track data
async function loadBuoyPositionTracks() {
  try {
    const tracks = await fetchWithTimeout('/data/buoy_position_tracks.json');
    return tracks;
  } catch (err) {
    console.warn('Could not load position tracks:', err);
    return null;
  }
}

// Add position track visualization for a buoy
function addBuoyPositionTrack(buoyId, nominalLat, nominalLon, tracks) {
  if (!tracks || !tracks.tracks[buoyId]) return;

  const positions = tracks.tracks[buoyId];

  // 1. Add nominal position marker (anchor)
  const anchorIcon = L.divIcon({
    className: 'anchor-marker',
    html: '⚓',
    iconSize: [24, 24]
  });

  L.marker([nominalLat, nominalLon], { icon: anchorIcon })
    .bindPopup(`<b>Nominal Position</b><br>Anchor location`)
    .addTo(stationsMap);

  // 2. Draw position track (polyline)
  const trackCoords = positions.map(p => [p.lat, p.lon]);

  L.polyline(trackCoords, {
    color: '#1e88e5',
    weight: 2,
    opacity: 0.6,
    smoothFactor: 1
  }).addTo(stationsMap);

  // 3. Add current position marker (existing buoy marker)
  const currentPos = positions[positions.length - 1];

  // 4. Draw drift line (dashed)
  L.polyline(
    [[nominalLat, nominalLon], [currentPos.lat, currentPos.lon]],
    {
      color: '#e53935',
      weight: 2,
      opacity: 0.7,
      dashArray: '5, 5'
    }
  ).addTo(stationsMap);

  // 5. Add drift distance label
  const midLat = (nominalLat + currentPos.lat) / 2;
  const midLon = (nominalLon + currentPos.lon) / 2;

  L.marker([midLat, midLon], {
    icon: L.divIcon({
      className: 'drift-label',
      html: `<div style="background: white; padding: 2px 6px; border-radius: 3px; font-size: 11px; font-weight: 600; box-shadow: 0 1px 3px rgba(0,0,0,0.3);">${currentPos.drift_m}m</div>`,
      iconSize: [60, 20]
    })
  }).addTo(stationsMap);
}
```

#### Toggle Control:
Add button to show/hide position tracks:
```html
<button id="toggle-drift-tracks" style="...">
  Show Drift Tracks
</button>
```

---

## UI/UX Design

### Map Display Modes

#### Mode 1: Normal (Default)
- Standard buoy markers at nominal positions
- No tracks shown

#### Mode 2: Drift Tracking Enabled
- Nominal positions (anchors)
- Current positions (buoys)
- Position tracks (lines)
- Drift indicators

### Popup Enhancements

Add to buoy popup when drift data available:
```
🌊 Halibut Bank

Current Conditions:
...

📍 Position Tracking:
  Current Drift: 12.5m NE
  Max Drift (7d): 45.2m
  Avg Drift (7d): 18.3m

  [View Drift History →]
```

### Drift History Panel (Optional)

Expandable panel showing:
- **Chart**: Drift distance over time
- **Table**: Last 24 hours of positions
- **Stats**: Min/Max/Avg drift
- **Alert**: If drift exceeds threshold (e.g., >100m)

---

## Drift Alerting

### Alert Thresholds
- **Normal**: < 50m from nominal
- **Caution**: 50-100m from nominal (yellow)
- **Warning**: > 100m from nominal (red)
- **Critical**: > 500m from nominal (possible anchor failure)

### Alert Display
1. **Map marker color change**: Yellow/red if drifting
2. **Popup banner**: "⚠️ Buoy drifting 125m from anchor"
3. **Status indicator**: On buoy card (main page)
4. **Optional**: Email/webhook notification

---

## Performance Considerations

### Data Volume
- **Per buoy**: ~1 position/hour = 24 positions/day
- **5 buoys**: 120 positions/day = 3,600/month
- **Storage**: ~200 bytes/position = ~720 KB/month
- **1 year**: ~8.6 MB (negligible)

### Map Performance
- **7-day tracks**: ~840 positions total
- **Leaflet polylines**: Handles this easily
- **Optimization**: Downsample to 1 position every 6 hours for display

### Load Strategy
- **Default**: Don't load tracks (performance)
- **On demand**: Load when user clicks "Show Drift Tracks"
- **Progressive**: Load current drift only, full tracks on expand

---

## Implementation Phases

### Phase 1: Data Collection (Week 1)
- ✅ Verify position data is being collected
- Create `buoy_positions.db` table
- Modify `buoy_to_influx_sqlite.py` to track positions
- Run for 1 week to collect baseline data

### Phase 2: Export & Display (Week 2)
- Create `export_buoy_positions.py`
- Add JSON export to cron
- Implement basic track display on map
- Add drift distance to popups

### Phase 3: UI Enhancements (Week 3)
- Add toggle control for tracks
- Enhance popups with drift stats
- Add drift history charts
- Implement color-coded drift alerts

### Phase 4: Advanced Features (Optional)
- Drift prediction (based on current/wind)
- Anchor drag detection
- Comparison between buoys
- Historical drift analysis

---

## Technical Challenges

### Challenge 1: GPS Accuracy
- **Issue**: GPS has ~5-10m accuracy
- **Solution**: Only alert if drift > 50m (above noise threshold)

### Challenge 2: Data Gaps
- **Issue**: Buoy may go offline, creating gaps in track
- **Solution**: Don't connect gaps > 6 hours in polyline

### Challenge 3: Datum/Projection
- **Issue**: GPS uses WGS84, need consistency
- **Solution**: Leaflet handles WGS84 natively, no conversion needed

### Challenge 4: Mobile Performance
- **Issue**: Drawing many tracks may lag on mobile
- **Solution**: Limit to 7 days, downsample for display

---

## Success Metrics

1. **Data completeness**: >95% of hourly positions captured
2. **Accuracy**: Drift calculations within GPS accuracy (±10m)
3. **Performance**: Map loads in <2 seconds with tracks
4. **User engagement**: Track toggle used by 20%+ of visitors
5. **Insights**: Identify seasonal drift patterns

---

## Future Enhancements

### V2 Features
1. **Drift heatmap**: Show most common drift patterns
2. **Current correlation**: Compare drift to current/wind direction
3. **Seasonal patterns**: Winter vs summer drift comparison
4. **Multiple buoy comparison**: See which drifts most
5. **Time-lapse animation**: Watch drift over 24h/7d
6. **Download tracks**: Export as KML/GPX for GIS tools

### V3 Features
1. **Predictive drift**: ML model to predict next position
2. **Anchor health score**: Rate anchor stability
3. **Maintenance alerts**: Suggest re-anchor if excessive drift
4. **Real-time tracking**: Update positions every 10min (if available)

---

## Example Use Cases

### Maritime Users
- **Navigators**: "Is the buoy where the chart says it should be?"
- **Fishers**: "How much does current move the buoy?"

### Researchers
- **Oceanographers**: "What are the current patterns at this location?"
- **Climate scientists**: "Are drift patterns changing over time?"

### Site Operators
- **Maintenance**: "Does this buoy need re-anchoring?"
- **Reliability**: "Which buoys are most stable?"

---

## Resources Needed

### Development Time
- Phase 1: 4-6 hours (data collection)
- Phase 2: 6-8 hours (export & basic display)
- Phase 3: 8-10 hours (UI enhancements)
- **Total**: ~20-24 hours

### Infrastructure
- SQLite database (already have)
- Cron job for export (already have)
- ~10 MB storage for 1 year of data
- No additional server resources

### Documentation
- User guide: "Understanding Buoy Drift"
- API docs: Position tracking data format
- Maintenance: Database cleanup procedures

---

## References

- **Haversine formula**: https://en.wikipedia.org/wiki/Haversine_formula
- **GPS accuracy**: https://www.gps.gov/systems/gps/performance/accuracy/
- **Leaflet polylines**: https://leafletjs.com/reference.html#polyline
- **Buoy mooring design**: https://www.ndbc.noaa.gov/moorings.shtml

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-12-07 | Use SQLite for storage | Efficient querying, already using SQLite |
| 2025-12-07 | 7-day default track | Balance between insight and performance |
| 2025-12-07 | Optional toggle | Don't overwhelm casual users |
| 2025-12-07 | 50m alert threshold | Above GPS noise, catches real drift |

---

## Next Steps

1. ✅ Create this planning document
2. ⏳ Verify which buoys report GPS positions
3. ⏳ Create `buoy_positions.db` schema
4. ⏳ Modify data collection to track positions
5. ⏳ Run for 1 week to collect baseline
6. ⏳ Implement basic track visualization
7. ⏳ Add drift statistics to popups
8. ⏳ Create toggle control
9. ⏳ Write user documentation

**Ready to start implementation when you give the go-ahead!** 🌊📍
