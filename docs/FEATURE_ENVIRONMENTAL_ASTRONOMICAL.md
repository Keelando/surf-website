# Feature Plan: Environmental & Astronomical Data Display

## Overview
Add environmental indicators (cloud cover from solar data) and astronomical information (sunrise/sunset, twilight, moon phase) to enhance the marine forecasting experience.

---

## Priority Features from Earlier Analysis

From `WEBSITE_PARAMETERS.md` "Top 5 Missing Parameters to Add":

### ✅ Already Planned/Implemented:
1. ✅ Pressure Trend (3h change) - DONE
2. ⏳ Solar Current (cloud indicator) - THIS DOC
3. ✅ Maximum Wave Height - DONE
4. ✅ Wave Direction Spread - DONE
5. ⏳ Moon Phase - THIS DOC

### 🆕 New Priority Features:
6. ⏳ Sunrise/Sunset Times - THIS DOC
7. ⏳ Twilight Times (nautical, civil) - THIS DOC
8. ⏳ Daylight Duration - THIS DOC

---

## Feature 1: Solar Current (Cloud Cover Indicator)

### Data Source
**Already collecting!** From EC buoys:
- Field: `solar_current` (milliamps)
- Units: mA
- Update: Hourly

### Interpretation Scale

Based on solar panel output from buoys:
```
Solar Current → Sky Condition
─────────────────────────────
> 4.0 mA      → ☀️ Sunny (clear skies)
2.0 - 4.0 mA  → ⛅ Partly Cloudy
1.0 - 2.0 mA  → ☁️ Mostly Cloudy
< 1.0 mA      → 🌫️ Overcast / Heavy Cloud
0.0 mA        → 🌙 Night (no solar)
```

**Refinements:**
- **Time of day adjustment**: Higher threshold at noon, lower at dawn/dusk
- **Seasonal adjustment**: Account for sun angle (winter vs summer)
- **Latitude correction**: Different thresholds for northern stations

### Display Locations

#### 1. Buoy Cards - Details Section
```
📊 Additional Metrics
    Sea Temperature: 8.6 °C | Air: 9.2 °C
    Pressure: 1011.2 hPa | Trend: +1.2 hPa ↑
    ☀️ Sky Conditions: Partly Cloudy (2.8 mA)
```

#### 2. Buoy Popup (Map)
```
🌊 Halibut Bank

Current Conditions:
  Wind: WNW 15 G 20 kn
  Waves: 0.4m @ 3.3s
  ☀️ Sky: Partly Cloudy

  Sea: 8.6°C | Air: 9.2°C
```

#### 3. Status Indicator Icon
Small sun/cloud icon next to buoy name in card header

### Implementation

#### Display Function:
```javascript
function getSkyCondition(solarCurrent, hour) {
  // Return null at night (no solar data meaningful)
  if (hour < 6 || hour > 20) {
    return { icon: '🌙', desc: 'Night', color: '#4a5568' };
  }

  if (solarCurrent === null || solarCurrent === undefined) {
    return { icon: '—', desc: 'Unknown', color: '#718096' };
  }

  // TODO: Adjust thresholds based on time of day and season
  if (solarCurrent > 4.0) {
    return { icon: '☀️', desc: 'Sunny', color: '#f59e0b' };
  } else if (solarCurrent > 2.0) {
    return { icon: '⛅', desc: 'Partly Cloudy', color: '#60a5fa' };
  } else if (solarCurrent > 1.0) {
    return { icon: '☁️', desc: 'Mostly Cloudy', color: '#9ca3af' };
  } else {
    return { icon: '🌫️', desc: 'Overcast', color: '#6b7280' };
  }
}
```

#### Add to Buoy Card:
```javascript
// In main.js, expandable details section
if (b.solar_current != null) {
  const skyCondition = getSkyCondition(b.solar_current, new Date().getHours());
  cardContent += `<p class="buoy-metric"><b>☀️ Sky Conditions:</b> ${skyCondition.icon} ${skyCondition.desc} <span style="color: #666; font-size: 0.9em;">(${b.solar_current.toFixed(1)} mA)</span></p>`;
}
```

### Validation
Compare solar current readings with:
- Satellite imagery (cloud cover)
- Weather station cloud observations
- Webcam images (if available)

---

## Feature 2: Sunrise / Sunset Times

### Data Source Options

#### Option A: API Service (Recommended)
**Sunrise-Sunset.org API** (Free, no key required)
- Endpoint: `https://api.sunrise-sunset.org/json?lat=49.337&lng=-123.731&date=today&formatted=0`
- Returns: sunrise, sunset, solar_noon, civil_twilight, nautical_twilight, astronomical_twilight
- Rate limit: Reasonable for our needs
- Format: ISO 8601 UTC

#### Option B: Python Library
**Astral** library
```python
from astral import LocationInfo
from astral.sun import sun

location = LocationInfo("Halibut Bank", "Canada", "America/Vancouver", 49.337, -123.731)
s = sun(location.observer, date=datetime.now())

sunrise = s['sunrise']
sunset = s['sunset']
```

#### Option C: SPA Algorithm (Most Accurate)
**NOAA Solar Position Algorithm**
- Most accurate (<1 minute error)
- No API dependency
- Requires implementation

**Recommendation:** Use Astral library (Option B) - accurate, offline, no API limits

### Data to Calculate

For each station location (buoy, tide station, lightstation):
```python
sun_data = {
    'date': '2025-12-07',
    'sunrise': '07:56 PST',
    'sunset': '16:21 PST',
    'solar_noon': '12:09 PST',
    'daylight_duration': '8h 25m',
    'civil_twilight_begin': '07:24 PST',
    'civil_twilight_end': '16:53 PST',
    'nautical_twilight_begin': '06:47 PST',
    'nautical_twilight_end': '17:30 PST',
    'astronomical_twilight_begin': '06:11 PST',
    'astronomical_twilight_end': '18:06 PST',
}
```

### Display Locations

#### 1. Site Header (All Pages)
```
┌─────────────────────────────────────────────┐
│ Salish Sea Wave Conditions                  │
│ 🌅 Sunrise: 7:56 AM  🌇 Sunset: 4:21 PM     │
│ ⏰ Daylight: 8h 25m  📍 49.3°N, 123.7°W     │
└─────────────────────────────────────────────┘
```

#### 2. Dedicated Panel (Optional)
```
☀️ Sun & Moon Information

  📅 Today: Saturday, December 7, 2025
  📍 Location: Halibut Bank (49.3°N, 123.7°W)

  Sunrise:    07:56 PST
  Solar Noon: 12:09 PST
  Sunset:     16:21 PST
  Daylight:   8h 25m

  Twilight Times:
    Civil:        06:47 - 17:30
    Nautical:     06:11 - 18:06
    Astronomical: 05:38 - 18:39
```

#### 3. Buoy Cards - Expandable Section
```
📊 Environmental Conditions
    ☀️ Sky: Partly Cloudy (2.8 mA)
    🌅 Sunrise: 7:56 AM | Sunset: 4:21 PM
    🌙 Moon Phase: First Quarter (52% illuminated)
```

### Implementation

#### Backend Script: `calculate_sun_moon.py`
```python
#!/usr/bin/env python3
"""
Calculate sunrise, sunset, twilight, and moon phase for all station locations.
Run daily at midnight to update for next 7 days.
"""

from astral import LocationInfo
from astral.sun import sun
from astral.moon import phase
from datetime import datetime, timedelta
import json
from pathlib import Path

def calculate_sun_data(lat, lon, date, timezone='America/Vancouver'):
    """Calculate sun times for a location."""
    location = LocationInfo("Station", "Canada", timezone, lat, lon)
    s = sun(location.observer, date=date)

    # Convert to local time
    local_tz = pytz.timezone(timezone)

    return {
        'sunrise': s['sunrise'].astimezone(local_tz).strftime('%H:%M'),
        'sunset': s['sunset'].astimezone(local_tz).strftime('%H:%M'),
        'solar_noon': s['noon'].astimezone(local_tz).strftime('%H:%M'),
        'civil_twilight_begin': s['dawn'].astimezone(local_tz).strftime('%H:%M'),
        'civil_twilight_end': s['dusk'].astimezone(local_tz).strftime('%H:%M'),
        'daylight_duration': str(s['sunset'] - s['sunrise']).split('.')[0]
    }

def calculate_moon_phase(date):
    """Calculate moon phase (0-28 days in lunar cycle)."""
    moon_phase_num = phase(date)

    # Convert to descriptive phase
    if moon_phase_num < 2:
        phase_name = 'New Moon'
        icon = '🌑'
    elif moon_phase_num < 7:
        phase_name = 'Waxing Crescent'
        icon = '🌒'
    elif moon_phase_num < 9:
        phase_name = 'First Quarter'
        icon = '🌓'
    elif moon_phase_num < 14:
        phase_name = 'Waxing Gibbous'
        icon = '🌔'
    elif moon_phase_num < 16:
        phase_name = 'Full Moon'
        icon = '🌕'
    elif moon_phase_num < 21:
        phase_name = 'Waning Gibbous'
        icon = '🌖'
    elif moon_phase_num < 23:
        phase_name = 'Last Quarter'
        icon = '🌗'
    else:
        phase_name = 'Waning Crescent'
        icon = '🌘'

    illumination = int((1 - abs(14 - moon_phase_num) / 14) * 100)

    return {
        'phase_num': moon_phase_num,
        'phase_name': phase_name,
        'icon': icon,
        'illumination_pct': illumination
    }

def export_astronomical_data():
    """Export sun and moon data for all station locations."""

    # Load stations
    with open('config/stations.json', 'r') as f:
        stations = json.load(f)

    # Use representative location (Halibut Bank)
    ref_lat = 49.337
    ref_lon = -123.731

    astronomical_data = {
        'generated': datetime.now().isoformat(),
        'location': {'lat': ref_lat, 'lon': ref_lon, 'name': 'Salish Sea (Central)'},
        'forecast': []
    }

    # Calculate for next 7 days
    for i in range(7):
        date = datetime.now().date() + timedelta(days=i)

        sun_data = calculate_sun_data(ref_lat, ref_lon, date)
        moon_data = calculate_moon_phase(date)

        astronomical_data['forecast'].append({
            'date': date.isoformat(),
            'sun': sun_data,
            'moon': moon_data
        })

    # Export to JSON
    output_file = Path('site/data/astronomical.json')
    with open(output_file, 'w') as f:
        json.dump(astronomical_data, f, indent=2)

    return astronomical_data

if __name__ == '__main__':
    export_astronomical_data()
```

#### Frontend Display: `astronomical-data.js`
```javascript
/**
 * Load and display astronomical data
 */
async function loadAstronomicalData() {
  try {
    const data = await fetchWithTimeout('/data/astronomical.json');
    const today = data.forecast[0];

    return {
      sunrise: today.sun.sunrise,
      sunset: today.sun.sunset,
      daylight: today.sun.daylight_duration,
      moon: today.moon
    };
  } catch (err) {
    console.warn('Could not load astronomical data:', err);
    return null;
  }
}

// Display in header or info panel
function displaySunMoonInfo(astroData) {
  const container = document.getElementById('sun-moon-info');
  if (!container || !astroData) return;

  container.innerHTML = `
    <div style="display: flex; gap: 2rem; justify-content: center; padding: 0.5rem; background: #f7fafc; border-radius: 6px; font-size: 0.9rem;">
      <span>🌅 Sunrise: ${astroData.sunrise}</span>
      <span>🌇 Sunset: ${astroData.sunset}</span>
      <span>⏰ Daylight: ${astroData.daylight}</span>
      <span>${astroData.moon.icon} ${astroData.moon.phase_name} (${astroData.moon.illumination_pct}%)</span>
    </div>
  `;
}
```

---

## Feature 3: Moon Phase & Tidal Influence

### Why Moon Phase Matters for Marine Forecasting

#### Tidal Range Connection
- **New & Full Moon** (Spring Tides):
  - Sun, Moon, Earth aligned
  - Strongest gravitational pull
  - **Higher high tides, lower low tides**
  - Greater tidal range

- **First & Last Quarter** (Neap Tides):
  - Sun and Moon at 90° angle
  - Gravitational forces partially cancel
  - **Moderate tidal range**
  - Less extreme tides

#### Practical Impact
- **King Tides**: Occur during new/full moon + perigee (moon closest to Earth)
- **Beach Access**: Low neap tides may not expose tidal pools
- **Navigation**: Spring tide currents are stronger
- **Flooding**: High spring tides + storm surge = coastal flooding

### Display Integration

#### On Tides Page (Primary)
```
🌊 Point Atkinson Tide Station

Today's Tides:
  High: 4.2m at 08:15 PST
  Low:  0.8m at 14:30 PST
  Range: 3.4m

🌕 Full Moon - Spring Tides
  Expect higher than average tidal range
  Strong tidal currents
```

#### Tidal Range Indicator
```
Tidal Range: 3.4m ━━━━━━━━●━━ (High)
             └─ Neap    Spring ─┘

🌕 Full Moon (Day 14/29)
Spring tides: Higher highs, lower lows
```

### Moon Phase Calculation

Already included in `calculate_sun_moon.py` above using Astral library.

**Alternative:** Calculate manually using lunar cycle formula:
```python
def moon_phase_manual(date):
    """Calculate moon phase from known new moon date."""
    # Known new moon: Jan 6, 2000
    known_new_moon = datetime(2000, 1, 6, 18, 14)
    lunar_cycle = 29.530588853  # days

    days_since = (date - known_new_moon).days
    phase = (days_since % lunar_cycle) / lunar_cycle

    return phase * 28  # 0-28 scale
```

---

## Feature 4: Additional Environmental Indicators

### From Original Analysis

#### High Priority (Easy Wins):

1. **✅ Pressure Trend** - DONE
2. **⏳ Solar Current** - THIS DOC
3. **✅ Maximum Wave Height** - DONE
4. **✅ Wave Direction Spread** - DONE
5. **⏳ Moon Phase** - THIS DOC

#### Medium Priority (Future):

6. **Beaufort Scale** - Convert wind speed to descriptive scale
7. **Douglas Sea State** - Convert wave height to sea state description
8. **Wind Chill / Heat Index** - For extreme conditions
9. **Visibility** - If available from observations
10. **UV Index** - For daytime marine activities

### Beaufort Scale (Wind Description)

```javascript
function getBeaufortScale(windSpeedKt) {
  if (windSpeedKt < 1) return { force: 0, desc: 'Calm', sea: 'Mirror-like' };
  if (windSpeedKt < 4) return { force: 1, desc: 'Light Air', sea: 'Ripples' };
  if (windSpeedKt < 7) return { force: 2, desc: 'Light Breeze', sea: 'Small wavelets' };
  if (windSpeedKt < 11) return { force: 3, desc: 'Gentle Breeze', sea: 'Large wavelets' };
  if (windSpeedKt < 17) return { force: 4, desc: 'Moderate Breeze', sea: 'Small waves' };
  if (windSpeedKt < 22) return { force: 5, desc: 'Fresh Breeze', sea: 'Moderate waves' };
  if (windSpeedKt < 28) return { force: 6, desc: 'Strong Breeze', sea: 'Large waves' };
  if (windSpeedKt < 34) return { force: 7, desc: 'Near Gale', sea: 'Sea heaps up' };
  if (windSpeedKt < 41) return { force: 8, desc: 'Gale', sea: 'Moderately high waves' };
  if (windSpeedKt < 48) return { force: 9, desc: 'Strong Gale', sea: 'High waves' };
  if (windSpeedKt < 56) return { force: 10, desc: 'Storm', sea: 'Very high waves' };
  if (windSpeedKt < 64) return { force: 11, desc: 'Violent Storm', sea: 'Exceptionally high waves' };
  return { force: 12, desc: 'Hurricane', sea: 'Air filled with foam' };
}
```

Display: "Wind: WNW 15 kn (Force 4 - Moderate Breeze)"

### Douglas Sea State (Wave Description)

```javascript
function getDouglasSeaState(waveHeightM) {
  if (waveHeightM < 0.1) return { code: 0, desc: 'Calm (glassy)' };
  if (waveHeightM < 0.5) return { code: 1, desc: 'Calm (rippled)' };
  if (waveHeightM < 1.25) return { code: 2, desc: 'Smooth' };
  if (waveHeightM < 2.5) return { code: 3, desc: 'Slight' };
  if (waveHeightM < 4.0) return { code: 4, desc: 'Moderate' };
  if (waveHeightM < 6.0) return { code: 5, desc: 'Rough' };
  if (waveHeightM < 9.0) return { code: 6, desc: 'Very Rough' };
  if (waveHeightM < 14.0) return { code: 7, desc: 'High' };
  return { code: 8, desc: 'Very High' };
}
```

Display: "Waves: 0.4m (Slight - comfortable for small craft)"

---

## Implementation Timeline

### Phase 1: Solar Current (Week 1)
- [x] Data already available
- [ ] Add sky condition display to buoy cards
- [ ] Add to buoy map popups
- [ ] Test correlation with actual conditions
- **Effort:** 2-3 hours

### Phase 2: Sun Times (Week 2)
- [ ] Install Astral library
- [ ] Create `calculate_sun_moon.py`
- [ ] Add to cron (daily at midnight)
- [ ] Display in site header
- [ ] Add to tides page
- **Effort:** 4-6 hours

### Phase 3: Moon Phase (Week 2)
- [ ] Already in `calculate_sun_moon.py`
- [ ] Add moon phase to tides page
- [ ] Add spring/neap tide indicator
- [ ] Explain tidal range correlation
- **Effort:** 3-4 hours

### Phase 4: Descriptive Scales (Week 3)
- [ ] Implement Beaufort scale
- [ ] Implement Douglas sea state
- [ ] Add to wind/wave displays
- [ ] User preference: toggle descriptions
- **Effort:** 3-4 hours

**Total Effort:** ~12-17 hours

---

## Success Metrics

1. **Solar current accuracy**: >80% match with satellite cloud data
2. **Sun times accuracy**: Within 1 minute of NOAA tables
3. **Moon phase correlation**: Users understand spring/neap tides
4. **User engagement**: Astronomical panel viewed by >30% of visitors
5. **Feedback**: Users report finding sky condition indicator useful

---

## Documentation Needed

1. **User Guide**: "Understanding Sky Conditions from Solar Data"
2. **Explainer**: "How Moon Phase Affects Tides"
3. **Reference**: "Beaufort Scale & Douglas Sea State"
4. **Technical**: "Astronomical Calculation Methods"

---

## Testing Plan

### Solar Current Validation
- Compare readings with:
  - [ ] Satellite cloud imagery
  - [ ] Weather station cloud cover reports
  - [ ] Manual observations (if available)
  - [ ] Webcam images

### Sun/Moon Calculation Validation
- Compare with:
  - [ ] NOAA Astronomical Data
  - [ ] timeanddate.com
  - [ ] Environment Canada sunrise/sunset
  - [ ] Manual observation (spot check)

---

## Future Enhancements

### V2 Features
1. **Moonrise/Moonset times**
2. **Moon position in sky** (altitude/azimuth)
3. **Solar elevation angle** (useful for photography)
4. **Golden hour / blue hour times** (photography)
5. **Best fishing times** (solunar theory)

### V3 Features
1. **Aurora forecast** (for northern locations)
2. **Stellar navigation data** (for sailors)
3. **Planet visibility** (Mars, Venus, etc.)
4. **ISS overflight times**

---

## Cost Analysis

### API Costs
- **Sunrise-Sunset API**: FREE (if using Option A)
- **Astral Library**: FREE, open source

### Development Costs
- **Time**: ~15 hours @ developer rate
- **Infrastructure**: None (uses existing cron)
- **Maintenance**: ~1 hour/month

**Total First Year Cost:** Minimal (developer time only)

---

## References

- **Astral Library**: https://astral.readthedocs.io/
- **NOAA Solar Calculator**: https://gml.noaa.gov/grad/solcalc/
- **Moon Phase Calculations**: https://www.subsystems.us/uploads/9/8/9/4/98948044/moonphase.pdf
- **Beaufort Scale**: https://www.weather.gov/mfl/beaufort
- **Douglas Sea State**: https://en.wikipedia.org/wiki/Sea_state#Douglas_sea_scale

---

## Next Steps

1. ✅ Create this planning document
2. ⏳ Add solar current display to buoy cards
3. ⏳ Install Astral library
4. ⏳ Create astronomical calculation script
5. ⏳ Add sun/moon panel to site header
6. ⏳ Integrate moon phase with tides page
7. ⏳ Add Beaufort/Douglas descriptive scales
8. ⏳ User testing and feedback

**Ready to implement! Which feature should we start with?** 🌅🌙
