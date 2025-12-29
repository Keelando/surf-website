# Website Parameters - Quick Reference

## Parameter Display Matrix

| Parameter | Buoys Page | Winds Page | Tides Page | Storm Surge | Lightstations |
|-----------|-----------|-----------|-----------|------------|--------------|
| **Wind Speed** | ✅ Main + History | ✅ Table + Chart | ❌ | ❌ | ✅ Cards |
| **Wind Gust** | ✅ Main + History | ✅ Table + Chart | ❌ | ❌ | ✅ Cards |
| **Wind Direction** | ✅ Main + History | ✅ Table + Chart | ❌ | ❌ | ✅ Cards |
| **Wave Height (Sig)** | ✅ Main + History | ❌ | ❌ | ❌ | ✅ Text |
| **Wave Period** | ✅ Main + History | ❌ | ❌ | ❌ | ✅ Text |
| **Wave Direction** | ✅ Details + Arrow | ❌ | ❌ | ❌ | ✅ Text |
| **Swell Height** | ✅ (NOAA only) | ❌ | ❌ | ❌ | ✅ Text |
| **Swell Period** | ✅ (NOAA only) | ❌ | ❌ | ❌ | ✅ Text |
| **Sea Temperature** | ✅ Details + History | ❌ | ❌ | ❌ | ✅ Cards |
| **Air Temperature** | ✅ Details + History | ✅ Table | ❌ | ❌ | ✅ Cards |
| **Pressure** | ✅ Details | ✅ Table | ❌ | ❌ | ✅ Cards |
| **Tide Level (Obs)** | ❌ | ❌ | ✅ Current | ❌ | ❌ |
| **Tide Level (Pred)** | ❌ | ❌ | ✅ Current + Chart | ✅ (implicit) | ❌ |
| **Storm Surge** | ❌ | ❌ | ✅ Current | ✅ Forecast | ❌ |
| **High/Low Tides** | ❌ | ❌ | ✅ Table | ❌ | ❌ |
| **Visibility** | ❌ | ❌ | ❌ | ❌ | ✅ Cards |
| **Sea State** | ❌ | ❌ | ❌ | ❌ | ✅ Text |
| **Forecast Text** | ❌ | ❌ | ❌ | ❌ | ✅ (on forecast page) |

---

## Data Available But NOT Displayed

### Wave Parameters (EC Buoys)
- ❌ Wave height max
- ❌ Wave height avg
- ❌ Wave crest height max
- ❌ Wave direction spread (avg & peak)
- ❌ Spectral wave metrics
- ❌ Various wave period alternatives

### Wind Parameters
- ❌ Secondary wind sensor data (sensor 2)
- ❌ Wind sensor height
- ❌ Bad sample counts (QC)

### Pressure Parameters
- ❌ **Pressure trend** (3hr change) - HIGH VALUE!
- ❌ Mean sea level pressure
- ❌ Secondary pressure sensor

### Environmental
- ❌ **Solar current** (cloudiness indicator) - HIGH VALUE!
- ❌ Buoy GPS position
- ❌ Position drift from nominal

### System Health
- ❌ Battery voltage
- ❌ Watchman boot count
- ❌ Obstruction lamp current
- ❌ Compass headings

---

## Display Format by Parameter

### Buoys Page Formats

| Parameter | Compact View | Details | History | Chart |
|-----------|-------------|---------|---------|-------|
| Wind | "WNW 15 G 20 kn (350°)" + arrow | - | Cardinal + speed + gust | 24h trend |
| Wave | "W 0.4m @ 3.3s (270°)" + arrow | Breakdown by type | Height + period | 24h trends |
| Swell | "WSW 1.8m @ 10.8s" + arrow | Height/period/dir | Height + period | - |
| Temp (Sea) | - | "8.6 °C" | 1 decimal | 24h trend |
| Temp (Air) | - | "9.2 °C" | 1 decimal | 24h trend |
| Pressure | - | "1011.2 hPa" | - | - |

### Precision Rules
- **EC Buoys**: Wave height 1 decimal, period 1 decimal
- **Boundary Bay**: Wave height 2 decimals (smaller waves)
- **NOAA**: Wave height 1 decimal, period 1 decimal
- **Surrey**: Sea/air temp 1 decimal
- **Wind**: Rounded to integer (knots)

---

## Station-Specific Display Logic

### Neah Bay (46087) - Special Handling
- **Compact view**: Shows SWELL instead of combined wave
- **Details**: Full spectral breakdown (wind waves + swell)
- **Rationale**: Measures open ocean swells vs local wind waves

### New Dungeness (46088) - Special Handling
- **Wave period chart**: Only station with dedicated period chart
- **Rationale**: Protected location, interesting period dynamics

### EC Buoys (4600xxx) - Standard Display
- Combined wave metrics (significant height + avg/peak period)
- Peak metrics in expandable details

### Surrey Buoys (CRPILE, CRCHAN) - Limited Data
- Higher precision (2 decimals for waves)
- Limited wave direction data
- Filtered to hourly observations only

---

## Data Freshness Thresholds

| Page | Warning Threshold | Display |
|------|------------------|---------|
| Buoys | 3 hours (180 min) | "⚠️ (Xh old)" in orange |
| Winds | Not specified | Shows last update time |
| Tides | 30 minutes | DFO data updated every 30min |
| Storm Surge | Model run time | Shows forecast issue time |

---

## Update Frequencies

| Data Type | Update Frequency | Source |
|-----------|-----------------|--------|
| EC Buoy (XML) | Hourly | Sarracenia broker |
| NOAA Buoy | 10-50 minutes | NDBC realtime2 |
| Surrey FlowWorks | 10 minutes | FlowWorks API |
| Wind Stations | Hourly | Environment Canada |
| Tides (Obs) | 6 minutes | DFO IWLS API |
| Tides (Pred) | Pre-calculated | DFO CHS |
| Storm Surge | 4x daily (6-hourly) | EC GDSPS model |
| Lightstations | Every 3 hours | Manual observer reports |

---

## Top 5 Missing Parameters to Add

Based on value to users:

1. **Pressure Trend (3h change)**
   - Why: Predicts incoming weather systems
   - Where: Buoys + Winds pages
   - Format: "+1.2 hPa ↑ (3h)" in green/red

2. **Solar Current (cloud indicator)**
   - Why: Real-time sky conditions
   - Where: Buoys page details
   - Format: "☀️ 2.8 mA (partly cloudy)"

3. **Maximum Wave Height**
   - Why: Safety - shows actual max vs average
   - Where: Buoys page details
   - Format: "Max: 0.7m (sig: 0.4m, ratio: 1.75x)"

4. **Wave Direction Spread**
   - Why: Indicates sea state organization
   - Where: Buoys page details
   - Format: "Spread: 36° (moderate confusion)"

5. **Moon Phase**
   - Why: Explains tidal range variations
   - Where: Tides page header
   - Format: "🌕 Full Moon (Spring Tides)"

---

## Chart Types Used

| Chart Type | Pages | Library | Parameters |
|-----------|-------|---------|-----------|
| Line chart | Buoys, Winds, Tides | ECharts | Time series data |
| Comparison chart | Buoys | ECharts | Multi-station waves |
| Map | Buoys, Winds | Leaflet | Station locations |
| Directional arrows | Buoys, Winds | SVG | Wind/wave direction |
| Data tables | All pages | HTML | Sortable summaries |

---

## Color Coding & Visual Indicators

### Currently Used
- **Blue arrows**: Wave direction
- **Red arrows**: Wind direction
- **Gray arrows**: Wind-only stations (no wave dir)
- **Orange warning**: Stale data (>3h old)
- **Purple markers**: Tide stations (map)
- **Lighthouse icon**: Lightstations (map)
- **Blue buoy borders**: NOAA stations
- **Green borders**: Surrey FlowWorks stations
- **Default borders**: EC stations

### Suggested Additions
- **Pressure trend colors**: Green ↑ (rising), Red ↓ (falling)
- **Wave steepness colors**: Green (safe), Orange (moderate), Red (breaking)
- **Data quality indicators**: Gray (questionable), Red (failed QC)
- **Forecast confidence**: Color intensity for uncertainty
