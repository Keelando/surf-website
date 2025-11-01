# Salish Sea Marine Monitoring - Unified Station Registry

A centralized metadata system for managing buoy and tide station information across the Salish Sea marine monitoring platform.

## 📁 Files

- **`stations.json`** - Master metadata file containing all station information
- **`stations.py`** - Python utility module for loading and accessing station data
- **`migration_guide.py`** - Examples showing how to update existing scripts

## 🌊 What's Included

### Buoy Stations (5 total)

**Environment Canada (4):**
- Halibut Bank (4600146)
- English Bay (4600304)
- Southern Georgia Strait (4600303)
- Sentry Shoal (4600131)

**NOAA NDBC (1):**
- Neah Bay (46087)

### Tide Stations (8 total)

**Permanent Stations (4):**
- Point Atkinson
- Kitsilano
- New Westminster
- Campbell River

**Temporary/Prediction-Only (4):**
- Tsawwassen
- White Rock
- Crescent Beach
- Rose Harbour (Nanaimo area)

## 📋 Station Metadata

Each station includes:

### Buoy Metadata
```json
{
  "id": "4600146",
  "name": "Halibut Bank",
  "location": "Off Vancouver",
  "lat": 49.337,
  "lon": -123.731,
  "source": "Environment Canada",
  "type": "wave_buoy",
  "update_frequency_minutes": 60,
  "data_types": [...]
}
```

### Tide Station Metadata
```json
{
  "id": "5cebf1de3d0f4a073c4bb94c",
  "code": "07795",
  "name": "Point Atkinson",
  "location": "West Vancouver",
  "lat": 49.3375,
  "lon": -123.253583,
  "source": "DFO IWLS",
  "type": "PERMANENT",
  "series": ["wlo", "wlp", "wlp-hilo"],
  "update_frequency_minutes": 6,
  "data_types": [...]
}
```

## 🚀 Quick Start

### Basic Usage

```python
from stations import get_buoy, get_tide_station, get_all_buoys

# Get specific buoy
halibut = get_buoy("4600146")
print(f"{halibut['name']} is at {halibut['lat']}, {halibut['lon']}")

# Get all buoys for processing
for buoy_id, metadata in get_all_buoys().items():
    print(f"Processing {metadata['name']}...")

# Get tide station
point_atk = get_tide_station("point_atkinson")
print(f"DFO Station ID: {point_atk['id']}")
```

### Advanced Queries

```python
from stations import STATIONS

# Get only stations with observations
obs_stations = STATIONS.get_tide_stations(has_observations=True)

# Get only permanent tide stations
permanent = STATIONS.get_tide_stations(station_type='PERMANENT')

# Get buoys by source
envcan = STATIONS.get_buoys_by_source("Environment Canada")
noaa = STATIONS.get_buoys_by_source("NOAA NDBC")

# Get all coordinates for mapping
coords = STATIONS.get_all_coordinates()
```

## 🔄 Migration from Old System

### Old Way (Hardcoded)
```python
BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    # ... etc
}
```

### New Way (Centralized)
```python
from stations import get_all_buoys

BUOYS = get_all_buoys()
# Now includes: name, location, lat, lon, source, update frequency, data types, etc.
```

## 📊 Data Sources

### Buoy Data
- **Environment Canada**: MEDS (Marine Environmental Data Service)
  - SWOB-ML XML format
  - Updates: Every 10-60 minutes depending on station
  
- **NOAA NDBC**: National Data Buoy Center
  - TXT and SPEC formats
  - Updates: Hourly

### Tide Data
- **DFO IWLS**: Fisheries and Oceans Canada - Integrated Water Level System
  - API endpoint: `https://api-iwls.dfo-mpo.gc.ca/`
  - Three series types:
    - `wlo`: Water Level Observed (real-time, every 6 minutes)
    - `wlp`: Water Level Predicted (astronomical predictions)
    - `wlp-hilo`: High/Low tide events

## 🛠️ Updating Existing Scripts

### Step 1: Copy Files
```bash
cp stations.json ~/envcan_wave/
cp stations.py ~/envcan_wave/
```

### Step 2: Update Scripts

**sqlite_to_json.py:**
```python
# Remove:
# BUOYS = {"4600146": {"name": ...}, ...}

# Add:
from stations import get_all_buoys
BUOYS = get_all_buoys()
```

**export_24hr_timeseries.py:**
```python
# Same as above
from stations import get_all_buoys
BUOYS = get_all_buoys()
```

**tide_to_sqlite.py:**
```python
# Remove:
# def load_stations():
#     with open(STATION_FILE, 'r') as f:
#         return json.load(f)

# Add:
from stations import get_all_tides
TIDE_STATIONS = {k: v["id"] for k, v in get_all_tides().items()}
```

**export_tide_json.py:**
```python
# Remove:
# def load_station_metadata():
#     ...

# Add:
from stations import get_all_tides
station_metadata = get_all_tides()
```

### Step 3: Remove Old Files
```bash
rm ~/envcan_wave/tide_stations.json  # Now redundant
```

## ✅ Benefits

1. **Single Source of Truth**
   - One file to update when adding/removing stations
   - Consistent metadata across all scripts

2. **Better Documentation**
   - All station details in one place
   - Clear data source attribution
   - Update frequency specified

3. **Easier Maintenance**
   - Add new stations: edit one JSON file
   - No need to update multiple scripts
   - Validation and error checking included

4. **Enhanced Functionality**
   - Query by source (Environment Canada vs NOAA)
   - Filter by capabilities (observations vs predictions)
   - Geographic queries (get all coordinates)

## 🗺️ Future Enhancements

Potential additions to `stations.json`:

- **Storm surge stations** (Point Atkinson, Crescent Beach, etc.)
- **Weather stations** (if added to system)
- **Historical data availability** (start/end dates)
- **Quality control flags** (known issues, maintenance windows)
- **Related stations** (nearest buoy to each tide station)

## 📝 Adding a New Station

1. Edit `stations.json`
2. Add entry under appropriate section (`buoys` or `tides`)
3. Include all required fields
4. Scripts automatically pick up the new station

Example:
```json
"buoys": {
  "46088": {
    "id": "46088",
    "name": "New Dungeness",
    "location": "Hein Bank",
    "lat": 48.333,
    "lon": -123.167,
    "source": "NOAA NDBC",
    "type": "wave_buoy",
    "update_frequency_minutes": 60,
    "data_types": ["wave_height", "wind_speed", ...]
  }
}
```

## 🧪 Testing

Run the module directly to validate:
```bash
cd ~/envcan_wave
python3 stations.py
```

Output shows:
- Total buoy count
- Total tide station count
- Stations with observations
- Test lookups

## 📞 Support

For issues or questions about the station registry:
1. Check `migration_guide.py` for examples
2. Review `stations.py` docstrings
3. Validate `stations.json` syntax with a JSON linter

## 📄 License

Part of the Salish Sea Wave Conditions project.
Data sources remain property of their respective agencies (Environment Canada, NOAA, DFO).

---

**Last Updated:** January 2025  
**Version:** 1.0.0
