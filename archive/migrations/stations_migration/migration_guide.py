#!/usr/bin/env python3
"""
MIGRATION GUIDE: Using the Unified stations.json
================================================

This shows how to update your existing scripts to use the new unified stations.json
instead of hardcoded station dictionaries.

Benefits:
  ✅ Single source of truth for all station metadata
  ✅ Easy to add/remove/update stations
  ✅ Consistent data across all scripts
  ✅ Better documentation and validation
"""

# ============================================================================
# BEFORE: Old sqlite_to_json.py approach
# ============================================================================

# Old hardcoded approach:
BUOYS_OLD = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    # ... etc
}

# ============================================================================
# AFTER: New unified approach
# ============================================================================

from stations import get_all_buoys, get_buoy

# Load all buoys from stations.json
BUOYS = get_all_buoys()

# Access is identical:
for buoy_id, metadata in BUOYS.items():
    print(f"{metadata['name']} - {metadata['location']}")

# Get specific buoy:
halibut = get_buoy("4600146")
print(f"Halibut Bank coordinates: {halibut['lat']}, {halibut['lon']}")


# ============================================================================
# EXAMPLE 1: Update sqlite_to_json.py
# ============================================================================

def update_sqlite_to_json():
    """Show how to update the main JSON export script."""
    
    # OLD WAY:
    # BUOYS = {
    #     "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    #     ...
    # }
    
    # NEW WAY:
    from stations import get_all_buoys
    BUOYS = get_all_buoys()
    
    # Rest of the script works exactly the same!
    for buoy_id in BUOYS.keys():
        buoy_data = {
            "name": BUOYS[buoy_id]["name"],
            "location": BUOYS[buoy_id]["location"],
            # ... rest of processing
        }


# ============================================================================
# EXAMPLE 2: Update export_24hr_timeseries.py
# ============================================================================

def update_timeseries_export():
    """Show how to update the timeseries export."""
    
    # OLD WAY:
    # BUOYS = {
    #     "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    #     ...
    # }
    
    # NEW WAY:
    from stations import get_all_buoys
    BUOYS = get_all_buoys()
    
    # Now you have access to MORE metadata:
    for buoy_id, metadata in BUOYS.items():
        print(f"Processing {metadata['name']}")
        print(f"  Source: {metadata['source']}")
        print(f"  Update freq: {metadata['update_frequency_minutes']} minutes")
        print(f"  Coordinates: {metadata['lat']}, {metadata['lon']}")


# ============================================================================
# EXAMPLE 3: Update tide_to_sqlite.py
# ============================================================================

def update_tide_script():
    """Show how to update tide scripts."""
    
    # OLD WAY:
    # def load_stations():
    #     with open(STATION_FILE, 'r') as f:
    #         data = json.load(f)
    #     return {k: v["id"] for k, v in data.items()}
    
    # NEW WAY:
    from stations import get_all_tides, get_tide_stations
    
    # Get all tide stations
    all_tides = get_all_tides()
    
    # Or get only stations with observations:
    obs_stations = get_tide_stations(has_observations=True)
    
    # Or get only permanent stations:
    from stations import STATIONS
    permanent = STATIONS.get_tide_stations(station_type='PERMANENT')
    
    for key, metadata in obs_stations.items():
        station_id = metadata['id']
        print(f"Fetching {metadata['name']} (ID: {station_id})")


# ============================================================================
# EXAMPLE 4: Dynamic dropdown population
# ============================================================================

def generate_dropdown_options():
    """Generate HTML dropdown options from stations.json."""
    from stations import get_all_buoys
    
    buoys = get_all_buoys()
    
    html = '<select id="buoy-select">\n'
    for buoy_id, metadata in sorted(buoys.items()):
        html += f'  <option value="{buoy_id}">{metadata["name"]}</option>\n'
    html += '</select>'
    
    return html


# ============================================================================
# EXAMPLE 5: Validation and error handling
# ============================================================================

def validate_user_input():
    """Validate user-provided station IDs."""
    from stations import STATIONS
    
    user_buoy_id = "4600146"
    
    if STATIONS.validate_buoy_id(user_buoy_id):
        print(f"✅ Valid buoy: {user_buoy_id}")
    else:
        print(f"❌ Unknown buoy: {user_buoy_id}")
        print(f"Valid buoys: {', '.join(STATIONS.buoys.keys())}")


# ============================================================================
# EXAMPLE 6: Source-specific processing
# ============================================================================

def process_by_source():
    """Process buoys differently based on their data source."""
    from stations import STATIONS
    
    # Get only Environment Canada buoys
    envcan_buoys = STATIONS.get_buoys_by_source("Environment Canada")
    for buoy_id, metadata in envcan_buoys.items():
        print(f"Processing EC buoy: {metadata['name']}")
        # Use SWOB-ML field mappings...
    
    # Get only NOAA buoys
    noaa_buoys = STATIONS.get_buoys_by_source("NOAA NDBC")
    for buoy_id, metadata in noaa_buoys.items():
        print(f"Processing NOAA buoy: {metadata['name']}")
        # Use NDBC field mappings...


# ============================================================================
# EXAMPLE 7: Coordinate-based operations
# ============================================================================

def map_all_stations():
    """Get coordinates for creating a map."""
    from stations import STATIONS
    
    coords = STATIONS.get_all_coordinates()
    
    for station in coords:
        print(f"{station['type'].upper()}: {station['name']}")
        print(f"  Location: {station['lat']}, {station['lon']}")


# ============================================================================
# QUICK MIGRATION CHECKLIST
# ============================================================================

MIGRATION_STEPS = """
1. ✅ Copy stations.json to ~/envcan_wave/stations.json
2. ✅ Copy stations.py to ~/envcan_wave/stations.py
3. ✅ Update each script:
   
   sqlite_to_json.py:
     - Remove BUOYS dict
     - Add: from stations import get_all_buoys
     - Add: BUOYS = get_all_buoys()
   
   export_24hr_timeseries.py:
     - Remove BUOYS dict
     - Add: from stations import get_all_buoys
     - Add: BUOYS = get_all_buoys()
   
   tide_to_sqlite.py:
     - Remove load_stations() function
     - Add: from stations import get_all_tides
     - Add: TIDE_STATIONS = {k: v["id"] for k, v in get_all_tides().items()}
   
   export_tide_json.py:
     - Remove load_station_metadata() function
     - Add: from stations import get_all_tides
     - Add: station_metadata = get_all_tides()

4. ✅ Test each script independently
5. ✅ Remove old tide_stations.json (now redundant)
6. ✅ Update any cron jobs if paths changed
"""

if __name__ == "__main__":
    print("=" * 70)
    print("MIGRATION GUIDE: Unified stations.json")
    print("=" * 70)
    print(MIGRATION_STEPS)
    print("\n" + "=" * 70)
    print("For complete examples, see the functions in this file.")
    print("=" * 70)
