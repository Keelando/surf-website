#!/usr/bin/env python3
"""
Storm Surge Forecast Fetcher for Surf Server
Fetches GDSPS data from Environment Canada GeoMet WMS
"""
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import re
import warnings
import time
import os
from owslib.wms import WebMapService

# Configuration
TESTING = False  # Set to True for verbose output and progress tracking

STATIONS = {
    "Crescent_Beach_Channel": {"lat": 49.0536, "lon": -122.8969, "name": "Crescent Beach Channel"},
    "Point_Atkinson": {"lat": 49.337, "lon": -123.253, "name": "Point Atkinson"},
    "Crescent_Pile": {"lat": 49.0122, "lon": -122.9411, "name": "Crescent Pile"}
}

OUTPUT_DIR = Path("~/site/data/storm_surge").expanduser()
LOCKFILE = Path("/tmp/storm_surge_fetch.lock")
WMS_URL = "https://geo.weather.gc.ca/geomet?SERVICE=WMS&REQUEST=GetCapabilities&layer=GDSPS_15km_StormSurge"
LAYER = "GDSPS_15km_StormSurge"

def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        if time.time() - LOCKFILE.stat().st_mtime > 300:
            print("⚠️  Removing stale lock file")
            LOCKFILE.unlink()
        else:
            print("⚠️  Another instance is running, exiting")
            return False
    LOCKFILE.touch()
    return True

def release_lock():
    if LOCKFILE.exists():
        LOCKFILE.unlink()

def get_bounding_box(lat, lon, offset=0.25):
    """Create bounding box around point."""
    return (lon - offset, lat - offset, lon + offset, lat + offset)

def get_time_parameters(wms, layer):
    """Extract temporal information from GeoMet metadata."""
    time_dim = wms[layer].dimensions["time"]["values"][0]
    start_time, end_time, interval = time_dim.split("/")
    
    iso_format = "%Y-%m-%dT%H:%M:%SZ"
    start_time = datetime.strptime(start_time, iso_format)
    end_time = datetime.strptime(end_time, iso_format)
    interval_hours = int(re.sub(r"\D", "", interval))
    
    return start_time, end_time, interval_hours

def fetch_pixel_value(wms, layer, bbox, timestamp):
    """Fetch storm surge value for specific location and time."""
    try:
        # Format timestamp as YYYY-MM-DDTHH:MM:SSZ (exactly as WMS expects)
        time_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        response = wms.getfeatureinfo(
            layers=[layer],
            srs="EPSG:4326",
            bbox=bbox,
            size=(100, 100),
            format="image/jpeg",
            query_layers=[layer],
            info_format="text/plain",
            xy=(50, 50),
            feature_count=1,
            time=time_str
        )
        
        text = response.read().decode("utf-8")
        
        # In testing mode, show raw response for first few requests
        if TESTING and not hasattr(fetch_pixel_value, '_debug_count'):
            fetch_pixel_value._debug_count = 0
        
        if TESTING and fetch_pixel_value._debug_count < 3:
            print(f"\n    🔍 Debug - Raw response for {time_str}:")
            print(f"    {text[:200]}")
            fetch_pixel_value._debug_count += 1
        
        match = re.search(r"value_0\s+=\s+'(-?\d+\.?\d*)'", text)
        
        if match:
            return float(match.group(1))
        
        # If no match, check if there's useful info in the response
        if TESTING and "no feature" in text.lower():
            if not hasattr(fetch_pixel_value, '_no_feature_warned'):
                print(f"    ⚠️  'No feature' response - coordinates may be outside model domain")
                fetch_pixel_value._no_feature_warned = True
        
        return None
        
    except Exception as e:
        if TESTING or not hasattr(fetch_pixel_value, '_error_count'):
            print(f"    ⚠️  Error fetching data for {timestamp}: {e}")
            if not hasattr(fetch_pixel_value, '_error_count'):
                fetch_pixel_value._error_count = 0
            fetch_pixel_value._error_count += 1
        return None

def fetch_station_forecast(wms, layer, station_id, station_info, time_list):
    """Fetch complete forecast for a station."""
    print(f"\n📍 Fetching {station_info['name']}...")
    
    if TESTING:
        print(f"    Total timesteps to fetch: {len(time_list)}")
        print(f"    Estimated time: ~{len(time_list) * 0.6:.0f} seconds ({len(time_list) * 0.6 / 60:.1f} minutes)")
    
    bbox = get_bounding_box(station_info['lat'], station_info['lon'])
    forecast_data = {}
    successful = 0
    failed = 0
    
    for idx, timestamp in enumerate(time_list, 1):
        value = fetch_pixel_value(wms, layer, bbox, timestamp)
        
        if value is not None:
            time_key = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            forecast_data[time_key] = round(value, 3)
            successful += 1
        else:
            failed += 1
            
        # Progress indicator
        if TESTING:
            if idx % 10 == 0 or idx == len(time_list):
                progress = (idx / len(time_list)) * 100
                print(f"    Progress: {idx}/{len(time_list)} ({progress:.1f}%) - Success: {successful}, Failed: {failed}")
        elif idx % 50 == 0:
            # Even in non-verbose mode, show some progress for long fetches
            print(f"    Progress: {idx}/{len(time_list)}...")
            
        time.sleep(0.5)  # Rate limiting
    
    print(f"    ✅ Retrieved {successful}/{len(time_list)} forecasts (Failed: {failed})")
    return forecast_data

def save_forecast(station_id, forecast_data, station_info):
    """Save forecast data to JSON file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    output_file = OUTPUT_DIR / f"{station_id}.json"
    
    output_data = {
        "station_id": station_id,
        "station_name": station_info["name"],
        "location": {
            "lat": station_info["lat"],
            "lon": station_info["lon"]
        },
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "forecast": forecast_data,
        "unit": "meters"
    }
    
    # Atomic write
    tmp_file = output_file.with_suffix(".json.tmp")
    tmp_file.write_text(json.dumps(output_data, indent=2))
    tmp_file.replace(output_file)
    
    print(f"    💾 Saved to {output_file}")

def create_combined_forecast():
    """Combine all station forecasts into single file."""
    combined = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "stations": {}
    }
    
    for station_file in OUTPUT_DIR.glob("*.json"):
        if station_file.name == "combined_forecast.json":
            continue
            
        try:
            station_data = json.loads(station_file.read_text())
            station_id = station_data["station_id"]
            combined["stations"][station_id] = station_data
        except Exception as e:
            print(f"⚠️  Error reading {station_file}: {e}")
    
    combined_file = OUTPUT_DIR / "combined_forecast.json"
    tmp_file = combined_file.with_suffix(".json.tmp")
    tmp_file.write_text(json.dumps(combined, indent=2))
    tmp_file.replace(combined_file)
    
    print(f"\n✅ Created combined forecast: {combined_file}")

def main():
    if TESTING:
        print("=" * 60)
        print("🧪 TESTING MODE ENABLED - Verbose output active")
        print("=" * 60)
    
    print("🌊 Storm Surge Forecast Fetcher")
    print("=" * 50)
    
    if not acquire_lock():
        return 1
    
    try:
        # Suppress OWSLib warnings
        warnings.filterwarnings("ignore", module="owslib", category=UserWarning)
        
        # Connect to WMS
        print("\n🔌 Connecting to Environment Canada GeoMet...")
        wms = WebMapService(WMS_URL, version="1.3.0", timeout=300)
        
        # Get time parameters
        start_time, end_time, interval = get_time_parameters(wms, LAYER)
        print(f"📅 Forecast period: {start_time} to {end_time}")
        print(f"⏱️  Interval: {interval} hours")
        
        # Build time list
        time_list = [start_time]
        while time_list[-1] < end_time:
            time_list.append(time_list[-1] + timedelta(hours=interval))
        
        print(f"📊 Total timesteps: {len(time_list)}")
        
        if TESTING:
            print(f"\n⏰ Estimated total time: ~{len(time_list) * len(STATIONS) * 0.6 / 60:.1f} minutes")
            print(f"   ({len(time_list)} timesteps × {len(STATIONS)} stations × 0.5s + overhead)")
        else:
            total_minutes = len(time_list) * len(STATIONS) * 0.6 / 60
            if total_minutes > 5:
                print(f"⏰ This will take approximately {total_minutes:.0f} minutes to complete...")
        
        # Fetch data for each station
        for station_id, station_info in STATIONS.items():
            try:
                forecast_data = fetch_station_forecast(
                    wms, LAYER, station_id, station_info, time_list
                )
                
                if forecast_data:
                    save_forecast(station_id, forecast_data, station_info)
                else:
                    print(f"    ❌ No data retrieved for {station_id}")
                    
            except Exception as e:
                print(f"    ❌ Error processing {station_id}: {e}")
        
        # Create combined forecast
        create_combined_forecast()
        
        print("\n✅ Storm surge forecast update complete!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        return 1
        
    finally:
        release_lock()

if __name__ == "__main__":
    exit(main())