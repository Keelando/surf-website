#!/usr/bin/env python3
"""
Storm Surge Forecast Fetcher for Surf Server
Fetches GDSPS data from Environment Canada GeoMet WMS
Stores 12Z run to database for hindcast analysis
"""
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import re
import warnings
import time
import sqlite3
from owslib.wms import WebMapService

# Configuration
TESTING = False  # Set to True for verbose output and progress tracking

STATIONS = {
    "Point_Atkinson": {"lat": 49.3375, "lon": -123.253583, "name": "Point Atkinson"},
    "Crescent_Beach_Channel": {"lat": 49.0536, "lon": -122.8969, "name": "Crescent Beach Channel"},
    "Campbell_River": {"lat": 50.042, "lon": -125.247, "name": "Campbell River"},
    "Neah_Bay": {"lat": 48.495, "lon": -124.728, "name": "Neah Bay"},
    "New_Dungeness": {"lat": 48.333, "lon": -123.167, "name": "New Dungeness"},
    "Tofino": {"lat": 49.15, "lon": -125.9, "name": "Tofino"}
}

OUTPUT_DIR = Path("~/site/data/storm_surge").expanduser()
LOCKFILE = Path("/tmp/storm_surge_fetch.lock")
WMS_URL = "https://geo.weather.gc.ca/geomet?SERVICE=WMS&REQUEST=GetCapabilities&layer=GDSPS_15km_StormSurge"
LAYER = "GDSPS_15km_StormSurge"

# Database configuration
DB_PATH = Path("~/.local/share/storm_surge_forecast.sqlite").expanduser()
DB_RETENTION_DAYS = 11

# Rate limiting
FETCH_DELAY = 0.5  # seconds between requests


def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        age = time.time() - LOCKFILE.stat().st_mtime
        if age > 300:  # 5 minutes
            print("⚠️  Removing stale lock file")
            LOCKFILE.unlink()
        else:
            print(f"⚠️  Another instance is running (lock age: {age:.0f}s), exiting")
            return False
    LOCKFILE.touch()
    return True


def release_lock():
    """Remove lock file."""
    if LOCKFILE.exists():
        LOCKFILE.unlink()


def ensure_db_schema(conn):
    """Create forecast storage table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast_archive (
            station_id TEXT NOT NULL,
            forecast_run_time TEXT NOT NULL,
            valid_time TEXT NOT NULL,
            surge_value REAL NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (station_id, forecast_run_time, valid_time)
        )
    """)
    
    # Index for efficient queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_forecast_station_time
        ON forecast_archive(station_id, forecast_run_time DESC)
    """)
    conn.commit()


def store_forecast_to_db(forecast_run_time, all_station_data):
    """Store complete forecast to database (12Z run only)."""
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        ensure_db_schema(conn)
        cur = conn.cursor()
        
        forecast_date = forecast_run_time.date().isoformat()
        stored_count = 0
        
        for station_id, forecast_data in all_station_data.items():
            for time_str, surge_value in forecast_data.items():
                try:
                    cur.execute("""
                        INSERT OR REPLACE INTO forecast_archive
                        (station_id, forecast_run_time, valid_time, surge_value)
                        VALUES (?, ?, ?, ?)
                    """, (station_id, forecast_date, time_str, surge_value))
                    stored_count += 1
                except Exception as e:
                    print(f"⚠️  DB insert error for {station_id} {time_str}: {e}")
        
        conn.commit()
        
        # Purge old data
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=DB_RETENTION_DAYS)).date().isoformat()
        cur.execute("DELETE FROM forecast_archive WHERE forecast_run_time < ?", (cutoff_date,))
        deleted = cur.rowcount
        conn.commit()
        conn.close()
        
        print(f"\n💾 Stored {stored_count} forecast points to database")
        if deleted > 0:
            print(f"🗑️  Purged {deleted} old records (before {cutoff_date})")
        
        return True
        
    except Exception as e:
        print(f"❌ Database storage error: {e}")
        return False


def get_bounding_box(lat, lon, offset=0.25):
    """Create bounding box around point (lon_min, lat_min, lon_max, lat_max)."""
    return (lon - offset, lat - offset, lon + offset, lat + offset)


def get_time_parameters(wms, layer):
    """Extract temporal information from GeoMet metadata."""
    time_dim = wms[layer].dimensions["time"]["values"][0]
    start_str, end_str, interval_str = time_dim.split("/")
    
    iso_format = "%Y-%m-%dT%H:%M:%SZ"
    start_time = datetime.strptime(start_str, iso_format)
    end_time = datetime.strptime(end_str, iso_format)
    interval_hours = int(re.sub(r"\D", "", interval_str))
    
    return start_time, end_time, interval_hours


def fetch_pixel_value(wms, layer, bbox, timestamp):
    """Fetch storm surge value for specific location and time."""
    try:
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
        
        # Debug output for testing mode
        if TESTING and not hasattr(fetch_pixel_value, '_debug_count'):
            fetch_pixel_value._debug_count = 0
        
        if TESTING and fetch_pixel_value._debug_count < 3:
            print(f"\n    🔍 Debug - Raw response for {time_str}:")
            print(f"    {text[:200]}")
            fetch_pixel_value._debug_count += 1
        
        # Extract value from response
        match = re.search(r"value_0\s+=\s+'(-?\d+\.?\d*)'", text)
        if match:
            return float(match.group(1))
        
        # Check for "no feature" response
        if TESTING and "no feature" in text.lower():
            if not hasattr(fetch_pixel_value, '_no_feature_warned'):
                print(f"    ⚠️  'No feature' response - coordinates may be outside model domain")
                fetch_pixel_value._no_feature_warned = True
        
        return None
        
    except Exception as e:
        # Only log errors in testing mode or first occurrence
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
        print(f"    Estimated time: ~{len(time_list) * FETCH_DELAY:.0f} seconds "
              f"({len(time_list) * FETCH_DELAY / 60:.1f} minutes)")
    
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
                print(f"    Progress: {idx}/{len(time_list)} ({progress:.1f}%) - "
                      f"Success: {successful}, Failed: {failed}")
        elif idx % 50 == 0:
            print(f"    Progress: {idx}/{len(time_list)}...")
        
        time.sleep(FETCH_DELAY)  # Rate limiting
    
    print(f"    ✅ Retrieved {successful}/{len(time_list)} forecasts (Failed: {failed})")
    return forecast_data


def save_forecast(station_id, forecast_data, station_info):
    """Save forecast data to JSON file with atomic write."""
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
    
    # Atomic write (temp file + rename)
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
    
    for station_file in sorted(OUTPUT_DIR.glob("*.json")):
        if station_file.name == "combined_forecast.json":
            continue
        
        try:
            station_data = json.loads(station_file.read_text())
            station_id = station_data["station_id"]
            combined["stations"][station_id] = station_data
        except Exception as e:
            print(f"⚠️  Error reading {station_file.name}: {e}")
    
    combined_file = OUTPUT_DIR / "combined_forecast.json"
    tmp_file = combined_file.with_suffix(".json.tmp")
    tmp_file.write_text(json.dumps(combined, indent=2))
    tmp_file.replace(combined_file)
    
    print(f"\n✅ Created combined forecast: {combined_file}")
    print(f"📊 Contains {len(combined['stations'])} stations")


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
        print(f"📅 Forecast period: {start_time.strftime('%Y-%m-%d %H:%M')} to "
              f"{end_time.strftime('%Y-%m-%d %H:%M')} UTC")
        print(f"⏱️  Interval: {interval} hours")
        
        # Build time list
        time_list = [start_time]
        while time_list[-1] < end_time:
            time_list.append(time_list[-1] + timedelta(hours=interval))
        
        print(f"📊 Total timesteps: {len(time_list)}")
        
        # Estimate total time
        total_minutes = len(time_list) * len(STATIONS) * FETCH_DELAY / 60
        
        if TESTING:
            print(f"\n⏰ Estimated total time: ~{total_minutes:.1f} minutes")
            print(f"   ({len(time_list)} timesteps × {len(STATIONS)} stations × {FETCH_DELAY}s)")
        elif total_minutes > 5:
            print(f"⏰ This will take approximately {total_minutes:.0f} minutes to complete...")
        
        # Fetch data for each station
        all_forecasts = {}
        for station_id, station_info in STATIONS.items():
            try:
                forecast_data = fetch_station_forecast(
                    wms, LAYER, station_id, station_info, time_list
                )
                
                if forecast_data:
                    save_forecast(station_id, forecast_data, station_info)
                    all_forecasts[station_id] = forecast_data
                else:
                    print(f"    ❌ No data retrieved for {station_id}")
            
            except Exception as e:
                print(f"    ❌ Error processing {station_id}: {e}")
        
        # Create combined forecast
        create_combined_forecast()
        
        # Store to database if this is the 12Z run (hour 13)
        current_hour = datetime.now(timezone.utc).hour
        if current_hour == 13:
            print("\n🎯 This is the 12Z run - storing to database for hindcast...")
            store_forecast_to_db(start_time, all_forecasts)
        else:
            print(f"\n⏭️  Skipping database storage (hour={current_hour}, not 12Z run)")
        
        print("\n✅ Storm surge forecast update complete!")
        return 0
    
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        return 1
    
    finally:
        release_lock()


if __name__ == "__main__":
    exit(main())
