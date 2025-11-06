#!/usr/bin/env python3
"""
Export Storm Surge Hindcast Data
Exports 38-61 hour predictions for charting (full Pacific calendar day predicted 2 days in advance)
"""
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta

# Configuration
DB_PATH = Path("~/.local/share/storm_surge_forecast.sqlite").expanduser()
OUTPUT_PATH = Path("~/site/data/storm_surge/hindcast.json").expanduser()
MAX_DAYS_BACK = 10

STATIONS = {
    "Point_Atkinson": {"name": "Point Atkinson", "lat": 49.3375, "lon": -123.253583},
    "Crescent_Beach_Channel": {"name": "Crescent Beach Channel", "lat": 49.0536, "lon": -122.8969},
    "Campbell_River": {"name": "Campbell River", "lat": 50.042, "lon": -125.247},
    "Neah_Bay": {"name": "Neah Bay", "lat": 48.495, "lon": -124.728},
    "New_Dungeness": {"name": "New Dungeness", "lat": 48.333, "lon": -123.167},
    "Tofino": {"name": "Tofino", "lat": 49.15, "lon": -125.9}
}

def export_hindcast():
    """Export +48h hindcast predictions to JSON."""
    
    if not DB_PATH.exists():
        print(f"❌ Database not found: {DB_PATH}")
        print("   Run fetch_storm_surge.py at 19:30 UTC to start collecting data")
        return False
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Get available date range
        cur.execute("""
            SELECT 
                MIN(forecast_run_time) as oldest,
                MAX(forecast_run_time) as newest,
                COUNT(DISTINCT forecast_run_time) as days
            FROM forecast_archive
        """)
        stats = cur.fetchone()
        
        if not stats or stats["days"] == 0:
            print("⚠️  No forecast data in database yet")
            print("   First data will be available after 19:30 UTC run")
            return False
        
        print(f"📊 Found {stats['days']} days of forecasts ({stats['oldest']} to {stats['newest']})")
        
        hindcast_data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "description": "Storm surge predictions for full Pacific calendar days made 2 days in advance (18Z run, hours 38-61 PST)",
            "forecast_horizon_hours": "38-61",
            "max_days_back": MAX_DAYS_BACK,
            "actual_days_available": stats["days"],
            "stations": {}
        }
        
        # Export each station
        for station_id, station_info in STATIONS.items():
            print(f"\n📍 Processing {station_info['name']}...")
            
            # Query: Get all forecasts and filter for hours 38-61 (full Pacific calendar day)
            # 18Z run on Tuesday → hours 38-61 = all of Thursday PST (00:00-23:00 Pacific)
            # Note: This uses PST offset (UTC-8). PDT would be 37-60.
            cur.execute("""
                SELECT
                    forecast_run_time,
                    valid_time,
                    surge_value,
                    ROUND((julianday(valid_time) - julianday(forecast_run_time)) * 24, 1) as hours_ahead
                FROM forecast_archive
                WHERE station_id = ?
                  AND hours_ahead BETWEEN 38 AND 61
                ORDER BY valid_time ASC
            """, (station_id,))
            
            rows = cur.fetchall()
            
            if not rows:
                print(f"   ⚠️  No 38-61h predictions found")
                continue
            
            # Build hindcast series
            hindcast_series = []
            for row in rows:
                hindcast_series.append({
                    "time": row["valid_time"],
                    "value": round(row["surge_value"], 3),
                    "forecast_date": row["forecast_run_time"],
                    "hours_ahead": row["hours_ahead"]
                })
            
            # Get time range
            first_time = datetime.fromisoformat(hindcast_series[0]["time"].replace('Z', '+00:00'))
            last_time = datetime.fromisoformat(hindcast_series[-1]["time"].replace('Z', '+00:00'))
            
            print(f"   ✅ {len(hindcast_series)} predictions")
            print(f"   📅 Range: {first_time.strftime('%Y-%m-%d')} to {last_time.strftime('%Y-%m-%d')}")
            
            # Add to output
            hindcast_data["stations"][station_id] = {
                "station_id": station_id,
                "station_name": station_info["name"],
                "location": {
                    "lat": station_info["lat"],
                    "lon": station_info["lon"]
                },
                "hindcast": hindcast_series
            }
        
        conn.close()
        
        # Write to JSON (atomic)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = OUTPUT_PATH.with_suffix(".json.tmp")
        tmp_file.write_text(json.dumps(hindcast_data, indent=2))
        tmp_file.replace(OUTPUT_PATH)
        
        print(f"\n💾 Wrote hindcast data to {OUTPUT_PATH}")
        print(f"📊 Total stations: {len(hindcast_data['stations'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Export error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("🌊 Storm Surge Hindcast Export (38-61h / 2-day ahead Pacific)")
    print("=" * 50)
    
    success = export_hindcast()
    
    if success:
        print("\n✅ Hindcast export complete!")
        return 0
    else:
        print("\n❌ Hindcast export failed")
        return 1


if __name__ == "__main__":
    exit(main())