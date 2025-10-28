#!/usr/bin/env python3
"""
Export Storm Surge Hindcast Data
Exports up to 10 days of +48h predictions for charting
"""
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta

# Configuration
SQLITE_PATH = Path("~/.local/share/storm_surge_hindcast.sqlite").expanduser()
OUTPUT_PATH = Path("~/site/data/storm_surge/hindcast.json").expanduser()
MAX_DAYS = 10  # Display up to 10 days back

STATIONS = {
    "Point_Atkinson": {"name": "Point Atkinson", "lat": 49.337, "lon": -123.253},
    "Crescent_Beach_Channel": {"name": "Crescent Beach Channel", "lat": 49.0536, "lon": -122.8969}
}

def export_hindcast():
    """Export up to 10 days of +48h hindcast predictions."""
    
    if not SQLITE_PATH.exists():
        print(f"❌ Database not found: {SQLITE_PATH}")
        return False
    
    try:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Get the date range we actually have
        cur.execute("""
            SELECT MIN(forecast_date) as oldest, MAX(forecast_date) as newest, COUNT(DISTINCT forecast_date) as days
            FROM storm_surge_hindcast
        """)
        stats = cur.fetchone()
        
        if not stats or stats["days"] == 0:
            print("⚠️  No hindcast data in database yet")
            return False
        
        print(f"📊 Found {stats['days']} days of data ({stats['oldest']} to {stats['newest']})")
        
        # Calculate date range (up to MAX_DAYS back from today)
        today = datetime.now(timezone.utc).date()
        start_date = (today - timedelta(days=MAX_DAYS)).isoformat()
        
        hindcast_data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "description": "Storm surge predictions made 48 hours in advance",
            "forecast_horizon_hours": 48,
            "max_days_back": MAX_DAYS,
            "actual_days_available": stats["days"],
            "stations": {}
        }
        
        # Export data for each station
        for station_id, station_info in STATIONS.items():
            # Query all hindcast data for this station
            cur.execute("""
                SELECT forecast_date, valid_time, surge_value
                FROM storm_surge_hindcast
                WHERE station_id = ?
                  AND forecast_date >= ?
                ORDER BY valid_time ASC
            """, (station_id, start_date))
            
            rows = cur.fetchall()
            
            if not rows:
                print(f"⚠️  No hindcast data for {station_id}")
                continue
            
            # Build timeseries: each point is a prediction from forecast_date for valid_time
            hindcast_series = []
            for row in rows:
                hindcast_series.append({
                    "time": row["valid_time"],
                    "value": round(row["surge_value"], 3),
                    "forecast_date": row["forecast_date"]  # When this prediction was made
                })
            
            # Sort by valid_time
            hindcast_series.sort(key=lambda x: x["time"])
            
            # Build station data
            hindcast_data["stations"][station_id] = {
                "station_id": station_id,
                "station_name": station_info["name"],
                "location": {
                    "lat": station_info["lat"],
                    "lon": station_info["lon"]
                },
                "hindcast": hindcast_series
            }
            
            # Get date range for this station
            if hindcast_series:
                first_time = hindcast_series[0]["time"]
                last_time = hindcast_series[-1]["time"]
                print(f"✅ {station_id}: {len(hindcast_series)} points ({first_time[:10]} to {last_time[:10]})")
        
        conn.close()
        
        # Write to JSON (atomic write)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = OUTPUT_PATH.with_suffix(".json.tmp")
        tmp_file.write_text(json.dumps(hindcast_data, indent=2))
        tmp_file.replace(OUTPUT_PATH)
        
        print(f"\n💾 Wrote hindcast data to {OUTPUT_PATH}")
        return True
        
    except Exception as e:
        print(f"❌ Error exporting hindcast: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("🌊 Storm Surge Hindcast Export")
    print("=" * 50)
    
    success = export_hindcast()
    
    if success:
        print("✅ Hindcast export complete!")
        return 0
    else:
        print("❌ Hindcast export failed")
        return 1

if __name__ == "__main__":
    exit(main())