#!/usr/bin/env python3
"""
Store Storm Surge Hindcast Data
Archives the 12Z model run for forecast verification
"""
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta

# Configuration
FORECAST_JSON = Path("~/site/data/storm_surge/combined_forecast.json").expanduser()
SQLITE_PATH = Path("~/.local/share/storm_surge_hindcast.sqlite").expanduser()
FORECAST_HORIZON_HOURS = 48  # Store 48-hour ahead predictions
RETENTION_DAYS = 11  # Keep 11 days, display 10

STATIONS = ["Point_Atkinson", "Crescent_Beach_Channel"]

def ensure_schema(conn):
    """Create hindcast table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS storm_surge_hindcast (
            station_id TEXT NOT NULL,
            forecast_date TEXT NOT NULL,
            valid_time TEXT NOT NULL,
            surge_value REAL NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (station_id, forecast_date, valid_time)
        )
    """)
    
    # Index for fast queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_station_date 
        ON storm_surge_hindcast(station_id, forecast_date DESC)
    """)
    conn.commit()

def load_forecast_data():
    """Load current forecast JSON."""
    if not FORECAST_JSON.exists():
        print(f"❌ Forecast file not found: {FORECAST_JSON}")
        return None
    
    try:
        data = json.loads(FORECAST_JSON.read_text())
        return data
    except Exception as e:
        print(f"❌ Error loading forecast: {e}")
        return None

def store_hindcast(conn, forecast_data):
    """Store 48-hour forecasts for each station."""
    if not forecast_data or "stations" not in forecast_data:
        print("⚠️  No station data in forecast")
        return 0
    
    today = datetime.now(timezone.utc).date().isoformat()
    cur = conn.cursor()
    stored_count = 0
    
    for station_id in STATIONS:
        station_data = forecast_data["stations"].get(station_id)
        if not station_data or "forecast" not in station_data:
            print(f"⚠️  No data for {station_id}")
            continue
        
        forecast = station_data["forecast"]
        now = datetime.now(timezone.utc)
        target_time = now + timedelta(hours=FORECAST_HORIZON_HOURS)
        
        # Store all forecast points (we'll filter to 48h target later if needed)
        # For now, store everything from today's run
        for time_str, surge_value in forecast.items():
            valid_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            
            # Only store future forecasts (within our horizon)
            hours_ahead = (valid_time - now).total_seconds() / 3600
            if 0 < hours_ahead <= (RETENTION_DAYS * 24):
                try:
                    cur.execute("""
                        INSERT OR REPLACE INTO storm_surge_hindcast 
                        (station_id, forecast_date, valid_time, surge_value)
                        VALUES (?, ?, ?, ?)
                    """, (station_id, today, time_str, surge_value))
                    stored_count += 1
                except Exception as e:
                    print(f"⚠️  Error storing {station_id} {time_str}: {e}")
        
        print(f"✅ Stored {station_id} forecasts for {today}")
    
    conn.commit()
    return stored_count

def purge_old_data(conn):
    """Remove data older than retention period."""
    cur = conn.cursor()
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).date().isoformat()
    
    cur.execute("""
        DELETE FROM storm_surge_hindcast 
        WHERE forecast_date < ?
    """, (cutoff_date,))
    
    deleted = cur.rowcount
    conn.commit()
    
    if deleted > 0:
        print(f"🗑️  Purged {deleted} old records (before {cutoff_date})")
    
    return deleted

def main():
    print("🌊 Storm Surge Hindcast Storage")
    print("=" * 50)
    
    # Load current forecast
    forecast_data = load_forecast_data()
    if not forecast_data:
        return 1
    
    # Connect to database
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    ensure_schema(conn)
    
    # Store hindcast data
    stored = store_hindcast(conn, forecast_data)
    print(f"\n📊 Stored {stored} forecast points")
    
    # Purge old data
    purge_old_data(conn)
    
    # Show stats
    cur = conn.cursor()
    cur.execute("""
        SELECT station_id, COUNT(*), MIN(forecast_date), MAX(forecast_date)
        FROM storm_surge_hindcast
        GROUP BY station_id
    """)
    
    print("\n📈 Database Statistics:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} records ({row[2]} to {row[3]})")
    
    conn.close()
    print("\n✅ Hindcast storage complete!")
    return 0

if __name__ == "__main__":
    exit(main())
