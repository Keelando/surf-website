#!/usr/bin/env python3
"""
Export tide data from SQLite to JSON for website display.

Outputs three types of data:
1. Current tide levels (latest observation per station)
2. Tide graph time series (observations + predictions, 15-min intervals)
3. High/Low tide events (discrete events with timestamps)

Usage:
  export_tide_json.py --current         # Export current conditions only
  export_tide_json.py --timeseries      # Export graph data only
  export_tide_json.py --highlow         # Export high/low events only
  export_tide_json.py --all             # Export everything (default)
  export_tide_json.py --hours 48        # Set timeseries lookback (default 24)
"""

import sqlite3
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ========== Configuration ==========
DB_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
STATION_FILE = Path("~/envcan_wave/tide_stations.json").expanduser()

# Output paths
CURRENT_OUT = Path("~/site/data/tide_current.json").expanduser()
TIMESERIES_OUT = Path("~/site/data/tide_timeseries.json").expanduser()
HIGHLOW_OUT = Path("~/site/data/tide_highlow.json").expanduser()


# ========== Utility Functions ==========

def load_station_metadata():
    """Load station names and metadata from external JSON file."""
    if not STATION_FILE.exists():
        print(f"??  Station file not found: {STATION_FILE}")
        return {}
    
    with open(STATION_FILE, "r") as f:
        return json.load(f)


def safe_json_write(path: Path, data: dict):
    """Atomic write: temp file + rename to avoid partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.replace(path)


# ========== Export Functions ==========

def export_current_levels(conn, station_metadata):
    """
    Export the most recent tide reading for each station.
    Only includes stations with observation data (wlo series).
    """
    cur = conn.cursor()
    
    # Get latest reading per station (observations only)
    query = """
    SELECT 
        station_id,
        station_name,
        timestamp_utc,
        water_level,
        quality,
        series_code
    FROM tide_observation
    WHERE (station_id, timestamp_utc) IN (
        SELECT station_id, MAX(timestamp_utc)
        FROM tide_observation
        WHERE series_code = 'wlo'
        GROUP BY station_id
    )
    ORDER BY station_name
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    current_data = {}
    observation_count = 0
    
    for row in rows:
        station_id, station_name, timestamp_utc, water_level, quality, series_code = row
        
        # Get metadata from external file
        metadata = station_metadata.get(station_name, {})
        
        # Calculate staleness
        now_ts = datetime.now(timezone.utc).timestamp()
        age_minutes = (now_ts - timestamp_utc) / 60
        is_stale = age_minutes > 120  # >2 hours old
        
        current_data[station_name] = {
            "station_id": station_id,
            "name": metadata.get("name", station_name.replace("_", " ").title()),
            "location": metadata.get("location", ""),
            "observation_time": datetime.fromtimestamp(timestamp_utc, tz=timezone.utc).isoformat(),
            "water_level": round(water_level, 3) if water_level is not None else None,
            "quality": quality,
            "series_code": series_code,
            "stale": is_stale,
            "age_minutes": round(age_minutes, 1)
        }
        observation_count += 1
    
    # Add metadata
    output = {
        "_meta": {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "station_count": observation_count,
            "type": "current_conditions",
            "units": "meters"
        },
        "stations": current_data
    }
    
    safe_json_write(CURRENT_OUT, output)
    print(f"? Exported current levels for {observation_count} stations ? {CURRENT_OUT}")
    
    return observation_count


def export_timeseries(conn, station_metadata, hours_back=24):
    """
    Export time series data for all stations (observations + predictions).
    All data is already at 15-minute intervals from the database.
    
    Includes:
      - wlo (observations) - historical data
      - wlp (predictions) - today's predictions
    
    Args:
        conn: SQLite connection
        station_metadata: Dict of station metadata
        hours_back: Number of hours to look back (default 24)
    """
    cur = conn.cursor()
    
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours_back)
    cutoff_timestamp = int(cutoff.timestamp())
    
    # Get all stations that have either observations or predictions
    cur.execute("""
    SELECT DISTINCT station_name, station_id
    FROM tide_observation
    WHERE series_code IN ('wlo', 'wlp')
    ORDER BY station_name
    """)
    
    stations = cur.fetchall()
    
    timeseries_data = {}
    total_points = 0
    
    for station_name, station_id in stations:
        metadata = station_metadata.get(station_name, {})
        
        # Query both observations and predictions
        query = """
        SELECT timestamp_utc, water_level, quality, series_code
        FROM tide_observation
        WHERE station_id = ?
          AND series_code IN ('wlo', 'wlp')
          AND timestamp_utc >= ?
          AND water_level IS NOT NULL
        ORDER BY timestamp_utc ASC
        """
        
        cur.execute(query, (station_id, cutoff_timestamp))
        rows = cur.fetchall()
        
        if not rows:
            continue
        
        # Build time series array
        series = []
        for timestamp_utc, water_level, quality, series_code in rows:
            series.append({
                "time": datetime.fromtimestamp(timestamp_utc, tz=timezone.utc).isoformat(),
                "value": round(water_level, 3),
                "quality": quality,
                "type": "observed" if series_code == "wlo" else "predicted"
            })
        
        timeseries_data[station_name] = {
            "station_id": station_id,
            "name": metadata.get("name", station_name.replace("_", " ").title()),
            "location": metadata.get("location", ""),
            "data": series
        }
        
        total_points += len(series)
        print(f"  ?? {station_name}: {len(series)} points")
    
    # Determine actual data time range
    data_start = None
    data_end = None
    for station_data in timeseries_data.values():
        if station_data["data"]:
            first_time = station_data["data"][0]["time"]
            last_time = station_data["data"][-1]["time"]
            if data_start is None or first_time < data_start:
                data_start = first_time
            if data_end is None or last_time > data_end:
                data_end = last_time
    
    # Build output
    output = {
        "_meta": {
            "generated_utc": now.isoformat(),
            "query_start": cutoff.isoformat(),
            "query_end": now.isoformat(),
            "data_start": data_start,
            "data_end": data_end,
            "hours_back": hours_back,
            "station_count": len(timeseries_data),
            "total_points": total_points,
            "type": "timeseries",
            "units": "meters",
            "interval": "15 minutes",
            "includes": "observations + current day predictions"
        },
        "stations": timeseries_data
    }
    
    safe_json_write(TIMESERIES_OUT, output)
    print(f"\n? Exported {hours_back}h timeseries ? {TIMESERIES_OUT}")
    print(f"?? Stations: {len(timeseries_data)} | Total points: {total_points}")
    
    return len(timeseries_data), total_points


def export_highlow_events(conn, station_metadata, days_forward=3):
    """
    Export high/low tide predictions for next N days.
    These are discrete events with original timestamps preserved.
    
    Args:
        conn: SQLite connection
        station_metadata: Dict of station metadata
        days_forward: Number of days to look forward (default 3)
    """
    cur = conn.cursor()
    
    now = datetime.now(timezone.utc)
    future = now + timedelta(days=days_forward)
    future_timestamp = int(future.timestamp())
    now_timestamp = int(now.timestamp())
    
    # Get all stations with high/low predictions
    cur.execute("""
    SELECT DISTINCT station_name, station_id
    FROM tide_observation
    WHERE series_code = 'wlp-hilo'
    ORDER BY station_name
    """)
    
    stations = cur.fetchall()
    
    highlow_data = {}
    total_events = 0
    
    for station_name, station_id in stations:
        metadata = station_metadata.get(station_name, {})
        
        # Query high/low events
        query = """
        SELECT timestamp_utc, water_level, event_type
        FROM tide_observation
        WHERE station_id = ?
          AND series_code = 'wlp-hilo'
          AND timestamp_utc >= ?
          AND timestamp_utc <= ?
          AND water_level IS NOT NULL
        ORDER BY timestamp_utc ASC
        """
        
        cur.execute(query, (station_id, now_timestamp, future_timestamp))
        rows = cur.fetchall()
        
        if not rows:
            continue
        
        # Build events array
        events = []
        for timestamp_utc, water_level, event_type in rows:
            events.append({
                "time": datetime.fromtimestamp(timestamp_utc, tz=timezone.utc).isoformat(),
                "water_level": round(water_level, 3),
                "type": event_type  # 'high' or 'low'
            })
        
        highlow_data[station_name] = {
            "station_id": station_id,
            "name": metadata.get("name", station_name.replace("_", " ").title()),
            "location": metadata.get("location", ""),
            "events": events
        }
        
        total_events += len(events)
        print(f"  ?? {station_name}: {len(events)} events")
    
    # Determine actual data time range
    data_start = None
    data_end = None
    for station_data in highlow_data.values():
        if station_data["events"]:
            first_time = station_data["events"][0]["time"]
            last_time = station_data["events"][-1]["time"]
            if data_start is None or first_time < data_start:
                data_start = first_time
            if data_end is None or last_time > data_end:
                data_end = last_time
    
    # Build output
    output = {
        "_meta": {
            "generated_utc": now.isoformat(),
            "query_start": now.isoformat(),
            "query_end": future.isoformat(),
            "data_start": data_start,
            "data_end": data_end,
            "days_forward": days_forward,
            "station_count": len(highlow_data),
            "total_events": total_events,
            "type": "highlow_events",
            "units": "meters",
            "event_types": ["high", "low"]
        },
        "stations": highlow_data
    }
    
    safe_json_write(HIGHLOW_OUT, output)
    print(f"\n? Exported high/low events ? {HIGHLOW_OUT}")
    print(f"?? Stations: {len(highlow_data)} | Total events: {total_events}")
    
    return len(highlow_data), total_events


# ========== Main Execution ==========

def main():
    parser = argparse.ArgumentParser(
        description="Export tide data from SQLite to JSON for website",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                    # Export everything (default)
  %(prog)s --current                # Export current conditions only
  %(prog)s --timeseries --hours 48  # Export 48h of graph data
  %(prog)s --highlow --days 3       # Export 3 days of high/low events
        """
    )
    parser.add_argument(
        "--current",
        action="store_true",
        help="Export current tide levels"
    )
    parser.add_argument(
        "--timeseries",
        action="store_true",
        help="Export time series graph data"
    )
    parser.add_argument(
        "--highlow",
        action="store_true",
        help="Export high/low tide events"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all data types (default)"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Hours of timeseries data to export (default: 24)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="Days of high/low events to export (default: 3)"
    )
    
    args = parser.parse_args()
    
    # Default to all if no specific options
    if not (args.current or args.timeseries or args.highlow or args.all):
        args.all = True
    
    # Validate database exists
    if not DB_PATH.exists():
        print(f"? Database not found: {DB_PATH}")
        return 1
    
    # Load station metadata
    station_metadata = load_station_metadata()
    
    print("?? Tide Data Export")
    print("=" * 70)
    
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            # Enable WAL mode for safe concurrent reads
            conn.execute("PRAGMA journal_mode=WAL;")
            
            # Export current conditions
            if args.current or args.all:
                print("\n?? Exporting current conditions...")
                export_current_levels(conn, station_metadata)
            
            # Export timeseries
            if args.timeseries or args.all:
                print(f"\n?? Exporting {args.hours}h timeseries (15-min intervals)...")
                export_timeseries(conn, station_metadata, args.hours)
            
            # Export high/low events
            if args.highlow or args.all:
                print(f"\n?? Exporting {args.days} days of high/low events...")
                export_highlow_events(conn, station_metadata, args.days)
        
        print("\n" + "=" * 70)
        print("? Export complete!")
        return 0
        
    except sqlite3.OperationalError as e:
        print(f"? Database error: {e}")
        return 1
    except Exception as e:
        print(f"? Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())