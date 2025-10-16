#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta
import time

# ---------- Config ----------
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
OUT_PATH = Path("~/site/data/buoy_timeseries_24h.json").expanduser()
LOCKFILE = Path("/tmp/buoy_timeseries.lock")

BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    "4600304": {"name": "English Bay", "location": "Vancouver Harbor"},
    "4600131": {"name": "Sentry Shoal", "location": "Northern Strait of Georgia"},
}

# All available metrics for timeseries
ALL_METRICS = {
    "wave_height_sig": {"name": "Significant Wave Height", "unit": "m"},
    "wave_height_peak": {"name": "Peak Wave Height", "unit": "m"},
    "wave_period_avg": {"name": "Average Wave Period", "unit": "s"},
    "wave_period_peak": {"name": "Peak Wave Period", "unit": "s"},
    "wave_direction_peak": {"name": "Peak Wave Direction", "unit": "°"},
    "wind_speed": {"name": "Wind Speed", "unit": "kt"},
    "wind_gust": {"name": "Wind Gust", "unit": "kt"},
    "wind_direction": {"name": "Wind Direction", "unit": "°"},
    "air_temp": {"name": "Air Temperature", "unit": "°C"},
    "sea_temp": {"name": "Sea Temperature", "unit": "°C"},
    "pressure": {"name": "Pressure", "unit": "hPa"},
}

def kmh_to_knots(kmh):
    """Convert km/h to knots for wind fields."""
    if kmh is None:
        return None
    try:
        return round(float(kmh) * 0.539957, 1)
    except (TypeError, ValueError):
        return None

def safe_json_write(path: Path, data: dict):
    """Atomic write: temp file + rename to avoid partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.replace(path)

def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        # Check if lock is stale (>5 minutes old)
        if time.time() - LOCKFILE.stat().st_mtime > 300:
            print("⚠️  Removing stale lock file")
            LOCKFILE.unlink()
        else:
            print("⚠️  Another instance is running, exiting")
            return False
    
    LOCKFILE.touch()
    return True

def release_lock():
    """Release the lock file."""
    if LOCKFILE.exists():
        LOCKFILE.unlink()

def query_and_export_timeseries():
    # Check database exists
    if not SQLITE_PATH.exists():
        print(f"❌ Database not found: {SQLITE_PATH}")
        return

    timeseries_json = {}
    now = datetime.now(timezone.utc)
    twenty_four_hours_ago = now - timedelta(hours=24)
    cutoff_timestamp = int(twenty_four_hours_ago.timestamp())
    
    try:
        with sqlite3.connect(SQLITE_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # Check which metrics actually exist in the database
            cur.execute("PRAGMA table_info(buoy_observation);")
            existing_cols = {row[1] for row in cur.fetchall()}
            
            available_metrics = {k: v for k, v in ALL_METRICS.items() if k in existing_cols}
            
            if not available_metrics:
                print("⚠️  No timeseries metrics found in database.")
                return

            print(f"📊 Available metrics: {', '.join(available_metrics.keys())}")

            for buoy_id in BUOYS.keys():
                buoy_data = {
                    "name": BUOYS[buoy_id]["name"],
                    "location": BUOYS[buoy_id]["location"],
                    "timeseries": {}
                }

                # Query each metric separately to handle sparse data
                for metric_key, metric_info in available_metrics.items():
                    # For buoys that report every 10 minutes, only grab hourly data
                    if buoy_id in ["4600304", "4600303"]:  # English Bay, Southern Georgia Strait
                        sql = f"""
                        SELECT observation_time, {metric_key}
                        FROM buoy_observation
                        WHERE buoy_id = ?
                          AND observation_time >= ?
                          AND {metric_key} IS NOT NULL
                          AND observation_time % 3600 = 0
                        ORDER BY observation_time ASC
                        """
                    else:  # Halibut Bank, Sentry Shoal - keep all data
                        sql = f"""
                        SELECT observation_time, {metric_key}
                        FROM buoy_observation
                        WHERE buoy_id = ?
                          AND observation_time >= ?
                          AND {metric_key} IS NOT NULL
                        ORDER BY observation_time ASC
                        """
                    
                    cur.execute(sql, (buoy_id, cutoff_timestamp))
                    rows = cur.fetchall()

                    if rows:
                        timeseries = []
                        for row in rows:
                            value = row[metric_key]
                            
                            # Convert wind speeds from km/h to knots
                            if metric_key in ['wind_speed', 'wind_gust']:
                                value = kmh_to_knots(value)
                            else:
                                value = round(value, 2)
                            
                            timeseries.append({
                                "time": datetime.fromtimestamp(row["observation_time"], tz=timezone.utc).isoformat(),
                                "value": value
                            })
                        
                        buoy_data["timeseries"][metric_key] = {
                            "name": metric_info["name"],
                            "unit": metric_info["unit"],
                            "data": timeseries
                        }
                        print(f"  📊 {buoy_id} - {metric_key}: {len(timeseries)} points")

                # Only add buoy if it has at least one timeseries
                if buoy_data["timeseries"]:
                    timeseries_json[buoy_id] = buoy_data
                    print(f"✅ Exported {buoy_id} ({BUOYS[buoy_id]['name']})")
                else:
                    print(f"⏭️  Skipped {buoy_id} (no data in last 24h)")

    except sqlite3.OperationalError as e:
        print(f"❌ SQLite error: {e}")
        return

    # Determine actual data time range from exported data
    data_start = None
    data_end = None
    for buoy_data in timeseries_json.values():
        if "timeseries" in buoy_data:
            for metric_data in buoy_data["timeseries"].values():
                if metric_data["data"]:
                    first_time = metric_data["data"][0]["time"]
                    last_time = metric_data["data"][-1]["time"]
                    if data_start is None or first_time < data_start:
                        data_start = first_time
                    if data_end is None or last_time > data_end:
                        data_end = last_time

    # Add metadata
    timeseries_json["_meta"] = {
        "generated_utc": now.isoformat(),
        "query_start": twenty_four_hours_ago.isoformat(),
        "query_end": now.isoformat(),
        "data_start": data_start,
        "data_end": data_end,
        "hours_covered": 24,
        "available_metrics": list(available_metrics.keys()),
        "buoy_count": len([k for k in timeseries_json.keys() if k != "_meta"])
    }

    # Atomic write
    safe_json_write(OUT_PATH, timeseries_json)
    print(f"\n✅ Wrote 24h timeseries to {OUT_PATH}")
    print(f"📊 Total buoys: {timeseries_json['_meta']['buoy_count']}")
    print(f"⏰ Time range: {twenty_four_hours_ago.strftime('%Y-%m-%d %H:%M')} to {now.strftime('%Y-%m-%d %H:%M')} UTC")

if __name__ == "__main__":
    if not acquire_lock():
        exit(1)
    
    try:
        query_and_export_timeseries()
    finally:
        release_lock()