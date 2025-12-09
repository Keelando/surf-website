#!/usr/bin/env python3
"""
Export 24-hour wind station timeseries for charts.

Output: ~/site/data/wind_timeseries_24hr.json

Format:
{
  "CWGT": {
    "name": "Sisters Islets",
    "timeseries": {
      "wind_speed": [{"time": "2025-01-20T12:00:00Z", "value": 15.2}, ...],
      "wind_gust": [...],
      "wind_direction": [...],
      "air_temp": [...],
      "pressure": [...]
    }
  },
  ...
}
"""

from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta
import time

# Shared utilities
from units import kmh_to_knots
from config import WIND_DATABASE, EXPORT_DIR, safe_json_write
from logging_config import setup_logging

logger = setup_logging('wind_timeseries_export')

# ---------- Config ----------
OUT_PATH = EXPORT_DIR / "wind_timeseries_24hr.json"
LOCKFILE = Path("/tmp/wind_timeseries.lock")

# Wind station IDs to export
WIND_STATIONS = {
    "CWGT": "Sisters Islets",
    "CWGB": "Ballenas",
    "CWEL": "Entrance Island",
    "CWSB": "Point Atkinson",
    "CVTF": "Tsawwassen",
    "CWVF": "Sand Heads",
    "CWEZ": "Saturna",
    "CWQK": "Race Rocks",
    "CYVR": "YVR Airport",
    "CZBB": "Boundary Bay Airport",
    "JERICHO": "Jericho Sailing Centre",
    "whiterock_pier": "White Rock East Beach",
}

# White Rock Pier database path (separate from wind database)
WHITEROCK_DATABASE = Path("~/.local/share/weather_data.sqlite").expanduser()

# Metrics to export as timeseries
ALL_METRICS = {
    "wind_speed_kmh": {"name": "Wind Speed", "unit": "kt", "convert_to_knots": True},
    "wind_gust_kmh": {"name": "Wind Gust", "unit": "kt", "convert_to_knots": True},
    "wind_direction_deg": {"name": "Wind Direction", "unit": "°", "convert_to_knots": False},
    "air_temp_c": {"name": "Air Temperature", "unit": "°C", "convert_to_knots": False},
    "pressure_hpa": {"name": "Pressure", "unit": "hPa", "convert_to_knots": False},
    "rainfall_1hr_mm": {"name": "Rainfall (1hr)", "unit": "mm", "convert_to_knots": False},
}


def downsample_to_hourly(timeseries_data):
    """Downsample high-frequency data to hourly intervals by keeping the closest point to each hour.
    Normalizes all timestamps to the top of the hour (:00) for alignment in charts."""
    if not timeseries_data:
        return []

    # Group points by hour
    hourly_buckets = {}
    for point in timeseries_data:
        time_obj = datetime.fromisoformat(point['time'])
        # Create hour key (round down to the hour)
        hour_key = time_obj.replace(minute=0, second=0, microsecond=0)

        # Keep the point closest to the top of the hour
        if hour_key not in hourly_buckets:
            hourly_buckets[hour_key] = point
        else:
            # Compare which point is closer to the hour
            existing_time = datetime.fromisoformat(hourly_buckets[hour_key]['time'])
            existing_offset = abs((existing_time - hour_key).total_seconds())
            new_offset = abs((time_obj - hour_key).total_seconds())
            if new_offset < existing_offset:
                hourly_buckets[hour_key] = point

    # Normalize timestamps to the exact hour and sort
    hourly = []
    for hour_key, point in sorted(hourly_buckets.items()):
        # Create new point with normalized timestamp
        normalized_point = point.copy()
        normalized_point['time'] = hour_key.isoformat()
        hourly.append(normalized_point)

    return hourly


def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        # Check if lock is stale (>5 minutes old)
        if time.time() - LOCKFILE.stat().st_mtime > 300:
            logger.warning("Removing stale lock file")
            LOCKFILE.unlink()
        else:
            logger.warning("Another instance is running, exiting")
            return False

    LOCKFILE.touch()
    return True


def release_lock():
    """Release the lock file."""
    if LOCKFILE.exists():
        LOCKFILE.unlink()


def query_and_export_timeseries():
    # Check database exists
    if not WIND_DATABASE.exists():
        logger.warning(f"Wind database not found: {WIND_DATABASE}")
        logger.info("No data to export yet. This is normal during initial setup.")
        return

    # Connect to database
    conn = sqlite3.connect(WIND_DATABASE, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Check schema
    cur.execute("PRAGMA table_info(wind_observation);")
    existing_cols = {row[1] for row in cur.fetchall()}
    available_metrics = [m for m in ALL_METRICS.keys() if m in existing_cols]

    if not available_metrics:
        logger.error("No metrics available in database")
        conn.close()
        return

    # Calculate 24-hour window
    now = datetime.now(timezone.utc)
    cutoff_time = int((now - timedelta(hours=24)).timestamp())

    output = {}

    for station_id, station_name in WIND_STATIONS.items():
        station_data = {
            "name": station_name,
            "timeseries": {}
        }

        # Special handling for White Rock Pier (separate database)
        if station_id == "whiterock_pier":
            if WHITEROCK_DATABASE.exists():
                wr_conn = sqlite3.connect(WHITEROCK_DATABASE, timeout=5)
                wr_conn.row_factory = sqlite3.Row
                wr_cur = wr_conn.cursor()

                # Map White Rock fields to standard format
                wr_metrics = {
                    "wind_speed": {"field": "wind_speed", "convert": False},  # Already in knots
                    "wind_gust": {"field": "wind_gust", "convert": False},  # Already in knots
                    "wind_direction": {"field": "wind_direction", "convert": False},
                    "air_temp": {"field": "temperature", "convert": False},
                    "pressure": {"field": "pressure", "convert": False},
                }

                for json_key, config in wr_metrics.items():
                    field = config["field"]
                    sql = f"""
                    SELECT observation_time, {field}
                    FROM whiterock_weather
                    WHERE observation_time >= ?
                      AND {field} IS NOT NULL
                    ORDER BY observation_time ASC
                    """
                    wr_cur.execute(sql, (cutoff_time,))
                    rows = wr_cur.fetchall()

                    if rows:
                        timeseries = [{
                            "time": datetime.fromtimestamp(row["observation_time"], tz=timezone.utc).isoformat(),
                            "value": round(row[field], 1) if row[field] is not None else None
                        } for row in rows]

                        # Downsample to hourly
                        hourly = downsample_to_hourly(timeseries)
                        if hourly:
                            station_data["timeseries"][json_key] = hourly

                wr_conn.close()
                logger.info(f"Exported 24hr timeseries for {station_id} ({station_name})")
            else:
                logger.warning(f"White Rock database not found: {WHITEROCK_DATABASE}")
        else:
            # Standard EnvCan wind stations
            for metric, meta in ALL_METRICS.items():
                if metric not in available_metrics:
                    continue

                # Query 24hr data for this metric
                sql = f"""
                SELECT observation_time, {metric}
                FROM wind_observation
                WHERE station_id = ?
                  AND observation_time >= ?
                  AND {metric} IS NOT NULL
                ORDER BY observation_time ASC
                """
                cur.execute(sql, (station_id, cutoff_time))
                rows = cur.fetchall()

                if not rows:
                    continue

                # Build timeseries
                timeseries = []
                for row in rows:
                    value = row[metric]

                    # Convert wind speeds to knots
                    if meta.get("convert_to_knots", False):
                        value = round(kmh_to_knots(value), 1)

                    timeseries.append({
                        "time": datetime.fromtimestamp(row["observation_time"], tz=timezone.utc).isoformat(),
                        "value": value
                    })

                # Downsample to hourly for chart display
                hourly = downsample_to_hourly(timeseries)

                if hourly:
                    # Store with simplified key (remove units from field name for JSON)
                    json_key = metric.replace("_kmh", "").replace("_deg", "").replace("_c", "").replace("_hpa", "").replace("_mm", "")
                    station_data["timeseries"][json_key] = hourly

            # Log if we got data for this station
            if station_data["timeseries"]:
                logger.info(f"Exported 24hr timeseries for {station_id} ({station_name})")

        # Only include stations with actual data
        if station_data["timeseries"]:
            output[station_id] = station_data

    conn.close()

    # Add metadata
    output["_meta"] = {
        "generated_utc": now.isoformat(),
        "window_hours": 24,
        "data_resolution": "hourly (downsampled)"
    }

    # Atomic write
    safe_json_write(OUT_PATH, output)

    station_count = len([k for k in output.keys() if k != "_meta"])
    logger.info(f"Wrote 24hr timeseries to {OUT_PATH}")
    logger.info(f"Total stations: {station_count}")


def main():
    if not acquire_lock():
        return

    try:
        query_and_export_timeseries()
    except Exception as e:
        logger.error(f"Export failed: {e}")
    finally:
        release_lock()


if __name__ == "__main__":
    main()
