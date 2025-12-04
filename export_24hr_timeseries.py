#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta
import time

# Shared utilities
from units import kmh_to_knots
from config import BUOY_DATABASE, EXPORT_DIR, safe_json_write
from stations import get_all_buoys
from logging_config import setup_logging

logger = setup_logging('timeseries_export')

# ---------- Config ----------
OUT_PATH = EXPORT_DIR / "buoy_timeseries_24h.json"
LOCKFILE = Path("/tmp/buoy_timeseries.lock")
BUOYS = get_all_buoys()

# All available metrics for timeseries
ALL_METRICS = {
    # Wave heights
    "wave_height_sig": {"name": "Significant Wave Height", "unit": "m"},
    "wave_height_peak": {"name": "Peak Wave Height", "unit": "m"},
    "wave_height_max": {"name": "Maximum Wave Height", "unit": "m"},
    "wave_height_avg": {"name": "Average Wave Height", "unit": "m"},
    "wave_height_spectral": {"name": "Spectral Significant Wave Height", "unit": "m"},
    "wave_crest_height_max": {"name": "Max Crest Height Above Water", "unit": "m"},
    # Wave periods
    "wave_period_sig": {"name": "Significant Wave Period", "unit": "s"},
    "wave_period_avg": {"name": "Average Wave Period", "unit": "s"},
    "wave_period_peak": {"name": "Peak Wave Period", "unit": "s"},
    "wave_period_max_wave": {"name": "Period of Max Wave", "unit": "s"},
    "wave_period_spectral": {"name": "Spectral Wave Period", "unit": "s"},
    "wave_period_energy_spectral": {"name": "Spectral Energy Period", "unit": "s"},
    # Wave directions & spread
    "wave_direction_avg": {"name": "Average Wave Direction", "unit": "°"},
    "wave_direction_peak": {"name": "Peak Wave Direction", "unit": "°"},
    "wave_direction_spread_avg": {"name": "Avg Direction Spread", "unit": "°"},
    "wave_direction_spread_peak": {"name": "Peak Direction Spread", "unit": "°"},
    # NOAA spectral (swell vs wind waves)
    "swell_height": {"name": "Swell Height", "unit": "m"},
    "swell_period": {"name": "Swell Period", "unit": "s"},
    "swell_direction": {"name": "Swell Direction", "unit": "°"},
    "wind_wave_height": {"name": "Wind Wave Height", "unit": "m"},
    "wind_wave_period": {"name": "Wind Wave Period", "unit": "s"},
    "wind_wave_direction": {"name": "Wind Wave Direction", "unit": "°"},
    # Wind (primary sensor)
    "wind_speed": {"name": "Wind Speed", "unit": "kt"},
    "wind_gust": {"name": "Wind Gust", "unit": "kt"},
    "wind_direction": {"name": "Wind Direction", "unit": "°"},
    "wind_sensor_height": {"name": "Wind Sensor Height", "unit": "m"},
    # Wind (secondary sensor)
    "wind_speed_sensor_2": {"name": "Wind Speed (Sensor 2)", "unit": "kt"},
    "wind_gust_sensor_2": {"name": "Wind Gust (Sensor 2)", "unit": "kt"},
    "wind_direction_sensor_2": {"name": "Wind Direction (Sensor 2)", "unit": "°"},
    "wind_samples_bad_1": {"name": "Bad Wind Samples (S1)", "unit": "count"},
    "wind_samples_bad_2": {"name": "Bad Wind Samples (S2)", "unit": "count"},
    # Temperature
    "air_temp": {"name": "Air Temperature", "unit": "°C"},
    "sea_temp": {"name": "Sea Temperature", "unit": "°C"},
    # Pressure
    "pressure": {"name": "Station Pressure", "unit": "hPa"},
    "pressure_msl": {"name": "Mean Sea Level Pressure", "unit": "hPa"},
    "pressure_sensor_2": {"name": "Pressure (Sensor 2)", "unit": "hPa"},
    "pressure_trend_char": {"name": "Pressure Tendency", "unit": "code"},
    "pressure_trend_amount": {"name": "3hr Pressure Change", "unit": "hPa"},
    # Position
    "buoy_lat_current": {"name": "Current Latitude", "unit": "°"},
    "buoy_lon_current": {"name": "Current Longitude", "unit": "°"},
    # Solar (cloudiness indicator)
    "solar_current": {"name": "Solar Panel Current", "unit": "A"},
}

def downsample_to_hourly(timeseries_data):
    """Downsample high-frequency data to hourly intervals by keeping the closest point to each hour.
    Normalizes all timestamps to the top of the hour (:00) for alignment in history tables."""
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
    if not BUOY_DATABASE.exists():
        logger.error(f"Database not found: {BUOY_DATABASE}")
        return

    timeseries_json = {}
    now = datetime.now(timezone.utc)
    twenty_four_hours_ago = now - timedelta(hours=24)
    cutoff_timestamp = int(twenty_four_hours_ago.timestamp())

    try:
        with sqlite3.connect(BUOY_DATABASE, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # Check which metrics actually exist in the database
            cur.execute("PRAGMA table_info(buoy_observation);")
            existing_cols = {row[1] for row in cur.fetchall()}
            
            available_metrics = {k: v for k, v in ALL_METRICS.items() if k in existing_cols}

            if not available_metrics:
                logger.warning("No timeseries metrics found in database.")
                return

            logger.info(f"Available metrics: {', '.join(available_metrics.keys())}")

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
                    else:  # Halibut Bank, Sentry Shoal, Neah Bay, New Dungeness - keep all data initially
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
                        
                        # Don't downsample NOAA buoys - wind and wave share timestamps every 30 min
                        # Frontend will filter to wave timestamps for history table display
                        pass  # Keep all data at native frequency
                        
                        buoy_data["timeseries"][metric_key] = {
                            "name": metric_info["name"],
                            "unit": metric_info["unit"],
                            "data": timeseries
                        }
                        logger.debug(f"  {buoy_id} - {metric_key}: {len(timeseries)} points")

                # Only add buoy if it has at least one timeseries
                if buoy_data["timeseries"]:
                    timeseries_json[buoy_id] = buoy_data
                    logger.info(f"Exported {buoy_id} ({BUOYS[buoy_id]['name']})")
                else:
                    logger.info(f"Skipped {buoy_id} (no data in last 24h)")

    except sqlite3.OperationalError as e:
        logger.error(f"SQLite error: {e}")
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
    safe_json_write(OUT_PATH, timeseries_json, sort_keys=True)
    logger.info(f"Wrote 24h timeseries to {OUT_PATH}")
    logger.info(f"Total buoys: {timeseries_json['_meta']['buoy_count']}")
    logger.info(f"Time range: {twenty_four_hours_ago.strftime('%Y-%m-%d %H:%M')} to {now.strftime('%Y-%m-%d %H:%M')} UTC")

if __name__ == "__main__":
    if not acquire_lock():
        exit(1)
    
    try:
        query_and_export_timeseries()
    finally:
        release_lock()