#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone

# Shared utilities
from units import kmh_to_knots
from directions import degrees_to_cardinal
from config import BUOY_DATABASE, EXPORT_DIR, BUOY_FRESHNESS_WINDOW, safe_json_write
from stations import get_all_buoys
from logging_config import setup_logging

logger = setup_logging('json_export')

# ---------- Config ----------
OUT_PATH = EXPORT_DIR / "latest_buoy_v2.json"
BUOYS = get_all_buoys()

# Fields to query individually (each gets most recent non-null value within 2 hours)
ALL_FIELDS = [
    # Wave heights
    "wave_height_sig", "wave_height_peak", "wave_height_max", "wave_height_avg",
    "wave_height_spectral", "wave_crest_height_max",
    # Wave periods
    "wave_period_sig", "wave_period_avg", "wave_period_peak", "wave_period_max_wave",
    "wave_period_spectral", "wave_period_energy_spectral", "wave_period_sig_basic",
    "wave_height_max_avg", "wave_period_max_avg",
    # Wave directions
    "wave_direction_avg", "wave_direction_peak",
    "wave_direction_spread_avg", "wave_direction_spread_peak",
    # NOAA spectral data (swell vs wind waves)
    "swell_height", "swell_period", "swell_direction",
    "wind_wave_height", "wind_wave_period", "wind_wave_direction",
    # Wind (primary sensor)
    "wind_speed", "wind_gust", "wind_direction", "wind_sensor_height",
    # Wind (secondary sensor for redundancy)
    "wind_speed_sensor_2", "wind_gust_sensor_2", "wind_direction_sensor_2",
    "wind_samples_bad_1", "wind_samples_bad_2",
    # Temperature
    "air_temp", "sea_temp",
    # Pressure
    "pressure", "pressure_msl", "pressure_sensor_2",
    "pressure_trend_char", "pressure_trend_amount",
    # Position
    "buoy_lat_current", "buoy_lon_current",
    # Solar (cloudiness indicator)
    "solar_current",
    # System health & monitoring (added 2025-12-06)
    "battery_voltage", "watchman_boot_count", "obstruction_lamp_current",
    # Orientation (compass headings, added 2025-12-06)
    "compass_heading_1", "compass_heading_2",
]

def query_and_export():
    latest_json = {}

    with sqlite3.connect(BUOY_DATABASE, timeout=5) as conn:
        # Enable WAL mode for safe concurrent reads during ingestion
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Guard against schema drift
        cur.execute("PRAGMA table_info(buoy_observation);")
        existing_cols = {row[1] for row in cur.fetchall()}
        
        available_fields = [f for f in ALL_FIELDS if f in existing_cols]

        if not {"buoy_id", "observation_time"}.issubset(existing_cols):
            logger.error("Table buoy_observation missing required columns")
            return

        for buoy_id in BUOYS.keys():
            buoy_json = {"name": BUOYS[buoy_id]["name"]}

            # Get the most recent observation time (for reference)
            cur.execute("""
                SELECT observation_time
                FROM buoy_observation
                WHERE buoy_id = ?
                ORDER BY observation_time DESC
                LIMIT 1
            """, (buoy_id,))
            latest_row = cur.fetchone()

            if not latest_row:
                continue

            latest_time = latest_row["observation_time"]
            buoy_json["observation_time"] = datetime.fromtimestamp(latest_time, tz=timezone.utc).isoformat()

            # Calculate staleness for UI indicators
            now_ts = datetime.now(timezone.utc).timestamp()
            age_minutes = (now_ts - latest_time) / 60
            buoy_json["stale"] = age_minutes > 120  # >2 hours old

            # Query each field individually - get most recent non-null value within freshness window
            cutoff_time = latest_time - BUOY_FRESHNESS_WINDOW

            for field in available_fields:
                sql = f"""
                SELECT observation_time, {field}
                FROM buoy_observation
                WHERE buoy_id = ?
                  AND observation_time >= ?
                  AND observation_time <= ?
                  AND {field} IS NOT NULL
                ORDER BY observation_time DESC
                LIMIT 1
                """
                cur.execute(sql, (buoy_id, cutoff_time, latest_time))
                row = cur.fetchone()

                if row:
                    value = row[field]
                    field_time = row["observation_time"]
                    
                    # Convert wind speeds from km/h to knots (future-proof for wind_wave_speed)
                    if "wind" in field and field.endswith(("speed", "gust")):
                        value = kmh_to_knots(value)
                    
                    buoy_json[field] = value
                    
                    # Track individual field timestamps if different from main observation
                    if field_time != latest_time:
                        if "field_times" not in buoy_json:
                            buoy_json["field_times"] = {}
                        buoy_json["field_times"][field] = datetime.fromtimestamp(field_time, tz=timezone.utc).isoformat()

            # Add cardinal directions for all directional fields
            direction_fields = [
                'wind_direction',
                'wind_direction_sensor_2',
                'wave_direction_avg',
                'wave_direction_peak',
                'swell_direction',
                'wind_wave_direction',
            ]

            for field in direction_fields:
                if field in buoy_json and buoy_json[field] is not None:
                    cardinal = degrees_to_cardinal(buoy_json[field])
                    if cardinal:
                        buoy_json[f"{field}_cardinal"] = cardinal

            # Skip buoys with no actual data (only name + observation_time + stale flag)
            if len(buoy_json.keys()) <= 3:
                logger.info(f"Skipped {buoy_id} (no data within freshness window)")
                continue

            latest_json[buoy_id] = buoy_json
            logger.info(f"Exported {buoy_id} ({BUOYS[buoy_id]['name']})")

    # Add metadata about this export
    latest_json["_meta"] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "freshness_window_seconds": BUOY_FRESHNESS_WINDOW,
        "freshness_window_human": f"{BUOY_FRESHNESS_WINDOW // 3600}h"
    }

    # Atomic write
    safe_json_write(OUT_PATH, latest_json)

    # Count actual buoys (exclude _meta)
    buoy_count = len([k for k in latest_json.keys() if k != "_meta"])

    logger.info(f"Wrote JSON snapshot to {OUT_PATH}")
    logger.info(f"Total buoys: {buoy_count}")

if __name__ == "__main__":
    query_and_export()