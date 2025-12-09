#!/usr/bin/env python3
"""
Export latest wind station observations to JSON for website display.

Similar to sqlite_to_json.py but for wind data.
Outputs: ~/site/data/latest_wind.json

Format:
{
  "CWGT": {
    "name": "Sisters Islets",
    "wind_speed_kt": 15.2,
    "wind_gust_kt": 22.1,
    "wind_direction_deg": 270,
    "wind_direction_cardinal": "W",
    "air_temp_c": 12.5,
    "pressure_hpa": 1013.2,
    "rainfall_1hr_mm": 0.5,
    "observation_time": "2025-01-20T18:30:00+00:00",
    "stale": false
  },
  ...
}
"""

from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone

# Shared utilities
from units import kmh_to_knots
from directions import degrees_to_cardinal
from config import WIND_DATABASE, EXPORT_DIR, safe_json_write
from logging_config import setup_logging

logger = setup_logging('wind_json_export')

# ---------- Config ----------
OUT_PATH = EXPORT_DIR / "latest_wind.json"
FRESHNESS_WINDOW = 7200  # 2 hours (same as buoys)

# Wind station IDs to export (ICAO codes)
WIND_STATIONS = [
    "CWGT",  # Sisters Islets
    "CWGB",  # Ballenas
    "CWEL",  # Entrance Island
    "CWSB",  # Point Atkinson
    "CVTF",  # Tsawwassen
    "CWVF",  # Sand Heads
    "CWEZ",  # Saturna
    "CWQK",  # Race Rocks
    "CYVR",  # YVR Airport
    "CZBB",  # Boundary Bay Airport
    "JERICHO",  # Jericho Sailing Centre
]

# Station name overrides (for consistent display names)
STATION_NAME_OVERRIDES = {
    "CWGT": "Sisters Island",
    "CWGB": "Ballenas Island",
    "CWEL": "Entrance Island",
    "CWSB": "Point Atkinson",
    "CVTF": "Tsawwassen Ferry",
    "CWVF": "Sand Heads",
    "CWEZ": "Saturna Island",
    "CWQK": "Race Rocks",
    "CYVR": "Vancouver Int'l Airport",
    "CZBB": "Boundary Bay Airport",
    "JERICHO": "Jericho Sailing Centre",
}

# Fields to query individually (each gets most recent non-null value within 2 hours)
ALL_FIELDS = [
    "wind_speed_kmh",
    "wind_gust_kmh",
    "wind_direction_deg",
    "air_temp_c",
    "pressure_hpa",
    "rainfall_1hr_mm",
    "rainfall_6hr_mm",
]


def query_and_export():
    if not WIND_DATABASE.exists():
        logger.warning(f"Wind database not found: {WIND_DATABASE}")
        logger.info("No data to export yet. This is normal during initial setup.")
        return

    latest_json = {}

    with sqlite3.connect(WIND_DATABASE, timeout=5) as conn:
        # Enable WAL mode for safe concurrent reads during ingestion
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Guard against schema drift
        cur.execute("PRAGMA table_info(wind_observation);")
        existing_cols = {row[1] for row in cur.fetchall()}

        available_fields = [f for f in ALL_FIELDS if f in existing_cols]

        if not {"station_id", "observation_time"}.issubset(existing_cols):
            logger.error("Table wind_observation missing required columns")
            return

        for station_id in WIND_STATIONS:
            # Use override name if available, otherwise get from database
            if station_id in STATION_NAME_OVERRIDES:
                station_name = STATION_NAME_OVERRIDES[station_id]
            else:
                cur.execute("""
                    SELECT station_name
                    FROM wind_observation
                    WHERE station_id = ?
                    AND station_name IS NOT NULL
                    LIMIT 1
                """, (station_id,))
                row = cur.fetchone()
                station_name = row[0] if row else station_id

            station_json = {"name": station_name}

            # Get the most recent observation time (for reference)
            cur.execute("""
                SELECT observation_time
                FROM wind_observation
                WHERE station_id = ?
                ORDER BY observation_time DESC
                LIMIT 1
            """, (station_id,))
            latest_row = cur.fetchone()

            if not latest_row:
                continue

            latest_time = latest_row["observation_time"]
            station_json["observation_time"] = datetime.fromtimestamp(latest_time, tz=timezone.utc).isoformat()

            # Calculate staleness for UI indicators
            now_ts = datetime.now(timezone.utc).timestamp()
            age_minutes = (now_ts - latest_time) / 60
            station_json["stale"] = age_minutes > 120  # >2 hours old

            # Query each field individually - get most recent non-null value within freshness window
            cutoff_time = latest_time - FRESHNESS_WINDOW

            for field in available_fields:
                sql = f"""
                SELECT observation_time, {field}
                FROM wind_observation
                WHERE station_id = ?
                  AND observation_time >= ?
                  AND observation_time <= ?
                  AND {field} IS NOT NULL
                ORDER BY observation_time DESC
                LIMIT 1
                """
                cur.execute(sql, (station_id, cutoff_time, latest_time))
                row = cur.fetchone()

                if row:
                    value = row[field]
                    field_time = row["observation_time"]

                    # Convert wind speeds from km/h to knots for display
                    if field in ["wind_speed_kmh", "wind_gust_kmh"]:
                        # Store knots with _kt suffix
                        knot_field = field.replace("_kmh", "_kt")
                        station_json[knot_field] = round(kmh_to_knots(value), 1)
                    else:
                        # Store other fields as-is
                        station_json[field] = value

                    # Track individual field timestamps if different from main observation
                    if field_time != latest_time:
                        if "field_times" not in station_json:
                            station_json["field_times"] = {}
                        station_json["field_times"][field] = datetime.fromtimestamp(field_time, tz=timezone.utc).isoformat()

            # Add cardinal direction
            if 'wind_direction_deg' in station_json and station_json['wind_direction_deg'] is not None:
                cardinal = degrees_to_cardinal(station_json['wind_direction_deg'])
                if cardinal:
                    station_json["wind_direction_cardinal"] = cardinal

            # Skip stations with no actual data (only name + observation_time + stale flag)
            if len(station_json.keys()) <= 3:
                logger.info(f"Skipped {station_id} (no data within freshness window)")
                continue

            latest_json[station_id] = station_json
            logger.info(f"Exported {station_id} ({station_name})")

    # Include White Rock Pier weather station data
    whiterock_json_path = EXPORT_DIR / "whiterock_weather.json"
    if whiterock_json_path.exists():
        try:
            with open(whiterock_json_path) as f:
                whiterock_data = json.load(f)

            # Convert to same format as wind stations
            latest_json["whiterock_pier"] = {
                "name": whiterock_data["station_name"],
                "observation_time": whiterock_data["observation_time"],
                "stale": whiterock_data.get("stale", False),
                "wind_speed_kt": whiterock_data.get("wind_speed"),
                "wind_gust_kt": whiterock_data.get("wind_gust"),
                "wind_direction_deg": whiterock_data.get("wind_direction"),
                "wind_direction_cardinal": whiterock_data.get("wind_direction_cardinal"),
                "air_temp_c": whiterock_data.get("temperature"),
                "pressure_hpa": whiterock_data.get("pressure"),
                "humidity": whiterock_data.get("humidity"),
                "dew_point_c": whiterock_data.get("dew_point"),
                "precipitation_mm": whiterock_data.get("precipitation"),
            }
            # Remove None values
            latest_json["whiterock_pier"] = {k: v for k, v in latest_json["whiterock_pier"].items() if v is not None}
            logger.info(f"Exported whiterock_pier (White Rock Pier)")
        except Exception as e:
            logger.warning(f"Failed to include White Rock Pier data: {e}")

    # Add metadata about this export
    latest_json["_meta"] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "freshness_window_seconds": FRESHNESS_WINDOW,
        "freshness_window_human": f"{FRESHNESS_WINDOW // 3600}h"
    }

    # Atomic write
    safe_json_write(OUT_PATH, latest_json)

    # Count actual stations (exclude _meta)
    station_count = len([k for k in latest_json.keys() if k != "_meta"])

    logger.info(f"Wrote JSON snapshot to {OUT_PATH}")
    logger.info(f"Total stations: {station_count}")


if __name__ == "__main__":
    query_and_export()
