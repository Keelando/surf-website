#!/usr/bin/env python3
"""
Export 24-hour lightstation timeseries for charts.

Output: ~/site/data/lightstation_timeseries_24hr.json

Format:
{
  "CAPE MUDGE": {
    "name": "Cape Mudge",
    "region": "STRAIT OF GEORGIA",
    "timeseries": {
      "wind_speed_kt": [{"time": "2025-11-25T18:00:00Z", "value": 27.0}, ...],
      "wind_direction": [{"time": "2025-11-25T18:00:00Z", "value": "SOUTHEAST"}, ...],
      "sea_height_ft": [{"time": "2025-11-25T18:00:00Z", "value": 4.0}, ...],
      "sea_condition": [{"time": "2025-11-25T18:00:00Z", "value": "MODERATE"}, ...]
    }
  },
  ...
}
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Shared utilities
from lib.config import EXPORT_DIR, safe_json_write
from lib.logging_config import setup_logging

logger = setup_logging('lightstation_timeseries_export')

# ---------- Config ----------
DB_PATH = Path.home() / ".local" / "share" / "lightstation_data.sqlite"
OUT_PATH = EXPORT_DIR / "lightstation_timeseries_24hr.json"

# Time window: past 24 hours
HOURS_BACK = 24


def export_timeseries():
    """Export 24hr timeseries for all lightstations."""
    if not DB_PATH.exists():
        logger.warning(f"Lightstation database not found: {DB_PATH}")
        return

    cutoff_time = int((datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)).timestamp())

    output = {}

    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get all unique stations
        cur.execute("SELECT DISTINCT station_name FROM lightstation_observation ORDER BY station_name")
        stations = [row[0] for row in cur.fetchall()]

        logger.info(f"Exporting 24hr timeseries for {len(stations)} lightstation(s)")

        for station_name in stations:
            # Get all observations for this station in the past 24 hours
            cur.execute("""
                SELECT
                    observation_time,
                    region,
                    wind_speed_kt,
                    wind_direction,
                    wind_gusting,
                    wind_calm,
                    sea_height_ft,
                    sea_condition,
                    swell_intensity,
                    swell_direction
                FROM lightstation_observation
                WHERE station_name = ?
                AND observation_time >= ?
                ORDER BY observation_time ASC
            """, (station_name, cutoff_time))

            rows = cur.fetchall()

            if not rows:
                continue

            # Initialize timeseries structure
            station_data = {
                "name": station_name,
                "station_name": station_name,
                "region": rows[0]["region"],
                "timeseries": {
                    "wind_speed_kt": [],
                    "wind_direction": [],
                    "sea_height_ft": [],
                    "sea_condition": [],
                    "swell_intensity": [],
                }
            }

            # Build timeseries for each metric
            for row in rows:
                timestamp = datetime.fromtimestamp(row["observation_time"], tz=timezone.utc).isoformat()

                # Wind speed (null if calm)
                if not row["wind_calm"] and row["wind_speed_kt"] is not None:
                    station_data["timeseries"]["wind_speed_kt"].append({
                        "time": timestamp,
                        "value": row["wind_speed_kt"],
                        "gusting": bool(row["wind_gusting"])
                    })

                # Wind direction (skip if calm or null)
                if not row["wind_calm"] and row["wind_direction"]:
                    station_data["timeseries"]["wind_direction"].append({
                        "time": timestamp,
                        "value": row["wind_direction"]
                    })

                # Sea height
                if row["sea_height_ft"] is not None:
                    station_data["timeseries"]["sea_height_ft"].append({
                        "time": timestamp,
                        "value": row["sea_height_ft"]
                    })

                # Sea condition
                if row["sea_condition"]:
                    station_data["timeseries"]["sea_condition"].append({
                        "time": timestamp,
                        "value": row["sea_condition"]
                    })

                # Swell intensity
                if row["swell_intensity"]:
                    station_data["timeseries"]["swell_intensity"].append({
                        "time": timestamp,
                        "value": row["swell_intensity"]
                    })

            output[station_name] = station_data

    # Write to file
    safe_json_write(OUT_PATH, output)
    logger.info(f"✓ Exported timeseries for {len(output)} station(s) to {OUT_PATH}")


def main():
    logger.info("=== Exporting Lightstation 24hr Timeseries ===")
    export_timeseries()
    logger.info("=== Export complete ===")


if __name__ == "__main__":
    main()
