#!/usr/bin/env python3
"""
Export latest lightstation observations to JSON for website display.

Outputs: ~/site/data/latest_lightstation.json

Format:
{
  "CAPE MUDGE": {
    "region": "STRAIT OF GEORGIA",
    "wind_speed_kt": 27.0,
    "wind_direction": "SOUTHEAST",
    "wind_gusting": true,
    "wind_estimated": true,
    "sea_height_ft": 4.0,
    "sea_condition": "MODERATE",
    "swell_intensity": "LOW",
    "swell_direction": "SOUTHERLY",
    "observation_time": "2025-11-25T18:10:00+00:00",
    "report_time_str": "10 AM Tuesday",
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
from config import EXPORT_DIR, safe_json_write
from logging_config import setup_logging

logger = setup_logging('lightstation_json_export')

# ---------- Config ----------
DB_PATH = Path.home() / ".local" / "share" / "lightstation_data.sqlite"
OUT_PATH = EXPORT_DIR / "latest_lightstation.json"
FRESHNESS_WINDOW = 21600  # 6 hours (reports are 3-hourly, but may have delays)


def query_and_export():
    if not DB_PATH.exists():
        logger.warning(f"Lightstation database not found: {DB_PATH}")
        logger.info("No data to export yet. This is normal during initial setup.")
        return

    latest_json = {}

    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        # Enable WAL mode for safe concurrent reads
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get all unique station names
        cur.execute("""
            SELECT DISTINCT station_name
            FROM lightstation_observation
            ORDER BY station_name
        """)
        stations = [row[0] for row in cur.fetchall()]

        logger.info(f"Exporting data for {len(stations)} lightstation(s)")

        for station_name in stations:
            # Get the most recent observation for this station
            cur.execute("""
                SELECT *
                FROM lightstation_observation
                WHERE station_name = ?
                ORDER BY observation_time DESC
                LIMIT 1
            """, (station_name,))

            row = cur.fetchone()
            if not row:
                continue

            # Check if this observation has null key values
            # If so, fall back to the most recent observation with non-null data
            has_data = (
                row["wind_speed_kt"] is not None or
                row["wind_calm"] or
                row["sea_height_ft"] is not None or
                row["sea_condition"] is not None
            )

            if not has_data:
                # Fall back to last observation with actual data
                cur.execute("""
                    SELECT *
                    FROM lightstation_observation
                    WHERE station_name = ?
                      AND (wind_speed_kt IS NOT NULL
                           OR wind_calm = 1
                           OR sea_height_ft IS NOT NULL
                           OR sea_condition IS NOT NULL)
                    ORDER BY observation_time DESC
                    LIMIT 1
                """, (station_name,))

                fallback_row = cur.fetchone()
                if fallback_row:
                    row = fallback_row

            # Calculate staleness (>6 hours = stale)
            observation_time = row["observation_time"]
            now_ts = datetime.now(timezone.utc).timestamp()
            age_hours = (now_ts - observation_time) / 3600
            is_stale = age_hours > 6

            # Build JSON entry
            station_json = {
                "region": row["region"],
                "wind_speed_kt": row["wind_speed_kt"],
                "wind_direction": row["wind_direction"],
                "wind_gusting": bool(row["wind_gusting"]),
                "wind_calm": bool(row["wind_calm"]),
                "wind_estimated": bool(row["wind_estimated"]),
                "sea_height_ft": row["sea_height_ft"],
                "sea_condition": row["sea_condition"],
                "swell_intensity": row["swell_intensity"],
                "swell_direction": row["swell_direction"],
                "observation_time": datetime.fromtimestamp(observation_time, tz=timezone.utc).isoformat(),
                "report_time_str": row["report_time_str"],
                "stale": is_stale,
            }

            latest_json[station_name] = station_json

    # Write to file
    safe_json_write(OUT_PATH, latest_json)
    logger.info(f"✓ Exported {len(latest_json)} station(s) to {OUT_PATH}")


def main():
    logger.info("=== Exporting Lightstation JSON ===")
    query_and_export()
    logger.info("=== Export complete ===")


if __name__ == "__main__":
    main()
