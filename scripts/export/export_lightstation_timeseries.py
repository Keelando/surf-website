#!/usr/bin/env python3
"""
Export recent lightstation timeseries for charts and the public API.

Output: ~/site/data/lightstation_timeseries.json

Format:
{
  "CAPE MUDGE": {
    "name": "Cape Mudge",
    "region": "STRAIT OF GEORGIA",
    "timeseries": {
      "wind_speed_kt": [{"time": "2025-11-25T18:00:00Z", "value": 27.0, "gusting": false}, ...],
                        (calm is {"value": 0, "calm": true})
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

# Shared utilities
from lib.config import EXPORT_DIR, safe_json_write
from lib.config import LIGHTSTATION_DATABASE as DB_PATH
from lib.logging_config import setup_logging
from lib.stations import get_lightstation_by_report_name

logger = setup_logging("lightstation_timeseries_export")

# ---------- Config ----------
OUT_PATH = EXPORT_DIR / "lightstation_timeseries.json"


# ---------- Region ----------
#
# `region` is per-observation in the database, and the two bulletin products
# disagree about it: SXCN can only name the area its whole bulletin covers, so
# it files the central-coast lights under HECATE STRAIT and Trial Island under
# STRAIT OF GEORGIA. Whichever bulletin wrote the newest row used to decide
# which group a station appeared in on the page, so nine stations drifted
# between sections as the feeds alternated.
#
# config/stations.json carries a per-station `region` that does not move. It is
# the only thing consulted here; the column stays in the database as a record
# of what each bulletin claimed, and is not exported.
def station_region(station_name, fallback=None):
    """The registry's region for a station, or `fallback` if it is unregistered.

    An unregistered station is a real case — TRIPLE ISLAND arrives on bulletins
    we parse but has never been added to config/stations.json — so say so once
    rather than dropping the station or filing it under nothing.
    """
    metadata = get_lightstation_by_report_name(station_name)
    if metadata and metadata.get("region"):
        return metadata["region"]
    logger.warning(
        f"{station_name} is not in config/stations.json; "
        f"falling back to the region its bulletin claimed ({fallback!r})"
    )
    return fallback

# Time window.
#
# 72 hours, not 24. Lightkeepers do not all report on the same schedule: over
# the 6.9 days of history in the database the fastest stations report every
# 1.5 h, but Chrome Island and Entrance Island run 16.5 h median and 33 h at
# the 90th percentile. A 24-hour window therefore held ZERO points for those
# two most of the time even though both were reporting normally, and the page
# could only answer "no data from the past 24 hours". 72 h clears the slowest
# observed p90 gap with better than 2x margin.
#
# The cost is payload: 24 h was ~31 KiB and 18 of 23 stations, 72 h is ~96 KiB
# and 20 of 23. A 7-day window would reach 21 of 23 for ~236 KiB, which is not
# worth it on a phone for one more station.
HOURS_BACK = 72


def export_timeseries():
    """Export the recent timeseries for all lightstations."""
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

        logger.info(f"Exporting {HOURS_BACK}h timeseries for {len(stations)} lightstation(s)")

        for station_name in stations:
            # Get all observations for this station inside the window
            cur.execute(
                """
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
            """,
                (station_name, cutoff_time),
            )

            rows = cur.fetchall()

            if not rows:
                continue

            # Initialize timeseries structure
            station_data = {
                "name": station_name,
                "station_name": station_name,
                "region": station_region(station_name, rows[0]["region"]),
                "timeseries": {
                    "wind_speed_kt": [],
                    "wind_direction": [],
                    "sea_height_ft": [],
                    "sea_condition": [],
                    "swell_intensity": [],
                },
            }

            # Build timeseries for each metric
            for row in rows:
                timestamp = datetime.fromtimestamp(row["observation_time"], tz=timezone.utc).isoformat()

                # Wind speed. Calm is a reading of 0 kt, not a missing one: it
                # used to be left out, so the chart drew straight across calm
                # spells and the table showed "—" where the lightkeeper had
                # written CALM. `calm` lets the page say so in words.
                if row["wind_calm"]:
                    station_data["timeseries"]["wind_speed_kt"].append(
                        {"time": timestamp, "value": 0, "gusting": False, "calm": True}
                    )
                elif row["wind_speed_kt"] is not None:
                    station_data["timeseries"]["wind_speed_kt"].append(
                        {"time": timestamp, "value": row["wind_speed_kt"], "gusting": bool(row["wind_gusting"])}
                    )

                # Wind direction (skip if calm or null)
                if not row["wind_calm"] and row["wind_direction"]:
                    station_data["timeseries"]["wind_direction"].append(
                        {"time": timestamp, "value": row["wind_direction"]}
                    )

                # Sea height
                if row["sea_height_ft"] is not None:
                    station_data["timeseries"]["sea_height_ft"].append(
                        {"time": timestamp, "value": row["sea_height_ft"]}
                    )

                # Sea condition
                if row["sea_condition"]:
                    station_data["timeseries"]["sea_condition"].append(
                        {"time": timestamp, "value": row["sea_condition"]}
                    )

                # Swell intensity
                if row["swell_intensity"]:
                    station_data["timeseries"]["swell_intensity"].append(
                        {"time": timestamp, "value": row["swell_intensity"]}
                    )

            output[station_name] = station_data

    # Write to file
    safe_json_write(OUT_PATH, output)
    logger.info(f"✓ Exported timeseries for {len(output)} station(s) to {OUT_PATH}")


def main():
    logger.info(f"=== Exporting Lightstation {HOURS_BACK}h Timeseries ===")
    export_timeseries()
    logger.info("=== Export complete ===")


if __name__ == "__main__":
    main()
