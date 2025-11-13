#!/usr/bin/env python3
"""
Export observed storm surge (observation - prediction) to JSON for hindcast comparison.

Calculates actual storm surge from tide observations and predictions, then exports
to JSON for plotting alongside GDSPS hindcast predictions.

Output: ~/site/data/storm_surge/observed_surge.json
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Shared utilities
from config import TIDE_DATABASE, EXPORT_DIR
from stations import STATIONS

# ---------- Config ----------
OUTPUT_PATH = EXPORT_DIR / "storm_surge" / "observed_surge.json"

# Map tide station keys to storm surge station names (for matching)
TIDE_TO_SURGE_MAP = {
    "point_atkinson": "Point_Atkinson",
    "campbell_river": "Campbell_River",
    "crescent_pile": "Crescent_Beach_Channel"
}

# Number of days back to export
DAYS_BACK = 14


def load_station_metadata():
    """Load station names and metadata from unified station registry."""
    return STATIONS.tides


def fetch_observations(conn, station_id, start_time):
    """Fetch tide observations for a station since start_time."""
    cur = conn.cursor()
    cur.execute("""
        SELECT observation_time, water_level, quality
        FROM tide_observation
        WHERE station_id = ?
        AND observation_time >= ?
        ORDER BY observation_time ASC
    """, (station_id, start_time))

    return cur.fetchall()


def fetch_predictions(conn, station_id, start_time):
    """Fetch tide predictions for a station since start_time."""
    cur = conn.cursor()
    cur.execute("""
        SELECT prediction_time, water_level
        FROM tide_prediction
        WHERE station_id = ?
        AND prediction_time >= ?
        ORDER BY prediction_time ASC
    """, (station_id, start_time))

    return cur.fetchall()


def calculate_observed_surge(observations, predictions):
    """
    Calculate observed storm surge = observation - prediction.

    Returns list of dicts with time, observed_surge, observation, prediction.
    Only includes times where both observation and prediction exist.

    IMPORTANT: Downsamples to 15-minute intervals for efficient JSON output.
    Database stores 5-minute data, but we export at 15-minute intervals to reduce file size.
    """
    # Build prediction lookup (timestamp -> water_level)
    pred_dict = {int(row[0]): float(row[1]) for row in predictions}

    surge_data = []
    for obs_row in observations:
        obs_time = int(obs_row[0])
        obs_level = float(obs_row[1])
        quality = obs_row[2]

        # Downsample: Only include at 15-minute intervals
        # Keep readings at :00, :15, :30, :45
        dt = datetime.fromtimestamp(obs_time, tz=timezone.utc)
        if dt.minute % 15 != 0 or dt.second != 0:
            continue

        # Find matching prediction (exact match or closest within 5 minutes)
        pred_level = None
        for offset in [0, 60, -60, 120, -120, 180, -180, 240, -240, 300, -300]:
            check_time = obs_time + offset
            if check_time in pred_dict:
                pred_level = pred_dict[check_time]
                break

        if pred_level is not None:
            surge = obs_level - pred_level
            surge_data.append({
                "time": dt.isoformat(),
                "observed_surge_m": round(surge, 4),
                "observation_m": round(obs_level, 3),
                "prediction_m": round(pred_level, 3),
                "quality": quality
            })

    return surge_data


def export_observed_surge():
    """Main export function."""
    if not TIDE_DATABASE.exists():
        print(f"ERROR: Database not found: {TIDE_DATABASE}")
        return

    station_metadata = load_station_metadata()
    if not station_metadata:
        return

    conn = sqlite3.connect(TIDE_DATABASE)

    # Calculate start time
    start_time = int((datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).timestamp())

    stations_data = {}

    for tide_key, surge_name in TIDE_TO_SURGE_MAP.items():
        if tide_key not in station_metadata:
            print(f"WARNING: Station {tide_key} not found in metadata")
            continue

        station = station_metadata[tide_key]
        station_id = station["id"]
        station_name = station["name"]

        print(f"Processing {station_name} ({tide_key})...")

        # Fetch observations and predictions
        observations = fetch_observations(conn, station_id, start_time)
        predictions = fetch_predictions(conn, station_id, start_time)

        if not observations:
            print(f"  ⚠️  No observations found")
            continue

        if not predictions:
            print(f"  ⚠️  No predictions found")
            continue

        # Calculate observed surge
        surge_data = calculate_observed_surge(observations, predictions)

        if surge_data:
            stations_data[surge_name] = {
                "station_name": station_name,
                "tide_station_id": tide_key,
                "location": {
                    "lat": station["lat"],
                    "lon": station["lon"]
                },
                "data": surge_data,
                "count": len(surge_data),
                "time_range": {
                    "start": surge_data[0]["time"],
                    "end": surge_data[-1]["time"]
                }
            }
            print(f"  ✅ Exported {len(surge_data)} data points")
        else:
            print(f"  ⚠️  No matching observation/prediction pairs")

    conn.close()

    # Build output JSON
    output = {
        "description": "Observed storm surge calculated from tide observations minus predictions",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "days_back": DAYS_BACK,
        "units": "meters",
        "calculation": "observed_surge = tide_observation - tide_prediction",
        "stations": stations_data
    }

    # Write to file
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Exported observed surge to {OUTPUT_PATH}")
    print(f"   Stations: {len(stations_data)}")


if __name__ == "__main__":
    export_observed_surge()
