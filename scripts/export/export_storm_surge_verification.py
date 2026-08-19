#!/usr/bin/env python3
"""
Export Storm Surge Forecast Verification Data

Storm surge predictions that were issued 56-79 hours ahead, exported for
comparison against the observed tide offset.

**Verification, not a hindcast** (renamed 2026-08-19). A hindcast re-runs a
model over past dates, usually with better forcing than a live forecast had.
These are forecasts that were genuinely issued in advance and archived at the
time, which is forecast verification. The old name survives only in historical
docs, where it is a record of what the file used to be called.

The lead figure was wrong in three places before the rename, and is worth
stating precisely because two different reference points were being conflated:
`forecast_archive` stores `forecast_run_time` as a bare date, so
`valid_time - forecast_run_time` counts hours from midnight UTC. The archived
run is the 00Z one (see ARCHIVED_RUN_HOUR in scripts/fetch/fetch_storm_surge.py),
so midnight *is* the run instant and those hours are genuine lead times. The
window is 56-79 h, i.e. 2.3-3.3 days ahead — not the "38-61 h" the payload
used to declare, nor the "48 hours" the page used to claim, nor an 18Z run.

Data windows:
- Forecast predictions: 12 days (today + 11 back) - shows predictions FOR these dates
- Requires forecast runs from ~14 days back to capture the full range

See site/docs/VERIFICATION_METHODOLOGY.md for methodology and rationale.

Output: site/data/storm_surge/verification.json
"""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytz

from lib.config import EXPORT_DIR
from lib.config import STORM_SURGE_DATABASE as DB_PATH
from lib.logging_config import setup_logging

logger = setup_logging("storm_surge_verification")

# Configuration

OUTPUT_PATH = EXPORT_DIR / "storm_surge" / "verification.json"
MAX_DAYS_BACK = 12  # Show predictions for last 12 days (today + 11 back)
# Note: Forecast runs go back ~14 days to capture predictions for the full 12-day window

STATIONS = {
    "Point_Atkinson": {
        "name": "Point Atkinson",
        "lat": 49.3375,
        "lon": -123.253583,
    },
    "Crescent_Beach_Channel": {
        "name": "Crescent Beach Channel",
        "lat": 49.0536,
        "lon": -122.8969,
    },
    "Crescent_Channel_Ocean": {
        "name": "Crescent Channel Ocean",
        "lat": 49.0536,
        "lon": -122.8969,
        "reuses": "Crescent_Beach_Channel",
    },
    "Crescent_Beach_Ocean": {
        "name": "Crescent Beach Ocean",
        "lat": 49.0122,
        "lon": -122.9411,
        "reuses": "Crescent_Beach_Channel",
    },
    "Campbell_River": {
        "name": "Campbell River",
        "lat": 50.042,
        "lon": -125.247,
    },
    "Neah_Bay": {
        "name": "Neah Bay",
        "lat": 48.495,
        "lon": -124.728,
    },
    "New_Dungeness": {
        "name": "New Dungeness",
        "lat": 48.333,
        "lon": -123.167,
    },
    "Tofino": {
        "name": "Tofino",
        "lat": 49.154,
        "lon": -125.913,
    },  # Updated to match DFO tide station
}


def export_verification():
    """Export the 56-79 h-ahead predictions, with their observed counterparts, to JSON."""

    if not DB_PATH.exists():
        logger.error(f"Database not found: {DB_PATH}")
        logger.info("Run fetch_storm_surge.py at 19:30 UTC to start collecting data")
        return False

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get available date range
        cur.execute("""
            SELECT 
                MIN(forecast_run_time) as oldest,
                MAX(forecast_run_time) as newest,
                COUNT(DISTINCT forecast_run_time) as days
            FROM forecast_archive
        """)
        stats = cur.fetchone()

        if not stats or stats["days"] == 0:
            logger.warning("No forecast data in database yet")
            logger.info("First data will be available after 19:30 UTC run")
            return False

        logger.info(f"Found {stats['days']} days of forecasts ({stats['oldest']} to {stats['newest']})")

        verification_data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "description": (
                "Storm surge predictions for full Pacific calendar days, as issued "
                "56-79 hours ahead by the 00Z GDSPS run"
            ),
            "forecast_horizon_hours": "56-79",
            "max_days_back": MAX_DAYS_BACK,
            # Filled in after the stations are built, from the runs actually
            # exported. It used to be COUNT(DISTINCT forecast_run_time) over
            # the whole archive, which spans DB_RETENTION_DAYS (30) rather than
            # this 12-day window — so the page rendered the flatly
            # self-contradictory "Historical Days: 28 days (max 12)".
            "actual_days_available": 0,
            "stations": {},
        }

        # Export each station
        for station_id, station_info in STATIONS.items():
            logger.info(f"Processing {station_info['name']}...")

            # Check if this station reuses another station's forecast
            query_station_id = station_info.get("reuses", station_id)

            # Hours 56-79 ahead of the archived 00Z run. forecast_run_time is stored
            # as a bare date, and the archived run is 00Z, so midnight is the run
            # instant and this arithmetic is a true lead time.
            # Goal: Show predictions FOR the last 10 days (today + 9 days back)
            # Since forecasts are ~2 days ahead (38-62 hrs), to get predictions FOR day X,
            # we need forecast runs from ~2 days before day X
            # Example: To show predictions for Nov 14-23 (10 days), we need runs from ~Nov 12-21 (12 days back)

            pacific = pytz.timezone("America/Vancouver")
            now_pacific = datetime.now(pacific)
            # Start of today Pacific
            today_midnight_pacific = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
            # End of today Pacific (23:59:59)
            today_end_pacific = today_midnight_pacific.replace(hour=23, minute=59, second=59)

            # Valid time range: 11 days back to today (show predictions FOR these dates)
            # Extended by 2 days per user request to show more historical forecast data
            valid_start_pacific = today_midnight_pacific - timedelta(days=11)

            # Forecast run range: Need ~2 extra days earlier to capture predictions for the start date
            # Hours 56-79 = 2.33 to 3.29 days ahead, so go back 3 days before valid_start to be safe
            forecast_start_pacific = valid_start_pacific - timedelta(days=3)

            # Convert to UTC for SQL query
            valid_start_utc = valid_start_pacific.astimezone(timezone.utc)
            today_end_utc = today_end_pacific.astimezone(timezone.utc)
            forecast_start_utc = forecast_start_pacific.astimezone(timezone.utc)

            # Use ISO format with 'Z' to match database storage format
            valid_start_str = valid_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            today_end_str = today_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            forecast_start_str = forecast_start_utc.strftime("%Y-%m-%d")

            cur.execute(
                """
                SELECT
                    forecast_run_time,
                    valid_time,
                    surge_value,
                    ROUND((julianday(valid_time) - julianday(forecast_run_time)) * 24, 1) as hours_ahead
                FROM forecast_archive
                WHERE station_id = ?
                  AND hours_ahead BETWEEN 56 AND 79
                  AND forecast_run_time >= ?
                  AND valid_time >= ?
                  AND valid_time <= ?
                ORDER BY valid_time ASC
            """,
                (query_station_id, forecast_start_str, valid_start_str, today_end_str),
            )

            rows = cur.fetchall()

            if not rows:
                logger.warning(f"No 56-79h predictions found for {station_info['name']}")
                continue

            # Build the verification series
            verification_series = []
            for row in rows:
                # Normalize forecast_date to just date (no time) for consistency
                run_time = row["forecast_run_time"]
                normalized_run_time = (
                    run_time.replace("Z", "+00:00") if "T" in run_time else f"{run_time}T00:00:00+00:00"
                )
                forecast_datetime = datetime.fromisoformat(normalized_run_time)
                forecast_date_str = forecast_datetime.strftime("%Y-%m-%d")

                verification_series.append(
                    {
                        "time": row["valid_time"],
                        "value": round(row["surge_value"], 3),
                        "forecast_date": forecast_date_str,
                        "hours_ahead": row["hours_ahead"],
                    }
                )

            # Get time range
            first_time = datetime.fromisoformat(verification_series[0]["time"].replace("Z", "+00:00"))
            last_time = datetime.fromisoformat(verification_series[-1]["time"].replace("Z", "+00:00"))

            logger.info(f"  {len(verification_series)} predictions")
            logger.info(f"  Range: {first_time.strftime('%Y-%m-%d')} to {last_time.strftime('%Y-%m-%d')}")

            # Add to output
            verification_data["stations"][station_id] = {
                "station_id": station_id,
                "station_name": station_info["name"],
                "location": {"lat": station_info["lat"], "lon": station_info["lon"]},
                "verification": verification_series,
            }

        conn.close()

        # Distinct run dates actually exported, so the count and its "max"
        # describe the same window.
        verification_data["actual_days_available"] = len(
            {
                point["forecast_date"]
                for station in verification_data["stations"].values()
                for point in station["verification"]
            }
        )

        # Write to JSON (atomic)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = OUTPUT_PATH.with_suffix(".json.tmp")
        tmp_file.write_text(json.dumps(verification_data, indent=2))
        tmp_file.replace(OUTPUT_PATH)

        logger.info(f"Wrote verification data to {OUTPUT_PATH}")
        logger.info(f"Total stations: {len(verification_data['stations'])}")

        return True

    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        return False


def main():
    logger.info("Storm Surge Forecast Verification Export (56-79 h ahead)")

    success = export_verification()

    if success:
        logger.info("Verification export complete!")
        return 0
    else:
        logger.error("Verification export failed")
        return 1


if __name__ == "__main__":
    exit(main())
