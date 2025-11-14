#!/usr/bin/env python3
"""
Export Storm Surge Hindcast Data
Exports 38-61 hour predictions for charting (full Pacific calendar day predicted 2 days in advance)
"""
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone, timedelta
from logging_config import setup_logging

logger = setup_logging('hindcast_export')

# Configuration
DB_PATH = Path("~/.local/share/storm_surge_forecast.sqlite").expanduser()
OUTPUT_PATH = Path("~/site/data/storm_surge/hindcast.json").expanduser()
MAX_DAYS_BACK = 10

STATIONS = {
    "Point_Atkinson": {"name": "Point Atkinson", "lat": 49.3375, "lon": -123.253583},
    "Crescent_Beach_Channel": {"name": "Crescent Beach Channel", "lat": 49.0536, "lon": -122.8969},
    "Campbell_River": {"name": "Campbell River", "lat": 50.042, "lon": -125.247},
    "Neah_Bay": {"name": "Neah Bay", "lat": 48.495, "lon": -124.728},
    "New_Dungeness": {"name": "New Dungeness", "lat": 48.333, "lon": -123.167},
    "Tofino": {"name": "Tofino", "lat": 49.154, "lon": -125.913}  # Updated to match DFO tide station
}

def export_hindcast():
    """Export +48h hindcast predictions to JSON."""

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
        
        hindcast_data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "description": "Storm surge predictions for full Pacific calendar days made 2 days in advance (18Z run, hours 38-61 PST)",
            "forecast_horizon_hours": "38-61",
            "max_days_back": MAX_DAYS_BACK,
            "actual_days_available": stats["days"],
            "stations": {}
        }
        
        # Export each station
        for station_id, station_info in STATIONS.items():
            logger.info(f"Processing {station_info['name']}...")

            # Query: Get all forecasts and filter for hours 38-61 FROM 18Z RUN
            # 18Z run on Nov 7 → hours 38-61 = all of Nov 9 PST (00:00-23:00 Pacific, 2 days ahead)
            # Since forecast_run_time is stored as date-only, we must add 18 hours to account for 18Z
            # Hours 38-61 from 18Z = Hours 56-79 from midnight = (38+18) to (61+18)
            cur.execute("""
                SELECT
                    forecast_run_time,
                    valid_time,
                    surge_value,
                    ROUND((julianday(valid_time) - julianday(forecast_run_time)) * 24, 1) as hours_ahead
                FROM forecast_archive
                WHERE station_id = ?
                  AND hours_ahead BETWEEN 56 AND 79
                ORDER BY valid_time ASC
            """, (station_id,))
            
            rows = cur.fetchall()

            if not rows:
                logger.warning(f"No 38-61h predictions found for {station_info['name']}")
                continue
            
            # Build hindcast series
            hindcast_series = []
            for row in rows:
                # Normalize forecast_date to just date (no time) for consistency
                forecast_datetime = datetime.fromisoformat(row["forecast_run_time"].replace('Z', '+00:00') if 'T' in row["forecast_run_time"] else row["forecast_run_time"] + 'T00:00:00+00:00')
                forecast_date_str = forecast_datetime.strftime('%Y-%m-%d')

                hindcast_series.append({
                    "time": row["valid_time"],
                    "value": round(row["surge_value"], 3),
                    "forecast_date": forecast_date_str,
                    "hours_ahead": row["hours_ahead"]
                })
            
            # Get time range
            first_time = datetime.fromisoformat(hindcast_series[0]["time"].replace('Z', '+00:00'))
            last_time = datetime.fromisoformat(hindcast_series[-1]["time"].replace('Z', '+00:00'))

            logger.info(f"  {len(hindcast_series)} predictions")
            logger.info(f"  Range: {first_time.strftime('%Y-%m-%d')} to {last_time.strftime('%Y-%m-%d')}")
            
            # Add to output
            hindcast_data["stations"][station_id] = {
                "station_id": station_id,
                "station_name": station_info["name"],
                "location": {
                    "lat": station_info["lat"],
                    "lon": station_info["lon"]
                },
                "hindcast": hindcast_series
            }
        
        conn.close()
        
        # Write to JSON (atomic)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = OUTPUT_PATH.with_suffix(".json.tmp")
        tmp_file.write_text(json.dumps(hindcast_data, indent=2))
        tmp_file.replace(OUTPUT_PATH)

        logger.info(f"Wrote hindcast data to {OUTPUT_PATH}")
        logger.info(f"Total stations: {len(hindcast_data['stations'])}")

        return True

    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        return False


def main():
    logger.info("Storm Surge Hindcast Export (38-61h / 2-day ahead Pacific)")

    success = export_hindcast()

    if success:
        logger.info("Hindcast export complete!")
        return 0
    else:
        logger.error("Hindcast export failed")
        return 1


if __name__ == "__main__":
    exit(main())