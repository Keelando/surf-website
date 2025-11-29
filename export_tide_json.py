#!/usr/bin/env python3
"""
Export tide data from SQLite to JSON for website display.

Outputs three JSON files:
1. tide-latest.json - Current conditions (observation + prediction)
2. tide-timeseries.json - Calendar day timeseries for charts (15-min intervals)
3. tide-hi-low.json - High/low events for text display

IMPORTANT: Two-stage downsampling strategy (DO NOT REMOVE):
1. Database stores 5-minute intervals (tide_to_sqlite.py)
2. This export further downsamples to 15-minute intervals (downsample_to_15min)
See function documentation for rationale.
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# Shared utilities
from config import TIDE_DATABASE, EXPORT_DIR, safe_json_write
from stations import STATIONS
from logging_config import setup_logging

logger = setup_logging('tide_export')

# Output paths
LATEST_OUT = EXPORT_DIR / "tide-latest.json"
TIMESERIES_OUT = EXPORT_DIR / "tide-timeseries.json"
HIGHLOW_OUT = EXPORT_DIR / "tide-hi-low.json"


def load_station_metadata():
    """Load station names and metadata from unified station registry."""
    return STATIONS.tides


def downsample_to_15min(rows):
    """
    Downsample from 5-minute DB data to 15-minute frontend data.
    Only keeps readings at :00, :15, :30, :45 minutes.

    CRITICAL: DO NOT REMOVE THIS DOWNSAMPLING!
    Two-stage downsampling strategy:
    1. Database: 5-minute intervals (from 1-min API data)
       - 2,016 points per 7 days
       - Good for analysis and storm surge offset calculations
    2. Frontend: 15-minute intervals (from 5-min DB data)
       - 672 points per 7 days
       - Smaller JSON files, faster page loads
       - Still very smooth for tide charts

    Tides change slowly - 15-minute resolution is excellent for visualization.
    """
    downsampled = []
    for row in rows:
        ts = row[0]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if dt.minute in (0, 15, 30, 45) and dt.second == 0:
            downsampled.append(row)
    return downsampled


def export_latest(conn, station_metadata):
    """
    Export current tide conditions - both observation and prediction.
    """
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    now_timestamp = int(now.timestamp())

    latest_data = {}

    # Get all stations from observations
    cur.execute("SELECT DISTINCT station_name, station_id FROM tide_observation")
    stations_obs = {row[0]: row[1] for row in cur.fetchall()}

    # Also get stations from predictions (in case they don't have recent observations)
    cur.execute("SELECT DISTINCT station_name, station_id FROM tide_prediction")
    stations_pred = {row[0]: row[1] for row in cur.fetchall()}

    # Merge station lists
    stations = {**stations_pred, **stations_obs}

    for station_name, station_id in stations.items():
        metadata = station_metadata.get(station_name, {})
        station_data = {
            "station_id": station_id,
            "name": metadata.get("name", station_name.replace("_", " ").title()),
            "location": metadata.get("location", "")
        }

        # Get latest observation
        cur.execute("""
            SELECT observation_time, water_level, quality
            FROM tide_observation
            WHERE station_id = ?
            ORDER BY observation_time DESC
            LIMIT 1
        """, (station_id,))

        obs_row = cur.fetchone()
        if obs_row:
            obs_time, obs_value, obs_quality = obs_row
            age_minutes = (now_timestamp - obs_time) / 60
            station_data["observation"] = {
                "time": datetime.fromtimestamp(obs_time, tz=timezone.utc).isoformat(),
                "value": round(obs_value, 3) if obs_value is not None else None,
                "quality": obs_quality,
                "age_minutes": round(age_minutes, 1),
                "stale": age_minutes > 120
            }

        # Get current prediction (closest to now)
        cur.execute("""
            SELECT prediction_time, water_level
            FROM tide_prediction
            WHERE station_id = ?
              AND prediction_time >= ?
              AND prediction_time <= ?
            ORDER BY ABS(prediction_time - ?)
            LIMIT 1
        """, (station_id, now_timestamp - 1800, now_timestamp + 1800, now_timestamp))

        pred_row = cur.fetchone()
        if pred_row:
            pred_time, pred_value = pred_row

            # Calculate tide trend (rising/falling/slack) by comparing with predictions 15 min before/after
            trend = None
            if pred_value is not None:
                # Get prediction 15 minutes before
                cur.execute("""
                    SELECT water_level FROM tide_prediction
                    WHERE station_id = ? AND prediction_time = ?
                """, (station_id, pred_time - 900))
                before_row = cur.fetchone()

                # Get prediction 15 minutes after
                cur.execute("""
                    SELECT water_level FROM tide_prediction
                    WHERE station_id = ? AND prediction_time = ?
                """, (station_id, pred_time + 900))
                after_row = cur.fetchone()

                if before_row and after_row:
                    before_value, after_value = before_row[0], after_row[0]
                    # Calculate rate of change (m per 15 min)
                    rate = after_value - before_value
                    if abs(rate) < 0.01:  # Less than 1cm change = slack
                        trend = 'slack'
                    elif rate > 0:
                        trend = 'rising'
                    else:
                        trend = 'falling'

            station_data["prediction_now"] = {
                "time": datetime.fromtimestamp(pred_time, tz=timezone.utc).isoformat(),
                "value": round(pred_value, 3) if pred_value is not None else None,
                "trend": trend
            }

        # Only add if we have at least one data point
        if "observation" in station_data or "prediction_now" in station_data:
            latest_data[station_name] = station_data

    output = {
        "_meta": {
            "generated_utc": now.isoformat(),
            "type": "current_conditions"
        },
        "stations": latest_data
    }

    safe_json_write(LATEST_OUT, output, sort_keys=True)
    logger.info(f"Exported latest conditions for {len(latest_data)} stations")
    return len(latest_data)


def export_timeseries(conn, station_metadata):
    """
    Export timeseries data for charts - 3 full calendar days (today, tomorrow, day after).
    Downsampled to 15-minute intervals.
    Includes both predictions and observations separately.
    This allows the frontend to navigate between days for all stations, even those without storm surge data.
    """
    cur = conn.cursor()

    # Get Pacific timezone
    pacific = ZoneInfo('America/Vancouver')
    now_pacific = datetime.now(pacific)

    # Start from today's midnight in Pacific time
    day_start = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)

    # End at end of day after tomorrow (3 full days total)
    day_end = day_start + timedelta(days=3)

    # Add 2-hour padding on both ends for smooth chart display
    query_start = day_start - timedelta(hours=2)
    query_end = day_end + timedelta(hours=2)

    # Convert to UTC timestamps
    start_ts = int(query_start.astimezone(timezone.utc).timestamp())
    end_ts = int(query_end.astimezone(timezone.utc).timestamp())

    # Get all stations from predictions
    cur.execute("SELECT DISTINCT station_name, station_id FROM tide_prediction")
    stations_pred = {row[0]: row[1] for row in cur.fetchall()}

    # Also get stations from observations
    cur.execute("SELECT DISTINCT station_name, station_id FROM tide_observation")
    stations_obs = {row[0]: row[1] for row in cur.fetchall()}

    # Merge station lists
    stations = {**stations_pred, **stations_obs}

    timeseries_data = {}

    for station_name, station_id in stations.items():
        metadata = station_metadata.get(station_name, {})

        station_data = {
            "station_id": station_id,
            "name": metadata.get("name", station_name.replace("_", " ").title()),
            "location": metadata.get("location", ""),
            "predictions": [],
            "observations": []
        }

        # Get predictions
        cur.execute("""
            SELECT prediction_time, water_level
            FROM tide_prediction
            WHERE station_id = ?
              AND prediction_time >= ?
              AND prediction_time <= ?
              AND water_level IS NOT NULL
            ORDER BY prediction_time ASC
        """, (station_id, start_ts, end_ts))

        pred_rows = cur.fetchall()
        pred_downsampled = downsample_to_15min(pred_rows)

        for ts, val in pred_downsampled:
            station_data["predictions"].append({
                "time": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "value": round(val, 3)
            })

        # Get observations
        cur.execute("""
            SELECT observation_time, water_level, quality
            FROM tide_observation
            WHERE station_id = ?
              AND observation_time >= ?
              AND observation_time <= ?
              AND water_level IS NOT NULL
            ORDER BY observation_time ASC
        """, (station_id, start_ts, end_ts))

        obs_rows = cur.fetchall()
        obs_downsampled = downsample_to_15min(obs_rows)

        for ts, val, quality in obs_downsampled:
            station_data["observations"].append({
                "time": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "value": round(val, 3),
                "quality": quality
            })

        # Add station if it has any data
        if station_data["predictions"] or station_data["observations"]:
            station_data["has_observations"] = len(station_data["observations"]) > 0
            timeseries_data[station_name] = station_data
            pred_orig = len(pred_rows)
            pred_down = len(station_data["predictions"])
            obs_orig = len(obs_rows)
            obs_down = len(station_data["observations"])
            logger.info(f"  {station_name}: {pred_down} predictions (from {pred_orig}), {obs_down} observations (from {obs_orig})")

    output = {
        "_meta": {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "type": "timeseries",
            "start_day": day_start.strftime("%Y-%m-%d"),
            "end_day": (day_end - timedelta(days=1)).strftime("%Y-%m-%d"),
            "days_included": 3,
            "query_start": query_start.isoformat(),
            "query_end": query_end.isoformat(),
            "timezone": "America/Vancouver",
            "padding_hours": 2,
            "interval": "15 minutes"
        },
        "stations": timeseries_data
    }

    safe_json_write(TIMESERIES_OUT, output, sort_keys=True)
    logger.info(f"Exported timeseries for {len(timeseries_data)} stations (15-min intervals)")
    return len(timeseries_data)


def export_highlow(conn, station_metadata):
    """
    Export high/low tide events for text display.
    Uses tide_highlow table.
    Exports 3 full calendar days: today, tomorrow, and day after tomorrow.
    """
    cur = conn.cursor()

    pacific = ZoneInfo('America/Vancouver')
    now_pacific = datetime.now(pacific)

    # Start from beginning of today (Pacific)
    today_start = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    # End at end of day after tomorrow (3 full days)
    query_start = today_start
    query_end = today_start + timedelta(days=3)

    # Convert to UTC timestamps
    start_ts = int(query_start.astimezone(timezone.utc).timestamp())
    end_ts = int(query_end.astimezone(timezone.utc).timestamp())

    # Get all stations from highlow table
    cur.execute("SELECT DISTINCT station_name, station_id FROM tide_highlow")
    stations = cur.fetchall()

    highlow_data = {}

    for station_name, station_id in stations:
        metadata = station_metadata.get(station_name, {})

        # Get high/low events from tide_highlow table
        cur.execute("""
            SELECT event_time, water_level, event_type
            FROM tide_highlow
            WHERE station_id = ?
              AND event_time >= ?
              AND event_time <= ?
              AND water_level IS NOT NULL
            ORDER BY event_time ASC
        """, (station_id, start_ts, end_ts))

        hilo_rows = cur.fetchall()

        if hilo_rows:
            events = []
            for ts, val, event_type in hilo_rows:
                dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
                dt_pacific = dt_utc.astimezone(pacific)

                events.append({
                    "time": dt_utc.isoformat(),
                    "time_pacific": dt_pacific.isoformat(),
                    "type": event_type,
                    "value": round(val, 3),
                    "time_display": dt_pacific.strftime("%I:%M %p").lstrip('0'),
                    "date": dt_pacific.strftime("%Y-%m-%d")
                })

            highlow_data[station_name] = {
                "station_id": station_id,
                "name": metadata.get("name", station_name.replace("_", " ").title()),
                "location": metadata.get("location", ""),
                "events": events
            }
            logger.info(f"  {station_name}: {len(events)} high/low events")

    output = {
        "_meta": {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "type": "highlow_events",
            "query_start": query_start.isoformat(),
            "query_end": query_end.isoformat(),
            "timezone": "America/Vancouver",
            "window": "3 full calendar days (today, tomorrow, day after tomorrow)",
            "days_included": 3
        },
        "stations": highlow_data
    }

    safe_json_write(HIGHLOW_OUT, output, sort_keys=True)
    logger.info(f"Exported high/low events for {len(highlow_data)} stations")
    return len(highlow_data)


def main():
    if not TIDE_DATABASE.exists():
        logger.error(f"Database not found: {TIDE_DATABASE}")
        return 1

    station_metadata = load_station_metadata()

    logger.info("Tide Data Export")
    logger.info("=" * 70)

    try:
        with sqlite3.connect(TIDE_DATABASE, timeout=10) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")

            export_latest(conn, station_metadata)
            export_timeseries(conn, station_metadata)
            export_highlow(conn, station_metadata)

        logger.info("=" * 70)
        logger.info("Export complete!")
        return 0

    except sqlite3.OperationalError as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
