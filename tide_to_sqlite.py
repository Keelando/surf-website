#!/usr/bin/env python3
"""
Fetch recent tide data from DFO IWLS API and store to local SQLite.

Uses separate database (tide_data.sqlite) with three tables:
- tide_observation: Real-time water levels (wlo series)
- tide_prediction: Astronomical forecasts (wlp series)
- tide_highlow: High/low events (wlp-hilo series)
"""

import requests
import sqlite3
import datetime
import json
import time
import argparse
from pathlib import Path

DB_PATH = Path("~/.local/share/tide_data.sqlite").expanduser()
STATION_FILE = Path("~/envcan_wave/tide_stations.json").expanduser()
BASE_URL = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations"
HEADERS = {"User-Agent": "keelan_w@hotmail.com"}

def load_stations():
    if not STATION_FILE.exists():
        raise FileNotFoundError(f"Missing {STATION_FILE}")
    with open(STATION_FILE, "r") as f:
        data = json.load(f)
    return {k: v["id"] for k, v in data.items()}

def ensure_db():
    """Create database and tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create tide_observation table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_observation (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            observation_time INTEGER NOT NULL,
            water_level REAL,
            quality TEXT,
            recorded_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, observation_time)
        )
    """)

    # Create tide_prediction table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_prediction (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            prediction_time INTEGER NOT NULL,
            water_level REAL,
            recorded_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, prediction_time)
        )
    """)

    # Create tide_highlow table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_highlow (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            event_time INTEGER NOT NULL,
            water_level REAL,
            event_type TEXT,
            recorded_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, event_time)
        )
    """)

    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn

def safe_get(url, params=None):
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return None

def detect_available_codes(station_id):
    meta = safe_get(f"{BASE_URL}/{station_id}")
    if not meta or "timeSeries" not in meta:
        return []
    return [ts["code"] for ts in meta.get("timeSeries", [])]

def insert_observations(cur, station_key, station_id, data):
    """Insert observation data (wlo series) into tide_observation table."""
    added = 0
    for row in data:
        try:
            ts = datetime.datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = row.get("value")
            qc = row.get("qcFlagCode")
            cur.execute("""
                INSERT OR IGNORE INTO tide_observation
                (station_id, station_name, observation_time, water_level, quality)
                VALUES (?, ?, ?, ?, ?)
            """, (station_id, station_key, timestamp, water_level, qc))
            if cur.rowcount > 0:
                added += 1
        except Exception as e:
            print(f"    WARNING: skipped bad observation row: {e}")
    return added

def insert_predictions(cur, station_key, station_id, data):
    """Insert prediction data (wlp series) into tide_prediction table."""
    added = 0
    for row in data:
        try:
            ts = datetime.datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = row.get("value")
            cur.execute("""
                INSERT OR IGNORE INTO tide_prediction
                (station_id, station_name, prediction_time, water_level)
                VALUES (?, ?, ?, ?)
            """, (station_id, station_key, timestamp, water_level))
            if cur.rowcount > 0:
                added += 1
        except Exception as e:
            print(f"    WARNING: skipped bad prediction row: {e}")
    return added

def insert_highlow(cur, station_key, station_id, data):
    """Insert high/low events (wlp-hilo series) into tide_highlow table."""
    added = 0

    # Need at least 2 points to determine type
    if len(data) < 2:
        return 0

    for i, row in enumerate(data):
        try:
            ts = datetime.datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = row.get("value")

            # Determine event type by comparing with neighbors
            event_type = "unknown"
            if i > 0 and i < len(data) - 1:
                prev_val = data[i-1].get("value")
                next_val = data[i+1].get("value")
                if water_level > prev_val and water_level > next_val:
                    event_type = "high"
                elif water_level < prev_val and water_level < next_val:
                    event_type = "low"
            elif i == 0 and len(data) > 1:
                next_val = data[i+1].get("value")
                event_type = "high" if water_level > next_val else "low"
            elif i == len(data) - 1 and len(data) > 1:
                prev_val = data[i-1].get("value")
                event_type = "high" if water_level > prev_val else "low"

            cur.execute("""
                INSERT OR IGNORE INTO tide_highlow
                (station_id, station_name, event_time, water_level, event_type)
                VALUES (?, ?, ?, ?, ?)
            """, (station_id, station_key, timestamp, water_level, event_type))
            if cur.rowcount > 0:
                added += 1
        except Exception as e:
            print(f"    WARNING: skipped bad high/low row: {e}")
    return added

def main():
    parser = argparse.ArgumentParser(description="Fetch tide data from DFO IWLS API")
    parser.add_argument('--observations', action='store_true',
                       help='Fetch wlo observations (last 2 hours)')
    parser.add_argument('--predictions', action='store_true',
                       help='Fetch wlp predictions (48-hour window)')
    parser.add_argument('--highlow', action='store_true',
                       help='Fetch wlp-hilo events (48-hour window)')
    parser.add_argument('--all', action='store_true',
                       help='Fetch all data types')
    args = parser.parse_args()

    if not (args.observations or args.predictions or args.highlow or args.all):
        args.all = True

    start_time = time.time()
    conn = ensure_db()
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    TIDE_STATIONS = load_stations()
    total_added = 0

    print(f"Fetching tide data from {len(TIDE_STATIONS)} stations...")
    if args.observations:
        print("Mode: Observations only (wlo)")
    elif args.predictions:
        print("Mode: Predictions only (wlp)")
    elif args.highlow:
        print("Mode: High/low events only (wlp-hilo)")
    else:
        print("Mode: All data types")
    print("=" * 70)

    for key, sid in TIDE_STATIONS.items():
        print(f"Station: {key} ({sid})")
        codes = detect_available_codes(sid)
        if not codes:
            print("  WARNING: no timeSeries metadata")
            continue

        # Fetch observations (wlo) - last 2 hours
        if args.observations or args.all:
            if "wlo" in codes:
                start = now - datetime.timedelta(hours=2)
                url = f"{BASE_URL}/{sid}/data"
                params = {
                    "time-series-code": "wlo",
                    "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to": now.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                data = safe_get(url, params)
                if data:
                    values = data if isinstance(data, list) else data.get("values", [])
                    if values:
                        added = insert_observations(cur, key, sid, values)
                        total_added += added
                        print(f"  OK: observations added {added} rows")

        # Fetch predictions (wlp) - 48-hour window
        if args.predictions or args.all:
            if "wlp" in codes:
                start = now - datetime.timedelta(hours=12)
                end = now + datetime.timedelta(hours=36)
                url = f"{BASE_URL}/{sid}/data"
                params = {
                    "time-series-code": "wlp",
                    "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to": end.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                data = safe_get(url, params)
                if data:
                    values = data if isinstance(data, list) else data.get("values", [])
                    if values:
                        added = insert_predictions(cur, key, sid, values)
                        total_added += added
                        print(f"  OK: predictions added {added} rows")

        # Fetch high/low events (wlp-hilo) - 48-hour window
        if args.highlow or args.all:
            if "wlp-hilo" in codes:
                start = now - datetime.timedelta(hours=12)
                end = now + datetime.timedelta(hours=36)
                url = f"{BASE_URL}/{sid}/data"
                params = {
                    "time-series-code": "wlp-hilo",
                    "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to": end.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                data = safe_get(url, params)
                if data:
                    values = data if isinstance(data, list) else data.get("values", [])
                    if values:
                        added = insert_highlow(cur, key, sid, values)
                        total_added += added
                        print(f"  OK: high/low events added {added} rows")

        time.sleep(2.1)  # Rate limiting

    conn.commit()
    conn.close()

    elapsed = time.time() - start_time
    print("=" * 70)
    print(f"Done in {elapsed:.1f}s - total {total_added} new rows")

if __name__ == "__main__":
    main()
