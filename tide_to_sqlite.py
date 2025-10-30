#!/usr/bin/env python3
"""
Fetch recent tide data from DFO IWLS API and store to local SQLite.
"""

import requests
import sqlite3
import datetime
import json
import time
import argparse
from pathlib import Path

DB_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
OUT_PATH = Path("~/site/data/latest_tide_v2.json").expanduser()
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
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_observation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            timestamp_utc INTEGER NOT NULL,
            water_level REAL,
            quality TEXT,
            series_code TEXT,
            recorded_at TEXT DEFAULT (datetime('now'))
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_tide_reading
        ON tide_observation(station_id, timestamp_utc)
    """)
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

def insert_observation_rows(cur, station_key, station_id, series_code, data):
    added = 0
    for row in data:
        try:
            ts = datetime.datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = row.get("value")
            qc = row.get("qcFlagCode")
            cur.execute("""
                INSERT OR IGNORE INTO tide_observation 
                (station_id, station_name, timestamp_utc, water_level, quality, series_code)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (station_id, station_key, timestamp, water_level, qc, series_code))
            if cur.rowcount > 0:
                added += 1
        except Exception as e:
            print(f"    WARNING: skipped bad row: {e}")
    return added

def main():
    parser = argparse.ArgumentParser(description="Fetch tide data from DFO IWLS API")
    parser.add_argument('--observations', action='store_true', 
                       help='Fetch wlo observations (last 2 hours)')
    parser.add_argument('--predictions', action='store_true',
                       help='Fetch wlp/wlp-hilo predictions (24 hour window)')
    parser.add_argument('--all', action='store_true',
                       help='Fetch both observations and predictions')
    args = parser.parse_args()
    
    if not (args.observations or args.predictions or args.all):
        args.all = True
    
    start_time = time.time()
    conn = ensure_db()
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    TIDE_STATIONS = load_stations()
    total_added = 0
    latest_json = {}

    print(f"Fetching tide data from {len(TIDE_STATIONS)} stations...")
    if args.observations:
        print("Mode: Observations only (wlo)")
    elif args.predictions:
        print("Mode: Predictions only (wlp/wlp-hilo)")
    else:
        print("Mode: All data types")
    print("=" * 70)

    for key, sid in TIDE_STATIONS.items():
        print(f"Station: {key}")
        codes = detect_available_codes(sid)
        if not codes:
            print("  WARNING: no timeSeries metadata")
            continue

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
                        added = insert_observation_rows(cur, key, sid, "wlo", values)
                        total_added += added
                        print(f"  OK: observations added {added} rows")
                        last = values[-1]
                        latest_json[key] = {
                            "station_id": sid,
                            "series": "wlo",
                            "timestamp": last.get("eventDate"),
                            "water_level": last.get("value"),
                            "quality": last.get("qcFlagCode")
                        }

        if args.predictions or args.all:
            # Prefer wlp-hilo (high/low events) over wlp (continuous predictions)
            if "wlp-hilo" in codes:
                series = "wlp-hilo"
            elif "wlp" in codes:
                series = "wlp"
            else:
                if not (args.observations or args.all):
                    print("  WARNING: no prediction series available")
                continue
            
            start = now - datetime.timedelta(hours=12)
            end = now + datetime.timedelta(hours=12)
            url = f"{BASE_URL}/{sid}/data"
            params = {
                "time-series-code": series,
                "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": end.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            data = safe_get(url, params)
            if data:
                values = data if isinstance(data, list) else data.get("values", [])
                if values:
                    added = insert_observation_rows(cur, key, sid, series, values)
                    total_added += added
                    print(f"  OK: predictions ({series}) added {added} rows")
                    if key not in latest_json:
                        last = values[-1]
                        latest_json[key] = {
                            "station_id": sid,
                            "series": series,
                            "timestamp": last.get("eventDate"),
                            "water_level": last.get("value"),
                            "quality": last.get("qcFlagCode")
                        }
        time.sleep(2.1)

    conn.commit()
    conn.close()

    if args.observations or args.all:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(json.dumps({
            "_meta": {
                "generated_utc": now.isoformat(),
                "freshness_window_h": 2
            },
            "stations": latest_json
        }, indent=2))
        print(f"Wrote latest JSON to {OUT_PATH}")

    elapsed = time.time() - start_time
    print("=" * 70)
    print(f"Done in {elapsed:.1f}s - total {total_added} new rows")

if __name__ == "__main__":
    main()
