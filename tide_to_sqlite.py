#!/usr/bin/env python3
"""
Fetch recent tide data (observations or predictions) from DFO IWLS API
and store to local SQLite. Outputs latest snapshot JSON for website.

Automatically:
  • uses 'wlo' when available (observations)
  • falls back to 'wlp' for predictions
  • uses 'wlp-hilo' for New Westminster (high/low tides only)
"""

import requests
import sqlite3
import datetime
import json
import time
from pathlib import Path

DB_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
OUT_PATH = Path("~/site/data/latest_tide_v2.json").expanduser()
STATION_FILE = Path("~/envcan_wave/tide_stations.json").expanduser()
BASE_URL = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations"
HEADERS = {"User-Agent": "keelan_w@hotmail.com"}


def load_stations():
    """Load tide stations from external JSON file."""
    if not STATION_FILE.exists():
        raise FileNotFoundError(f"Missing {STATION_FILE}")
    with open(STATION_FILE, "r") as f:
        data = json.load(f)
    return {k: v["id"] for k, v in data.items()}


def ensure_db():
    """Create or update database schema safely."""
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
    """Make HTTP request with error handling."""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  ❌  {e}")
        return None


def detect_available_codes(station_id):
    """Query station metadata to find available time series codes."""
    meta = safe_get(f"{BASE_URL}/{station_id}")
    if not meta or "timeSeries" not in meta:
        return []
    return [ts["code"] for ts in meta.get("timeSeries", [])]


def insert_rows(cur, station_key, station_id, series_code, data):
    """Insert tide data rows, skipping duplicates."""
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
            print(f"    ⚠️  skipped bad row: {e}")
    return added


def main():
    start_time = time.time()
    conn = ensure_db()
    cur = conn.cursor()

    now = datetime.datetime.now(datetime.timezone.utc)
    start = now - datetime.timedelta(hours=2)

    # Load stations dynamically
    TIDE_STATIONS = load_stations()

    total_added = 0
    latest_json = {}

    print(f"🌊 Fetching tide data from {len(TIDE_STATIONS)} stations...")
    print("=" * 70)

    for key, sid in TIDE_STATIONS.items():
        print(f"→ {key}")

        codes = detect_available_codes(sid)
        if not codes:
            print(f"  ⚠️  no timeSeries metadata")
            continue

        if "wlo" in codes:
            series = "wlo"
        elif key == "new_westminster" and "wlp-hilo" in codes:
            series = "wlp-hilo"
        elif "wlp" in codes:
            series = "wlp"
        else:
            print(f"  ⚠️  no usable series (available: {codes})")
            continue

        url = f"{BASE_URL}/{sid}/data"
        params = {
            "time-series-code": series,
            "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        data = safe_get(url, params)
        if not data:
            print(f"  ❌  no response")
            continue

        values = data if isinstance(data, list) else data.get("values", [])
        if not values:
            print(f"  ⚠️  empty dataset")
            continue

        added = insert_rows(cur, key, sid, series, values)
        total_added += added
        print(f"  ✅  added {added} rows (series: {series})")

        if values:
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

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps({
        "_meta": {
            "generated_utc": now.isoformat(),
            "freshness_window_h": 2
        },
        "stations": latest_json
    }, indent=2))

    elapsed = time.time() - start_time
    print("=" * 70)
    print(f"🟢  Wrote latest JSON → {OUT_PATH}")
    print(f"🏁  Done in {elapsed:.1f}s — total {total_added} new rows")


if __name__ == "__main__":
    main()
