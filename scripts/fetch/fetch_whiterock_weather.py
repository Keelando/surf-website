#!/usr/bin/env python3
"""
White Rock East Beach Weather Station - Direct JSON API Integration
Fetches from https://maps.whiterockcity.ca/weather/weatherResults.txt

    fetch_whiterock_weather.py           # fetch, store, export (every 10 min)
    fetch_whiterock_weather.py --probe   # out of service: one look, nothing stored

While the station is out of service (stations.json "out_of_service"), cron
runs only --probe, once a day. It compares the feed with the last stored
reading — the values the sensors froze at — and logs one verdict line:
"STILL DOWN" or "BACK ONLINE?". The second means the sensors are moving
again: check, then remove the flag and restore the 10-minute cron line.
"""

import argparse
import json
import sqlite3
import ssl
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import certifi
import requests
from requests.adapters import HTTPAdapter

from lib.config import EXPORT_DIR, PROJECT_ROOT
from lib.config import WEATHER_DATABASE as SQLITE_PATH
from lib.frozen_sensor import frozen_iso, hidden_columns, whiterock_frozen
from lib.stations import get_wind_station

# Configuration
OUT_PATH = EXPORT_DIR / "whiterock_weather.json"
API_URL = "https://maps.whiterockcity.ca/weather/weatherResults.txt"
LOCKFILE = Path("/tmp/whiterock_weather.lock")
STATION_ID = "whiterock_east"
# Every field that is a measurement, as opposed to identity or metadata.
READING_FIELDS = (
    "temperature",
    "dew_point",
    "humidity",
    "wind_speed",
    "wind_speed_kt",
    "wind_direction",
    "wind_direction_cardinal",
    "wind_gust",
    "wind_gust_kt",
    "wind_gust_direction",
    "wind_gust_direction_cardinal",
    "pressure",
    "heat_index",
    "wind_chill",
    "precipitation",
    "precipitation_rate",
)
# The host's chain ends at a root certifi does not carry yet; see the file.
EXTRA_ROOT = PROJECT_ROOT / "config" / "certs" / "godaddy-tls-root-r1.pem"

# Database setup
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS whiterock_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_time INTEGER NOT NULL,
    temperature REAL,
    dew_point REAL,
    humidity REAL,
    wind_speed REAL,
    wind_direction REAL,
    wind_direction_cardinal TEXT,
    wind_gust REAL,
    wind_gust_direction REAL,
    wind_gust_direction_cardinal TEXT,
    pressure REAL,
    altimeter REAL,
    station_pressure REAL,
    heat_index REAL,
    wind_chill REAL,
    visibility TEXT,
    precipitation REAL,
    precipitation_rate REAL,
    latitude REAL,
    longitude REAL,
    elevation REAL,
    battery REAL,
    recorded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(observation_time)
);
"""


class ExtraRootAdapter(HTTPAdapter):
    """Verify against certifi's roots plus EXTRA_ROOT — added to, not replacing."""

    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        context.load_verify_locations(cafile=str(EXTRA_ROOT))
        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)


def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        if time.time() - LOCKFILE.stat().st_mtime > 300:
            print("⚠️  Removing stale lock file")
            LOCKFILE.unlink()
        else:
            print("⚠️  Another instance is running, exiting")
            return False
    LOCKFILE.touch()
    return True


def release_lock():
    if LOCKFILE.exists():
        LOCKFILE.unlink()


def ensure_db():
    """Create database and table if needed"""
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute(CREATE_TABLE_SQL)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn


def safe_float(value):
    """Safely convert to float, return None if invalid"""
    if value is None or value == "" or value == " ":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_timestamp(ts_str):
    """Parse timestamp like '2025/11/19 01:15:38' to UTC epoch"""
    try:
        # Parse as UTC
        dt = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception as e:
        print(f"⚠️  Failed to parse timestamp '{ts_str}': {e}")
        return int(datetime.now(timezone.utc).timestamp())


def fetch_weather():
    """Fetch weather data from White Rock JSON API"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; SalishSeaWeather/1.0)",
            "pragma": "no-cache",
            "cache-control": "no-cache",
        }
        with requests.Session() as session:
            session.mount("https://", ExtraRootAdapter())
            response = session.get(API_URL, headers=headers, timeout=15)
        response.raise_for_status()

        raw_data = response.json()

        # Extract values from the nested structure
        def get_value(key):
            try:
                return raw_data.get(key, [{}])[0].get("value")
            except (IndexError, KeyError, TypeError):
                return None

        # Parse timestamp
        ts_str = get_value("timestamp")
        observation_time = parse_timestamp(ts_str) if ts_str else int(datetime.now(timezone.utc).timestamp())

        # Build clean data dict
        data = {
            "observation_time": observation_time,
            "temperature": safe_float(get_value("temperature")),
            "dew_point": safe_float(get_value("dewPoint")),
            "humidity": safe_float(get_value("humidity")),
            "wind_speed": safe_float(get_value("windSpeed")),  # Already in knots
            "wind_direction": safe_float(get_value("windDir")),
            "wind_direction_cardinal": get_value("windDirCard"),
            "wind_gust": safe_float(get_value("maxWindSpeed")),  # Peak wind speed
            "wind_gust_direction": safe_float(get_value("maxWindDir")),
            "wind_gust_direction_cardinal": get_value("maxWindDirCard"),
            "pressure": safe_float(get_value("pressure")),  # mbar
            "altimeter": safe_float(get_value("altimeter")),  # inHg
            "station_pressure": safe_float(get_value("stationPressure")),  # inHg
            "heat_index": safe_float(get_value("heatIndex")),
            "wind_chill": safe_float(get_value("windChillIndex")),
            "visibility": get_value("visibility"),
            "precipitation": safe_float(get_value("precip")),
            "precipitation_rate": safe_float(get_value("precipMm")),
            "latitude": safe_float(get_value("lat")),
            "longitude": safe_float(get_value("lon")),
            "elevation": safe_float(get_value("elevation")),
            "battery": safe_float(get_value("battery")),
        }

        # Check if we got any valid data
        has_data = any(v is not None for k, v in data.items() if k not in ["observation_time", "visibility"])

        if has_data:
            return data
        else:
            print("⚠️  No valid weather data in API response")
            return None

    except requests.RequestException as e:
        print(f"❌ Failed to fetch: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON: {e}")
        return None


def insert_weather(conn, data):
    """Insert weather observation into database"""
    cur = conn.cursor()

    cols = [k for k, v in data.items() if v is not None]
    vals = [data[k] for k in cols]

    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT OR IGNORE INTO whiterock_weather ({','.join(cols)}) VALUES ({placeholders})"

    cur.execute(sql, vals)
    conn.commit()

    return cur.rowcount > 0


def export_json(conn):
    """Export latest observation to JSON"""
    cur = conn.cursor()

    cur.execute("""
        SELECT observation_time, temperature, dew_point, humidity,
               wind_speed, wind_direction, wind_direction_cardinal,
               wind_gust, wind_gust_direction, wind_gust_direction_cardinal,
               pressure, heat_index, wind_chill,
               precipitation, precipitation_rate,
               latitude, longitude, elevation
        FROM whiterock_weather
        ORDER BY observation_time DESC
        LIMIT 1
    """)

    row = cur.fetchone()
    if not row:
        print("⚠️  No data in database")
        return

    obs_time = datetime.fromtimestamp(row[0], tz=timezone.utc)
    now = datetime.now(timezone.utc)
    age_minutes = (now.timestamp() - row[0]) / 60

    data = {
        "station_id": STATION_ID,
        "station_name": "White Rock East Beach",
        "location": "East Beach Promenade, White Rock, BC",
        "observation_time": obs_time.isoformat(),
        "age_minutes": round(age_minutes, 1),
        "stale": age_minutes > 15,  # Flag if data is >15 minutes old
        "temperature": row[1],
        "dew_point": row[2],
        "humidity": row[3],
        "wind_speed": row[4],
        # Explicit _kt aliases alongside the bare names. The API documents
        # that every wind speed carries an explicit _kt name; the bare names
        # are retained because the site's own pages read them.
        "wind_speed_kt": row[4],
        "wind_direction": row[5],
        "wind_direction_cardinal": row[6],
        "wind_gust": row[7],
        "wind_gust_kt": row[7],
        "wind_gust_direction": row[8],
        "wind_gust_direction_cardinal": row[9],
        "pressure": row[10],
        "heat_index": row[11],
        "wind_chill": row[12],
        "precipitation": row[13],
        "precipitation_rate": row[14],
        "latitude": row[15],
        "longitude": row[16],
        "elevation": row[17],
        "generated_utc": now.isoformat(),
        "data_source": "White Rock City microWeather Station",
        "units": {"temperature": "°C", "wind_speed": "kt", "pressure": "mbar", "precipitation": "mm"},
    }

    # A stuck sensor keeps publishing one value with a fresh timestamp;
    # withhold it rather than show it, and say which and since when.
    frozen = whiterock_frozen(conn)
    for column in hidden_columns(frozen):
        data[column] = None
    for alias, column in (("wind_speed_kt", "wind_speed"), ("wind_gust_kt", "wind_gust")):
        data[alias] = data[column]
    data["frozen_sensors"] = frozen_iso(frozen)
    if frozen:
        print(f"⚠️  Frozen sensors withheld: {', '.join(sorted(frozen))}")

    # Out of service by the owner's word (stations.json): the feed may still
    # publish, but nothing it says is trusted until the flag is removed.
    out_of_service = (get_wind_station(STATION_ID) or {}).get("out_of_service")
    if out_of_service:
        for field in READING_FIELDS:
            data[field] = None
        data["out_of_service"] = out_of_service
        print(f"⚠️  Out of service since {out_of_service.get('since')}: all readings withheld")

    # Atomic write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(OUT_PATH)

    print(f"✅ Exported to {OUT_PATH}")


# Sensors the probe watches; any of them changing means the station moved.
PROBE_FIELDS = ("wind_speed", "wind_direction", "wind_gust", "humidity", "pressure")


def probe():
    """One look at the feed while out of service; logs a verdict, stores nothing."""
    data = fetch_weather()
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not data:
        print(f"{stamp} PROBE: STILL DOWN (feed unreachable or empty)")
        return 0

    conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True)
    row = conn.execute(
        f"SELECT observation_time, {', '.join(PROBE_FIELDS)} FROM whiterock_weather "
        "ORDER BY observation_time DESC LIMIT 1"
    ).fetchone()
    conn.close()

    feed_time = datetime.fromtimestamp(data["observation_time"], tz=timezone.utc)
    if feed_time < datetime.now(timezone.utc) - timedelta(hours=2):
        print(f"{stamp} PROBE: STILL DOWN (feed last updated {feed_time:%Y-%m-%d %H:%M} UTC)")
        return 0

    stored = dict(zip(PROBE_FIELDS, row[1:])) if row else {}
    moved = [f for f in PROBE_FIELDS if data.get(f) is not None and data.get(f) != stored.get(f)]
    if moved:
        print(
            f"{stamp} PROBE: BACK ONLINE? {', '.join(moved)} changed since the freeze "
            f"({', '.join(f'{f} {stored.get(f)} -> {data.get(f)}' for f in moved)})"
        )
    else:
        print(f"{stamp} PROBE: STILL DOWN (feed publishing, sensors still frozen)")
    return 0


def main():
    parser = argparse.ArgumentParser(description="White Rock East Beach weather")
    parser.add_argument("--probe", action="store_true", help="Out-of-service check: one look, nothing stored")
    args = parser.parse_args()
    if args.probe:
        return probe()

    print("White Rock East Beach Weather Station")
    print("=" * 50)

    if not acquire_lock():
        return 1

    try:
        conn = ensure_db()

        data = fetch_weather()
        if data:
            inserted = insert_weather(conn, data)
            if inserted:
                obs_dt = datetime.fromtimestamp(data["observation_time"], tz=timezone.utc)
                print(f"✅ Inserted observation from {obs_dt.strftime('%Y-%m-%d %H:%M')} UTC")
                print(
                    f"   Temp: {data['temperature']}°C, "
                    f"Wind: {data['wind_speed']} kt, "
                    f"Pressure: {data['pressure']} mbar"
                )
                export_json(conn)
            else:
                print("⏭️  Duplicate observation, skipped")

        conn.close()
        return 0

    finally:
        release_lock()


if __name__ == "__main__":
    exit(main())
