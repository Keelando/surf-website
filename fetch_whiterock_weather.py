#!/usr/bin/env python3
"""
White Rock Pier Weather Station - Direct JSON API Integration
Fetches from https://maps.whiterockcity.ca/weather/weatherResults.txt
"""

import requests
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
import time

# Configuration
SQLITE_PATH = Path("~/.local/share/weather_data.sqlite").expanduser()
OUT_PATH = Path("~/site/data/whiterock_weather.json").expanduser()
API_URL = "https://maps.whiterockcity.ca/weather/weatherResults.txt"
LOCKFILE = Path("/tmp/whiterock_weather.lock")

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
            'User-Agent': 'Mozilla/5.0 (compatible; SalishSeaWeather/1.0)',
            'pragma': 'no-cache',
            'cache-control': 'no-cache'
        }
        response = requests.get(API_URL, headers=headers, timeout=15)
        response.raise_for_status()
        
        raw_data = response.json()
        
        # Extract values from the nested structure
        def get_value(key):
            try:
                return raw_data.get(key, [{}])[0].get('value')
            except (IndexError, KeyError, TypeError):
                return None
        
        # Parse timestamp
        ts_str = get_value('timestamp')
        observation_time = parse_timestamp(ts_str) if ts_str else int(datetime.now(timezone.utc).timestamp())
        
        # Build clean data dict
        data = {
            'observation_time': observation_time,
            'temperature': safe_float(get_value('temperature')),
            'dew_point': safe_float(get_value('dewPoint')),
            'humidity': safe_float(get_value('humidity')),
            'wind_speed': safe_float(get_value('windSpeed')),  # Already in knots
            'wind_direction': safe_float(get_value('windDir')),
            'wind_direction_cardinal': get_value('windDirCard'),
            'wind_gust': safe_float(get_value('maxWindSpeed')),  # Peak wind speed
            'wind_gust_direction': safe_float(get_value('maxWindDir')),
            'wind_gust_direction_cardinal': get_value('maxWindDirCard'),
            'pressure': safe_float(get_value('pressure')),  # mbar
            'altimeter': safe_float(get_value('altimeter')),  # inHg
            'station_pressure': safe_float(get_value('stationPressure')),  # inHg
            'heat_index': safe_float(get_value('heatIndex')),
            'wind_chill': safe_float(get_value('windChillIndex')),
            'visibility': get_value('visibility'),
            'precipitation': safe_float(get_value('precip')),
            'precipitation_rate': safe_float(get_value('precipMm')),
            'latitude': safe_float(get_value('lat')),
            'longitude': safe_float(get_value('lon')),
            'elevation': safe_float(get_value('elevation')),
            'battery': safe_float(get_value('battery'))
        }
        
        # Check if we got any valid data
        has_data = any(v is not None for k, v in data.items() if k not in ['observation_time', 'visibility'])
        
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
    
    placeholders = ','.join('?' * len(cols))
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
        "station_id": "whiterock_pier",
        "station_name": "White Rock Pier",
        "location": "East Beach, White Rock, BC",
        "observation_time": obs_time.isoformat(),
        "age_minutes": round(age_minutes, 1),
        "stale": age_minutes > 15,  # Flag if data is >15 minutes old
        "temperature": row[1],
        "dew_point": row[2],
        "humidity": row[3],
        "wind_speed": row[4],
        "wind_direction": row[5],
        "wind_direction_cardinal": row[6],
        "wind_gust": row[7],
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
        "units": {
            "temperature": "°C",
            "wind_speed": "kt",
            "pressure": "mbar",
            "precipitation": "mm"
        }
    }
    
    # Atomic write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(OUT_PATH)
    
    print(f"✅ Exported to {OUT_PATH}")

def main():
    print("White Rock Pier Weather Station")
    print("=" * 50)
    
    if not acquire_lock():
        return 1
    
    try:
        conn = ensure_db()
        
        data = fetch_weather()
        if data:
            inserted = insert_weather(conn, data)
            if inserted:
                obs_dt = datetime.fromtimestamp(data['observation_time'], tz=timezone.utc)
                print(f"✅ Inserted observation from {obs_dt.strftime('%Y-%m-%d %H:%M')} UTC")
                print(f"   Temp: {data['temperature']}°C, Wind: {data['wind_speed']} kt, Pressure: {data['pressure']} mbar")
                export_json(conn)
            else:
                print("⏭️  Duplicate observation, skipped")
        
        conn.close()
        return 0
        
    finally:
        release_lock()

if __name__ == "__main__":
    exit(main())
