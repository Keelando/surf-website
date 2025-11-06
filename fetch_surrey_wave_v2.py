#!/usr/bin/env python3
"""
Fetch Surrey FlowWorks wave/wind data (API v2) and store to SQLite.
Based on v1 implementation - migrated to v2 API with JWT auth.

Stations:
- Crescent Pile (20182): Full wave + wind + temp
- Crescent Channel (20183): Wind + radar wave + temp
- Colebrook (18507): Wind + temp only
"""

import requests
import sqlite3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import time

# ---- Configuration ----
API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()

# Station configuration (from v1)
STATIONS = {
    "crescentpile": {
        "site_id": 20182,
        "name": "Crescent Pile",
        "buoy_id": "CRPILE",  # Short ID for database
        "channels": {
            "wind_speed": 1810,
            "wind_direction": 1811,
            "wind_gust": 1814,
            "wave_height_sig": 2002,      # Hs_Anderra
            "wave_height_peak": 2008,     # Hmax_Anderra
            "wave_period_avg": 2009,      # Tmean_Anderra
            "wave_period_peak": 2012,     # Tpeak_Anderra
            "sea_temp": 2007,             # Temperature_Anderra
            "air_temp": 1794,             # PTemp
        }
    },
    "crescentchannel": {
        "site_id": 20183,
        "name": "Crescent Channel",
        "buoy_id": "CRCHAN",
        "channels": {
            "wind_speed": 1837,
            "wind_direction": 1838,
            "wind_gust": 1841,
            "wave_height_sig": 2155,      # Hm0_Radar
            "air_temp": 1821,
        }
    },
    "colebrook": {
        "site_id": 18507,
        "name": "Colebrook",
        "buoy_id": "COLEB",
        "channels": {
            "wind_speed": 1425,
            "wind_direction": 1426,
            "wind_gust": 1427,
            "air_temp": 1439,
        }
    }
}


class FlowWorksAPI:
    def __init__(self, username, password):
        self.base_url = API_BASE
        self.username = username
        self.password = password
        self.token = None
        self.token_expiry = None
        
    def authenticate(self):
        """Get JWT token from FlowWorks API v2."""
        url = f"{self.base_url}/authenticate"
        payload = {"username": self.username, "password": self.password}

        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()

            # Handle actual API response format: Token and Expires (capital T)
            if "Token" in data:
                self.token = data.get("Token")
                expires_str = data.get("Expires")

                if expires_str:
                    self.token_expiry = datetime.fromisoformat(
                        expires_str.replace("Z", "+00:00")
                    )

                print(f"✅ Authenticated - expires {self.token_expiry}")
                return True
            else:
                print(f"❌ Auth failed: {data}")
                return False

        except Exception as e:
            print(f"❌ Auth error: {e}")
            return False
    
    def _ensure_token(self):
        """Ensure valid token exists."""
        if not self.token or not self.token_expiry:
            return self.authenticate()
        
        # Refresh if expires in <5 min
        if datetime.now(timezone.utc) >= self.token_expiry - timedelta(minutes=5):
            return self.authenticate()
        
        return True
    
    def _get_headers(self):
        """Get auth headers."""
        if not self._ensure_token():
            raise Exception("Failed to get valid token")
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def get_channel_data(self, site_id, channel_id, hours=2):
        """Fetch data from specific channel (last N hours)."""
        url = f"{self.base_url}/sites/{site_id}/channels/{channel_id}/data"

        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)

        params = {
            "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
        }

        try:
            response = requests.get(
                url, headers=self._get_headers(), params=params, timeout=15
            )
            response.raise_for_status()
            data = response.json()

            # API returns Resources array directly
            if data.get("ResultCode") == 0:
                return data.get("Resources", [])
            else:
                print(f"⚠️  API error: {data.get('ResultMessage')}")
                return []

        except Exception as e:
            print(f"❌ Fetch error: {e}")
            return []


def ensure_columns(conn):
    """Ensure all columns exist in buoy_observation table."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing = {row[1] for row in cur.fetchall()}
    
    # All possible columns from Surrey stations
    required = {
        "wave_height_sig", "wave_height_peak",
        "wave_period_avg", "wave_period_peak",
        "wind_speed", "wind_gust", "wind_direction",
        "air_temp", "sea_temp"
    }
    
    for col in required:
        if col not in existing:
            cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col} REAL;")
            print(f"ℹ️  Added column: {col}")
    
    conn.commit()


def ms_to_kmh(ms):
    """Convert m/s to km/h (for wind speeds)."""
    if ms is None:
        return None
    return round(float(ms) * 3.6, 2)


def parse_data_point(point):
    """Extract timestamp and value from FlowWorks data point."""
    timestamp_str = point.get("DataTime")
    value_str = point.get("DataValue")  # API uses DataValue not Value

    if not timestamp_str or value_str is None:
        return None, None

    try:
        # Parse ISO timestamp - Surrey API returns Pacific time (no TZ indicator)
        # Parse as naive datetime first
        dt_naive = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

        # If timestamp has no timezone, assume Pacific time
        if dt_naive.tzinfo is None:
            dt = dt_naive.replace(tzinfo=ZoneInfo("America/Vancouver"))
        else:
            dt = dt_naive

        # Round to 10-minute intervals (matching v1 behavior)
        if dt.minute % 10 != 0 or dt.second != 0:
            return None, None

        return dt, float(value_str)
    except (ValueError, TypeError):
        return None, None


def upsert_data(cur, buoy_id, timestamp, field, value):
    """Insert or update a data point in SQLite."""
    ts_epoch = int(timestamp.timestamp())
    
    # Check if record exists
    cur.execute("""
        SELECT COUNT(*) FROM buoy_observation 
        WHERE buoy_id = ? AND observation_time = ?
    """, (buoy_id, ts_epoch))
    
    exists = cur.fetchone()[0] > 0
    
    if exists:
        # Update existing
        cur.execute(f"""
            UPDATE buoy_observation 
            SET {field} = ?
            WHERE buoy_id = ? AND observation_time = ?
        """, (value, buoy_id, ts_epoch))
    else:
        # Insert new
        cur.execute(f"""
            INSERT INTO buoy_observation 
            (buoy_id, observation_time, {field}, source_file)
            VALUES (?, ?, ?, ?)
        """, (buoy_id, ts_epoch, value, "flowworks_surrey"))
    
    return cur.rowcount > 0


def fetch_and_store(api, station_key, station_config, conn, hours=2):
    """Fetch all channels for a station and store to SQLite."""
    site_id = station_config["site_id"]
    buoy_id = station_config["buoy_id"]
    channels = station_config["channels"]
    
    print(f"\n📡 Fetching {station_config['name']}...")
    
    cur = conn.cursor()
    total_inserted = 0
    
    for field_name, channel_id in channels.items():
        data_points = api.get_channel_data(site_id, channel_id, hours)
        
        if not data_points:
            continue
        
        inserted = 0
        for point in data_points:
            timestamp, value = parse_data_point(point)
            
            if timestamp is None or value is None:
                continue
            
            # Convert wind speeds from m/s to km/h
            if field_name in ["wind_speed", "wind_gust"]:
                value = ms_to_kmh(value)
            
            if upsert_data(cur, buoy_id, timestamp, field_name, value):
                inserted += 1
        
        conn.commit()
        total_inserted += inserted
        
        if inserted > 0:
            print(f"  ✅ {field_name}: {inserted} points")

        time.sleep(1.0)  # Rate limiting - increased to reduce API load
    
    return total_inserted


def main():
    print("🌊 Surrey FlowWorks Data Fetcher (API v2)")
    print("=" * 70)
    
    # Authenticate
    api = FlowWorksAPI(USERNAME, PASSWORD)
    if not api.authenticate():
        return 1
    
    # Connect to database
    conn = sqlite3.connect(SQLITE_PATH)
    ensure_columns(conn)
    
    # Fetch each station (use 24 hours to handle Surrey's reporting delays)
    total = 0
    for station_key, station_config in STATIONS.items():
        try:
            count = fetch_and_store(api, station_key, station_config, conn, hours=24)
            total += count
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"✅ Complete - inserted {total} data points")
    print(f"💾 Database: {SQLITE_PATH}")
    
    return 0


if __name__ == "__main__":
    exit(main())
