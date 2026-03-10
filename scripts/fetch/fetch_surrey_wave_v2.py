#!/usr/bin/env python3
"""
Fetch Surrey FlowWorks wave/wind data (API v2) and store to SQLite.
Based on v1 implementation - migrated to v2 API with JWT auth.

Stations:
- Crescent Beach Ocean (20182): Full wave + wind + temp
- Crescent Channel (20183): Wind + radar wave + temp
- Colebrook (18507): Wind + temp only
"""

import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from lib.config import BUOY_DATABASE, WIND_DATABASE
from lib.logging_config import setup_logging

# Shared utilities
from lib.units import ms_to_kmh

logger = setup_logging("surrey_fetch")

# ---- Configuration ----
API_BASE = "https://developers.flowworks.com/fwapi/v2"


# Surrey FlowWorks API credentials
def _require_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise RuntimeError(f"{var_name} environment variable not set")
    return value


USERNAME = _require_env("SURREY_API_USERNAME")
PASSWORD = _require_env("SURREY_API_PASSWORD")

# Windy API Configuration
WINDY_API_KEY = os.environ.get("WINDY_API_KEY")

# Windy station metadata
# Station IDs match registered stations in Windy (0, 1, 2)
WINDY_STATIONS = {
    "crescentpile": {
        "stationid": 0,
        "name": "Crescent Beach Ocean",
        "lat": 49.0121666,
        "lon": -122.9402834,
        "elevation": 5,  # meters
    },
    "crescentchannel": {
        "stationid": 1,
        "name": "Crescent Beach Channel",
        "lat": 49.05392053,
        "lon": -122.8970759,
        "elevation": 5,  # meters
    },
    "colebrook": {
        "stationid": 2,
        "name": "Colebrook Pump House",
        "lat": 49.08583,
        "lon": -122.845,
        "elevation": 2,  # meters
    },
}

# Station configuration (from v1)
STATIONS = {
    "crescentpile": {
        "site_id": 20182,
        "name": "Crescent Beach Ocean",
        "buoy_id": "CRPILE",  # Short ID for database
        "channels": {
            "wind_speed": 1810,
            "wind_direction": 1811,
            "wind_gust": 1814,
            "wave_height_sig": 2002,  # Hs_Anderra
            "wave_height_peak": 2008,  # Hmax_Anderra
            "wave_period_avg": 2009,  # Tmean_Anderra
            "wave_period_peak": 2012,  # Tpeak_Anderra
            "sea_temp": 2007,  # Temperature_Anderra
            "air_temp": 1794,  # PTemp
        },
    },
    "crescentchannel": {
        "site_id": 20183,
        "name": "Crescent Channel",
        "buoy_id": "CRCHAN",
        "channels": {
            "wind_speed": 1837,
            "wind_direction": 1838,
            "wind_gust": 1841,
            "wave_height_sig": 2155,  # Hm0_Radar
            "air_temp": 1821,
        },
    },
    "colebrook": {
        "site_id": 18507,
        "name": "Colebrook",
        "buoy_id": "COLEB",  # Station ID (NOT a buoy - land-based wind station, uses wind_observation table)
        "channels": {
            "wind_speed": 1425,
            "wind_direction": 1426,
            "wind_gust": 1427,
            "air_temp": 1439,
        },
    },
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
                    self.token_expiry = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))

                logger.info(f"Authenticated - expires {self.token_expiry}")
                return True
            else:
                logger.error(f"Auth failed: {data}")
                return False

        except Exception as e:
            logger.error(f"Auth error: {e}")
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
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def get_channel_data(self, site_id, channel_id, hours=2):
        """Fetch data from specific channel (last N hours)."""
        url = f"{self.base_url}/sites/{site_id}/channels/{channel_id}/data"

        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)

        params = {
            "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        try:
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            # API returns Resources array directly
            if data.get("ResultCode") == 0:
                return data.get("Resources", [])
            else:
                logger.warning(f"API error: {data.get('ResultMessage')}")
                return []

        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return []


def ensure_columns(conn):
    """Ensure all columns exist in buoy_observation table."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing = {row[1] for row in cur.fetchall()}

    # All possible columns from Surrey stations
    required = {
        "wave_height_sig",
        "wave_height_peak",
        "wave_period_avg",
        "wave_period_peak",
        "wind_speed",
        "wind_gust",
        "wind_direction",
        "air_temp",
        "sea_temp",
    }

    for col in required:
        if col not in existing:
            cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col} REAL;")
            logger.info(f"Added column: {col}")

    conn.commit()


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


def upsert_data_buoy(cur, buoy_id, timestamp, field, value):
    """Insert or update a data point in buoy database."""
    ts_epoch = int(timestamp.timestamp())

    # Check if record exists
    cur.execute(
        """
        SELECT COUNT(*) FROM buoy_observation
        WHERE buoy_id = ? AND observation_time = ?
    """,
        (buoy_id, ts_epoch),
    )

    exists = cur.fetchone()[0] > 0

    if exists:
        # Update existing
        cur.execute(
            f"""
            UPDATE buoy_observation
            SET {field} = ?
            WHERE buoy_id = ? AND observation_time = ?
        """,
            (value, buoy_id, ts_epoch),
        )
    else:
        # Insert new
        cur.execute(
            f"""
            INSERT INTO buoy_observation
            (buoy_id, observation_time, {field}, source_file)
            VALUES (?, ?, ?, ?)
        """,
            (buoy_id, ts_epoch, value, "flowworks_surrey"),
        )

    return cur.rowcount > 0


def upsert_data_wind(cur, station_id, station_name, timestamp, field, value):
    """Insert or update a data point in wind database."""
    ts_epoch = int(timestamp.timestamp())

    # Map field names from buoy schema to wind schema
    field_map = {
        "wind_speed": "wind_speed_kmh",
        "wind_gust": "wind_gust_kmh",
        "wind_direction": "wind_direction_deg",
        "air_temp": "air_temp_c",
    }
    wind_field = field_map.get(field, field)

    # Check if record exists
    cur.execute(
        """
        SELECT COUNT(*) FROM wind_observation
        WHERE station_id = ? AND observation_time = ?
    """,
        (station_id, ts_epoch),
    )

    exists = cur.fetchone()[0] > 0

    if exists:
        # Update existing
        cur.execute(
            f"""
            UPDATE wind_observation
            SET {wind_field} = ?
            WHERE station_id = ? AND observation_time = ?
        """,
            (value, station_id, ts_epoch),
        )
    else:
        # Insert new
        cur.execute(
            f"""
            INSERT INTO wind_observation
            (station_id, observation_time, station_name, {wind_field})
            VALUES (?, ?, ?, ?)
        """,
            (station_id, ts_epoch, station_name, value),
        )

    return cur.rowcount > 0


def fetch_and_store(api, station_key, station_config, conn, is_wind_station=False, hours=2):
    """Fetch all channels for a station and store to SQLite.

    Args:
        api: FlowWorks API instance
        station_key: Station key (e.g., 'colebrook')
        station_config: Station configuration dict
        conn: Database connection (buoy or wind database)
        is_wind_station: True if wind-only station (uses wind_observation table)
        hours: Hours of data to fetch
    """
    site_id = station_config["site_id"]
    station_id = station_config["buoy_id"]  # Used as station_id for both
    station_name = station_config["name"]
    channels = station_config["channels"]

    logger.info(f"Fetching {station_name}...")

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

            # Route to appropriate database
            if is_wind_station:
                if upsert_data_wind(cur, station_id, station_name, timestamp, field_name, value):
                    inserted += 1
            else:
                if upsert_data_buoy(cur, station_id, timestamp, field_name, value):
                    inserted += 1

        conn.commit()
        total_inserted += inserted

        if inserted > 0:
            logger.debug(f"  {field_name}: {inserted} points")

        time.sleep(1.0)  # Rate limiting - increased to reduce API load

    return total_inserted


def get_latest_station_data(conn, buoy_id):
    """Get the most recent observation for a station from SQLite."""
    cur = conn.cursor()

    # Get the latest observation with required wind data
    cur.execute(
        """
        SELECT observation_time, wind_speed, wind_direction, wind_gust, air_temp
        FROM buoy_observation
        WHERE buoy_id = ?
          AND wind_speed IS NOT NULL
          AND wind_direction IS NOT NULL
          AND wind_gust IS NOT NULL
        ORDER BY observation_time DESC
        LIMIT 1
    """,
        (buoy_id,),
    )

    row = cur.fetchone()
    if not row:
        return None

    # If air_temp is NULL, try to get the most recent non-null value
    air_temp = row[4]
    if air_temp is None:
        cur.execute(
            """
            SELECT air_temp
            FROM buoy_observation
            WHERE buoy_id = ? AND air_temp IS NOT NULL
            ORDER BY observation_time DESC
            LIMIT 1
        """,
            (buoy_id,),
        )
        temp_row = cur.fetchone()
        if temp_row:
            air_temp = temp_row[0]

    return {
        "timestamp": row[0],
        "wind_speed": row[1],
        "wind_direction": row[2],
        "wind_gust": row[3],
        "air_temp": air_temp,
    }


def get_latest_wind_station_data(conn, station_id):
    """Get the most recent observation for a wind station from SQLite."""
    cur = conn.cursor()

    # Get the latest observation with required wind data
    cur.execute(
        """
        SELECT observation_time, wind_speed_kmh, wind_direction_deg, wind_gust_kmh, air_temp_c
        FROM wind_observation
        WHERE station_id = ?
          AND wind_speed_kmh IS NOT NULL
          AND wind_direction_deg IS NOT NULL
          AND wind_gust_kmh IS NOT NULL
        ORDER BY observation_time DESC
        LIMIT 1
    """,
        (station_id,),
    )

    row = cur.fetchone()
    if not row:
        return None

    # If air_temp is NULL, try to get the most recent non-null value
    air_temp = row[4]
    if air_temp is None:
        cur.execute(
            """
            SELECT air_temp_c
            FROM wind_observation
            WHERE station_id = ? AND air_temp_c IS NOT NULL
            ORDER BY observation_time DESC
            LIMIT 1
        """,
            (station_id,),
        )
        temp_row = cur.fetchone()
        if temp_row:
            air_temp = temp_row[0]

    return {
        "timestamp": row[0],
        "wind_speed": row[1],
        "wind_direction": row[2],
        "wind_gust": row[3],
        "air_temp": air_temp,
    }


def push_to_windy(station_key, data, windy_config):
    """Push station data to Windy API."""
    if not WINDY_API_KEY:
        logger.warning("WINDY_API_KEY not set - skipping Windy push")
        return False

    if not data:
        logger.warning(f"{station_key}: No data to push to Windy")
        return False

    try:
        # Convert timestamp to UTC ISO format
        dt = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)
        dt_utc_str = dt.strftime("%Y-%m-%d %H:%M:%S")

        # Convert wind speeds from km/h to m/s (Windy expects m/s)
        wind_ms = data["wind_speed"] / 3.6
        gust_ms = data["wind_gust"] / 3.6

        # Build query parameters
        params = {
            "station": windy_config["stationid"],
            "name": windy_config["name"],
            "latitude": windy_config["lat"],
            "longitude": windy_config["lon"],
            "elevation": windy_config["elevation"],
            "dateutc": dt_utc_str,
            "wind": round(wind_ms, 2),
            "winddir": int(data["wind_direction"]),
            "gust": round(gust_ms, 2),
            "shareOption": "Open",
        }

        # Add temperature if available
        if data["air_temp"] is not None:
            params["temp"] = round(data["air_temp"], 1)

        # Make request
        url = f"https://stations.windy.com/pws/update/{WINDY_API_KEY}"
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        # Log response details for debugging
        response_text = response.text.strip()
        logger.debug(f"{station_key}: Windy response - HTTP {response.status_code}: {response_text}")
        logger.debug(f"{station_key}: Sent params: {params}")

        # Check for success indicators in response
        if response_text.lower() in ["success", "ok"] or response.status_code == 200:
            return True
        else:
            logger.warning(f"{station_key}: Unexpected Windy response: {response_text}")
            return False

    except Exception as e:
        logger.error(f"{station_key}: Windy push failed - {e}")
        return False


def main():
    logger.info("Surrey FlowWorks Data Fetcher (API v2)")

    # Authenticate
    api = FlowWorksAPI(USERNAME, PASSWORD)
    if not api.authenticate():
        return 1

    # Connect to both databases
    buoy_conn = sqlite3.connect(BUOY_DATABASE)
    wind_conn = sqlite3.connect(WIND_DATABASE)
    ensure_columns(buoy_conn)

    # Fetch each station (use 24 hours to handle Surrey's reporting delays)
    # COLEB is wind-only → wind database
    # CRPILE, CRCHAN have wave data → buoy database
    total = 0
    for station_key, station_config in STATIONS.items():
        try:
            # Route to appropriate database
            is_wind_only = station_key == "colebrook"
            conn = wind_conn if is_wind_only else buoy_conn

            count = fetch_and_store(api, station_key, station_config, conn, is_wind_station=is_wind_only, hours=24)
            total += count
        except Exception as e:
            logger.error(f"  Error: {e}")

    logger.info(f"Complete - inserted {total} data points")

    # Push latest data to Windy for each station
    logger.info("Pushing data to Windy...")
    windy_success = 0
    for station_key, station_config in STATIONS.items():
        try:
            if station_key not in WINDY_STATIONS:
                continue

            station_id = station_config["buoy_id"]

            # Route to appropriate database (wind-only stations use wind database)
            is_wind_only = station_key == "colebrook"
            if is_wind_only:
                data = get_latest_wind_station_data(wind_conn, station_id)
            else:
                data = get_latest_station_data(buoy_conn, station_id)

            if data and push_to_windy(station_key, data, WINDY_STATIONS[station_key]):
                windy_success += 1
        except Exception as e:
            logger.error(f"  Windy push error for {station_key}: {e}")

    logger.info(f"Windy: {windy_success}/{len(WINDY_STATIONS)} stations updated")

    buoy_conn.close()
    wind_conn.close()
    logger.info(f"Buoy database: {BUOY_DATABASE}")
    logger.info(f"Wind database: {WIND_DATABASE}")

    return 0


if __name__ == "__main__":
    exit(main())
