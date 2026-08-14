#!/usr/bin/env python3
"""
Fetch Surrey FlowWorks wave/wind data into SQLite, then republish the wind
readings to Windy.

Stations, and which database each lands in:

| Station              | Site  | Key      | Data                     | Database |
|----------------------|-------|----------|--------------------------|----------|
| Crescent Beach Ocean | 20182 | `CRPILE` | wave + wind + temp       | buoy     |
| Crescent Channel     | 20183 | `CRCHAN` | wind + radar wave + temp | buoy     |
| Colebrook            | 18507 | `COLEB`  | wind + temp only         | wind     |

Colebrook is a land-based wind station, not a buoy, so its readings live in
`wind_data.sqlite`; the other two are marine sites in `buoy_data.sqlite`. Both
the fetch and the Windy push route per station accordingly — getting this
wrong once pushed month-old data to Windy while reporting success (see
`docs/KNOWN_ISSUES.md`).

**Two unrelated APIs here are both called "v2".** Keep them apart:

- *FlowWorks API v2* (inbound, `API_BASE`) — Surrey's data source. JWT auth
  from a username/password pair; `FlowWorksAPI` manages the token.
- *Windy Stations API v2* (outbound, `lib/windy.py`) — where the wind readings
  are republished. Per-station passwords, no account-wide key. In force since
  January 2026; the legacy endpoint answers HTTP 410 and dies end of 2026.

Air temperature is fetched and stored but deliberately never pushed to Windy:
these sensors are unshielded and read up to ~19 °C high in daylight. See
`docs/DATA_FEEDS.md` § Surrey FlowWorks.
"""

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from lib.config import BUOY_DATABASE, WIND_DATABASE
from lib.env import require_env
from lib.logging_config import setup_logging
from lib.stations import get_all_buoys, get_all_wind

# Shared utilities
from lib.units import ms_to_kmh
from lib.windy import (
    WINDY_PUSH_ENABLED,
    WINDY_UPDATE_URL,
    auth_headers,
    load_windy_credentials,
)

logger = setup_logging("surrey_fetch")

# ---- Configuration ----
API_BASE = "https://developers.flowworks.com/fwapi/v2"


# Surrey FlowWorks API credentials (environment, else config/.env)
USERNAME = require_env("SURREY_API_USERNAME")
PASSWORD = require_env("SURREY_API_PASSWORD")

# Windy configuration, credentials and read-back live in lib/windy.py, shared
# with the health check so both agree on which stations we publish and how
# their credentials are named. See that module for the two traps this API sets:
# the read endpoint echoes station passwords, and the update endpoint returns
# an empty 200 whether or not the observation lands.

# Channel fields this fetcher pulls from each station's stations.json channel map.
# Water-level and geodetic channels live in the same map but are owned by the
# tide fetcher, so they are excluded here.
BUOY_FIELDS = {
    "wind_speed",
    "wind_direction",
    "wind_gust",
    "wave_height_sig",
    "wave_height_peak",
    "wave_period_avg",
    "wave_period_peak",
    "sea_temp",
    "air_temp",
}
WIND_FIELDS = {"wind_speed", "wind_direction", "wind_gust", "air_temp"}


def get_surrey_stations():
    """Surrey FlowWorks stations from the registry.

    Yields (station_id, metadata, is_wind_station) tuples. Buoy-type stations
    (wave data) route to the buoy database; wind-type stations to the wind database.
    """
    for sid, meta in get_all_buoys().items():
        if meta.get("source") == "Surrey FlowWorks":
            yield sid, meta, False
    for sid, meta in get_all_wind().items():
        if meta.get("source") == "Surrey FlowWorks":
            yield sid, meta, True


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


def upsert_data_buoy(cur, buoy_id, timestamp, field, value, only_if_null=False):
    """Insert or update a data point in buoy database.

    only_if_null: when True, never overwrite an existing non-null value (used for
    fallback channels so the primary sensor always wins).
    """
    ts_epoch = int(timestamp.timestamp())

    # Check existing value for this field
    cur.execute(
        f"""
        SELECT {field} FROM buoy_observation
        WHERE buoy_id = ? AND observation_time = ?
    """,
        (buoy_id, ts_epoch),
    )

    row = cur.fetchone()
    exists = row is not None

    if only_if_null and exists and row[0] is not None:
        return False

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


def fetch_and_store(api, station_id, meta, conn, is_wind_station=False, hours=2):
    """Fetch the relevant channels for a station and store to SQLite.

    Args:
        api: FlowWorks API instance
        station_id: Registry station ID (e.g., 'CRPILE'), used as the DB key
        meta: Station metadata dict from stations.json
        conn: Database connection (buoy or wind database)
        is_wind_station: True if wind-only station (uses wind_observation table)
        hours: Hours of data to fetch
    """
    site_id = meta["flowworks_site_id"]
    station_name = meta.get("short_name") or meta["name"]
    allowed = WIND_FIELDS if is_wind_station else BUOY_FIELDS
    channels = {f: cid for f, cid in meta.get("channels", {}).items() if f in allowed}
    fallback = {f: cid for f, cid in meta.get("fallback_channels", {}).items() if f in allowed}

    logger.info(f"Fetching {station_name}...")

    cur = conn.cursor()
    total_inserted = 0

    # Primary channels first, then fallback channels (only_if_null) so the primary
    # sensor always wins and fallbacks only fill gaps.
    channel_items = [(f, cid, False) for f, cid in channels.items()]
    channel_items += [(f, cid, True) for f, cid in fallback.items()]

    for field_name, channel_id, only_if_null in channel_items:
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
                if upsert_data_buoy(cur, station_id, timestamp, field_name, value, only_if_null=only_if_null):
                    inserted += 1

        conn.commit()
        total_inserted += inserted

        if inserted > 0:
            logger.debug(f"  {field_name}: {inserted} points")

        time.sleep(1.0)  # Rate limiting - increased to reduce API load

    return total_inserted


def get_latest_station_data(conn, buoy_id):
    """Latest wind observation for a station — feeds the Windy push only.

    Air temperature is not selected: `push_to_windy` no longer publishes it
    (unshielded sensors, see that function).
    """
    cur = conn.cursor()

    # Get the latest observation with required wind data
    cur.execute(
        """
        SELECT observation_time, wind_speed, wind_direction, wind_gust
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

    return {
        "timestamp": row[0],
        "wind_speed": row[1],
        "wind_direction": row[2],
        "wind_gust": row[3],
    }


def get_latest_wind_station_data(conn, station_id):
    """Latest observation for a wind station — feeds the Windy push only.

    Air temperature is not selected: `push_to_windy` no longer publishes it
    (unshielded sensors, see that function).
    """
    cur = conn.cursor()

    # Get the latest observation with required wind data
    cur.execute(
        """
        SELECT observation_time, wind_speed_kmh, wind_direction_deg, wind_gust_kmh
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

    return {
        "timestamp": row[0],
        "wind_speed": row[1],
        "wind_direction": row[2],
        "wind_gust": row[3],
    }


def push_to_windy(station_key, data, credentials):
    """Upload one station's latest wind observation to Windy.

    Measurements only: the v2 update endpoint has no parameters for station
    name, position or elevation, which now live on Windy's side and are set
    under My Stations (or via the PWS endpoints), not on every upload.
    """
    if not data:
        logger.warning(f"{station_key}: No data to push to Windy")
        return False

    # Air temperature is deliberately NOT pushed. All three FlowWorks sensors
    # sit in unshielded enclosures and read high whenever the sun is on them —
    # Colebrook averages 38.6 °C at 16:00 PDT and has hit 42.6 °C, roughly
    # +19 °C against a marine reference, while matching that reference
    # overnight (measured 2026-08-13, see docs/DATA_FEEDS.md § Surrey
    # FlowWorks). On our own pages that number carries a footnote; on Windy it
    # would appear as a bare public observation with nothing to qualify it.
    # Wind is the trustworthy signal from these stations, so wind is all we
    # publish.
    params = {
        "id": credentials["id"],
        # POSIX seconds — the same epoch the observation is stored under.
        "ts": int(data["timestamp"]),
        # Windy expects m/s; the databases store km/h.
        "wind": round(data["wind_speed"] / 3.6, 2),
        "gust": round(data["wind_gust"] / 3.6, 2),
        "winddir": int(data["wind_direction"]),
    }

    # Bearer header, never the PASSWORD query parameter Windy also accepts —
    # see lib/windy.py for why.
    try:
        response = requests.get(
            WINDY_UPDATE_URL, params=params, headers=auth_headers(credentials), timeout=10
        )

        # 409 means Windy already holds this exact observation. The fetcher
        # runs every 20 minutes but Surrey often reports less often, so
        # re-sending an unchanged reading is routine, not a failure.
        if response.status_code == 409:
            logger.debug(f"{station_key}: Windy already has this observation (HTTP 409)")
            return True

        if response.status_code == 429:
            retry_after = response.json().get("retry_after", "unknown")
            logger.warning(f"{station_key}: Windy rate limit hit - retry after {retry_after}")
            return False

        response.raise_for_status()

        # A 200 is not proof the observation landed: stations have been seen
        # returning clean 200s for hours while still showing Offline. Log the
        # body rather than just the status, so the log is evidence instead of
        # an assumption, and confirm against the station detail page before
        # trusting a green run. Logged at INFO during bring-up; drop to DEBUG
        # once the success body's shape is known and checked for explicitly.
        body = response.text.strip()
        logger.info(
            f"{station_key}: Windy HTTP {response.status_code} ts={params['ts']} body={body[:200]!r}"
        )
        return True

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status in (400, 401):
            logger.error(
                f"{station_key}: Windy rejected the station password (HTTP {status}) - "
                f"check WINDY_{station_key}_ID and WINDY_{station_key}_PASSWORD in config/.env"
            )
        else:
            logger.error(f"{station_key}: Windy push failed - {e}")
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
    # Wind-only stations (COLEB) → wind database; wave stations → buoy database.
    surrey_stations = list(get_surrey_stations())
    total = 0
    for station_id, meta, is_wind_only in surrey_stations:
        try:
            conn = wind_conn if is_wind_only else buoy_conn
            count = fetch_and_store(api, station_id, meta, conn, is_wind_station=is_wind_only, hours=24)
            total += count
        except Exception as e:
            logger.error(f"  Error: {e}")

    logger.info(f"Complete - inserted {total} data points")

    if not WINDY_PUSH_ENABLED:
        logger.info("Windy push paused (WINDY_PUSH_ENABLED is False) - skipping")
        buoy_conn.close()
        wind_conn.close()
        logger.info(f"Buoy database: {BUOY_DATABASE}")
        logger.info(f"Wind database: {WIND_DATABASE}")
        return 0

    # Push latest data to Windy for each station
    windy_credentials = load_windy_credentials()
    if not windy_credentials:
        logger.warning("No Windy credentials configured - skipping Windy push")
        buoy_conn.close()
        wind_conn.close()
        return 0

    logger.info("Pushing data to Windy...")
    windy_success = 0
    for station_id, _meta, is_wind_only in surrey_stations:
        try:
            if station_id not in windy_credentials:
                continue

            # Route to appropriate database (wind-only stations use wind database)
            if is_wind_only:
                data = get_latest_wind_station_data(wind_conn, station_id)
            else:
                data = get_latest_station_data(buoy_conn, station_id)

            if data and push_to_windy(station_id, data, windy_credentials[station_id]):
                windy_success += 1
        except Exception as e:
            logger.error(f"  Windy push error for {station_id}: {e}")

    logger.info(f"Windy: {windy_success}/{len(windy_credentials)} stations updated")

    buoy_conn.close()
    wind_conn.close()
    logger.info(f"Buoy database: {BUOY_DATABASE}")
    logger.info(f"Wind database: {WIND_DATABASE}")

    return 0


if __name__ == "__main__":
    exit(main())
