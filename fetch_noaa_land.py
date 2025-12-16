#!/usr/bin/env python3
"""
Fetch NOAA NDBC land/C-MAN station data and store in wind database.

Similar to fetch_noaa_buoy.py but for land-based wind stations.
Uses 5-day .txt feeds for meteorological data.

Stations:
- CPMW1: Cherry Point, WA
- SISW1: Smith Island, WA
"""

import requests
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Shared utilities
from units import ms_to_kmh
from config import WIND_DATABASE
from logging_config import setup_logging

logger = setup_logging('noaa_land', console=False)

# ---- Configuration ----
STATIONS = {
    "CPMW1": "Cherry Point",
    "SISW1": "Smith Island",
}

# ---- Field Mappings ----
FIELD_MAP_TXT = {
    "WSPD": "wind_speed_kmh",
    "GST":  "wind_gust_kmh",
    "WDIR": "wind_direction_deg",
    "ATMP": "air_temp_c",
    "PRES": "pressure_hpa",
}

# ---- Utilities ----
def parse_direction(val):
    """Handle cardinal ('WSW') or numeric degrees ('275')."""
    if val is None or str(val).strip().upper() in ("MM", "M", "NA", ""):
        return None
    try:
        deg = float(val)
        return int(deg % 360.0)
    except ValueError:
        pass
    dir_map = {
        'N': 0, 'NNE': 22, 'NE': 45, 'ENE': 68,
        'E': 90, 'ESE': 113, 'SE': 135, 'SSE': 158,
        'S': 180, 'SSW': 203, 'SW': 225, 'WSW': 248,
        'W': 270, 'WNW': 293, 'NW': 315, 'NNW': 338
    }
    return dir_map.get(str(val).strip().upper())

def is_missing(val, field=None):
    """Return True if a NOAA field value is missing or invalid."""
    if val is None:
        return True
    s = str(val).strip().upper()
    if s in ("MM", "M", "NA", ""):
        return True
    # Special handling for pressure - 999 is valid
    if field == "PRES":
        try:
            p = float(val)
            return p < 900 or p > 1100
        except ValueError:
            return True
    return False

def parse_noaa_txt(content):
    """Parse NOAA .txt format and return list of observation dicts."""
    lines = content.strip().split('\n')
    if len(lines) < 3:
        return []

    # Line 0: Headers
    # Line 1: Units
    # Line 2+: Data
    headers = lines[0].split()
    data_lines = lines[2:]

    # Map headers to 0-based indices
    hdr_idx = {h: i for i, h in enumerate(headers)}

    observations = []
    for line in data_lines:
        parts = line.split()
        if len(parts) < 5:
            continue

        # Parse timestamp (YY MM DD hh mm)
        try:
            year = int(parts[0])
            if year < 100:
                year += 2000 if year < 70 else 1900
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3])
            minute = int(parts[4])
            obs_dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
            obs_ts = int(obs_dt.timestamp())
        except (ValueError, IndexError):
            continue

        obs = {"observation_time": obs_ts}

        # Extract fields based on mapping
        for noaa_field, db_field in FIELD_MAP_TXT.items():
            if noaa_field not in hdr_idx:
                continue

            idx = hdr_idx[noaa_field]
            if idx >= len(parts):
                continue

            val = parts[idx]

            if is_missing(val, noaa_field):
                continue

            try:
                # Handle direction separately
                if noaa_field == "WDIR":
                    parsed = parse_direction(val)
                    if parsed is not None:
                        obs[db_field] = parsed
                # Wind speeds: convert m/s to km/h
                elif noaa_field in ["WSPD", "GST"]:
                    obs[db_field] = ms_to_kmh(float(val))
                # Everything else
                else:
                    obs[db_field] = float(val)
            except (ValueError, TypeError):
                pass

        if len(obs) > 1:  # Has data beyond timestamp
            observations.append(obs)

    return observations


def fetch_station(station_id, station_name):
    """Fetch and store data for a single NOAA land station."""
    url_txt = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt"

    try:
        logger.info(f"Fetching {station_id} ({station_name})...")
        resp = requests.get(url_txt, timeout=30)
        resp.raise_for_status()

        observations = parse_noaa_txt(resp.text)

        if not observations:
            logger.warning(f"{station_id}: No valid observations in .txt feed")
            return

        # Filter to last 5 days to avoid duplicates
        cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        cutoff_ts = int(cutoff.timestamp())
        recent_obs = [o for o in observations if o["observation_time"] >= cutoff_ts]

        if not recent_obs:
            logger.info(f"{station_id}: No recent observations (last 5 days)")
            return

        # Store in database
        with sqlite3.connect(WIND_DATABASE, timeout=10) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            cur = conn.cursor()

            inserted = 0
            skipped = 0

            for obs in recent_obs:
                obs_time = obs["observation_time"]

                # Check if already exists
                cur.execute("""
                    SELECT 1 FROM wind_observation
                    WHERE station_id = ? AND observation_time = ?
                """, (station_id, obs_time))

                if cur.fetchone():
                    skipped += 1
                    continue

                # Build INSERT with available fields
                fields = ["station_id", "observation_time", "station_name"]
                values = [station_id, obs_time, station_name]

                for field in ["wind_speed_kmh", "wind_gust_kmh", "wind_direction_deg",
                             "air_temp_c", "pressure_hpa"]:
                    if field in obs:
                        fields.append(field)
                        values.append(obs[field])

                placeholders = ",".join(["?"] * len(fields))
                sql = f"INSERT INTO wind_observation ({','.join(fields)}) VALUES ({placeholders})"

                cur.execute(sql, values)
                inserted += 1

            conn.commit()
            logger.info(f"{station_id}: Inserted {inserted}, skipped {skipped} duplicate(s)")

    except requests.RequestException as e:
        logger.error(f"{station_id}: HTTP error - {e}")
    except Exception as e:
        logger.error(f"{station_id}: Error - {e}", exc_info=True)


def main():
    """Fetch all configured NOAA land stations."""
    if not WIND_DATABASE.exists():
        logger.error(f"Wind database not found: {WIND_DATABASE}")
        return

    for station_id, station_name in STATIONS.items():
        fetch_station(station_id, station_name)

    logger.info("NOAA land station fetch complete")


if __name__ == "__main__":
    main()
