#!/usr/bin/env python3
"""
Fetch Jericho Sailing Centre wind station data and store to SQLite.

Data source: https://jsca.bc.ca/main/downld02.txt
Format: Space-delimited text table with header rows
Update frequency: 30-minute intervals

Integrates into existing wind_data.sqlite database.
"""

import requests
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

# Shared utilities
from config import WIND_DATABASE
from logging_config import setup_logging

logger = setup_logging('jericho_fetch')

# Configuration
JERICHO_URL = "https://jsca.bc.ca/main/downld02.txt"
STATION_ID = "JERICHO"
STATION_NAME = "Jericho Sailing Centre"
PACIFIC_TZ = ZoneInfo("America/Vancouver")

# Unit conversions
def mph_to_kmh(mph):
    """Convert miles per hour to kilometers per hour."""
    if mph is None:
        return None
    return mph * 1.60934

def f_to_c(fahrenheit):
    """Convert Fahrenheit to Celsius."""
    if fahrenheit is None:
        return None
    return (fahrenheit - 32) * 5.0 / 9.0

def inches_to_mm(inches):
    """Convert inches to millimeters."""
    if inches is None:
        return None
    return inches * 25.4

def cardinal_to_degrees(cardinal):
    """Convert cardinal direction (N, NE, E, etc.) to degrees."""
    if not cardinal or cardinal == '---':
        return None

    # Map cardinal directions to degrees
    directions = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5,
        'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5,
        'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
        'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }

    return directions.get(cardinal.upper())


def parse_jericho_timestamp(date_str, time_str):
    """
    Parse Jericho date/time to Unix timestamp.

    Args:
        date_str: MM/DD/YY format (e.g., "12/07/25")
        time_str: h:mma format (e.g., "12:30a", "1:00p")

    Returns:
        Unix timestamp (seconds since epoch) or None if parse fails
    """
    try:
        # Parse date (MM/DD/YY)
        month, day, year = date_str.split('/')
        year = int('20' + year)  # Assume 20xx
        month = int(month)
        day = int(day)

        # Parse time (h:mma or hh:mma)
        time_str = time_str.strip()
        am_pm = time_str[-1].lower()  # 'a' or 'p'
        time_part = time_str[:-1]  # Remove 'a' or 'p'

        if ':' in time_part:
            hour, minute = time_part.split(':')
            hour = int(hour)
            minute = int(minute)
        else:
            hour = int(time_part)
            minute = 0

        # Convert to 24-hour format
        if am_pm == 'p' and hour != 12:
            hour += 12
        elif am_pm == 'a' and hour == 12:
            hour = 0

        # Create datetime in Pacific timezone
        dt = datetime(year, month, day, hour, minute, tzinfo=PACIFIC_TZ)

        return int(dt.timestamp())

    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse timestamp '{date_str} {time_str}': {e}")
        return None


def parse_jericho_line(line):
    """
    Parse a single data line from Jericho feed.

    Returns:
        dict with parsed fields or None if invalid
    """
    parts = line.split()

    # Need at least 17 columns for required fields
    if len(parts) < 17:
        return None

    try:
        # Extract fields (0-indexed after split)
        date_str = parts[0]           # MM/DD/YY
        time_str = parts[1]           # h:mma
        temp_c = float(parts[2])      # °C (already in Celsius!)
        humidity = float(parts[5])    # %
        dewpoint_c = float(parts[6])  # °C (already in Celsius!)
        wind_speed_mph = float(parts[7])  # mph
        wind_dir_card = parts[8]      # Cardinal (E, NE, etc.)
        wind_gust_mph = float(parts[10])  # mph (hi speed)
        wind_chill_c = float(parts[12])   # °C (already in Celsius!)
        heat_index_c = float(parts[13])   # °C (already in Celsius!)
        pressure_mb = float(parts[15])    # mb (= hPa)
        rain_inches = float(parts[16])    # inches

        # Parse timestamp
        timestamp = parse_jericho_timestamp(date_str, time_str)
        if timestamp is None:
            return None

        # Convert units (wind still in mph→kmh, rain in inches→mm)
        return {
            'timestamp': timestamp,
            'wind_speed_kmh': mph_to_kmh(wind_speed_mph),
            'wind_gust_kmh': mph_to_kmh(wind_gust_mph),
            'wind_direction_deg': cardinal_to_degrees(wind_dir_card),
            'air_temp_c': temp_c,  # Already in Celsius
            'pressure_hpa': pressure_mb,  # mb = hPa
            'rainfall_1hr_mm': inches_to_mm(rain_inches),
            'humidity_percent': humidity,
            'dewpoint_c': dewpoint_c,  # Already in Celsius
            'wind_chill_c': wind_chill_c,  # Already in Celsius
            'heat_index_c': heat_index_c,  # Already in Celsius
        }

    except (ValueError, IndexError) as e:
        logger.debug(f"Failed to parse line: {e}")
        return None


def fetch_jericho_data():
    """Fetch latest data from Jericho Sailing Centre."""
    try:
        logger.info(f"Fetching data from {JERICHO_URL}")
        response = requests.get(JERICHO_URL, timeout=15)
        response.raise_for_status()

        text = response.text
        lines = text.strip().split('\n')

        # Skip header rows (first 3 lines)
        data_lines = lines[3:]

        logger.info(f"Received {len(data_lines)} data rows")
        return data_lines

    except Exception as e:
        logger.error(f"Failed to fetch Jericho data: {e}")
        return []


def ensure_columns(conn):
    """Ensure required columns exist in wind_observation table."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(wind_observation);")
    existing = {row[1] for row in cur.fetchall()}

    # Additional columns for Jericho (beyond standard wind stations)
    required = ['wind_chill_c', 'heat_index_c']

    for col in required:
        if col not in existing:
            cur.execute(f"ALTER TABLE wind_observation ADD COLUMN {col} REAL;")
            logger.info(f"Added column: {col}")

    conn.commit()


def store_observations(conn, observations):
    """Store observations to SQLite."""
    # Ensure all columns exist first
    ensure_columns(conn)

    cur = conn.cursor()
    inserted = 0
    updated = 0

    for obs in observations:
        try:
            # Check if record exists
            cur.execute("""
                SELECT COUNT(*) FROM wind_observation
                WHERE station_id = ? AND observation_time = ?
            """, (STATION_ID, obs['timestamp']))

            exists = cur.fetchone()[0] > 0

            if exists:
                # Update existing record
                cur.execute("""
                    UPDATE wind_observation
                    SET wind_speed_kmh = ?,
                        wind_gust_kmh = ?,
                        wind_direction_deg = ?,
                        air_temp_c = ?,
                        pressure_hpa = ?,
                        rainfall_1hr_mm = ?,
                        humidity_percent = ?,
                        dewpoint_c = ?,
                        wind_chill_c = ?,
                        heat_index_c = ?,
                        source_file = ?
                    WHERE station_id = ? AND observation_time = ?
                """, (
                    obs['wind_speed_kmh'],
                    obs['wind_gust_kmh'],
                    obs['wind_direction_deg'],
                    obs['air_temp_c'],
                    obs['pressure_hpa'],
                    obs['rainfall_1hr_mm'],
                    obs['humidity_percent'],
                    obs['dewpoint_c'],
                    obs['wind_chill_c'],
                    obs['heat_index_c'],
                    'jericho_jsca',
                    STATION_ID,
                    obs['timestamp']
                ))
                updated += 1
            else:
                # Insert new record
                cur.execute("""
                    INSERT INTO wind_observation (
                        station_id,
                        station_name,
                        observation_time,
                        wind_speed_kmh,
                        wind_gust_kmh,
                        wind_direction_deg,
                        air_temp_c,
                        pressure_hpa,
                        rainfall_1hr_mm,
                        humidity_percent,
                        dewpoint_c,
                        wind_chill_c,
                        heat_index_c,
                        source_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    STATION_ID,
                    STATION_NAME,
                    obs['timestamp'],
                    obs['wind_speed_kmh'],
                    obs['wind_gust_kmh'],
                    obs['wind_direction_deg'],
                    obs['air_temp_c'],
                    obs['pressure_hpa'],
                    obs['rainfall_1hr_mm'],
                    obs['humidity_percent'],
                    obs['dewpoint_c'],
                    obs['wind_chill_c'],
                    obs['heat_index_c'],
                    'jericho_jsca'
                ))
                inserted += 1

        except Exception as e:
            logger.error(f"Failed to store observation: {e}")

    conn.commit()
    logger.info(f"Database: inserted={inserted}, updated={updated}")
    return inserted, updated


def main():
    logger.info("Jericho Sailing Centre Wind Data Fetcher")

    # Fetch data
    lines = fetch_jericho_data()
    if not lines:
        logger.warning("No data received")
        return 1

    # Parse all lines
    observations = []
    for line in lines:
        obs = parse_jericho_line(line)
        if obs:
            observations.append(obs)

    if not observations:
        logger.warning("No valid observations parsed")
        return 1

    logger.info(f"Parsed {len(observations)} valid observations")

    # Store to database
    conn = sqlite3.connect(WIND_DATABASE)
    inserted, updated = store_observations(conn, observations)
    conn.close()

    logger.info(f"Complete - Database: {WIND_DATABASE}")
    return 0


if __name__ == "__main__":
    exit(main())
