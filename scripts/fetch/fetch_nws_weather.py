#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
"""
Fetch NWS (National Weather Service) weather station data and store to SQLite.

Data source: https://api.weather.gov/stations/{STATION_ID}/observations/latest
Format: JSON API
Update frequency: Every 5-10 minutes (varies by station)

Stations:
- KBLI: Bellingham International Airport, WA
- KORS: Orcas Island Airport, WA

Integrates into existing wind_data.sqlite database.
"""

import requests
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Shared utilities
from lib.config import WIND_DATABASE
from lib.logging_config import setup_logging

logger = setup_logging('nws_fetch')

# NWS Stations Configuration
NWS_STATIONS = {
    'KBLI': {
        'name': 'Bellingham International Airport',
        'url': 'https://api.weather.gov/stations/KBLI/observations/latest'
    },
    'KORS': {
        'name': 'Orcas Island Airport',
        'url': 'https://api.weather.gov/stations/KORS/observations/latest'
    }
}

# Unit conversions
def ms_to_kmh(ms):
    """Convert meters per second to kilometers per hour."""
    if ms is None:
        return None
    return ms * 3.6

def pa_to_hpa(pa):
    """Convert Pascals to hectopascals (millibars)."""
    if pa is None:
        return None
    return pa / 100.0

def m_to_km(m):
    """Convert meters to kilometers."""
    if m is None:
        return None
    return m / 1000.0

def knots_to_kmh(knots):
    """Convert knots to kilometers per hour."""
    if knots is None:
        return None
    return knots * 1.852

def parse_metar_wind(raw_metar):
    """
    Parse wind data from METAR rawMessage when API fields are null.

    METAR wind format: DDDSSGGGkt or DDDSSkt
    - DDD: direction in degrees (3 digits, or VRB for variable)
    - SS: speed in knots (2-3 digits)
    - GGG: gust in knots (optional, after G)
    - kt: units

    Examples:
      19025G33KT -> dir=190, speed=25kt, gust=33kt
      VRB05KT -> dir=variable, speed=5kt
      00000KT -> calm

    Returns:
        tuple: (wind_direction_deg, wind_speed_kmh, wind_gust_kmh)
    """
    import re

    if not raw_metar:
        return None, None, None

    # METAR wind pattern: direction (3 digits or VRB), speed (2-3 digits), optional gust (G + 2-3 digits), KT
    # Examples: 19025G33KT, VRB05KT, 00000KT, 27015KT
    pattern = r'\b(VRB|\d{3})(\d{2,3})(?:G(\d{2,3}))?KT\b'
    match = re.search(pattern, raw_metar)

    if not match:
        return None, None, None

    dir_str, speed_str, gust_str = match.groups()

    # Parse direction
    if dir_str == 'VRB':
        wind_direction = None  # Variable direction
    elif dir_str == '000':
        wind_direction = None  # Calm
    else:
        wind_direction = int(dir_str)

    # Parse speed and gust (convert knots to km/h)
    wind_speed_kt = int(speed_str)
    wind_speed_kmh = knots_to_kmh(wind_speed_kt) if wind_speed_kt > 0 else None

    wind_gust_kmh = None
    if gust_str:
        wind_gust_kt = int(gust_str)
        wind_gust_kmh = knots_to_kmh(wind_gust_kt)

    return wind_direction, wind_speed_kmh, wind_gust_kmh


def fetch_nws_observation(station_id, url):
    """
    Fetch latest observation from NWS API.

    Args:
        station_id: Station identifier (e.g., 'KBLI')
        url: NWS API URL for station

    Returns:
        Dictionary with parsed observation data, or None if fetch fails
    """
    try:
        logger.info(f"Fetching NWS data for {station_id} from {url}")

        # NWS API requires User-Agent header
        headers = {
            'User-Agent': '(halibutbank.ca, contact@halibutbank.ca)'
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        props = data.get('properties', {})

        # Parse timestamp (ISO 8601 format)
        timestamp_str = props.get('timestamp')
        if not timestamp_str:
            logger.warning(f"No timestamp in NWS response for {station_id}")
            return None

        # Parse ISO 8601 timestamp
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        observation_time = int(dt.timestamp())

        # Extract and convert values
        # Wind speed/direction (NWS API returns km/h, not m/s!)
        wind_speed_kmh = props.get('windSpeed', {}).get('value')  # Already in km/h
        wind_gust_kmh = props.get('windGust', {}).get('value')    # Already in km/h
        wind_direction = props.get('windDirection', {}).get('value')

        # Fallback: Parse METAR rawMessage if API fields are null
        # Many NOAA land stations don't return parsed wind fields, but METAR has the data
        if (wind_direction is None or wind_speed_kmh is None):
            raw_metar = props.get('rawMessage', '')
            if raw_metar:
                metar_dir, metar_speed, metar_gust = parse_metar_wind(raw_metar)
                if metar_dir is not None:
                    wind_direction = metar_dir
                    logger.debug(f"Parsed wind direction from METAR: {metar_dir}°")
                if metar_speed is not None:
                    wind_speed_kmh = metar_speed
                    logger.debug(f"Parsed wind speed from METAR: {metar_speed:.1f} km/h")
                # Use METAR gust only if API didn't provide it
                if wind_gust_kmh is None and metar_gust is not None:
                    wind_gust_kmh = metar_gust
                    logger.debug(f"Parsed wind gust from METAR: {metar_gust:.1f} km/h")

        # Temperature
        air_temp_c = props.get('temperature', {}).get('value')
        dewpoint_c = props.get('dewpoint', {}).get('value')
        wind_chill_c = props.get('windChill', {}).get('value')
        heat_index_c = props.get('heatIndex', {}).get('value')

        # Pressure (use sea level pressure if available, otherwise barometric)
        pressure_pa = props.get('seaLevelPressure', {}).get('value')
        if pressure_pa is None:
            pressure_pa = props.get('barometricPressure', {}).get('value')

        # Other metrics
        humidity = props.get('relativeHumidity', {}).get('value')
        visibility_m = props.get('visibility', {}).get('value')

        # Precipitation (3hr is more commonly available than 1hr for airports)
        precip_3hr_mm = props.get('precipitationLast3Hours', {}).get('value')
        if precip_3hr_mm is not None:
            # Convert meters to mm
            precip_3hr_mm = precip_3hr_mm * 1000.0

        # Convert units (wind speed already in km/h, no conversion needed!)
        pressure_hpa = pa_to_hpa(pressure_pa)
        visibility_km = m_to_km(visibility_m)

        observation = {
            'station_id': station_id,
            'station_name': NWS_STATIONS[station_id]['name'],
            'observation_time': observation_time,
            'wind_speed_kmh': wind_speed_kmh,
            'wind_gust_kmh': wind_gust_kmh,
            'wind_direction_deg': int(wind_direction) if wind_direction is not None else None,
            'air_temp_c': air_temp_c,
            'dewpoint_c': dewpoint_c,
            'wind_chill_c': wind_chill_c,
            'heat_index_c': heat_index_c,
            'pressure_mslp_hpa': pressure_hpa,  # Use MSLP field since NWS provides sea level
            'humidity_percent': humidity,
            'visibility_km': visibility_km,
            'rainfall_1hr_mm': None,  # Not commonly available from airports
            'rainfall_6hr_mm': precip_3hr_mm,  # Store 3hr precip in 6hr field for now
            'source_file': f'NWS API: {url}'
        }

        wind_str = f"{wind_speed_kmh:.1f}" if wind_speed_kmh is not None else "N/A"
        temp_str = f"{air_temp_c:.1f}" if air_temp_c is not None else "N/A"
        logger.info(f"Successfully parsed NWS observation for {station_id}: "
                   f"Wind {wind_str} km/h, Temp {temp_str}°C")

        return observation

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch NWS data for {station_id}: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Failed to parse NWS data for {station_id}: {e}")
        return None


def insert_observation(observation):
    """Insert observation into wind_data.sqlite."""
    try:
        conn = sqlite3.connect(WIND_DATABASE)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO wind_observation (
                station_id, observation_time, wind_speed_kmh, wind_gust_kmh,
                wind_direction_deg, air_temp_c, pressure_hpa, rainfall_1hr_mm,
                rainfall_6hr_mm, source_file, humidity_percent, dewpoint_c,
                pressure_mslp_hpa, visibility_km, station_name, wind_chill_c,
                heat_index_c
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            observation['station_id'],
            observation['observation_time'],
            observation['wind_speed_kmh'],
            observation['wind_gust_kmh'],
            observation['wind_direction_deg'],
            observation['air_temp_c'],
            None,  # pressure_hpa (leaving null, using MSLP instead)
            observation['rainfall_1hr_mm'],
            observation['rainfall_6hr_mm'],
            observation['source_file'],
            observation['humidity_percent'],
            observation['dewpoint_c'],
            observation['pressure_mslp_hpa'],
            observation['visibility_km'],
            observation['station_name'],
            observation['wind_chill_c'],
            observation['heat_index_c']
        ))

        conn.commit()
        conn.close()

        logger.info(f"Inserted observation for {observation['station_id']} at "
                   f"{datetime.fromtimestamp(observation['observation_time'], tz=timezone.utc).isoformat()}")

    except sqlite3.Error as e:
        logger.error(f"Database error inserting {observation['station_id']}: {e}")


def main():
    """Fetch all NWS stations and store to database."""
    logger.info("=== NWS Weather Fetch Started ===")

    success_count = 0
    fail_count = 0

    for station_id, config in NWS_STATIONS.items():
        observation = fetch_nws_observation(station_id, config['url'])

        if observation:
            insert_observation(observation)
            success_count += 1
        else:
            fail_count += 1

    logger.info(f"=== NWS Weather Fetch Complete: {success_count} success, {fail_count} failed ===")


if __name__ == '__main__':
    main()
