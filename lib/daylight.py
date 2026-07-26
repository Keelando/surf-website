#!/usr/bin/env python3
"""
Daylight Detection Utility

Determines whether it's currently daylight hours at a given location.
Uses the astral library for accurate sunrise/sunset calculations.

Usage:
    from lib.daylight import is_daylight

    if is_daylight(49.0, -122.8):
        # It's daylight, capture webcam image
        pass
    else:
        # It's nighttime, skip capture
        pass
"""

from datetime import datetime, timedelta, timezone

from astral import LocationInfo
from astral.sun import sun


def calculate_sunrise_sunset(lat, lon, date=None):
    """
    Calculate sunrise and sunset times for a given location and date.

    Args:
        lat: Latitude in decimal degrees (positive = North, negative = South)
        lon: Longitude in decimal degrees (positive = East, negative = West)
        date: datetime object in UTC (defaults to now in UTC)

    Returns:
        tuple: (sunrise_datetime, sunset_datetime) in UTC
    """
    if date is None:
        date = datetime.now(timezone.utc)

    # Create a LocationInfo object (name is arbitrary)
    location = LocationInfo("Custom", "Region", "UTC", lat, lon)

    # IMPORTANT: Calculate sunrise/sunset for the LOCAL date at this location,
    # not the UTC date. This fixes timezone boundary issues where UTC midnight
    # occurs in the middle of the local day (e.g., 4 PM PST = 00:00 UTC next day).
    #
    # For locations west of UTC (negative longitude = west), we need to account
    # for the fact that their local time is behind UTC. A simple approximation
    # is to subtract the longitude-based timezone offset.
    #
    # More robust: estimate timezone offset from longitude (15° ≈ 1 hour)
    estimated_tz_offset_hours = lon / 15.0  # Positive = east of UTC, negative = west
    local_offset = timedelta(hours=estimated_tz_offset_hours)
    local_date = date + local_offset

    # Use the local date to calculate sunrise/sunset
    try:
        s = sun(location.observer, date=local_date.date(), tzinfo=timezone.utc)
        sunrise_time = s["sunrise"]
        sunset_time = s["sunset"]

        # Fix for timezone crossing midnight: if sunset appears before sunrise in UTC,
        # it's actually on the next day
        if sunset_time < sunrise_time:
            sunset_time = sunset_time + timedelta(days=1)

        return sunrise_time, sunset_time
    except ValueError:
        # Handle polar day/night cases where sun never rises or sets
        return None, None


def is_daylight(lat, lon, margin_minutes=30, current_time=None):
    """
    Check if it's currently daylight at the given location.

    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        margin_minutes: Minutes to add before sunrise and after sunset
                       (e.g., 30 means start capturing 30 min before sunrise
                       and stop 30 min after sunset)
        current_time: datetime object to check (defaults to now in UTC)

    Returns:
        bool: True if it's daylight (or within margin), False otherwise
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    sunrise, sunset = calculate_sunrise_sunset(lat, lon, current_time)

    # Handle polar day/night
    if sunrise is None or sunset is None:
        # In polar regions during extreme seasons
        # For simplicity, we'll say it's never daylight if we can't calculate
        # (in practice, you'd want more sophisticated handling)
        return False

    # Add margin
    margin = timedelta(minutes=margin_minutes)
    sunrise_with_margin = sunrise - margin
    sunset_with_margin = sunset + margin

    return sunrise_with_margin <= current_time <= sunset_with_margin


def get_daylight_info(lat, lon, date=None):
    """
    Get detailed daylight information for a location.

    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        date: datetime object (defaults to today)

    Returns:
        dict: Contains sunrise, sunset times and current daylight status
    """
    if date is None:
        date = datetime.now(timezone.utc)

    sunrise, sunset = calculate_sunrise_sunset(lat, lon, date)
    current_is_daylight = is_daylight(lat, lon, current_time=date)

    return {
        "sunrise": sunrise.isoformat() if sunrise else None,
        "sunset": sunset.isoformat() if sunset else None,
        "is_daylight": current_is_daylight,
        "checked_at": date.isoformat(),
    }


# Location presets for common webcam locations
LOCATIONS = {
    "whiterock": {"name": "White Rock, BC", "lat": 49.0253, "lon": -122.8031},
    "boundarybay": {"name": "Boundary Bay, BC", "lat": 49.0042, "lon": -123.0128},
    "coxbay": {"name": "Cox Bay (Tofino), BC", "lat": 49.1167, "lon": -125.9000},
}


def is_daylight_for_location(location_name, margin_minutes=30):
    """
    Check if it's daylight at a predefined location.

    Args:
        location_name: Key from LOCATIONS dict (e.g., "whiterock")
        margin_minutes: Minutes margin before/after sunrise/sunset

    Returns:
        bool: True if it's daylight, False otherwise

    Raises:
        KeyError: If location_name not found in LOCATIONS
    """
    if location_name not in LOCATIONS:
        raise KeyError(f"Unknown location: {location_name}. Available: {list(LOCATIONS.keys())}")

    loc = LOCATIONS[location_name]
    return is_daylight(loc["lat"], loc["lon"], margin_minutes=margin_minutes)


if __name__ == "__main__":
    """Test/demo the daylight detection"""
    import sys

    if len(sys.argv) > 1:
        location = sys.argv[1]
        if location not in LOCATIONS:
            print(f"Unknown location: {location}")
            print(f"Available locations: {', '.join(LOCATIONS.keys())}")
            sys.exit(1)
    else:
        location = "whiterock"

    loc = LOCATIONS[location]
    print(f"\nDaylight check for {loc['name']} ({loc['lat']}, {loc['lon']})")

    now = datetime.now(timezone.utc)
    print(f"Current time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # Also show local time (Pacific)
    import pytz

    try:
        pacific = pytz.timezone("America/Vancouver")
        local_time = now.astimezone(pacific)
        print(f"Current time (Local): {local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    except Exception:
        pass

    info = get_daylight_info(loc["lat"], loc["lon"])

    if info["sunrise"]:
        sunrise_utc = datetime.fromisoformat(info["sunrise"])
        print(f"\nSunrise (UTC): {sunrise_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        try:
            sunrise_local = sunrise_utc.astimezone(pacific)
            print(f"Sunrise (Local): {sunrise_local.strftime('%H:%M:%S %Z')}")
        except Exception:
            pass

    if info["sunset"]:
        sunset_utc = datetime.fromisoformat(info["sunset"])
        print(f"Sunset (UTC):  {sunset_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        try:
            sunset_local = sunset_utc.astimezone(pacific)
            print(f"Sunset (Local):  {sunset_local.strftime('%H:%M:%S %Z')}")
        except Exception:
            pass

    print(f"\nIs daylight? {info['is_daylight']}")

    # Test with margin
    is_daylight_with_margin = is_daylight(loc["lat"], loc["lon"], margin_minutes=30)
    print(f"Is daylight (with 30min margin)? {is_daylight_with_margin}")
