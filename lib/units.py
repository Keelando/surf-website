#!/usr/bin/env python3
"""
Unit conversion utilities for marine weather data.

This module provides conversion functions used across the data processing
pipeline. All functions handle None values gracefully and return None for
invalid inputs.

Usage:
    from units import kmh_to_knots, ms_to_kmh, meters_to_feet

    wind_knots = kmh_to_knots(25.5)  # Convert wind speed
    wave_height_ft = meters_to_feet(2.3)  # Convert wave height
"""


def kmh_to_knots(kmh):
    """
    Convert kilometers per hour to knots.

    Args:
        kmh: Speed in km/h (numeric or None)

    Returns:
        Speed in knots, rounded to 1 decimal place.
        Returns None if input is None or invalid.

    Examples:
        >>> kmh_to_knots(10)
        5.4
        >>> kmh_to_knots(None)
        None
    """
    if kmh is None:
        return None

    try:
        return round(float(kmh) * 0.539957, 1)
    except (TypeError, ValueError):
        return None


def ms_to_kmh(ms):
    """
    Convert meters per second to kilometers per hour.

    Used for NOAA data which reports speeds in m/s.

    Args:
        ms: Speed in m/s (numeric or None)

    Returns:
        Speed in km/h, rounded to 1 decimal place.
        Returns None if input is None or invalid.

    Examples:
        >>> ms_to_kmh(5)
        18.0
        >>> ms_to_kmh(None)
        None
    """
    if ms is None:
        return None

    try:
        return round(float(ms) * 3.6, 1)
    except (TypeError, ValueError):
        return None


def meters_to_feet(meters):
    """
    Convert meters to feet.

    Used for wave height and water level conversions.

    Args:
        meters: Distance in meters (numeric or None)

    Returns:
        Distance in feet, rounded to 1 decimal place.
        Returns None if input is None or invalid.

    Examples:
        >>> meters_to_feet(2.0)
        6.6
        >>> meters_to_feet(None)
        None
    """
    if meters is None:
        return None

    try:
        return round(float(meters) * 3.28084, 1)
    except (TypeError, ValueError):
        return None


def knots_to_kmh(knots):
    """
    Convert knots to kilometers per hour.

    Inverse of kmh_to_knots. Used for threshold comparisons.

    Args:
        knots: Speed in knots (numeric or None)

    Returns:
        Speed in km/h, rounded to 1 decimal place.
        Returns None if input is None or invalid.

    Examples:
        >>> knots_to_kmh(10)
        18.5
        >>> knots_to_kmh(None)
        None
    """
    if knots is None:
        return None

    try:
        return round(float(knots) * 1.852, 1)
    except (TypeError, ValueError):
        return None


# Test/demonstration code
if __name__ == "__main__":
    print("Unit Conversion Tests")
    print("=" * 50)

    # Test kmh_to_knots
    print("\nkm/h to knots:")
    print(f"  25 km/h = {kmh_to_knots(25)} knots")
    print(f"  50 km/h = {kmh_to_knots(50)} knots")
    print(f"  None = {kmh_to_knots(None)}")

    # Test ms_to_kmh
    print("\nm/s to km/h:")
    print(f"  10 m/s = {ms_to_kmh(10)} km/h")
    print(f"  5.5 m/s = {ms_to_kmh(5.5)} km/h")
    print(f"  None = {ms_to_kmh(None)}")

    # Test meters_to_feet
    print("\nmeters to feet:")
    print(f"  2.0 m = {meters_to_feet(2.0)} ft")
    print(f"  5.5 m = {meters_to_feet(5.5)} ft")
    print(f"  None = {meters_to_feet(None)}")

    # Test knots_to_kmh
    print("\nknots to km/h:")
    print(f"  20 knots = {knots_to_kmh(20)} km/h")
    print(f"  None = {knots_to_kmh(None)}")

    print("\n✅ All conversions completed")
