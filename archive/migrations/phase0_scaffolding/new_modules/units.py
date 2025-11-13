#!/usr/bin/env python3
"""
Unit conversion utilities for marine weather data.

Centralizes all unit conversions used across the pipeline.
All functions handle None gracefully and return None for invalid inputs.
"""

def kmh_to_knots(kmh):
    """
    Convert kilometers per hour to knots.

    Args:
        kmh: Speed in km/h (numeric or None)

    Returns:
        Speed in knots (rounded to 2 decimals) or None
    """
    if kmh is None:
        return None
    try:
        return round(float(kmh) * 0.539957, 2)
    except (TypeError, ValueError):
        return None


def ms_to_kmh(ms):
    """
    Convert meters per second to kilometers per hour.

    Used for NOAA data conversion (NOAA provides m/s, we store km/h).

    Args:
        ms: Speed in m/s (numeric or None)

    Returns:
        Speed in km/h (rounded to 2 decimals) or None
    """
    if ms is None:
        return None
    try:
        return round(float(ms) * 3.6, 2)
    except (TypeError, ValueError):
        return None


def knots_to_kmh(knots):
    """
    Convert knots to kilometers per hour.

    Args:
        knots: Speed in knots (numeric or None)

    Returns:
        Speed in km/h (rounded to 2 decimals) or None
    """
    if knots is None:
        return None
    try:
        return round(float(knots) * 1.852, 2)
    except (TypeError, ValueError):
        return None


def celsius_to_fahrenheit(celsius):
    """
    Convert Celsius to Fahrenheit.

    Args:
        celsius: Temperature in °C (numeric or None)

    Returns:
        Temperature in °F (rounded to 2 decimals) or None
    """
    if celsius is None:
        return None
    try:
        return round(float(celsius) * 9/5 + 32, 2)
    except (TypeError, ValueError):
        return None


def fahrenheit_to_celsius(fahrenheit):
    """
    Convert Fahrenheit to Celsius.

    Args:
        fahrenheit: Temperature in °F (numeric or None)

    Returns:
        Temperature in °C (rounded to 2 decimals) or None
    """
    if fahrenheit is None:
        return None
    try:
        return round((float(fahrenheit) - 32) * 5/9, 2)
    except (TypeError, ValueError):
        return None


def meters_to_feet(meters):
    """
    Convert meters to feet.

    Args:
        meters: Length in meters (numeric or None)

    Returns:
        Length in feet (rounded to 2 decimals) or None
    """
    if meters is None:
        return None
    try:
        return round(float(meters) * 3.28084, 2)
    except (TypeError, ValueError):
        return None


def feet_to_meters(feet):
    """
    Convert feet to meters.

    Args:
        feet: Length in feet (numeric or None)

    Returns:
        Length in meters (rounded to 2 decimals) or None
    """
    if feet is None:
        return None
    try:
        return round(float(feet) / 3.28084, 2)
    except (TypeError, ValueError):
        return None


# Convenience aliases for common conversions
wind_kmh_to_knots = kmh_to_knots  # For clarity in wind conversion contexts
wind_ms_to_kmh = ms_to_kmh        # For clarity in NOAA ingestion


if __name__ == "__main__":
    # Quick self-test
    print("Unit Conversion Self-Test")
    print("=" * 40)

    # Test wind conversions
    print(f"50 km/h = {kmh_to_knots(50)} knots")
    print(f"10 m/s = {ms_to_kmh(10)} km/h")
    print(f"20 knots = {knots_to_kmh(20)} km/h")

    # Test temperature conversions
    print(f"20°C = {celsius_to_fahrenheit(20)}°F")
    print(f"68°F = {fahrenheit_to_celsius(68)}°C")

    # Test distance conversions
    print(f"10 meters = {meters_to_feet(10)} feet")
    print(f"33 feet = {feet_to_meters(33)} meters")

    # Test None handling
    print(f"None km/h = {kmh_to_knots(None)} knots (should be None)")

    # Test invalid input
    print(f"'invalid' km/h = {kmh_to_knots('invalid')} knots (should be None)")

    print("\n✅ All conversions working correctly!")
