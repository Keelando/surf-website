#!/usr/bin/env python3
"""
Direction and heading utilities for wind and wave data.

This module provides functions to convert meteorological directions (degrees)
to human-readable cardinal directions (N, NE, E, etc.).

Important: Meteorological directions indicate WHERE wind/waves are COMING FROM,
not where they're going.

Usage:
    from directions import degrees_to_cardinal, DIRS_16

    wind_dir = degrees_to_cardinal(270)  # Returns "W" (west wind)
    wave_dir = degrees_to_cardinal(315, points=8)  # Returns "NW" (8-point)
"""

# 16-point compass rose (most precise)
DIRS_16 = [
    "N", "NNE", "NE", "ENE",
    "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW",
    "W", "WNW", "NW", "NNW"
]

# 8-point compass rose (less precise)
DIRS_8 = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def degrees_to_cardinal(degrees, points=16):
    """
    Convert degrees (0-360) to cardinal direction.

    Meteorological convention: Direction indicates where wind/waves are
    coming FROM (not going TO).

    Args:
        degrees: Heading in degrees (0-360)
                 - 0° = North
                 - 90° = East
                 - 180° = South
                 - 270° = West
        points: Number of compass points (8 or 16)
                8 = N, NE, E, SE, S, SW, W, NW
                16 = adds NNE, ENE, ESE, SSE, SSW, WSW, WNW, NNW

    Returns:
        Cardinal direction string (e.g., "NW", "SSE")
        Returns None if degrees is None or invalid

    Examples:
        >>> degrees_to_cardinal(0)
        'N'
        >>> degrees_to_cardinal(45)
        'NE'
        >>> degrees_to_cardinal(270)
        'W'
        >>> degrees_to_cardinal(315, points=8)
        'NW'
        >>> degrees_to_cardinal(None)
        None
    """
    if degrees is None:
        return None

    try:
        degrees = float(degrees)
    except (TypeError, ValueError):
        return None

    # Normalize to 0-360 range
    degrees = degrees % 360

    if points == 8:
        # 8-point compass: 45° per direction
        # Add 22.5° offset so ranges are centered on cardinal directions
        index = int((degrees + 22.5) / 45) % 8
        return DIRS_8[index]
    elif points == 16:
        # 16-point compass: 22.5° per direction
        # Add 11.25° offset so ranges are centered on cardinal directions
        index = int((degrees + 11.25) / 22.5) % 16
        return DIRS_16[index]
    else:
        raise ValueError(f"points must be 8 or 16, got {points}")


def cardinal_to_degrees(cardinal):
    """
    Convert cardinal direction to approximate degrees.

    Returns the center angle for the given direction.

    Args:
        cardinal: Cardinal direction string (e.g., "N", "NE", "SSW")

    Returns:
        Degrees (0-360) as integer, or None if invalid

    Examples:
        >>> cardinal_to_degrees("N")
        0
        >>> cardinal_to_degrees("E")
        90
        >>> cardinal_to_degrees("NW")
        315
        >>> cardinal_to_degrees("invalid")
        None
    """
    if cardinal is None:
        return None

    cardinal = cardinal.upper().strip()

    # Try 16-point first (more specific)
    if cardinal in DIRS_16:
        index = DIRS_16.index(cardinal)
        return int(index * 22.5)

    # Fall back to 8-point
    if cardinal in DIRS_8:
        index = DIRS_8.index(cardinal)
        return int(index * 45)

    return None


def is_offshore_wind(wind_direction_degrees, coast_bearing):
    """
    Determine if wind is blowing offshore based on wind direction and coast bearing.

    Args:
        wind_direction_degrees: Direction wind is coming FROM (0-360)
        coast_bearing: Direction coast faces (perpendicular to shore)
                      E.g., for west-facing coast, use 270 (west)

    Returns:
        True if wind is offshore, False if onshore, None if invalid input

    Examples:
        >>> is_offshore_wind(270, 270)  # West wind on west-facing coast
        True
        >>> is_offshore_wind(90, 270)  # East wind on west-facing coast
        False
    """
    if wind_direction_degrees is None or coast_bearing is None:
        return None

    try:
        wind_direction_degrees = float(wind_direction_degrees)
        coast_bearing = float(coast_bearing)
    except (TypeError, ValueError):
        return None

    # Normalize
    wind_direction_degrees = wind_direction_degrees % 360
    coast_bearing = coast_bearing % 360

    # Wind is offshore if coming from land side
    # Check if wind direction is within 90° of coast bearing
    diff = abs(wind_direction_degrees - coast_bearing)
    if diff > 180:
        diff = 360 - diff

    return diff < 90


# Test/demonstration code
if __name__ == "__main__":
    print("Direction Conversion Tests")
    print("=" * 50)

    # Test cardinal directions (16-point)
    print("\n16-point compass:")
    test_angles = [0, 22.5, 45, 90, 135, 180, 225, 270, 315, 359]
    for angle in test_angles:
        cardinal = degrees_to_cardinal(angle, points=16)
        print(f"  {angle:5.1f}° = {cardinal}")

    # Test cardinal directions (8-point)
    print("\n8-point compass:")
    for angle in [0, 45, 90, 135, 180, 225, 270, 315]:
        cardinal = degrees_to_cardinal(angle, points=8)
        print(f"  {angle:5.1f}° = {cardinal}")

    # Test reverse conversion
    print("\nCardinal to degrees:")
    for direction in ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]:
        degrees = cardinal_to_degrees(direction)
        print(f"  {direction} = {degrees}°")

    # Test offshore wind detection
    print("\nOffshore wind detection (west-facing coast at 270°):")
    test_winds = [0, 90, 180, 270, 315]
    for wind_dir in test_winds:
        offshore = is_offshore_wind(wind_dir, 270)
        status = "offshore" if offshore else "onshore"
        cardinal = degrees_to_cardinal(wind_dir)
        print(f"  {cardinal:3s} wind ({wind_dir:3d}°) = {status}")

    # Test None handling
    print("\nNone handling:")
    print(f"  degrees_to_cardinal(None) = {degrees_to_cardinal(None)}")
    print(f"  cardinal_to_degrees(None) = {cardinal_to_degrees(None)}")

    print("\n✅ All direction tests completed")
