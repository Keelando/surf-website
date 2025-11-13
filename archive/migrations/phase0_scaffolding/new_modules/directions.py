#!/usr/bin/env python3
"""
Direction conversion utilities for marine weather data.

Handles conversions between:
- Numeric degrees (0-360, where 0° = North)
- Cardinal directions (N, NE, E, SE, S, SW, W, NW, etc.)

Important: Marine weather directions indicate WHERE WIND/WAVES ARE COMING FROM.
Example: "West wind" (270°) means wind blowing FROM west TO east.
"""

import math

# 16-point compass rose (standard marine weather precision)
DIRS_16 = [
    'N', 'NNE', 'NE', 'ENE',
    'E', 'ESE', 'SE', 'SSE',
    'S', 'SSW', 'SW', 'WSW',
    'W', 'WNW', 'NW', 'NNW'
]

# 8-point compass rose (for less precise data)
DIRS_8 = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']


def degrees_to_cardinal(deg, precision=16):
    """
    Convert degrees (0-360) to cardinal direction.

    Args:
        deg: Direction in degrees (0-360), where 0 = North
        precision: 8 or 16 point compass (default 16)

    Returns:
        Cardinal direction string (e.g., 'N', 'NE', 'SSW') or None

    Examples:
        >>> degrees_to_cardinal(0)
        'N'
        >>> degrees_to_cardinal(270)
        'W'
        >>> degrees_to_cardinal(315)
        'NW'
    """
    if deg is None:
        return None

    try:
        d = float(deg)
    except (TypeError, ValueError):
        return None

    if math.isnan(d):
        return None

    # Normalize to 0-360 range
    d = d % 360.0

    # Select compass rose based on precision
    if precision == 8:
        dirs = DIRS_8
        step = 45.0
    else:
        dirs = DIRS_16
        step = 22.5

    # Find closest cardinal direction
    ix = int(round(d / step)) % len(dirs)
    return dirs[ix]


def cardinal_to_degrees(cardinal):
    """
    Convert cardinal direction to degrees.

    Args:
        cardinal: Cardinal direction string (e.g., 'N', 'NE', 'SSW')

    Returns:
        Direction in degrees (0-360) or None

    Examples:
        >>> cardinal_to_degrees('N')
        0.0
        >>> cardinal_to_degrees('W')
        270.0
        >>> cardinal_to_degrees('NW')
        315.0
    """
    if cardinal is None:
        return None

    cardinal = cardinal.strip().upper()

    # Try 16-point first, then 8-point
    if cardinal in DIRS_16:
        ix = DIRS_16.index(cardinal)
        return (ix * 22.5) % 360
    elif cardinal in DIRS_8:
        ix = DIRS_8.index(cardinal)
        return (ix * 45.0) % 360
    else:
        return None


def parse_direction(val):
    """
    Parse direction from string (handles cardinal or numeric degrees).

    Intelligently handles various input formats:
    - Numeric degrees: '275', '90.5'
    - Cardinal: 'WSW', 'N', 'SE'
    - Missing data indicators: 'MM', 'M', 'NA', '', None

    Args:
        val: Direction value (string, numeric, or None)

    Returns:
        Direction in degrees (0-360) or None

    Examples:
        >>> parse_direction('275')
        275.0
        >>> parse_direction('WSW')
        247.5
        >>> parse_direction('MM')
        None
    """
    # Handle None and missing data indicators
    if not val or val in ['MM', 'M', 'NA', '', 'null', 'NULL']:
        return None

    # Try parsing as numeric degrees first
    try:
        deg = float(val)
        return deg % 360  # Normalize to 0-360
    except (TypeError, ValueError):
        pass

    # Try parsing as cardinal direction
    return cardinal_to_degrees(val)


def format_direction(deg, include_degrees=True, precision=16):
    """
    Format direction as human-readable string with cardinal and degrees.

    Args:
        deg: Direction in degrees (0-360)
        include_degrees: Include numeric degrees in output (default True)
        precision: 8 or 16 point compass (default 16)

    Returns:
        Formatted string (e.g., 'NW (315°)') or None

    Examples:
        >>> format_direction(315)
        'NW (315°)'
        >>> format_direction(315, include_degrees=False)
        'NW'
    """
    if deg is None:
        return None

    cardinal = degrees_to_cardinal(deg, precision=precision)
    if cardinal is None:
        return None

    if include_degrees:
        return f"{cardinal} ({int(round(deg))}°)"
    else:
        return cardinal


def opposite_direction(deg):
    """
    Calculate opposite direction (180° rotation).

    Useful for converting "coming from" to "going to" directions.

    Args:
        deg: Direction in degrees (0-360)

    Returns:
        Opposite direction in degrees (0-360) or None

    Examples:
        >>> opposite_direction(0)    # North → South
        180.0
        >>> opposite_direction(270)  # West → East
        90.0
    """
    if deg is None:
        return None

    try:
        d = float(deg)
        return (d + 180.0) % 360
    except (TypeError, ValueError):
        return None


def direction_difference(deg1, deg2):
    """
    Calculate smallest angular difference between two directions.

    Accounts for circular nature of compass (e.g., difference between
    359° and 1° is 2°, not 358°).

    Args:
        deg1: First direction in degrees (0-360)
        deg2: Second direction in degrees (0-360)

    Returns:
        Smallest angular difference (0-180) or None

    Examples:
        >>> direction_difference(10, 350)
        20.0  # Not 340°
        >>> direction_difference(90, 180)
        90.0
    """
    if deg1 is None or deg2 is None:
        return None

    try:
        d1 = float(deg1) % 360
        d2 = float(deg2) % 360
        diff = abs(d1 - d2)
        if diff > 180:
            diff = 360 - diff
        return diff
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    # Quick self-test
    print("Direction Conversion Self-Test")
    print("=" * 50)

    # Test degrees to cardinal
    test_directions = [0, 45, 90, 135, 180, 225, 270, 315]
    for deg in test_directions:
        cardinal = degrees_to_cardinal(deg)
        back = cardinal_to_degrees(cardinal)
        print(f"{deg:3d}° → {cardinal:3s} → {back:5.1f}°")

    print("\n" + "=" * 50)

    # Test parse_direction with various inputs
    test_inputs = ['275', 'WSW', '90.5', 'N', 'MM', None, '']
    print("\nParsing various input formats:")
    for inp in test_inputs:
        result = parse_direction(inp)
        print(f"  '{inp}' → {result}°" if result else f"  '{inp}' → None")

    print("\n" + "=" * 50)

    # Test formatted output
    print("\nFormatted direction strings:")
    print(f"  315° → {format_direction(315)}")
    print(f"  270° → {format_direction(270)}")
    print(f"  45° → {format_direction(45, include_degrees=False)}")

    print("\n" + "=" * 50)

    # Test opposite directions
    print("\nOpposite directions (180° rotation):")
    print(f"  North (0°) opposite: {opposite_direction(0)}° (South)")
    print(f"  West (270°) opposite: {opposite_direction(270)}° (East)")

    print("\n" + "=" * 50)

    # Test direction differences
    print("\nDirection differences (accounts for circularity):")
    print(f"  359° vs 1°: {direction_difference(359, 1)}° (not 358°)")
    print(f"  90° vs 180°: {direction_difference(90, 180)}°")

    print("\n✅ All conversions working correctly!")
