#!/usr/bin/env python3
"""
Test suite for Phase 0 new modules (units.py, directions.py, config.py).

Run this AFTER copying new modules to verify they work correctly.

Usage:
    python3 test_new_modules.py
"""

import sys
from pathlib import Path

# Add current directory to path (for root-level imports)
sys.path.insert(0, str(Path.cwd()))

def test_units():
    """Test units.py module."""
    print("\n" + "="*60)
    print("Testing units.py")
    print("="*60)

    try:
        from units import kmh_to_knots, ms_to_kmh, knots_to_kmh
    except ImportError as e:
        print(f"❌ FAILED to import units: {e}")
        return False

    tests = [
        # (function, input, expected_output, description)
        (kmh_to_knots, 50, 26.998, "50 km/h → knots"),
        (ms_to_kmh, 10, 36.0, "10 m/s → km/h"),
        (knots_to_kmh, 20, 37.04, "20 knots → km/h"),
        (kmh_to_knots, None, None, "None handling"),
        (kmh_to_knots, "invalid", None, "Invalid input handling"),
    ]

    passed = 0
    failed = 0

    for func, inp, expected, desc in tests:
        result = func(inp)
        if result == expected or (expected is not None and abs(result - expected) < 0.01):
            print(f"  ✅ {desc}: {result}")
            passed += 1
        else:
            print(f"  ❌ {desc}: Expected {expected}, got {result}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_directions():
    """Test directions.py module."""
    print("\n" + "="*60)
    print("Testing directions.py")
    print("="*60)

    try:
        from directions import degrees_to_cardinal, cardinal_to_degrees, parse_direction, DIRS_16
    except ImportError as e:
        print(f"❌ FAILED to import directions: {e}")
        return False

    tests = [
        # (function, input, expected_output, description)
        (degrees_to_cardinal, 0, "N", "0° → N"),
        (degrees_to_cardinal, 90, "E", "90° → E"),
        (degrees_to_cardinal, 180, "S", "180° → S"),
        (degrees_to_cardinal, 270, "W", "270° → W"),
        (degrees_to_cardinal, 315, "NW", "315° → NW"),
        (cardinal_to_degrees, "N", 0.0, "N → 0°"),
        (cardinal_to_degrees, "W", 270.0, "W → 270°"),
        (parse_direction, "275", 275.0, "Parse numeric string"),
        (parse_direction, "WSW", 247.5, "Parse cardinal string"),
        (parse_direction, "MM", None, "Missing data indicator"),
        (parse_direction, None, None, "None handling"),
    ]

    passed = 0
    failed = 0

    for func, inp, expected, desc in tests:
        result = func(inp)
        if result == expected or (expected is not None and isinstance(result, float) and abs(result - expected) < 0.1):
            print(f"  ✅ {desc}: {result}")
            passed += 1
        else:
            print(f"  ❌ {desc}: Expected {expected}, got {result}")
            failed += 1

    # Test DIRS_16 list
    if len(DIRS_16) == 16:
        print(f"  ✅ DIRS_16 has 16 directions")
        passed += 1
    else:
        print(f"  ❌ DIRS_16 should have 16 directions, has {len(DIRS_16)}")
        failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_config():
    """Test config.py module."""
    print("\n" + "="*60)
    print("Testing config.py")
    print("="*60)

    try:
        from config import (
            BUOY_DATABASE, TIDE_DATABASE, STORM_SURGE_DATABASE,
            BUOY_FRESHNESS_WINDOW, BUOY_FIELDS, FIELD_METADATA,
            get_database_path, validate_config
        )
    except ImportError as e:
        print(f"❌ FAILED to import config: {e}")
        return False

    passed = 0
    failed = 0

    # Test database paths are Path objects
    if isinstance(BUOY_DATABASE, Path):
        print(f"  ✅ BUOY_DATABASE is Path: {BUOY_DATABASE}")
        passed += 1
    else:
        print(f"  ❌ BUOY_DATABASE should be Path, is {type(BUOY_DATABASE)}")
        failed += 1

    if isinstance(TIDE_DATABASE, Path):
        print(f"  ✅ TIDE_DATABASE is Path: {TIDE_DATABASE}")
        passed += 1
    else:
        print(f"  ❌ TIDE_DATABASE should be Path, is {type(TIDE_DATABASE)}")
        failed += 1

    # Test freshness window is reasonable (2 hours = 7200 seconds)
    if BUOY_FRESHNESS_WINDOW == 7200:
        print(f"  ✅ BUOY_FRESHNESS_WINDOW: {BUOY_FRESHNESS_WINDOW} seconds (2 hours)")
        passed += 1
    else:
        print(f"  ⚠️  BUOY_FRESHNESS_WINDOW: {BUOY_FRESHNESS_WINDOW} seconds (expected 7200)")
        # Don't fail, just warn

    # Test BUOY_FIELDS has expected fields
    expected_fields = ['wave_height_sig', 'wind_speed', 'sea_temp']
    for field in expected_fields:
        if field in BUOY_FIELDS:
            print(f"  ✅ BUOY_FIELDS contains '{field}'")
            passed += 1
        else:
            print(f"  ❌ BUOY_FIELDS missing '{field}'")
            failed += 1

    # Test FIELD_METADATA has entries
    if len(FIELD_METADATA) > 10:
        print(f"  ✅ FIELD_METADATA has {len(FIELD_METADATA)} entries")
        passed += 1
    else:
        print(f"  ❌ FIELD_METADATA should have 10+ entries, has {len(FIELD_METADATA)}")
        failed += 1

    # Test helper functions
    try:
        buoy_db = get_database_path('buoy')
        if buoy_db == BUOY_DATABASE:
            print(f"  ✅ get_database_path('buoy') works")
            passed += 1
        else:
            print(f"  ❌ get_database_path('buoy') returned wrong path")
            failed += 1
    except Exception as e:
        print(f"  ❌ get_database_path('buoy') raised: {e}")
        failed += 1

    # Test validate_config (doesn't fail if dirs don't exist yet)
    try:
        validate_config()
        print(f"  ✅ validate_config() runs without error")
        passed += 1
    except Exception as e:
        print(f"  ❌ validate_config() raised: {e}")
        failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_stations_integration():
    """Test that stations.py integration works."""
    print("\n" + "="*60)
    print("Testing stations.py integration")
    print("="*60)

    try:
        from stations import get_all_buoys, get_tide_station
    except ImportError as e:
        print(f"❌ FAILED to import stations: {e}")
        return False

    passed = 0
    failed = 0

    # Test get_all_buoys()
    try:
        buoys = get_all_buoys()
        if isinstance(buoys, dict) and len(buoys) > 0:
            print(f"  ✅ get_all_buoys() returns {len(buoys)} buoys")
            passed += 1

            # Check structure
            sample_buoy = next(iter(buoys.values()))
            if 'name' in sample_buoy and 'location' in sample_buoy:
                print(f"  ✅ Buoy data has correct structure (name, location)")
                passed += 1
            else:
                print(f"  ❌ Buoy data missing required fields")
                failed += 1
        else:
            print(f"  ❌ get_all_buoys() should return non-empty dict")
            failed += 1
    except Exception as e:
        print(f"  ❌ get_all_buoys() raised: {e}")
        failed += 1

    # Test get_tide_station()
    try:
        station = get_tide_station('point_atkinson')
        if station and 'name' in station:
            print(f"  ✅ get_tide_station() works: {station['name']}")
            passed += 1
        else:
            print(f"  ❌ get_tide_station() returned invalid data")
            failed += 1
    except Exception as e:
        print(f"  ❌ get_tide_station() raised: {e}")
        failed += 1

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("Phase 0 Module Testing Suite")
    print("="*60)

    all_passed = True

    # Test new modules
    all_passed &= test_units()
    all_passed &= test_directions()
    all_passed &= test_config()
    all_passed &= test_stations_integration()

    # Summary
    print("\n" + "="*60)
    if all_passed:
        print("✅ ALL TESTS PASSED!")
        print("   New modules are working correctly.")
        print("   Ready to test scripts that use them.")
    else:
        print("❌ SOME TESTS FAILED")
        print("   Review errors above and fix before proceeding.")
    print("="*60 + "\n")

    return 0 if all_passed else 1


if __name__ == '__main__':
    exit(main())
