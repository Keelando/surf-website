#!/usr/bin/env python3
"""
Validate stations.json for completeness and consistency.

Usage:
    python validate_stations.py

Checks:
  ✅ JSON syntax is valid
  ✅ All required fields are present
  ✅ Coordinates are in valid ranges
  ✅ No duplicate IDs or names
  ✅ Data types are reasonable
  ✅ Cross-references are valid
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set

STATIONS_FILE = Path("~/envcan_wave/config/stations.json").expanduser()

# Required fields for each station type
REQUIRED_BUOY_FIELDS = {"id", "name", "location", "lat", "lon", "source", "type", "data_types"}
REQUIRED_TIDE_FIELDS = {"id", "code", "name", "location", "lat", "lon", "source", "type", "series", "data_types"}

# Valid value ranges
VALID_LAT = (-90, 90)
VALID_LON = (-180, 180)
VALID_BUOY_SOURCES = {"Environment Canada", "NOAA NDBC"}
VALID_TIDE_SOURCES = {"DFO IWLS"}
VALID_TIDE_TYPES = {"PERMANENT", "TEMPORARY"}
VALID_TIDE_SERIES = {"wlo", "wlp", "wlp-hilo"}


class ValidationError(Exception):
    """Custom exception for validation failures."""

    pass


def load_stations() -> Dict:
    """Load and parse stations.json."""
    if not STATIONS_FILE.exists():
        raise FileNotFoundError(f"Stations file not found: {STATIONS_FILE}")

    try:
        with open(STATIONS_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON syntax: {e}")


def validate_coordinates(lat: float, lon: float, station_name: str) -> None:
    """Validate latitude and longitude are in valid ranges."""
    if not (VALID_LAT[0] <= lat <= VALID_LAT[1]):
        raise ValidationError(
            f"{station_name}: Invalid latitude {lat} (must be between {VALID_LAT[0]} and {VALID_LAT[1]})"
        )

    if not (VALID_LON[0] <= lon <= VALID_LON[1]):
        raise ValidationError(
            f"{station_name}: Invalid longitude {lon} (must be between {VALID_LON[0]} and {VALID_LON[1]})"
        )


def validate_buoy(buoy_id: str, data: Dict, all_ids: Set, all_names: Set) -> List[str]:
    """Validate a single buoy station."""
    warnings = []
    name = data.get("name", buoy_id)

    # Check required fields
    missing = REQUIRED_BUOY_FIELDS - set(data.keys())
    if missing:
        raise ValidationError(f"Buoy {buoy_id}: Missing required fields: {missing}")

    # Check ID consistency
    if data["id"] != buoy_id:
        warnings.append(f"Buoy {buoy_id}: ID mismatch (key: {buoy_id}, value: {data['id']})")

    # Check for duplicates
    if data["id"] in all_ids:
        raise ValidationError(f"Buoy {buoy_id}: Duplicate ID {data['id']}")
    if data["name"] in all_names:
        warnings.append(f"Buoy {buoy_id}: Duplicate name '{data['name']}'")

    all_ids.add(data["id"])
    all_names.add(data["name"])

    # Validate coordinates
    validate_coordinates(data["lat"], data["lon"], name)

    # Check source
    if data["source"] not in VALID_BUOY_SOURCES:
        warnings.append(
            f"Buoy {buoy_id}: Unknown source '{data['source']}' " f"(expected: {', '.join(VALID_BUOY_SOURCES)})"
        )

    # Check data types
    if not isinstance(data["data_types"], list) or len(data["data_types"]) == 0:
        warnings.append(f"Buoy {buoy_id}: data_types should be a non-empty list")

    # Check update frequency
    if "update_frequency_minutes" in data:
        freq = data["update_frequency_minutes"]
        if not isinstance(freq, (int, type(None))):
            warnings.append(f"Buoy {buoy_id}: update_frequency_minutes should be int or null")
        elif isinstance(freq, int) and freq <= 0:
            warnings.append(f"Buoy {buoy_id}: update_frequency_minutes should be positive")

    return warnings


def validate_tide(station_key: str, data: Dict, all_ids: Set, all_names: Set, all_codes: Set) -> List[str]:
    """Validate a single tide station."""
    warnings = []
    name = data.get("name", station_key)

    # Check required fields
    missing = REQUIRED_TIDE_FIELDS - set(data.keys())
    if missing:
        raise ValidationError(f"Tide station {station_key}: Missing required fields: {missing}")

    # Check for duplicates
    if data["id"] in all_ids:
        raise ValidationError(f"Tide station {station_key}: Duplicate ID {data['id']}")
    if data["code"] in all_codes:
        warnings.append(f"Tide station {station_key}: Duplicate code {data['code']}")
    if data["name"] in all_names:
        warnings.append(f"Tide station {station_key}: Duplicate name '{data['name']}'")

    all_ids.add(data["id"])
    all_codes.add(data["code"])
    all_names.add(data["name"])

    # Validate coordinates
    validate_coordinates(data["lat"], data["lon"], name)

    # Check source
    if data["source"] not in VALID_TIDE_SOURCES:
        warnings.append(
            f"Tide station {station_key}: Unknown source '{data['source']}' "
            f"(expected: {', '.join(VALID_TIDE_SOURCES)})"
        )

    # Check type
    if data["type"] not in VALID_TIDE_TYPES:
        warnings.append(
            f"Tide station {station_key}: Invalid type '{data['type']}' " f"(expected: {', '.join(VALID_TIDE_TYPES)})"
        )

    # Check series
    if not isinstance(data["series"], list) or len(data["series"]) == 0:
        warnings.append(f"Tide station {station_key}: series should be a non-empty list")
    else:
        invalid_series = set(data["series"]) - VALID_TIDE_SERIES
        if invalid_series:
            warnings.append(
                f"Tide station {station_key}: Invalid series codes: {invalid_series} "
                f"(valid: {', '.join(VALID_TIDE_SERIES)})"
            )

    # Check data types
    if not isinstance(data["data_types"], list) or len(data["data_types"]) == 0:
        warnings.append(f"Tide station {station_key}: data_types should be a non-empty list")

    # Consistency check: TEMPORARY stations should not have observations
    if data["type"] == "TEMPORARY" and "wlo" in data.get("series", []):
        warnings.append(f"Tide station {station_key}: TEMPORARY stations should not have 'wlo' series")

    # Check update frequency consistency
    if "update_frequency_minutes" in data:
        freq = data["update_frequency_minutes"]
        has_obs = "wlo" in data.get("series", [])

        if has_obs and freq is None:
            warnings.append(f"Tide station {station_key}: Has observations but update_frequency_minutes is null")
        elif not has_obs and freq is not None:
            warnings.append(f"Tide station {station_key}: No observations but update_frequency_minutes is set")

    return warnings


def validate_metadata(data: Dict) -> List[str]:
    """Validate the metadata section."""
    warnings = []

    if "_metadata" not in data:
        warnings.append("No _metadata section found")
        return warnings

    meta = data["_metadata"]

    # Check for expected metadata fields
    expected = {"version", "created", "description", "sources", "units"}
    missing = expected - set(meta.keys())
    if missing:
        warnings.append(f"Metadata missing recommended fields: {missing}")

    return warnings


def main():
    """Run all validations."""
    print("🔍 Validating stations.json...")
    print("=" * 70)

    try:
        # Load data
        data = load_stations()
        print("✅ JSON syntax is valid")

        # Track all IDs and names for duplicate detection
        all_buoy_ids = set()
        all_buoy_names = set()
        all_tide_ids = set()
        all_tide_codes = set()
        all_tide_names = set()

        all_warnings = []

        # Validate buoys
        buoys = data.get("buoys", {})
        print(f"\n📊 Validating {len(buoys)} buoy stations...")

        for buoy_id, buoy_data in buoys.items():
            try:
                warnings = validate_buoy(buoy_id, buoy_data, all_buoy_ids, all_buoy_names)
                all_warnings.extend(warnings)
            except ValidationError as e:
                print(f"  ❌ {e}")
                return 1

        print("  ✅ All buoy stations validated")

        # Validate tide stations
        tides = data.get("tides", {})
        print(f"\n🌊 Validating {len(tides)} tide stations...")

        for station_key, station_data in tides.items():
            try:
                warnings = validate_tide(station_key, station_data, all_tide_ids, all_tide_codes, all_tide_names)
                all_warnings.extend(warnings)
            except ValidationError as e:
                print(f"  ❌ {e}")
                return 1

        print("  ✅ All tide stations validated")

        # Validate metadata
        print("\n📋 Validating metadata...")
        meta_warnings = validate_metadata(data)
        all_warnings.extend(meta_warnings)

        if not meta_warnings:
            print("  ✅ Metadata complete")

        # Summary
        print("\n" + "=" * 70)
        print("📊 VALIDATION SUMMARY")
        print("=" * 70)
        print(f"Total stations: {len(buoys) + len(tides)}")
        print(f"  • Buoys: {len(buoys)}")
        print(f"  • Tide stations: {len(tides)}")

        if all_warnings:
            print(f"\n⚠️  {len(all_warnings)} warnings:")
            for warning in all_warnings:
                print(f"  • {warning}")
            print("\n⚠️  Validation completed with warnings")
            return 0
        else:
            print("\n✅ All validations passed!")
            return 0

    except FileNotFoundError as e:
        print(f"❌ {e}")
        return 1
    except ValidationError as e:
        print(f"❌ {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
