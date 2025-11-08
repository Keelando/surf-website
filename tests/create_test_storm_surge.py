#!/usr/bin/env python3
"""
Generate test storm surge forecast data for offline development.

This script reads storm surge fixture files and updates timestamps to be recent,
then creates the combined_forecast.json file needed for testing combined water level predictions.

Usage:
    python3 tests/create_test_storm_surge.py
"""

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Paths
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures" / "storm_surge"
OUTPUT_DIR = SCRIPT_DIR / "data" / "storm_surge"

# Test station files
STATION_FILES = [
    "Point_Atkinson.json",
    "Campbell_River.json",
    "Crescent_Beach_Channel.json"
]


def update_forecast_timestamps(forecast_data):
    """
    Update forecast timestamps to be recent (starting from now).

    Args:
        forecast_data: dict of {timestamp_str: surge_value}

    Returns:
        dict with updated timestamps
    """
    if not forecast_data:
        return {}

    # Get sorted list of original timestamps
    original_times = sorted(forecast_data.keys())
    if not original_times:
        return {}

    # Calculate time offset to make first forecast start from now
    first_time = datetime.fromisoformat(original_times[0].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    time_delta = now - first_time

    # Update all timestamps
    updated_forecast = {}
    for time_str, value in forecast_data.items():
        original_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        new_time = original_time + time_delta
        new_time_str = new_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        updated_forecast[new_time_str] = value

    return updated_forecast


def process_station_file(station_file):
    """Process a single station fixture file and update timestamps."""
    fixture_path = FIXTURES_DIR / station_file

    if not fixture_path.exists():
        print(f"⚠️  Fixture not found: {station_file}")
        return None

    with open(fixture_path, 'r') as f:
        station_data = json.load(f)

    # Update forecast timestamps
    station_data["forecast"] = update_forecast_timestamps(station_data["forecast"])
    station_data["generated_utc"] = datetime.now(timezone.utc).isoformat()

    return station_data


def main():
    print("🌊 Test Storm Surge Data Generator")
    print("=" * 70)

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process each station
    combined_stations = {}

    for station_file in STATION_FILES:
        print(f"📍 Processing {station_file}...")

        station_data = process_station_file(station_file)

        if station_data:
            station_id = station_data["station_id"]

            # Write individual station file
            output_file = OUTPUT_DIR / station_file
            with open(output_file, 'w') as f:
                json.dump(station_data, f, indent=2)

            # Add to combined
            combined_stations[station_id] = station_data

            forecast_count = len(station_data["forecast"])
            print(f"   ✅ {forecast_count} forecast points")
            print(f"   📁 {output_file}")

    # Create combined forecast
    combined_data = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "stations": combined_stations
    }

    combined_file = OUTPUT_DIR / "combined_forecast.json"
    with open(combined_file, 'w') as f:
        json.dump(combined_data, f, indent=2)

    print(f"\n✅ Combined forecast created: {combined_file}")
    print(f"   Stations: {len(combined_stations)}")

    # Print time range
    if combined_stations:
        first_station = list(combined_stations.values())[0]
        times = sorted(first_station["forecast"].keys())
        if times:
            start_time = datetime.fromisoformat(times[0].replace("Z", "+00:00"))
            end_time = datetime.fromisoformat(times[-1].replace("Z", "+00:00"))
            print(f"   Time range: {start_time} to {end_time}")

    print("=" * 70)
    print(f"✅ Test storm surge data ready!")
    print(f"\nNext steps:")
    print(f"  1. Generate test tide database: python3 tests/create_test_tide_database.py")
    print(f"  2. Test combined water level: python3 export_combined_water_level.py --test-mode")

    return 0


if __name__ == "__main__":
    exit(main())
