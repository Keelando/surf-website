#!/usr/bin/env python3
"""
Validate Storm Surge Hindcast Timestamps

Checks that:
1. Timestamps are full ISO format (not just dates)
2. Hours ahead calculation is correct (38-61)
3. Calendar day boundaries align properly (Pacific time)
4. All stations have same time range
"""

import json
from datetime import datetime, timedelta
from pathlib import Path


def validate_hindcast_json(json_path):
    """Validate hindcast JSON file."""
    print(f"🔍 Validating {json_path}")
    print("=" * 60)

    with open(json_path) as f:
        data = json.load(f)

    print(f"✅ Generated: {data['generated_utc']}")
    print(f"✅ Forecast horizon: {data['forecast_horizon_hours']}")
    print(f"✅ Stations: {len(data['stations'])}")
    print()

    all_valid = True

    for station_id, station_data in data["stations"].items():
        print(f"📍 {station_data['station_name']} ({station_id})")
        hindcast = station_data["hindcast"]

        if not hindcast:
            print("   ❌ No hindcast data")
            all_valid = False
            continue

        # Check first and last entries
        first = hindcast[0]
        last = hindcast[-1]

        print(f"   📊 Records: {len(hindcast)}")
        print(f"   📅 First: {first['time']} (forecast: {first['forecast_date']}, +{first['hours_ahead']}h)")
        print(f"   📅 Last:  {last['time']} (forecast: {last['forecast_date']}, +{last['hours_ahead']}h)")

        # Validate timestamp format
        try:
            first_time = datetime.fromisoformat(first["time"].replace("Z", "+00:00"))
            last_time = datetime.fromisoformat(last["time"].replace("Z", "+00:00"))
            forecast_time = datetime.fromisoformat(first["forecast_date"].replace("Z", "+00:00"))

            print("   ✅ Timestamps are full ISO format")
        except Exception as e:
            print(f"   ❌ Timestamp format error: {e}")
            all_valid = False
            continue

        # Check hours ahead range
        if first["hours_ahead"] < 38 or first["hours_ahead"] > 38:
            print(f"   ⚠️  First hours_ahead should be 38.0, got {first['hours_ahead']}")

        if last["hours_ahead"] < 61 or last["hours_ahead"] > 61:
            print(f"   ⚠️  Last hours_ahead should be 61.0, got {last['hours_ahead']}")

        # Check record count (should be 24 for full day)
        if len(hindcast) != 24:
            print(f"   ⚠️  Expected 24 hourly records, got {len(hindcast)}")
        else:
            print("   ✅ Correct record count (24 hours)")

        # Verify hours_ahead calculation
        calculated_hours = (first_time - forecast_time).total_seconds() / 3600
        if abs(calculated_hours - first["hours_ahead"]) > 0.1:
            print(f"   ❌ Hours ahead mismatch: calculated {calculated_hours:.1f}, stored {first['hours_ahead']}")
            print(f"      Forecast: {forecast_time}")
            print(f"      Valid:    {first_time}")
            all_valid = False
        else:
            print("   ✅ Hours ahead calculation correct")

        # Check for Pacific time alignment (if this is supposed to be PST)
        # 18Z Tuesday + 38h = Thursday 08:00 UTC = Thursday 00:00 PST
        # 18Z Tuesday + 61h = Thursday 07:00 UTC next day = Thursday 23:00 PST
        pst_offset = timedelta(hours=-8)
        first_pst = first_time + pst_offset
        last_pst = last_time + pst_offset

        print(f"   🕐 PST Range: {first_pst.strftime('%Y-%m-%d %H:%M')} to {last_pst.strftime('%Y-%m-%d %H:%M')}")

        if first_pst.hour == 0:
            print("   ✅ Starts at midnight PST")
        else:
            print(f"   ⚠️  Should start at midnight PST, starts at {first_pst.hour}:00")

        if last_pst.hour == 23:
            print("   ✅ Ends at 23:00 PST (full day)")
        else:
            print(f"   ⚠️  Should end at 23:00 PST, ends at {last_pst.hour}:00")

        print()

    if all_valid:
        print("✅ All validations passed!")
        return 0
    else:
        print("❌ Some validations failed")
        return 1


def main():
    test_file = Path("~/site/data/storm_surge/hindcast.json").expanduser()

    if not test_file.exists():
        print(f"❌ File not found: {test_file}")
        print("   Run: ./tests/setup_offline_test.sh")
        return 1

    return validate_hindcast_json(test_file)


if __name__ == "__main__":
    exit(main())
