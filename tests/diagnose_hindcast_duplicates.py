#!/usr/bin/env python3
"""
Diagnose Duplicate Hindcast Data

Checks for:
1. Duplicate timestamps across stations
2. Identical values across stations (copied data)
3. Date ranges where data differs vs identical
4. Which stations are affected
"""

import json
from datetime import datetime
from pathlib import Path


def analyze_hindcast(json_path):
    """Analyze hindcast JSON for duplicate/junk data."""
    print(f"🔍 Analyzing {json_path}")
    print("=" * 80)

    with open(json_path) as f:
        data = json.load(f)

    stations = data["stations"]
    print(f"📊 Found {len(stations)} stations")
    print(f"📅 Generated: {data['generated_utc']}")
    print()

    # Collect all timestamps and values per station
    station_data = {}
    all_times = set()

    for station_id, station_info in stations.items():
        hindcast = station_info["hindcast"]
        station_data[station_id] = {
            "name": station_info["station_name"],
            "data": {item["time"]: item["value"] for item in hindcast},
        }
        all_times.update(station_data[station_id]["data"].keys())

    all_times = sorted(all_times)
    print(f"📅 Total unique timestamps: {len(all_times)}")
    if all_times:
        print(f"   Range: {all_times[0]} to {all_times[-1]}")
    print()

    # Find duplicate values (same value at same time across stations)
    print("🔍 Checking for duplicate values across stations...")
    print("-" * 80)

    cutoff_date = datetime(2024, 11, 7, tzinfo=datetime.now().astimezone().tzinfo)

    before_cutoff = []
    after_cutoff = []

    for time_str in all_times:
        time_dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))

        # Get values for all stations at this time
        values_at_time = {}
        for station_id in station_data:
            if time_str in station_data[station_id]["data"]:
                values_at_time[station_id] = station_data[station_id]["data"][time_str]

        if len(values_at_time) > 1:
            # Check if all values are identical
            unique_values = set(values_at_time.values())

            if len(unique_values) == 1:
                # All stations have same value - likely duplicate
                if time_dt < cutoff_date:
                    before_cutoff.append((time_str, values_at_time))
                else:
                    after_cutoff.append((time_str, values_at_time))

    print(f"🔴 Identical values BEFORE 2024-11-07: {len(before_cutoff)} timestamps")
    print(f"🟢 Identical values AFTER 2024-11-07: {len(after_cutoff)} timestamps")
    print()

    # Show examples of duplicates
    if before_cutoff:
        print("📋 Example duplicates BEFORE Nov 7 (first 5):")
        for time_str, values in before_cutoff[:5]:
            print(f"   {time_str}:")
            for station_id, value in values.items():
                name = station_data[station_id]["name"]
                print(f"      {name:30} = {value}")
        print()

    if after_cutoff:
        print("⚠️  WARNING: Found identical values AFTER Nov 7 (first 5):")
        for time_str, values in after_cutoff[:5]:
            print(f"   {time_str}:")
            for station_id, value in values.items():
                name = station_data[station_id]["name"]
                print(f"      {name:30} = {value}")
        print()

    # Analyze per-station date ranges
    print("📊 Per-Station Analysis:")
    print("-" * 80)

    for station_id in sorted(station_data.keys()):
        station_info = station_data[station_id]
        times = sorted(station_info["data"].keys())

        if not times:
            print(f"{station_info['name']:30} NO DATA")
            continue

        # Find first date where data differs from other stations
        unique_from = None
        for time_str in times:
            time_dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))

            if time_dt >= cutoff_date:
                # Check if this value differs from at least one other station
                value = station_info["data"][time_str]
                differs = False

                for other_id, other_info in station_data.items():
                    if other_id == station_id:
                        continue
                    if time_str in other_info["data"] and other_info["data"][time_str] != value:
                        differs = True
                        break

                if differs:
                    unique_from = time_str
                    break

        first = times[0]
        last = times[-1]
        count = len(times)

        print(f"{station_info['name']:30} {count:4} records")
        print(f"  {'':30} Range: {first} to {last}")

        if unique_from:
            print(f"  {'':30} ✅ Unique data from: {unique_from}")
        else:
            print(f"  {'':30} ⚠️  No unique data found (may be all duplicates)")

        print()

    # Summary and recommendations
    print("=" * 80)
    print("📋 SUMMARY")
    print("=" * 80)

    if before_cutoff:
        print(f"🔴 Found {len(before_cutoff)} duplicate timestamps before Nov 7")
        print("   → This is likely junk data from before the timestamp bug fix")
        print("   → Recommendation: Filter out data before 2024-11-07 in frontend charts")
        print()

    if after_cutoff:
        print(f"⚠️  Found {len(after_cutoff)} duplicate timestamps after Nov 7")
        print("   → This might indicate an ongoing issue")
        print("   → Recommendation: Check fetch_storm_surge.py and database queries")
        print()

    if not before_cutoff and not after_cutoff:
        print("✅ No duplicate values found - all stations have unique data!")
        print()

    return len(before_cutoff), len(after_cutoff)


def main():
    # Check both test and production data
    test_file = Path("~/site/data/storm_surge/verification.json").expanduser()

    if not test_file.exists():
        print(f"❌ File not found: {test_file}")
        print("   Run: ./tests/setup_offline_test.sh")
        return 1

    before_nov7, after_nov7 = analyze_hindcast(test_file)

    if before_nov7 > 0:
        print("💡 RECOMMENDATION:")
        print("   Add date filter in frontend JavaScript:")
        print()
        print("   // Filter out junk data before Nov 7, 2024")
        print("   const cutoffDate = new Date('2024-11-07T00:00:00Z');")
        print("   const filteredData = hindcastData.filter(item => ")
        print("       new Date(item.time) >= cutoffDate")
        print("   );")
        print()

    return 0 if after_nov7 == 0 else 1


if __name__ == "__main__":
    exit(main())
