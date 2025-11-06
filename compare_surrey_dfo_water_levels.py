#!/usr/bin/env python3
"""
Compare Surrey FlowWorks water level data with DFO tide predictions.

Purpose: Determine if Surrey's TideLevel_Anderra (channel 2004) is useful
despite potential datum offset from DFO predictions.
"""

import requests
import sqlite3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import json

# Surrey API configuration
API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"
CRESCENT_PILE_SITE_ID = 20182
TIDE_LEVEL_CHANNEL = 2004  # TideLevel_Anderra

# DFO configuration
DFO_BASE_URL = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations"
CRESCENT_BEACH_STATION_ID = "5dd3064fe0fdc4b9b4be69d7"  # From stations.json
DFO_HEADERS = {"User-Agent": "keelan_w@hotmail.com"}

# Database paths
TIDE_DB = Path("~/.local/share/tide_data.sqlite").expanduser()


class FlowWorksAPI:
    """Minimal FlowWorks API client."""
    def __init__(self, username, password):
        self.base_url = API_BASE
        self.username = username
        self.password = password
        self.token = None

    def authenticate(self):
        """Get JWT token."""
        url = f"{self.base_url}/authenticate"
        payload = {"username": self.username, "password": self.password}

        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()

            if "Token" in data:
                self.token = data.get("Token")
                print(f"✅ Surrey API authenticated")
                return True
            else:
                print(f"❌ Surrey auth failed: {data}")
                return False
        except Exception as e:
            print(f"❌ Surrey auth error: {e}")
            return False

    def get_channel_data(self, site_id, channel_id, hours=24):
        """Fetch data from specific channel."""
        url = f"{self.base_url}/sites/{site_id}/channels/{channel_id}/data"

        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=hours)

        params = {
            "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
        }

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if data.get("ResultCode") == 0:
                return data.get("Resources", [])
            else:
                print(f"⚠️  Surrey API error: {data.get('ResultMessage')}")
                return []
        except Exception as e:
            print(f"❌ Surrey fetch error: {e}")
            return []


def parse_surrey_data(data_points):
    """Parse Surrey water level data into (timestamp, value) tuples."""
    results = []
    for point in data_points:
        timestamp_str = point.get("DataTime")
        value_str = point.get("DataValue")

        if not timestamp_str or value_str is None:
            continue

        try:
            # Parse timestamp - Surrey returns Pacific time
            dt_naive = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

            if dt_naive.tzinfo is None:
                dt = dt_naive.replace(tzinfo=ZoneInfo("America/Vancouver"))
            else:
                dt = dt_naive

            value = float(value_str)
            results.append((dt, value))
        except (ValueError, TypeError):
            continue

    return sorted(results, key=lambda x: x[0])


def get_dfo_predictions(hours=24):
    """Fetch DFO tide predictions for Crescent Beach."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=12)
    end = now + timedelta(hours=hours)

    url = f"{DFO_BASE_URL}/{CRESCENT_BEACH_STATION_ID}/data"
    params = {
        "time-series-code": "wlp",
        "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": end.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    try:
        response = requests.get(url, params=params, headers=DFO_HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()

        values = data if isinstance(data, list) else data.get("values", [])
        results = []

        for row in values:
            try:
                ts = datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
                water_level = row.get("value")
                if water_level is not None:
                    results.append((ts, water_level))
            except Exception:
                continue

        return sorted(results, key=lambda x: x[0])
    except Exception as e:
        print(f"❌ DFO fetch error: {e}")
        return []


def find_closest_match(surrey_data, dfo_data):
    """Find matching timestamps and calculate offset statistics."""
    matches = []

    for surrey_ts, surrey_val in surrey_data:
        # Find closest DFO prediction (within 10 minutes)
        closest = None
        min_diff = timedelta(minutes=10)

        for dfo_ts, dfo_val in dfo_data:
            diff = abs(surrey_ts - dfo_ts)
            if diff < min_diff:
                min_diff = diff
                closest = (dfo_ts, dfo_val)

        if closest:
            offset = surrey_val - closest[1]
            matches.append({
                "surrey_time": surrey_ts,
                "surrey_val": surrey_val,
                "dfo_time": closest[0],
                "dfo_val": closest[1],
                "offset": offset,
                "time_diff": min_diff.total_seconds() / 60  # minutes
            })

    return matches


def main():
    print("🌊 Comparing Surrey Water Levels vs DFO Predictions")
    print("=" * 70)

    # Fetch Surrey data
    print("\n1️⃣  Fetching Surrey water level data (TideLevel_Anderra)...")
    api = FlowWorksAPI(USERNAME, PASSWORD)
    if not api.authenticate():
        return 1

    surrey_raw = api.get_channel_data(CRESCENT_PILE_SITE_ID, TIDE_LEVEL_CHANNEL, hours=24)
    surrey_data = parse_surrey_data(surrey_raw)

    if not surrey_data:
        print("❌ No Surrey data available")
        return 1

    print(f"✅ Got {len(surrey_data)} Surrey data points")
    print(f"   Range: {surrey_data[0][1]:.2f}m to {max(d[1] for d in surrey_data):.2f}m")
    print(f"   Time: {surrey_data[0][0]} to {surrey_data[-1][0]}")

    # Fetch DFO predictions
    print("\n2️⃣  Fetching DFO tide predictions for Crescent Beach...")
    dfo_data = get_dfo_predictions(hours=24)

    if not dfo_data:
        print("❌ No DFO data available")
        return 1

    print(f"✅ Got {len(dfo_data)} DFO predictions")
    print(f"   Range: {min(d[1] for d in dfo_data):.2f}m to {max(d[1] for d in dfo_data):.2f}m")
    print(f"   Time: {dfo_data[0][0]} to {dfo_data[-1][0]}")

    # Compare
    print("\n3️⃣  Finding matching timestamps and calculating offset...")
    matches = find_closest_match(surrey_data, dfo_data)

    if not matches:
        print("❌ No matching timestamps found")
        return 1

    print(f"✅ Found {len(matches)} matching data points")

    # Calculate statistics
    offsets = [m["offset"] for m in matches]
    avg_offset = sum(offsets) / len(offsets)
    min_offset = min(offsets)
    max_offset = max(offsets)
    offset_range = max_offset - min_offset

    print("\n" + "=" * 70)
    print("📊 OFFSET ANALYSIS")
    print("=" * 70)
    print(f"Average offset: {avg_offset:.3f}m")
    print(f"Min offset:     {min_offset:.3f}m")
    print(f"Max offset:     {max_offset:.3f}m")
    print(f"Offset range:   {offset_range:.3f}m")
    print(f"\nConsistent offset: {'✅ YES' if offset_range < 0.1 else '⚠️  NO'}")

    # Show sample matches
    print("\n" + "=" * 70)
    print("📋 SAMPLE COMPARISONS (first 10 matches)")
    print("=" * 70)
    print(f"{'Time (Pacific)':<20} {'Surrey':<10} {'DFO':<10} {'Offset':<10}")
    print("-" * 70)

    for i, match in enumerate(matches[:10]):
        pacific_time = match["surrey_time"].astimezone(ZoneInfo("America/Vancouver"))
        print(f"{pacific_time.strftime('%Y-%m-%d %H:%M'):<20} "
              f"{match['surrey_val']:<10.3f} "
              f"{match['dfo_val']:<10.3f} "
              f"{match['offset']:<10.3f}")

    # Conclusion
    print("\n" + "=" * 70)
    print("💡 RECOMMENDATION")
    print("=" * 70)

    if offset_range < 0.1:
        print(f"✅ Surrey data has consistent offset of ~{avg_offset:.2f}m from DFO")
        print(f"   This can be corrected by subtracting {avg_offset:.2f}m from Surrey values")
        print("   → RECOMMENDED: Integrate Surrey data with offset correction")
    elif offset_range < 0.3:
        print(f"⚠️  Surrey data has variable offset ({offset_range:.2f}m range)")
        print(f"   Average offset: {avg_offset:.2f}m")
        print("   → MAYBE: Could integrate with offset, but verify carefully")
    else:
        print(f"❌ Surrey data has inconsistent offset ({offset_range:.2f}m range)")
        print("   → NOT RECOMMENDED: Data may not be directly comparable")

    return 0


if __name__ == "__main__":
    exit(main())
