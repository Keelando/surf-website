#!/usr/bin/env python3
"""
Test which water level channels actually have data.
"""

import requests
from datetime import datetime, timezone, timedelta

API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"

def authenticate():
    """Get JWT token."""
    url = f"{API_BASE}/authenticate"
    payload = {"username": USERNAME, "password": PASSWORD}
    response = requests.post(url, json=payload, timeout=15)
    data = response.json()
    return data.get("Token")

def test_channel(token, site_id, site_name, channel_id, channel_name):
    """Test if a channel has recent data."""
    url = f"{API_BASE}/sites/{site_id}/channels/{channel_id}/data"

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=48)

    params = {
        "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("ResultCode") == 0:
            points = data.get("Resources", [])
            if points:
                # Get some sample values
                samples = points[-5:] if len(points) > 5 else points
                values = [float(p.get("DataValue", 0)) for p in samples if p.get("DataValue") is not None]

                if values:
                    avg = sum(values) / len(values)
                    min_val = min(values)
                    max_val = max(values)
                    print(f"  ✓ [{channel_id:5d}] {channel_name:50s} | {len(points):4d} pts | Avg: {avg:7.2f} | Range: {min_val:7.2f} to {max_val:7.2f}")
                    return True

            print(f"  ✗ [{channel_id:5d}] {channel_name:50s} | NO DATA")
            return False
    except Exception as e:
        print(f"  ✗ [{channel_id:5d}] {channel_name:50s} | ERROR: {e}")
        return False

def main():
    print("Testing Water Level Channels")
    print("="*100)

    token = authenticate()
    if not token:
        print("Auth failed!")
        return 1

    # Test Crescent Beach Ocean channels
    print("\n📍 CRESCENT BEACH OCEAN (20182)")
    print("-"*100)

    cb_channels = [
        (2620, "Tidal_Prediction_CGVD28_GVRD"),
        (2295, "5_Preliminary_Total_Water_Level_Anderra"),
        (2410, "6_Preliminary_Total_Water_Level_Radar"),
        (2401, "7_Preliminary_Total_Water_Level_PT"),
        (1796, "TideLevel (Raw)"),
        (2004, "TideLevel_Anderra"),
        (2278, "Anderra - CGVD28 GVRD Stage"),
        (2281, "PT - CGVD28 GVRD Stage"),
        (2283, "Radar - CGVD28 GVRD Stage"),
        (2414, "Tidal Residual"),
        (2415, "Storm Surge Forecast"),
    ]

    for ch_id, ch_name in cb_channels:
        test_channel(token, 20182, "Crescent Beach Ocean", ch_id, ch_name)

    # Test Crescent Channel Ocean channels
    print("\n📍 CRESCENT CHANNEL OCEAN (20183)")
    print("-"*100)

    cc_channels = [
        (2621, "Tidal_Prediction_CGVD28_GVRD"),
        (2413, "5Preliminary_Total_Water_Level_Radar"),
        (2395, "6Preliminary_Total_Water_Level_PT"),
        (1997, "Tide_Level_10_min"),
        (2279, "PT - CGVD28 GVRD Stage"),
        (2285, "Radar - CGVD28 GVRD Stage"),
    ]

    for ch_id, ch_name in cc_channels:
        test_channel(token, 20183, "Crescent Channel Ocean", ch_id, ch_name)

    print("\n" + "="*100)
    print("Testing complete!\n")

    return 0

if __name__ == "__main__":
    exit(main())
