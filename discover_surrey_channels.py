#!/usr/bin/env python3
"""
Discover available channels for Surrey FlowWorks stations.
Helps identify channel IDs for water level and other parameters.
"""

import requests
import json
from datetime import datetime, timezone

API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"

def authenticate():
    """Get JWT token from FlowWorks API v2."""
    url = f"{API_BASE}/authenticate"
    payload = {"username": USERNAME, "password": PASSWORD}

    try:
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "Token" in data:
            token = data.get("Token")
            print(f"✓ Authenticated successfully")
            return token
        else:
            print(f"✗ Auth failed: {data}")
            return None
    except Exception as e:
        print(f"✗ Auth error: {e}")
        return None

def get_site_channels(token, site_id, site_name):
    """Get all channels for a site."""
    url = f"{API_BASE}/sites/{site_id}/channels"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("ResultCode") == 0:
            channels = data.get("Resources", [])
            print(f"\n{'='*80}")
            print(f"SITE: {site_name} (ID: {site_id})")
            print(f"{'='*80}")
            print(f"Found {len(channels)} channels\n")

            # Group by category/type
            water_level = []
            wave = []
            wind = []
            temp = []
            other = []

            for ch in channels:
                ch_id = ch.get("Id")
                ch_name = ch.get("Name", "")
                ch_type = ch.get("ChannelType", "")
                unit = ch.get("Unit", "")

                # Categorize
                name_lower = ch_name.lower()
                if any(x in name_lower for x in ["level", "tide", "depth", "elevation"]):
                    water_level.append((ch_id, ch_name, unit, ch_type))
                elif any(x in name_lower for x in ["wave", "hm0", "hs", "height", "period", "tpeak", "tmean"]):
                    wave.append((ch_id, ch_name, unit, ch_type))
                elif any(x in name_lower for x in ["wind", "gust"]):
                    wind.append((ch_id, ch_name, unit, ch_type))
                elif any(x in name_lower for x in ["temp", "temperature"]):
                    temp.append((ch_id, ch_name, unit, ch_type))
                else:
                    other.append((ch_id, ch_name, unit, ch_type))

            # Print by category
            def print_category(name, items):
                if items:
                    print(f"\n{name}:")
                    print(f"{'-'*80}")
                    for ch_id, ch_name, unit, ch_type in items:
                        ch_id_str = str(ch_id) if ch_id else "N/A"
                        ch_name_str = str(ch_name) if ch_name else "N/A"
                        unit_str = str(unit) if unit else ""
                        ch_type_str = str(ch_type) if ch_type else ""
                        print(f"  [{ch_id_str:>5}] {ch_name_str:50s} {unit_str:10s} ({ch_type_str})")

            print_category("WATER LEVEL / TIDE", water_level)
            print_category("WAVE", wave)
            print_category("WIND", wind)
            print_category("TEMPERATURE", temp)
            print_category("OTHER", other)

            return channels
        else:
            print(f"✗ API error: {data.get('ResultMessage')}")
            return []

    except Exception as e:
        print(f"✗ Error fetching channels: {e}")
        return []

def main():
    print("FlowWorks Channel Discovery")
    print("="*80)

    # Authenticate
    token = authenticate()
    if not token:
        return 1

    # Query both sites
    sites = [
        (20182, "Crescent Beach Ocean"),
        (20183, "Crescent Channel Ocean")
    ]

    for site_id, site_name in sites:
        get_site_channels(token, site_id, site_name)

    print(f"\n{'='*80}")
    print("Discovery complete!")
    print(f"{'='*80}\n")

    return 0

if __name__ == "__main__":
    exit(main())
