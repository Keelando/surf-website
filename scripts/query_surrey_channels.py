#!/usr/bin/env python3
"""
Quick script to query FlowWorks API for channel list at a specific site.
Usage: python3 query_surrey_channels.py <site_id>
"""
import sys
import requests
import json
import os

API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = os.environ.get("SURREY_API_USERNAME", "surreyrain")
PASSWORD = os.environ.get("SURREY_API_PASSWORD", "surreyrain")

def authenticate():
    """Get JWT token."""
    url = f"{API_BASE}/authenticate"
    payload = {"username": USERNAME, "password": PASSWORD}
    response = requests.post(url, json=payload, timeout=15)
    response.raise_for_status()
    data = response.json()
    return data.get("Token")

def get_channels(token, site_id):
    """Get all channels for a site."""
    url = f"{API_BASE}/sites/{site_id}/channels"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 query_surrey_channels.py <site_id>")
        print("Example: python3 query_surrey_channels.py 20183")
        sys.exit(1)

    site_id = sys.argv[1]

    print(f"Authenticating...")
    token = authenticate()

    print(f"Fetching channels for site {site_id}...")
    data = get_channels(token, site_id)

    if data.get("ResultCode") == 0:
        channels = data.get("Resources", [])
        print(f"\nFound {len(channels)} channels\n")

        # Filter for tide/water level related channels
        tide_keywords = ["tidal", "tide", "stage", "level", "prediction", "residual", "geodiff"]

        print("=== TIDE/WATER LEVEL CHANNELS ===")
        for ch in channels:
            name = ch.get("ChannelName", "")
            if any(kw in name.lower() for kw in tide_keywords):
                print(f"ID: {ch.get('ChannelID')} | Name: {name} | Unit: {ch.get('UnitName', 'N/A')}")

        print("\n=== ALL CHANNELS (JSON) ===")
        print(json.dumps(channels, indent=2))
    else:
        print(f"API Error: {data.get('ResultMessage')}")
