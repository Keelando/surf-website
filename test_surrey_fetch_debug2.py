#!/usr/bin/env python3
import requests
import json
from datetime import datetime, timezone, timedelta

API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"

# Authenticate
url = f"{API_BASE}/authenticate"
payload = {"username": USERNAME, "password": PASSWORD}
response = requests.post(url, json=payload, timeout=15)
data = response.json()
token = data.get("Token")

print("✅ Authenticated")
print()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Test multiple time windows and stations
test_configs = [
    ("Crescent Pile - Wind Speed", 20182, 1810, 24),
    ("Crescent Pile - Wave Height", 20182, 2002, 24),
    ("Crescent Channel - Wind", 20183, 1837, 24),
    ("Colebrook - Wind", 18507, 1425, 24),
]

for name, site_id, channel_id, hours in test_configs:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    
    params = {
        "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
    }
    
    url = f"{API_BASE}/sites/{site_id}/channels/{channel_id}/data"
    response = requests.get(url, headers=headers, params=params, timeout=15)
    data = response.json()
    
    print(f"📡 {name} (last {hours}h):")
    print(f"   ResultCode: {data.get('ResultCode')}")
    print(f"   Data points: {len(data.get('Resources', []))}")
    
    if data.get('Resources'):
        first = data['Resources'][0]
        last = data['Resources'][-1]
        print(f"   First: {first.get('DataTime')} = {first.get('DataValue')}")
        print(f"   Last:  {last.get('DataTime')} = {last.get('DataValue')}")
    print()
