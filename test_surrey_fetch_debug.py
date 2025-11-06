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

# Test fetching one channel from Crescent Pile (wind speed)
site_id = 20182
channel_id = 1810

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

end = datetime.now(timezone.utc)
start = end - timedelta(hours=2)

params = {
    "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
    "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
}

url = f"{API_BASE}/sites/{site_id}/channels/{channel_id}/data"
response = requests.get(url, headers=headers, params=params, timeout=15)
data = response.json()

print("📡 Raw API Response:")
print(json.dumps(data, indent=2))
print()
print(f"ResultCode: {data.get('ResultCode')}")
print(f"Number of resources: {len(data.get('Resources', []))}")
if data.get('Resources'):
    print("\nFirst data point:")
    print(json.dumps(data['Resources'][0], indent=2))
