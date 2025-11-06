#!/usr/bin/env python3
"""
Test script to verify Surrey FlowWorks data parsing.
Run this to check that we're correctly parsing API responses.
"""

import requests
from datetime import datetime, timezone, timedelta

# Test authentication
print("=" * 70)
print("🔐 Testing Authentication")
print("=" * 70)

auth_response = requests.post(
    "https://developers.flowworks.com/fwapi/v2/authenticate",
    json={"username": "surreyrain", "password": "surreyrain"},
    timeout=15
)

print(f"Status Code: {auth_response.status_code}")
auth_data = auth_response.json()
print(f"Response Keys: {list(auth_data.keys())}")
print(f"Token (first 50 chars): {auth_data.get('Token', 'N/A')[:50]}...")
print(f"Expires: {auth_data.get('Expires', 'N/A')}")
print()

if "Token" not in auth_data:
    print("❌ Authentication failed!")
    exit(1)

TOKEN = auth_data["Token"]
print("✅ Authentication successful!")
print()

# Test data fetch for Crescent Pile wind speed
print("=" * 70)
print("📡 Testing Data Fetch (Crescent Pile - Wind Speed)")
print("=" * 70)

API_BASE = "https://developers.flowworks.com/fwapi/v2"
end = datetime.now(timezone.utc)
start = end - timedelta(hours=4)

url = f"{API_BASE}/sites/20182/channels/1810/data"
params = {
    "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
    "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
}
headers = {"Authorization": f"Bearer {TOKEN}"}

response = requests.get(url, headers=headers, params=params, timeout=15)
data = response.json()

print(f"Status Code: {response.status_code}")
print(f"ResultCode: {data.get('ResultCode')}")
print(f"ResultMessage: {data.get('ResultMessage')}")
print(f"Number of data points: {len(data.get('Resources', []))}")
print()

if not data.get('Resources'):
    print("⚠️  No data returned - this might be normal if there's a delay")
    print("   FlowWorks stations typically have ~30min reporting delay")
    exit(0)

print("=" * 70)
print("📊 Sample Data Points")
print("=" * 70)

resources = data.get('Resources', [])
print(f"\nFirst 3 data points:")
for i, point in enumerate(resources[:3]):
    print(f"\n{i+1}. Raw point: {point}")

    # Parse like our script does
    timestamp_str = point.get("DataTime")
    value_str = point.get("DataValue")

    if timestamp_str and value_str is not None:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            value = float(value_str)

            # Check if it's on 10-minute boundary
            on_boundary = (dt.minute % 10 == 0 and dt.second == 0)

            # Convert m/s to km/h
            value_kmh = round(value * 3.6, 2)

            print(f"   Parsed:")
            print(f"   - Timestamp: {dt.isoformat()}")
            print(f"   - On 10-min boundary: {on_boundary}")
            print(f"   - Value (m/s): {value}")
            print(f"   - Value (km/h): {value_kmh}")
        except Exception as e:
            print(f"   ❌ Parse error: {e}")

print()
print("=" * 70)
print("📋 Parsing Summary")
print("=" * 70)

# Count how many points are on 10-minute boundaries
on_boundary_count = 0
total_valid = 0

for point in resources:
    timestamp_str = point.get("DataTime")
    value_str = point.get("DataValue")

    if timestamp_str and value_str is not None:
        total_valid += 1
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            if dt.minute % 10 == 0 and dt.second == 0:
                on_boundary_count += 1
        except:
            pass

print(f"Total points received: {len(resources)}")
print(f"Valid parseable points: {total_valid}")
print(f"Points on 10-min boundaries: {on_boundary_count}")
print(f"Points that will be stored: {on_boundary_count}")
print()

if on_boundary_count > 0:
    print("✅ Parsing looks good! Data will be stored correctly.")
else:
    print("⚠️  No points on 10-minute boundaries found.")
    print("   This might be normal depending on current time.")
    print("   FlowWorks reports every 10 minutes (00, 10, 20, 30, 40, 50)")

print()
