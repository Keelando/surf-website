import requests
from datetime import datetime, timezone, timedelta
import json

API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"

# Auth
url = f"{API_BASE}/authenticate"
response = requests.post(url, json={"username": USERNAME, "password": PASSWORD}, timeout=15)
token = response.json().get("Token")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Get one data point from Crescent Pile
end = datetime.now(timezone.utc)
start = end - timedelta(hours=24)
params = {
    "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
    "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
}

url = f"{API_BASE}/sites/20182/channels/1810/data"
response = requests.get(url, headers=headers, params=params, timeout=15)
data = response.json()

if data.get('Resources'):
    print("Sample timestamps from Surrey API:")
    for point in data['Resources'][-5:]:  # Last 5 points
        print(f"  {point.get('DataTime')} = {point.get('DataValue')}")
    print()
    print(f"Current Pacific Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current UTC Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
