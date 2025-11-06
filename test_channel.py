import requests
from datetime import datetime, timezone, timedelta

TOKEN = "YOUR_TOKEN_HERE"  # Paste your current token
API_BASE = "https://developers.flowworks.com/fwapi/v2"

end = datetime.now(timezone.utc)
start = end - timedelta(hours=12)

url = f"{API_BASE}/sites/20182/channels/1810/data"
params = {
    "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
    "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
}
headers = {"Authorization": f"Bearer {TOKEN}"}

response = requests.get(url, headers=headers, params=params)
data = response.json()

print(f"ResultCode: {data.get('ResultCode')}")
print(f"ResultMessage: {data.get('ResultMessage')}")
print(f"Resources: {len(data.get('Resources', []))} points")
print(f"First point: {data.get('Resources', [{}])[0]}")
