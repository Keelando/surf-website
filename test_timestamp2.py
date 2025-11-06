from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Show current time
utc_now = datetime.now(timezone.utc)
pacific_now = utc_now.astimezone(ZoneInfo("America/Vancouver"))

print(f"Current UTC: {utc_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"Current Pacific: {pacific_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print()

# Surrey's last timestamp
surrey_timestamp_str = "2025-11-06T10:10:00"
print(f"Surrey timestamp (bare): {surrey_timestamp_str}")
print()

# If we parse as UTC (current behavior)
dt_as_utc = datetime.fromisoformat(surrey_timestamp_str).replace(tzinfo=timezone.utc)
print(f"If parsed as UTC: {dt_as_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"  Age: {(utc_now - dt_as_utc).total_seconds() / 3600:.1f} hours")
print()

# If we parse as Pacific
dt_as_pacific = datetime.fromisoformat(surrey_timestamp_str).replace(tzinfo=ZoneInfo("America/Vancouver"))
dt_as_pacific_utc = dt_as_pacific.astimezone(timezone.utc)
print(f"If parsed as Pacific: {dt_as_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"  As UTC: {dt_as_pacific_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"  Age: {(utc_now - dt_as_pacific_utc).total_seconds() / 60:.1f} minutes")
