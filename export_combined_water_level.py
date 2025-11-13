#!/usr/bin/env python3
"""
Export Combined Total Water Level Predictions

Combines astronomical tide predictions with storm surge forecasts to produce
total predicted water level for the next 2 calendar days.

Total Water Level = Astronomical Tide (from DFO IWLS) + Storm Surge (from EC GDSPS)

Architecture:
- Astronomical tides are cached in SQLite (updated once daily via tide_to_sqlite.py)
- Storm surge forecasts are read from JSON (updated 4x daily via fetch_storm_surge.py)
- This script runs after each storm surge update to produce combined predictions

Output: ~/site/data/combined-water-level.json
"""

import sqlite3
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# Shared utilities
from config import TIDE_DATABASE, EXPORT_DIR

# ---------- Config ----------
TIDE_DB = TIDE_DATABASE
SURGE_DIR = EXPORT_DIR / "storm_surge"
OUTPUT_FILE = EXPORT_DIR / "combined-water-level.json"

# Test mode paths
TEST_TIDE_DB = Path(__file__).parent / "tests" / "databases" / "tide_data_test.sqlite"
TEST_SURGE_DIR = Path(__file__).parent / "tests" / "data" / "storm_surge"
TEST_OUTPUT_FILE = Path(__file__).parent / "tests" / "data" / "combined-water-level.json"

# Station mapping: tide station ID -> storm surge station ID
# Only stations that have both tide predictions AND storm surge forecasts
STATION_MAPPING = {
    "point_atkinson": "Point_Atkinson",
    "campbell_river": "Campbell_River",
    # Crescent Beach has predictions but not observations in tide_stations.json
    # Using crescent_pile as the tide station key
    "crescent_pile": "Crescent_Beach_Channel",
    "tofino": "Tofino"
}


def load_storm_surge_forecasts(surge_dir):
    """
    Load latest storm surge forecasts from JSON files.
    Returns dict: {station_id: {timestamp: surge_value}}
    """
    combined_file = surge_dir / "combined_forecast.json"

    if not combined_file.exists():
        print(f"⚠️  Storm surge forecast not found: {combined_file}")
        return {}

    with open(combined_file, 'r') as f:
        data = json.load(f)

    # Extract forecasts by station
    surge_by_station = {}

    for station_id, station_data in data.get("stations", {}).items():
        forecasts = {}
        # Storm surge forecast is a dict: {"2024-01-15T08:00:00Z": 0.05, ...}
        for time_str, surge_value in station_data.get("forecast", {}).items():
            # Parse timestamp
            ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            forecasts[timestamp] = surge_value

        if forecasts:
            surge_by_station[station_id] = forecasts

    print(f"✅ Loaded storm surge forecasts for {len(surge_by_station)} stations")
    return surge_by_station


def load_tide_predictions(station_id, start_ts, end_ts, tide_db):
    """
    Load astronomical tide predictions from SQLite database.
    Returns dict: {timestamp: tide_value}
    """
    if not tide_db.exists():
        print(f"⚠️  Tide database not found: {tide_db}")
        return {}

    conn = sqlite3.connect(tide_db)
    cur = conn.cursor()

    cur.execute("""
        SELECT prediction_time, water_level
        FROM tide_prediction
        WHERE station_name = ?
          AND prediction_time >= ?
          AND prediction_time <= ?
          AND water_level IS NOT NULL
        ORDER BY prediction_time ASC
    """, (station_id, start_ts, end_ts))

    predictions = {ts: level for ts, level in cur.fetchall()}
    conn.close()

    return predictions


def interpolate_surge(surge_forecasts, target_timestamp):
    """
    Interpolate storm surge value at target timestamp using linear interpolation.

    Args:
        surge_forecasts: dict of {timestamp: surge_value}
        target_timestamp: int (unix epoch)

    Returns:
        float: interpolated surge value, or None if target is outside forecast range
    """
    if not surge_forecasts:
        return None

    timestamps = sorted(surge_forecasts.keys())

    # Exact match
    if target_timestamp in surge_forecasts:
        return surge_forecasts[target_timestamp]

    # Find bounding timestamps
    before = None
    after = None

    for ts in timestamps:
        if ts < target_timestamp:
            before = ts
        elif ts > target_timestamp:
            after = ts
            break

    # Can't interpolate if outside range
    if before is None or after is None:
        return None

    # Linear interpolation
    t1, v1 = before, surge_forecasts[before]
    t2, v2 = after, surge_forecasts[after]

    # Interpolate
    ratio = (target_timestamp - t1) / (t2 - t1)
    interpolated = v1 + (v2 - v1) * ratio

    return interpolated


def combine_predictions(station_tide_id, station_surge_id, surge_forecasts, start_ts, end_ts, tide_db):
    """
    Combine astronomical tide predictions with storm surge forecasts.

    Returns: dict with:
        - forecast: list of {time, astronomical_tide, storm_surge, total_water_level}
        - peak: dict with peak total water level info

    IMPORTANT: Downsamples to 15-minute intervals for efficient frontend rendering.
    Database stores 5-minute data, but we export at 15-minute intervals to reduce file size.
    """
    # Load tide predictions
    tide_predictions = load_tide_predictions(station_tide_id, start_ts, end_ts, tide_db)

    if not tide_predictions:
        print(f"  ⚠️  No tide predictions found for {station_tide_id}")
        return None

    if station_surge_id not in surge_forecasts:
        print(f"  ⚠️  No storm surge forecast for {station_surge_id}")
        return None

    surge_data = surge_forecasts[station_surge_id]

    # Combine tide + surge
    combined = []
    peak_total = None
    peak_entry = None

    for tide_ts, tide_level in sorted(tide_predictions.items()):
        dt = datetime.fromtimestamp(tide_ts, tz=timezone.utc)

        # Interpolate surge at this timestamp
        surge_value = interpolate_surge(surge_data, tide_ts)

        if surge_value is not None:
            total_level = tide_level + surge_value

            # Track peak (check all data points, not just downsampled)
            if peak_total is None or total_level > peak_total:
                peak_total = total_level
                peak_entry = {
                    "time": dt.isoformat(),
                    "astronomical_tide_m": round(tide_level, 3),
                    "storm_surge_m": round(surge_value, 3),
                    "total_water_level_m": round(total_level, 3),
                    "description": f"Peak occurs at {dt.astimezone(ZoneInfo('America/Vancouver')).strftime('%Y-%m-%d %I:%M %p PST')}"
                }

            # Downsample output: Only include at 15-minute intervals
            # Keep readings at :00, :15, :30, :45
            if dt.minute % 15 == 0 and dt.second == 0:
                entry = {
                    "time": dt.isoformat(),
                    "astronomical_tide_m": round(tide_level, 3),
                    "storm_surge_m": round(surge_value, 3),
                    "total_water_level_m": round(total_level, 3)
                }
                combined.append(entry)

    return {
        "forecast": combined,
        "peak": peak_entry
    }


def update_latest_with_surge(surge_dir):
    """
    Update tide-latest.json with current storm surge info.
    Adds surge and total water level to current conditions.
    """
    latest_tide_file = Path("~/site/data/tide-latest.json").expanduser()

    if not latest_tide_file.exists():
        print(f"⚠️  tide-latest.json not found: {latest_tide_file}")
        return

    # Load existing tide latest
    with open(latest_tide_file) as f:
        tide_latest = json.load(f)

    # Load storm surge data
    combined_file = surge_dir / "combined_forecast.json"
    if not combined_file.exists():
        print("⚠️  No storm surge data for latest update")
        return

    with open(combined_file) as f:
        surge_data = json.load(f)

    surge_stations = surge_data.get("stations", {})
    if not surge_stations:
        return

    now = datetime.now(timezone.utc)
    updated_count = 0

    # Station mapping: tide station key -> storm surge station key
    station_mapping = {
        "point_atkinson": "Point_Atkinson",
        "campbell_river": "Campbell_River",
        "crescent_pile": "Crescent_Beach_Channel",
        "tofino": "Tofino"
    }

    # Update each matching station
    for tide_station, surge_station in station_mapping.items():
        if tide_station not in tide_latest.get("stations", {}):
            continue

        if surge_station not in surge_stations:
            continue

        surge_forecast = surge_stations[surge_station].get("forecast", {})
        if not surge_forecast:
            continue

        # Find current storm surge (closest to now)
        closest_surge = None
        min_diff = float('inf')

        for iso_time, surge_value in surge_forecast.items():
            dt = datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
            diff = abs((dt - now).total_seconds())
            if diff < min_diff:
                min_diff = diff
                closest_surge = surge_value

        if closest_surge is None:
            continue

        station_data = tide_latest["stations"][tide_station]

        # Add surge to prediction_now
        if "prediction_now" in station_data:
            tide_value = station_data["prediction_now"]["value"]
            station_data["prediction_now"]["surge"] = round(closest_surge, 3)
            station_data["prediction_now"]["total_water_level"] = round(tide_value + closest_surge, 3)

        # Add surge to observation if available
        if "observation" in station_data:
            tide_value = station_data["observation"]["value"]
            station_data["observation"]["surge"] = round(closest_surge, 3)
            station_data["observation"]["total_water_level"] = round(tide_value + closest_surge, 3)

        updated_count += 1

    if updated_count > 0:
        # Update metadata
        tide_latest["_meta"]["updated_with_surge"] = datetime.now(timezone.utc).isoformat()

        # Write updated file (atomic write)
        tmp = latest_tide_file.with_suffix(latest_tide_file.suffix + ".tmp")
        tmp.write_text(json.dumps(tide_latest, indent=2, sort_keys=True))
        tmp.replace(latest_tide_file)

        print(f"✅ Updated {updated_count} stations in tide-latest.json with surge data")


def export_combined_water_level(tide_db, surge_dir, output_file):
    """Main export function."""
    print("🌊 Combined Water Level Forecast Export")
    print("=" * 70)

    # Determine time range: current day + next 2 full calendar days (Pacific time)
    pacific = ZoneInfo('America/Vancouver')
    now_pacific = datetime.now(pacific)

    # Start from beginning of today (Pacific)
    today = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    # End at end of day after tomorrow (3 full days: today, tomorrow, day after)
    three_days_end = today + timedelta(days=3)

    # Convert to UTC timestamps
    start_ts = int(today.astimezone(timezone.utc).timestamp())
    end_ts = int(three_days_end.astimezone(timezone.utc).timestamp())

    print(f"Forecast range: {today.date()} to {(today + timedelta(days=2)).date()} (Pacific)")
    print(f"UTC range: {datetime.fromtimestamp(start_ts, tz=timezone.utc)} to {datetime.fromtimestamp(end_ts, tz=timezone.utc)}")
    print()

    # Load storm surge forecasts
    surge_forecasts = load_storm_surge_forecasts(surge_dir)

    if not surge_forecasts:
        print("❌ No storm surge forecasts available")
        return 1

    # Combine predictions for each station
    combined_data = {}

    for tide_station, surge_station in STATION_MAPPING.items():
        print(f"📍 {tide_station} + {surge_station}")

        result = combine_predictions(tide_station, surge_station, surge_forecasts, start_ts, end_ts, tide_db)

        if result and result["forecast"]:
            combined_data[tide_station] = {
                "tide_station_id": tide_station,
                "surge_station_id": surge_station,
                "forecast": result["forecast"],
                "peak": result["peak"]
            }
            print(f"  ✅ {len(result['forecast'])} combined predictions")
            if result["peak"]:
                print(f"  🔝 Peak: {result['peak']['total_water_level_m']}m at {result['peak']['description']}")
        else:
            print(f"  ⚠️  No data")

    # Create output JSON
    output = {
        "_meta": {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "type": "combined_water_level",
            "description": "Total water level = astronomical tide + storm surge forecast",
            "forecast_start": today.isoformat(),
            "forecast_end": three_days_end.isoformat(),
            "timezone": "America/Vancouver",
            "forecast_days": 3,
            "units": "meters",
            "data_sources": {
                "astronomical_tide": "DFO IWLS (Integrated Water Level System)",
                "storm_surge": "Environment Canada GDSPS (Global Deterministic Storm Surge Prediction System)"
            },
            "important_notes": {
                "wave_effects_not_included": "These predictions do NOT include wave setup, wave runup, or wave overtopping effects. During storms with large waves, actual water levels at the shore can be significantly higher due to breaking waves pushing water inland (wave setup ~0.2-0.5m, wave runup ~1-3m+ depending on location and wave height).",
                "what_is_storm_surge": "Storm surge is the static elevation of water level caused by meteorological forcing (wind stress and atmospheric pressure). GDSPS predicts this component using ocean circulation models driven by weather forecasts.",
                "total_flooding_risk": "For complete coastal flooding assessment, wave effects must be added separately. Wave runup is often the dominant factor during major storms on exposed coasts.",
                "inland_vs_exposed": "Wave effects are minimal in protected areas (e.g., Vancouver Harbour) but can dominate on exposed outer coasts (e.g., Tofino, Neah Bay)."
            }
        },
        "stations": combined_data
    }

    # Write output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2, sort_keys=True)

    print("=" * 70)
    print(f"✅ Exported combined water level for {len(combined_data)} stations")
    print(f"   Output: {output_file}")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Export combined water level predictions")
    parser.add_argument('--test-mode', action='store_true',
                       help='Use test data instead of production data')
    args = parser.parse_args()

    # Select paths based on mode
    if args.test_mode:
        print("   [TEST MODE - Using test data]")
        tide_db = TEST_TIDE_DB
        surge_dir = TEST_SURGE_DIR
        output_file = TEST_OUTPUT_FILE

        # Check if test data exists
        if not tide_db.exists():
            print(f"❌ Test tide database not found: {tide_db}")
            print(f"   Run: python3 tests/create_test_tide_database.py")
            return 1

        if not (surge_dir / "combined_forecast.json").exists():
            print(f"❌ Test storm surge data not found: {surge_dir}")
            print(f"   Run: python3 tests/create_test_storm_surge.py")
            return 1
    else:
        tide_db = TIDE_DB
        surge_dir = SURGE_DIR
        output_file = OUTPUT_FILE

    try:
        result = export_combined_water_level(tide_db, surge_dir, output_file)
        if result == 0:
            # Update tide-latest.json with current storm surge values
            print()
            update_latest_with_surge(surge_dir)
        return result
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
