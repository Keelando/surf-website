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

# ---------- Config ----------
TIDE_DB = Path("~/.local/share/tide_data.sqlite").expanduser()
SURGE_DIR = Path("~/site/data/storm_surge").expanduser()
OUTPUT_FILE = Path("~/site/data/combined-water-level.json").expanduser()

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
    "crescent_pile": "Crescent_Beach_Channel"
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
        WHERE station_id = ?
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
        # Interpolate surge at this timestamp
        surge_value = interpolate_surge(surge_data, tide_ts)

        if surge_value is not None:
            total_level = tide_level + surge_value

            dt = datetime.fromtimestamp(tide_ts, tz=timezone.utc)

            entry = {
                "time": dt.isoformat(),
                "astronomical_tide_m": round(tide_level, 3),
                "storm_surge_m": round(surge_value, 3),
                "total_water_level_m": round(total_level, 3)
            }
            combined.append(entry)

            # Track peak
            if peak_total is None or total_level > peak_total:
                peak_total = total_level
                peak_entry = {
                    "time": dt.isoformat(),
                    "astronomical_tide_m": round(tide_level, 3),
                    "storm_surge_m": round(surge_value, 3),
                    "total_water_level_m": round(total_level, 3),
                    "description": f"Peak occurs at {dt.astimezone(ZoneInfo('America/Vancouver')).strftime('%Y-%m-%d %I:%M %p PST')}"
                }

    return {
        "forecast": combined,
        "peak": peak_entry
    }


def export_combined_water_level(tide_db, surge_dir, output_file):
    """Main export function."""
    print("🌊 Combined Water Level Forecast Export")
    print("=" * 70)

    # Determine time range: next 2 full calendar days (Pacific time)
    pacific = ZoneInfo('America/Vancouver')
    now_pacific = datetime.now(pacific)

    # Start from beginning of tomorrow (Pacific)
    tomorrow = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    # End at end of day after tomorrow
    day_after_tomorrow_end = tomorrow + timedelta(days=2)

    # Convert to UTC timestamps
    start_ts = int(tomorrow.astimezone(timezone.utc).timestamp())
    end_ts = int(day_after_tomorrow_end.astimezone(timezone.utc).timestamp())

    print(f"Forecast range: {tomorrow.date()} to {(tomorrow + timedelta(days=1)).date()} (Pacific)")
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
            "forecast_start": tomorrow.isoformat(),
            "forecast_end": day_after_tomorrow_end.isoformat(),
            "timezone": "America/Vancouver",
            "forecast_days": 2,
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
        return export_combined_water_level(tide_db, surge_dir, output_file)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
