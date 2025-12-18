#!/usr/bin/env python3
"""
Debug script to check station data freshness and identify stale sensors.
Helps diagnose feed issues by showing which stations have outdated data.

Usage:
    python3 debug_station_status.py           # Show all stations
    python3 debug_station_status.py --stale   # Show only stale stations
    python3 debug_station_status.py --json    # Output as JSON
    python3 debug_station_status.py --log     # Log stale stations to file
"""

import sys
import json
import csv
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.config import EXPORT_DIR

# Stale station log file
STALE_LOG = Path("~/.local/share/stale_stations.log").expanduser()

# JSON export paths
BUOY_JSON = EXPORT_DIR / "latest_buoy_v2.json"
WIND_JSON = EXPORT_DIR / "latest_wind.json"
TIDE_JSON = EXPORT_DIR / "tide-latest.json"
LIGHTSTATION_JSON = EXPORT_DIR / "latest_lightstation.json"
STATIONS_JSON = EXPORT_DIR / "stations.json"


def load_json(path: Path) -> dict:
    """Load JSON file, return empty dict if missing."""
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}", file=sys.stderr)
        return {}


def calculate_age_minutes(obs_time_str: str) -> float:
    """Calculate age of observation in minutes."""
    if not obs_time_str:
        return float('inf')
    try:
        obs_time = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        return (now - obs_time).total_seconds() / 60
    except Exception:
        return float('inf')


def format_age(age_minutes: float) -> str:
    """Format age as human-readable string."""
    if age_minutes == float('inf'):
        return "Never"
    if age_minutes < 60:
        return f"{int(age_minutes)}m ago"
    elif age_minutes < 1440:
        return f"{int(age_minutes / 60)}h ago"
    else:
        return f"{int(age_minutes / 1440)}d ago"


def check_wind_stations(wind_data: dict, stations_config: dict) -> List[Tuple[str, str, bool, str, float]]:
    """Check wind station statuses. Returns list of (id, name, is_stale, obs_time, age_minutes)."""
    results = []
    for station_id, station_meta in stations_config.get('wind', {}).items():
        data = wind_data.get(station_id, {})
        obs_time = data.get('observation_time', '')
        is_stale = data.get('stale', True)
        age_minutes = calculate_age_minutes(obs_time)

        results.append((
            station_id,
            station_meta.get('name', station_id),
            is_stale,
            obs_time,
            age_minutes
        ))

    return results


def check_buoy_stations(buoy_data: dict, stations_config: dict) -> List[Tuple[str, str, bool, str, float]]:
    """Check buoy station statuses."""
    results = []
    for station_id, station_meta in stations_config.get('buoys', {}).items():
        data = buoy_data.get(station_id, {})
        obs_time = data.get('observation_time', '')
        is_stale = data.get('stale', True)
        age_minutes = calculate_age_minutes(obs_time)

        results.append((
            station_id,
            station_meta.get('name', station_id),
            is_stale,
            obs_time,
            age_minutes
        ))

    return results


def check_tide_stations(tide_data: dict, stations_config: dict) -> List[Tuple[str, str, bool, str, float]]:
    """Check tide station statuses."""
    results = []
    # Tide data is nested under 'stations' key
    tide_stations = tide_data.get('stations', {})

    for station_id, station_meta in stations_config.get('tides', {}).items():
        data = tide_stations.get(station_id, {})
        obs_data = data.get('observation', {})
        obs_time = obs_data.get('time', '')
        is_stale = obs_data.get('stale', True) if obs_data else True
        age_minutes = calculate_age_minutes(obs_time)

        results.append((
            station_id,
            station_meta.get('name', station_id),
            is_stale,
            obs_time,
            age_minutes
        ))

    return results


def check_lightstation_stations(lightstation_data: dict, stations_config: dict) -> List[Tuple[str, str, bool, str, float]]:
    """Check lightstation statuses."""
    results = []
    for station_id, station_meta in stations_config.get('lightstations', {}).items():
        # Lightstation JSON uses spaces instead of underscores in keys
        # Convert "CHROME_ISLAND" -> "CHROME ISLAND"
        json_key = station_id.replace('_', ' ')
        data = lightstation_data.get(json_key, {})
        obs_time = data.get('observation_time', '')
        is_stale = data.get('stale', True) if data else True
        age_minutes = calculate_age_minutes(obs_time)

        results.append((
            station_id,
            station_meta.get('name', station_id),
            is_stale,
            obs_time,
            age_minutes
        ))

    return results


def print_table(title: str, data: List[Tuple[str, str, bool, str, float]], show_only_stale: bool = False):
    """Print formatted table of station statuses."""
    if show_only_stale:
        data = [row for row in data if row[2]]  # Filter to only stale

    if not data:
        return

    print(f"\n{'=' * 100}")
    print(f"{title}")
    print(f"{'=' * 100}")
    print(f"{'ID':<20} {'Name':<30} {'Status':<10} {'Age':<15} {'Last Observation'}")
    print(f"{'-' * 100}")

    stale_count = 0
    for station_id, name, is_stale, obs_time, age_minutes in sorted(data, key=lambda x: x[4]):
        status = "STALE" if is_stale else "ACTIVE"
        age_str = format_age(age_minutes)

        # Color coding for terminal (if supported)
        status_colored = f"\033[91m{status}\033[0m" if is_stale else f"\033[92m{status}\033[0m"

        obs_display = obs_time[:19] if obs_time else "No data"
        print(f"{station_id:<20} {name:<30} {status_colored:<18} {age_str:<15} {obs_display}")

        if is_stale:
            stale_count += 1

    print(f"{'-' * 100}")
    print(f"Total: {len(data)} stations | Active: {len(data) - stale_count} | Stale: {stale_count}")


def log_stale_stations(all_results: List[Tuple[str, str, bool, str, float, str]]):
    """Log stale stations to CSV file for trend analysis.

    Args:
        all_results: List of (station_id, name, is_stale, obs_time, age_minutes, station_type)
    """
    stale_stations = [r for r in all_results if r[2]]  # Filter to only stale

    if not stale_stations:
        print("\nNo stale stations to log.")
        return

    # Create log file with headers if it doesn't exist
    write_header = not STALE_LOG.exists()

    try:
        with open(STALE_LOG, 'a', newline='') as f:
            writer = csv.writer(f)

            if write_header:
                writer.writerow(['timestamp', 'station_id', 'station_name', 'station_type', 'last_observation', 'age_hours'])

            now = datetime.now(timezone.utc).isoformat()
            for station_id, name, is_stale, obs_time, age_minutes, station_type in stale_stations:
                age_hours = round(age_minutes / 60, 1) if age_minutes != float('inf') else 9999
                writer.writerow([now, station_id, name, station_type, obs_time or 'Never', age_hours])

        print(f"\nLogged {len(stale_stations)} stale station(s) to: {STALE_LOG}")
        print(f"View with: cat {STALE_LOG}")

    except Exception as e:
        print(f"\nError writing to log file: {e}", file=sys.stderr)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Debug station data freshness')
    parser.add_argument('--stale', action='store_true', help='Show only stale stations')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--log', action='store_true', help='Log stale stations to CSV file')
    args = parser.parse_args()

    # Load data
    print("Loading station data...")
    wind_data = load_json(WIND_JSON)
    buoy_data = load_json(BUOY_JSON)
    tide_data = load_json(TIDE_JSON)
    lightstation_data = load_json(LIGHTSTATION_JSON)
    stations_config = load_json(STATIONS_JSON)

    # Remove _meta entries
    wind_data.pop('_meta', None)
    buoy_data.pop('_meta', None)

    # Check all station types
    wind_results = check_wind_stations(wind_data, stations_config)
    buoy_results = check_buoy_stations(buoy_data, stations_config)
    tide_results = check_tide_stations(tide_data, stations_config)
    lightstation_results = check_lightstation_stations(lightstation_data, stations_config)

    if args.json:
        # Output as JSON
        output = {
            'wind_stations': [
                {'id': r[0], 'name': r[1], 'stale': r[2], 'observation_time': r[3], 'age_minutes': r[4]}
                for r in wind_results
            ],
            'buoys': [
                {'id': r[0], 'name': r[1], 'stale': r[2], 'observation_time': r[3], 'age_minutes': r[4]}
                for r in buoy_results
            ],
            'tides': [
                {'id': r[0], 'name': r[1], 'stale': r[2], 'observation_time': r[3], 'age_minutes': r[4]}
                for r in tide_results
            ],
            'lightstations': [
                {'id': r[0], 'name': r[1], 'stale': r[2], 'observation_time': r[3], 'age_minutes': r[4]}
                for r in lightstation_results
            ]
        }
        print(json.dumps(output, indent=2))
    else:
        # Print tables
        print(f"\nStation Status Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_table("WIND STATIONS", wind_results, args.stale)
        print_table("WAVE BUOYS", buoy_results, args.stale)
        print_table("TIDE STATIONS", tide_results, args.stale)
        print_table("LIGHTSTATIONS", lightstation_results, args.stale)

        # Summary
        all_results = wind_results + buoy_results + tide_results + lightstation_results
        total = len(all_results)
        stale = sum(1 for r in all_results if r[2])
        active = total - stale

        print(f"\n{'=' * 100}")
        print(f"OVERALL SUMMARY")
        print(f"{'=' * 100}")
        print(f"Total Stations: {total}")
        print(f"Active: {active} ({active/total*100:.1f}%)")
        print(f"Stale: {stale} ({stale/total*100:.1f}%)")

        if stale > 0:
            print(f"\n\033[93mWARNING: {stale} station(s) have stale data. Check feeds or sensor status.\033[0m")

    # Log stale stations if requested
    if args.log:
        # Add station type to results for logging
        all_results_with_type = (
            [(r[0], r[1], r[2], r[3], r[4], 'wind') for r in wind_results] +
            [(r[0], r[1], r[2], r[3], r[4], 'buoy') for r in buoy_results] +
            [(r[0], r[1], r[2], r[3], r[4], 'tide') for r in tide_results] +
            [(r[0], r[1], r[2], r[3], r[4], 'lightstation') for r in lightstation_results]
        )
        log_stale_stations(all_results_with_type)


if __name__ == '__main__':
    main()
