#!/usr/bin/env python3
"""
Export Sunlight Times to JSON

Calculates and exports sunrise, sunset, dawn, dusk, and twilight times
for display on the website.

Generates JSON files with sunlight information for each webcam location.
"""

import json
from datetime import datetime, timedelta, timezone

import pytz
from astral import Depression, LocationInfo
from astral.sun import blue_hour, dawn, dusk, golden_hour, sun

from lib.config import EXPORT_DIR

# Webcam locations (same as in fetch_webcam.py)
WEBCAM_LOCATIONS = {
    "whiterock": {
        "name": "White Rock, BC",
        "lat": 49.0253,
        "lon": -122.8031,
        "output_file": EXPORT_DIR / "wrcam" / "sunlight.json",
    },
    "boundarybay": {
        "name": "Boundary Bay, BC",
        "lat": 49.0042,
        "lon": -123.0128,
        "output_file": EXPORT_DIR / "bbcam" / "sunlight.json",
    },
    "coxbay": {
        "name": "Cox Bay (Tofino), BC",
        "lat": 49.1167,
        "lon": -125.9000,
        "output_file": EXPORT_DIR / "coxbay" / "sunlight.json",
    },
}


def get_sunlight_times(lat, lon, date=None, tz_name="America/Vancouver"):
    """
    Get comprehensive sunlight information for a location.

    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        date: datetime object (defaults to today in local timezone)
        tz_name: Timezone name (defaults to 'America/Vancouver')

    Returns:
        dict: Contains all sunlight events (sunrise, sunset, dawn, dusk, etc.)
    """
    # Use local timezone for proper date handling
    local_tz = pytz.timezone(tz_name)

    if date is None:
        date = datetime.now(local_tz)
    elif date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
        # If date is naive, localize it
        date = local_tz.localize(date)

    location = LocationInfo("Location", "Region", tz_name, lat, lon)

    try:
        # Basic sun times
        s = sun(location.observer, date=date.date(), tzinfo=timezone.utc)

        # Dawn times using different depression angles
        # Civil dawn = 6° below horizon (enough light for outdoor activities)
        dawn_civil = dawn(location.observer, date=date.date(), depression=Depression.CIVIL, tzinfo=timezone.utc)

        # Nautical dawn = 12° below horizon (horizon visible at sea)
        dawn_nautical = dawn(location.observer, date=date.date(), depression=Depression.NAUTICAL, tzinfo=timezone.utc)

        # Astronomical dawn = 18° below horizon (sky no longer completely dark)
        dawn_astronomical = dawn(
            location.observer, date=date.date(), depression=Depression.ASTRONOMICAL, tzinfo=timezone.utc
        )

        # Dusk times using different depression angles
        dusk_civil = dusk(location.observer, date=date.date(), depression=Depression.CIVIL, tzinfo=timezone.utc)

        dusk_nautical = dusk(location.observer, date=date.date(), depression=Depression.NAUTICAL, tzinfo=timezone.utc)

        dusk_astronomical = dusk(
            location.observer, date=date.date(), depression=Depression.ASTRONOMICAL, tzinfo=timezone.utc
        )

        # Golden hour (best light for photography)
        # Note: golden_hour returns (start, end) for the evening golden hour only
        try:
            gh = golden_hour(location.observer, date=date.date(), tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            # May not be available in all locations/seasons or astral versions
            gh = None

        # Blue hour (twilight period)
        # Note: blue_hour returns (start, end) for the evening blue hour only
        try:
            bh = blue_hour(location.observer, date=date.date(), tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            bh = None

        # Fix for timezone crossing midnight: if sunset appears before sunrise in UTC,
        # it's actually on the next day. Same for dusk events.
        sunrise_time = s["sunrise"]
        sunset_time = s["sunset"]

        # Fix sunset if it wrapped to previous day (UTC timezone handling)
        if sunset_time < sunrise_time:
            # Sunset is on the next UTC day - use timedelta to handle month boundaries
            sunset_time = sunset_time + timedelta(days=1)

        # Fix dusk times if they wrapped to previous day
        # Dusk events should all be after sunset
        dusk_civil_time = dusk_civil
        dusk_nautical_time = dusk_nautical
        dusk_astronomical_time = dusk_astronomical

        if dusk_civil_time < sunset_time:
            dusk_civil_time = dusk_civil_time + timedelta(days=1)
        if dusk_nautical_time < sunset_time:
            dusk_nautical_time = dusk_nautical_time + timedelta(days=1)
        if dusk_astronomical_time < sunset_time:
            dusk_astronomical_time = dusk_astronomical_time + timedelta(days=1)

        result = {
            "date": date.date().isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            # Main events
            "sunrise": sunrise_time.isoformat(),
            "sunset": sunset_time.isoformat(),
            "solar_noon": s["noon"].isoformat(),
            # Dawn events (in order of occurrence)
            "dawn_astronomical": dawn_astronomical.isoformat(),  # First light
            "dawn_nautical": dawn_nautical.isoformat(),
            "dawn_civil": dawn_civil.isoformat(),  # Civil twilight begins
            # Dusk events (in order of occurrence)
            "dusk_civil": dusk_civil_time.isoformat(),  # Civil twilight ends
            "dusk_nautical": dusk_nautical_time.isoformat(),
            "dusk_astronomical": dusk_astronomical_time.isoformat(),  # Last light
            # Photography times (if available)
            # Golden hour and blue hour in astral library return (start, end)
            "golden_hour": {"start": gh[0].isoformat(), "end": gh[1].isoformat()} if gh and len(gh) >= 2 else None,
            "blue_hour": {"start": bh[0].isoformat(), "end": bh[1].isoformat()} if bh and len(bh) >= 2 else None,
            # Daylight duration
            "daylight_duration_seconds": int((sunset_time - sunrise_time).total_seconds()),
            # Current status
            "is_daylight_now": is_currently_daylight(sunrise_time, sunset_time),
            "current_phase": get_current_phase(
                sunrise_time,
                sunset_time,
                dawn_astronomical,
                dawn_nautical,
                dawn_civil,
                dusk_civil,
                dusk_nautical,
                dusk_astronomical,
            ),
        }

        return result

    except ValueError as e:
        # Polar day/night or other edge cases
        return {
            "date": date.date().isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
            "is_daylight_now": False,
            "current_phase": "unknown",
        }


def is_currently_daylight(sunrise, sunset, margin_minutes=30):
    """Check if it's currently daylight"""
    now = datetime.now(timezone.utc)
    margin = timedelta(minutes=margin_minutes)
    return (sunrise - margin) <= now <= (sunset + margin)


def get_current_phase(sunrise, sunset, dawn_astro, dawn_naut, dawn_civil, dusk_civil, dusk_naut, dusk_astro):
    """Determine the current light phase"""
    now = datetime.now(timezone.utc)

    # Define phases in chronological order
    if now < dawn_astro:
        return "night"
    elif now < dawn_naut:
        return "astronomical_twilight"
    elif now < dawn_civil:
        return "nautical_twilight"
    elif now < sunrise:
        return "civil_twilight"
    elif now < sunset:
        return "daylight"
    elif now < dusk_civil:
        return "civil_twilight"
    elif now < dusk_naut:
        return "nautical_twilight"
    elif now < dusk_astro:
        return "astronomical_twilight"
    else:
        return "night"


def load_tide_stations():
    """Load tide station coordinates from stations.json"""
    stations_file = EXPORT_DIR / "stations.json"
    try:
        with open(stations_file, "r") as f:
            data = json.load(f)
            return data.get("tides", {})
    except Exception as e:
        print(f"Warning: Could not load tide stations: {e}")
        return {}


def export_all_locations(days_ahead=4):
    """
    Export sunlight times for all configured locations (webcams and tide stations).

    Args:
        days_ahead: Number of days to calculate ahead (default: 4 to account for
                    timezone offset - cron runs at 00:06 UTC which is still previous
                    day in Pacific time, so we need an extra day buffer)
    """
    local_tz = pytz.timezone("America/Vancouver")
    today = datetime.now(local_tz).date()

    results = {"generated_at": datetime.now(timezone.utc).isoformat(), "days_ahead": days_ahead, "stations": {}}

    # Combine webcams and tide stations into one structure
    all_locations = {}

    # Add webcams
    print("=== Preparing Webcam Locations ===")
    for location_key, location in WEBCAM_LOCATIONS.items():
        all_locations[location_key] = {
            "name": location["name"],
            "lat": location["lat"],
            "lon": location["lon"],
            "type": "webcam",
        }
        print(f"  Added: {location['name']}")

    # Add tide stations
    print("\n=== Preparing Tide Station Locations ===")
    tide_stations = load_tide_stations()
    for station_key, station in tide_stations.items():
        if "lat" in station and "lon" in station:
            all_locations[station_key] = {
                "name": station.get("name", station_key),
                "lat": station["lat"],
                "lon": station["lon"],
                "type": "tide",
            }
            print(f"  Added: {station.get('name', station_key)}")

    # Calculate sunlight times for each location for the next N days
    print(f"\n=== Calculating Sunlight Times for {days_ahead} Days ===")
    for station_key, location in all_locations.items():
        print(f"\n{location['name']} ({location['type']})...")

        station_data = {"name": location["name"], "lat": location["lat"], "lon": location["lon"], "days": {}}

        # Calculate for each day
        for day_offset in range(days_ahead):
            target_date = today + timedelta(days=day_offset)
            date_str = target_date.isoformat()

            print(f"  {date_str}...", end=" ")

            # Get sunlight times for this day
            sunlight = get_sunlight_times(
                location["lat"],
                location["lon"],
                date=local_tz.localize(datetime.combine(target_date, datetime.min.time())),
            )

            # Extract only the times we need for plotting
            if "error" not in sunlight:
                station_data["days"][date_str] = {
                    "first_light": sunlight["dawn_civil"],
                    "sunrise": sunlight["sunrise"],
                    "sunset": sunlight["sunset"],
                    "last_light": sunlight["dusk_civil"],
                }
                print("✓")
            else:
                station_data["days"][date_str] = {"error": sunlight["error"]}
                print(f"✗ ({sunlight['error']})")

        results["stations"][station_key] = station_data

    # Write combined file
    combined_file = EXPORT_DIR / "sunlight_times.json"
    combined_file.parent.mkdir(parents=True, exist_ok=True)

    with open(combined_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"✓ Combined data written to: {combined_file}")
    print(f"  Stations: {len(results['stations'])}")
    print(f"  Days calculated: {days_ahead}")
    print(f"  Total data points: {len(results['stations']) * days_ahead * 4}")

    # Also write individual webcam files for backwards compatibility
    print("\n=== Writing Individual Webcam Files ===")
    for location_key, location in WEBCAM_LOCATIONS.items():
        if location_key in results["stations"]:
            # Just write today's data to individual files (backwards compat)
            today_str = today.isoformat()
            today_data = results["stations"][location_key]["days"].get(today_str, {})

            output_file = location["output_file"]
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w") as f:
                json.dump({"date": today_str, "generated_at": results["generated_at"], **today_data}, f, indent=2)

            print(f"  {location['name']}: {output_file}")

    return results


if __name__ == "__main__":
    export_all_locations()
