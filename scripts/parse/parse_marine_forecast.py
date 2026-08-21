#!/usr/bin/env python3
"""
Marine Forecast Parser for Environment Canada XML files

Parses every marine weather zone XML that sr3 has delivered and writes them
as a single JSON document keyed by area. Zone and area keys are slugified
straight from the names in the XML, so adding a new zone is purely a matter
of widening the `accept` regex in `config/sr3/marine_forecast.conf` — this
parser needs no edit.
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from defusedxml import ElementTree as ET

# Use centralized logging configuration
from lib.config import EXPORT_DIR
from lib.logging_config import setup_logging

logger = setup_logging("marine_forecast")

# Directories
DATA_DIR = Path.home() / "envcan_wave" / "data" / "marine_forecast"

OUTPUT_FILE = EXPORT_DIR / "marine_forecast.json"

# sr3 writes files as <timestamp>_MSC_MarineWeather_<zone>_en.xml
FILENAME_RE = re.compile(r"_MSC_MarineWeather_(?P<zone>[^_]+)_en\.xml$")


def slugify(name):
    """Turn an XML area/location name into a stable snake_case key.

    "Strait of Georgia - north of Nanaimo" -> strait_of_georgia_north_of_nanaimo
    """
    slug = re.sub(r"[^a-z0-9]+", "_", (name or "").lower())
    return slug.strip("_")


def parse_datetime(dt_element):
    """Parse EC datetime element to ISO 8601 string"""
    if dt_element is None:
        return None

    year = dt_element.find("year")
    month = dt_element.find("month")
    day = dt_element.find("day")
    hour = dt_element.find("hour")
    minute = dt_element.find("minute")

    if None in [year, month, day, hour, minute]:
        return None

    try:
        dt = datetime(
            int(year.text), int(month.text), int(day.text), int(hour.text), int(minute.text), tzinfo=timezone.utc
        )
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        logger.warning(f"Error parsing datetime: {e}")
        return None


def parse_warning(event_element, location_name):
    """Parse a warning/watch event"""
    warning = {
        "location": location_name,
        "type": event_element.get("name", ""),
        "status": event_element.get("status", ""),
        "category": event_element.get("category", ""),
    }

    # Get issued time (UTC)
    issued_utc = event_element.find(".//dateTime[@name='Issued'][@zone='UTC']")
    if issued_utc is not None:
        warning["issued_utc"] = parse_datetime(issued_utc)

    return warning


def parse_weather_condition(condition_element):
    """Parse weatherCondition element"""
    condition = {}

    period = condition_element.find("periodOfCoverage")
    if period is not None and period.text:
        condition["period"] = period.text.strip()

    wind = condition_element.find("wind")
    if wind is not None and wind.text:
        condition["wind"] = wind.text.strip()

    weather_vis = condition_element.find("weatherVisibility")
    if weather_vis is not None and weather_vis.text:
        condition["weather"] = weather_vis.text.strip()

    return condition


def parse_extended_forecast(forecast_element, area_name):
    """Parse extended forecast periods (Thursday, Friday, etc.) per location.

    A zone bulletin repeats the extended forecast once per location it covers,
    so the periods have to stay keyed by location — flattening them yields the
    same three days over and over for a multi-location zone like Juan de Fuca.

    An unnamed location applies to the area as a whole and is keyed None.

    Returns {location_key or None: {"name": ..., "periods": [...]}}.
    """
    by_location = {}

    for location in forecast_element.findall(".//location"):
        loc_name = location.get("name")
        zone_key = slugify(loc_name) if loc_name else None

        periods = []
        for condition in location.findall(".//weatherCondition"):
            for period in condition.findall("forecastPeriod"):
                period_name = period.get("name", "")
                period_text = period.text.strip() if period.text else ""

                if period_name and period_text:
                    periods.append({"period": period_name, "forecast": period_text})

        if periods:
            by_location[zone_key] = {"name": loc_name, "periods": periods}

    return by_location


def parse_wave_forecast(wave_element):
    """Parse wave forecast if present"""
    if wave_element is None:
        return None

    wave_data = {}

    # Get issued time
    issued_utc = wave_element.find(".//dateTime[@name='Issued'][@zone='UTC']")
    if issued_utc is not None:
        wave_data["issued_utc"] = parse_datetime(issued_utc)

    # Get wave conditions
    for location in wave_element.findall(".//location"):
        for condition in location.findall(".//weatherCondition"):
            period = condition.find("periodOfCoverage")
            if period is not None and period.text:
                wave_data["period"] = period.text.strip()

            text_summary = condition.find("textSummary")
            if text_summary is not None and text_summary.text:
                wave_data["forecast"] = text_summary.text.strip()

    return wave_data if wave_data else None


def parse_marine_xml(xml_file, zone_code):
    """Parse a single marine forecast XML file into one area entry"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Get creation time (UTC)
        created_utc = root.find(".//dateTime[@name='xmlCreation'][@zone='UTC']")
        creation_time = parse_datetime(created_utc)

        # Get area info
        area = root.find("area")
        region = area.get("region", "") if area is not None else ""
        sub_region = area.get("subRegion", "") if area is not None else ""
        area_name = area.text.strip() if area is not None and area.text else ""

        if not area_name:
            logger.warning(f"{xml_file.name}: no area name, skipping")
            return None

        result = {
            "file": xml_file.name,
            "zone_code": zone_code,
            "generated_utc": creation_time,
            "region": region,
            "sub_region": sub_region,
            "area": area_name,
            "locations": {},
        }

        # Parse warnings by location
        warnings_section = root.find("warnings")
        location_warnings = defaultdict(list)
        if warnings_section is not None:
            for location in warnings_section.findall("location"):
                # A zone with a single location omits the name attribute
                loc_name = location.get("name") or area_name
                zone_key = slugify(loc_name)
                if not zone_key:
                    continue

                for event in location.findall("event"):
                    location_warnings[zone_key].append(parse_warning(event, loc_name))

        # Parse regular forecast by location
        regular_forecast = root.find("regularForecast")
        if regular_forecast is not None:
            issued_utc = regular_forecast.find(".//dateTime[@name='Issued'][@zone='UTC']")
            issued_time = parse_datetime(issued_utc)

            for location in regular_forecast.findall(".//location"):
                loc_name = location.get("name") or area_name
                zone_key = slugify(loc_name)
                if not zone_key:
                    continue

                if zone_key not in result["locations"]:
                    result["locations"][zone_key] = {
                        "zone_name": loc_name,
                        "warnings": location_warnings.get(zone_key, []),
                    }

                result["locations"][zone_key]["issued_utc"] = issued_time

                # Parse weather conditions
                condition = location.find(".//weatherCondition")
                if condition is not None:
                    result["locations"][zone_key]["forecast"] = parse_weather_condition(condition)

        # A zone can carry a warning without appearing in the regular forecast
        for zone_key, warnings in location_warnings.items():
            if zone_key not in result["locations"]:
                result["locations"][zone_key] = {
                    "zone_name": warnings[0]["location"],
                    "warnings": warnings,
                }

        # Parse extended forecast (one block per location in the area)
        extended = root.find("extendedForecast")
        if extended is not None:
            extended_by_location = parse_extended_forecast(extended, area_name)

            for zone_key, entry in extended_by_location.items():
                if zone_key is None:
                    continue
                if zone_key not in result["locations"]:
                    result["locations"][zone_key] = {
                        "zone_name": entry["name"],
                        "warnings": location_warnings.get(zone_key, []),
                    }
                result["locations"][zone_key]["extended_forecast"] = entry["periods"]

            # Area-level copy: the unnamed block if there is one, otherwise the
            # per-location text when every location agrees. Without this guard a
            # multi-location zone concatenates the same days once per location.
            if None in extended_by_location:
                result["extended_forecast"] = extended_by_location[None]["periods"]
            else:
                distinct = [e["periods"] for e in extended_by_location.values()]
                if distinct and all(periods == distinct[0] for periods in distinct):
                    result["extended_forecast"] = distinct[0]

        # Parse wave forecast if present
        wave = root.find("waveForecast")
        if wave is not None:
            wave_data = parse_wave_forecast(wave)
            if wave_data:
                result["wave_forecast"] = wave_data

        return result

    except ET.ParseError as e:
        logger.error(f"XML parse error in {xml_file}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing {xml_file}: {e}")
        return None


def latest_file_per_zone():
    """Map each zone code to its most recently modified XML file"""
    newest = {}

    for xml_file in DATA_DIR.glob("*_MSC_MarineWeather_*_en.xml"):
        match = FILENAME_RE.search(xml_file.name)
        if not match:
            continue

        zone_code = match.group("zone")
        current = newest.get(zone_code)
        if current is None or xml_file.stat().st_mtime > current.stat().st_mtime:
            newest[zone_code] = xml_file

    return newest


def build_document():
    """Parse every zone on disk into the {generated_utc, areas} document"""
    areas = {}

    for zone_code, xml_file in sorted(latest_file_per_zone().items()):
        parsed = parse_marine_xml(xml_file, zone_code)
        if parsed is None:
            continue

        area_key = slugify(parsed["area"])
        existing = areas.get(area_key)

        if existing is None:
            areas[area_key] = parsed
            continue

        # Two zone codes describing the same area: merge their locations and
        # keep the metadata from whichever file is newer.
        logger.info(f"Merging {zone_code} into existing area '{area_key}'")
        if (parsed.get("generated_utc") or "") > (existing.get("generated_utc") or ""):
            merged_locations = {**existing["locations"], **parsed["locations"]}
            parsed["locations"] = merged_locations
            areas[area_key] = parsed
        else:
            existing["locations"].update(parsed["locations"])

    generated = [a["generated_utc"] for a in areas.values() if a.get("generated_utc")]

    return {
        "generated_utc": max(generated) if generated else None,
        "areas": dict(sorted(areas.items(), key=lambda kv: kv[1]["area"])),
    }


def main():
    """Parse every marine forecast XML on disk and save as JSON"""
    logger.info("Starting marine forecast parser")

    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        return

    document = build_document()

    if not document["areas"]:
        logger.warning(f"No parsable marine forecast XML files in {DATA_DIR}")
        return

    # Write JSON output
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(OUTPUT_FILE, "w") as f:
            json.dump(document, f, indent=2)

        logger.info(f"Wrote {len(document['areas'])} area(s) to {OUTPUT_FILE}")

        # Log summary
        for area_key, area_data in document["areas"].items():
            for zone_key, zone_data in area_data["locations"].items():
                warnings = zone_data.get("warnings", [])
                logger.info(f"  {area_key}/{zone_key}: {len(warnings)} warning(s)")

    except Exception as e:
        logger.error(f"Error writing JSON: {e}")


if __name__ == "__main__":
    main()
