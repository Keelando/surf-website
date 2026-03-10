#!/usr/bin/env python3
"""
Marine Forecast Parser for Environment Canada XML files
Parses marine weather forecast XMLs and stores as JSON
"""

import json
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

# Zone mapping - normalize location names to internal keys
ZONE_MAP = {
    "Strait of Georgia - north of Nanaimo": "strait_georgia_north",
    "Strait of Georgia - south of Nanaimo": "strait_georgia_south",
}


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


def parse_extended_forecast(forecast_element):
    """Parse extended forecast periods (Thursday, Friday, etc.)"""
    periods = []

    for location in forecast_element.findall(".//location"):
        for condition in location.findall(".//weatherCondition"):
            for period in condition.findall("forecastPeriod"):
                period_name = period.get("name", "")
                period_text = period.text.strip() if period.text else ""

                if period_name and period_text:
                    periods.append({"period": period_name, "forecast": period_text})

    return periods


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


def parse_marine_xml(xml_file):
    """Parse a single marine forecast XML file"""
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

        result = {
            "file": xml_file.name,
            "generated_utc": creation_time,
            "region": region,
            "sub_region": sub_region,
            "area": area_name,
            "locations": {},
        }

        # Parse warnings by location
        warnings_section = root.find("warnings")
        location_warnings = {}
        if warnings_section is not None:
            for location in warnings_section.findall("location"):
                loc_name = location.get("name", "")
                if loc_name in ZONE_MAP:
                    zone_key = ZONE_MAP[loc_name]
                    location_warnings[zone_key] = []

                    for event in location.findall("event"):
                        warning = parse_warning(event, loc_name)
                        location_warnings[zone_key].append(warning)

        # Parse regular forecast by location
        regular_forecast = root.find("regularForecast")
        if regular_forecast is not None:
            issued_utc = regular_forecast.find(".//dateTime[@name='Issued'][@zone='UTC']")
            issued_time = parse_datetime(issued_utc)

            for location in regular_forecast.findall(".//location"):
                loc_name = location.get("name", "")
                if loc_name in ZONE_MAP:
                    zone_key = ZONE_MAP[loc_name]

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

        # Parse extended forecast (applies to all locations)
        extended = root.find("extendedForecast")
        if extended is not None:
            extended_periods = parse_extended_forecast(extended)
            if extended_periods:
                result["extended_forecast"] = extended_periods

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


def get_latest_xml_for_zone(zone_code="m0000028"):
    """Find the most recent XML file for a given zone"""
    xml_files = list(DATA_DIR.glob(f"*_MSC_MarineWeather_{zone_code}_en.xml"))

    if not xml_files:
        return None

    # Sort by modification time, newest first
    xml_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return xml_files[0]


def main():
    """Parse latest marine forecast XML and save as JSON"""
    logger.info("Starting marine forecast parser")

    if not DATA_DIR.exists():
        logger.error(f"Data directory not found: {DATA_DIR}")
        return

    # Get latest XML for Strait of Georgia (contains both north and south)
    latest_xml = get_latest_xml_for_zone("m0000028")

    if latest_xml is None:
        logger.warning(f"No XML files found in {DATA_DIR}")
        return

    logger.info(f"Parsing {latest_xml.name}")
    data = parse_marine_xml(latest_xml)

    if data is None:
        logger.error("Failed to parse XML")
        return

    # Write JSON output
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Wrote forecast data to {OUTPUT_FILE}")

        # Log summary
        if data.get("locations"):
            for zone_key, zone_data in data["locations"].items():
                warnings = zone_data.get("warnings", [])
                logger.info(f"  {zone_key}: {len(warnings)} warning(s)")

    except Exception as e:
        logger.error(f"Error writing JSON: {e}")


if __name__ == "__main__":
    main()
