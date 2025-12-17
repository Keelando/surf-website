#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
"""
Parse BC Lightstation Reports (FPCN61 format) into SQLite.

Extracts wind speed/direction, sea state, and swell information from
text-based lightstation reports.

Usage:
    python3 parse_lightstation.py
"""

import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from lib.logging_config import setup_logging

# Disable console logging (runs from cron, file logging only)
logger = setup_logging('lightstation_parse', console=False)

# Configuration
DB_PATH = Path.home() / ".local" / "share" / "lightstation_data.sqlite"
DATA_DIR = Path.home() / "envcan_wave" / "data" / "lightstation"

# Regional sections in the report
REGIONS = [
    "STRAIT OF GEORGIA",
    "JUAN DE FUCA STRAIT",
    "WEST COAST VANCOUVER ISLAND",
    "CENTRAL COAST",
    "HECATE STRAIT",
]


def parse_report_time(header_line, report_time_line):
    """
    Parse the report timestamp from header and time lines.

    Args:
        header_line: "FPCN61 CWVR 251810" (DDHHMM format)
        report_time_line: "10 AM Tuesday"

    Returns:
        Unix timestamp (int) or None
    """
    try:
        # Extract DDHHMM from header
        match = re.search(r'FPCN61\s+CWVR\s+(\d{6})', header_line)
        if not match:
            logger.warning(f"Could not parse header: {header_line}")
            return None

        ddhhmm = match.group(1)
        day = int(ddhhmm[0:2])
        hour = int(ddhhmm[2:4])
        minute = int(ddhhmm[4:6])

        # Get current year/month (reports are always current month)
        now_utc = datetime.now(timezone.utc)
        year = now_utc.year
        month = now_utc.month

        # Create datetime (UTC)
        dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

        # Sanity check: if report date is > 5 days in future, assume it's from last month
        if (dt - now_utc).days > 5:
            month -= 1
            if month == 0:
                month = 12
                year -= 1
            dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

        return int(dt.timestamp())

    except Exception as e:
        logger.error(f"Error parsing report time: {e}")
        return None


def parse_station_entry(line, region):
    """
    Parse a single station observation line.

    Example formats:
        "CAPE MUDGE. ESTIMATED WIND SOUTHEAST 27 KNOTS AND GUSTING. SEAS 4 FEET MODERATE. LOW SOUTHERLY SWELL."
        "BOAT BLUFF. WIND CALM. SEAS RIPPLED."
        "CHROME ISLAND. ESTIMATED WIND SOUTHEAST 16 KNOTS. SEAS 2 FOOT CHOP. LOW EASTERLY SWELL."

    Returns:
        dict with parsed fields or None if not a valid station line
    """
    # Must start with station name (all caps) followed by period
    if not re.match(r'^[A-Z][A-Z\s]+\.', line):
        return None

    # Extract station name (everything before first period)
    station_name = line.split('.')[0].strip()

    # Initialize data dict
    data = {
        'station_name': station_name,
        'region': region,
        'wind_speed_kt': None,
        'wind_direction': None,
        'wind_gusting': 0,
        'wind_calm': 0,
        'wind_estimated': 0,
        'sea_height_ft': None,
        'sea_condition': None,
        'swell_intensity': None,
        'swell_direction': None,
    }

    # Check for WIND CALM
    if 'WIND CALM' in line:
        data['wind_calm'] = 1
    else:
        # Check for ESTIMATED WIND
        if 'ESTIMATED WIND' in line:
            data['wind_estimated'] = 1

        # Extract wind direction and speed
        # Pattern: "WIND SOUTHEAST 27 KNOTS"
        wind_match = re.search(r'WIND\s+([A-Z]+)\s+(\d+)\s+KNOTS?', line)
        if wind_match:
            data['wind_direction'] = wind_match.group(1)
            data['wind_speed_kt'] = float(wind_match.group(2))

        # Check for "AND GUSTING"
        if 'AND GUSTING' in line or 'GUSTING' in line:
            data['wind_gusting'] = 1

    # Check for SEAS RIPPLED
    if 'SEAS RIPPLED' in line or 'SEA RIPPLED' in line:
        data['sea_condition'] = 'RIPPLED'
        data['sea_height_ft'] = 0  # Calm/rippled
    else:
        # Extract sea height and condition
        # Pattern: "SEAS 4 FEET MODERATE" or "SEAS 2 FOOT CHOP"
        seas_match = re.search(r'SEAS?\s+(\d+)\s+FEET?\s+([A-Z]+)', line)
        if seas_match:
            data['sea_height_ft'] = float(seas_match.group(1))
            data['sea_condition'] = seas_match.group(2)

    # Extract swell information
    # Pattern: "LOW SOUTHERLY SWELL" or "MODERATE SOUTHWESTERLY SWELL"
    swell_match = re.search(r'(LOW|MODERATE|HEAVY)\s+([A-Z]+)\s+SWELL', line)
    if swell_match:
        data['swell_intensity'] = swell_match.group(1)
        data['swell_direction'] = swell_match.group(2)
    # Also check for intensity without direction: "LOW TO MODERATE ... SWELL"
    elif re.search(r'(LOW|MODERATE|HEAVY).*SWELL', line):
        intensity_match = re.search(r'(LOW|MODERATE|HEAVY)', line)
        if intensity_match:
            data['swell_intensity'] = intensity_match.group(1)

    return data


def parse_report_file(filepath):
    """
    Parse a complete FPCN61 report file.

    Returns:
        list of dicts with station observations
    """
    try:
        text = filepath.read_text(encoding='utf-8', errors='ignore')
        lines = text.split('\n')

        # Parse header to get report time
        header_line = lines[0] if lines else ""
        observation_time = None
        report_time_str = None
        current_region = None
        observations = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Capture report time line (e.g., "10 AM Tuesday")
            if re.match(r'\d+\s+(AM|PM)\s+\w+', line):
                report_time_str = line
                if observation_time is None:
                    observation_time = parse_report_time(header_line, line)
                continue

            # Check if this is a regional header
            if line.rstrip('.') in REGIONS:
                current_region = line.rstrip('.')
                continue

            # Skip other metadata lines
            if any(skip in line for skip in ['FPCN61', 'CURRENT OBSERVATIONS', 'YBL', 'YZT', 'YAZ', 'YCD']):
                continue

            # Try to parse as station entry
            if current_region:
                station_data = parse_station_entry(line, current_region)
                if station_data:
                    station_data['observation_time'] = observation_time
                    station_data['report_time_str'] = report_time_str
                    station_data['source_file'] = filepath.name
                    observations.append(station_data)

        logger.info(f"Parsed {len(observations)} station observations from {filepath.name}")
        return observations

    except Exception as e:
        logger.error(f"Error parsing {filepath}: {e}")
        return []


def insert_observations(observations):
    """Insert parsed observations into SQLite database."""
    if not observations:
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for obs in observations:
        try:
            cur.execute("""
                INSERT INTO lightstation_observation (
                    station_name, region, observation_time, report_time_str,
                    wind_speed_kt, wind_direction, wind_gusting, wind_calm, wind_estimated,
                    sea_height_ft, sea_condition, swell_intensity, swell_direction,
                    source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                obs['station_name'], obs['region'], obs['observation_time'], obs['report_time_str'],
                obs['wind_speed_kt'], obs['wind_direction'], obs['wind_gusting'],
                obs['wind_calm'], obs['wind_estimated'],
                obs['sea_height_ft'], obs['sea_condition'],
                obs['swell_intensity'], obs['swell_direction'],
                obs['source_file']
            ))
            inserted += 1

        except sqlite3.IntegrityError:
            # Duplicate entry (same station + time already exists)
            skipped += 1

        except Exception as e:
            logger.warning(f"Failed to insert {obs['station_name']}: {e}")

    conn.commit()
    conn.close()

    logger.info(f"Inserted {inserted} new observations, skipped {skipped} duplicates")


def main():
    logger.info("=== Parsing BC Lightstation Reports ===")

    # Find all report files
    if not DATA_DIR.exists():
        logger.warning(f"Data directory does not exist: {DATA_DIR}")
        return

    report_files = sorted(DATA_DIR.glob("FPCN61_CWVR_*"))

    if not report_files:
        logger.warning(f"No report files found in {DATA_DIR}")
        return

    logger.info(f"Found {len(report_files)} report file(s)")

    # Parse all reports
    all_observations = []
    for filepath in report_files:
        observations = parse_report_file(filepath)
        all_observations.extend(observations)

    # Insert into database
    if all_observations:
        insert_observations(all_observations)
        logger.info(f"✓ Processing complete! Total: {len(all_observations)} observations")
    else:
        logger.warning("No observations parsed")

    logger.info("=== Parse complete ===")


if __name__ == "__main__":
    main()
