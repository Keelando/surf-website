#!/usr/bin/env python3
"""
Parse BC Lightstation Reports (FPCN61 + SXCN) into SQLite.

Extracts wind speed/direction, sea state, and swell information from
text-based lightstation reports in two formats:
  - FPCN61: verbose text format (every ~3 hours via sr3/AMQP)
  - SXCN23/25/26: compact coded format (every 6 hours via sr3/AMQP)

Usage:
    python3 parse_lightstation.py
"""

import re
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from lib.config import LIGHTSTATION_DATABASE as DB_PATH
from lib.config import LIGHTSTATION_RETENTION_DAYS, PROJECT_ROOT
from lib.logging_config import setup_logging

# Disable console logging (runs from cron, file logging only)
logger = setup_logging("lightstation_parse", console=False)

# Configuration. Both bulletin families arrive by sr3/AMQP into one directory —
# FPCN61 used to be HTTP-polled into data/lightstation, which meant walking
# Datamart directory listings to find new files. MSC's usage policy forbids
# that ("the AMQPS notification service must be used for this need"), and the
# subscription was already delivering byte-identical FPCN61 files ~55 minutes
# sooner than the hourly poller could notice them. The poller is gone.
FPCN61_DATA_DIR = PROJECT_ROOT / "data" / "lightstation_bulletins"
SXCN_DATA_DIR = PROJECT_ROOT / "data" / "lightstation_bulletins"

# FPCN61/SXCN bulletins are issued by CWVR (Vancouver) and their observation
# lines name a local day and hour, not a UTC one.
BULLETIN_TZ = ZoneInfo("America/Vancouver")

# Regional sections in the FPCN61 report
REGIONS = [
    "STRAIT OF GEORGIA",
    "JUAN DE FUCA STRAIT",
    "WEST COAST VANCOUVER ISLAND",
    "CENTRAL COAST",
    "HECATE STRAIT",
]

# SXCN abbreviated name → full name (matching FPCN61 conventions)
SXCN_STATION_NAMES = {
    # SXCN23 — North Coast / Hecate
    "GREEN": "GREEN ISLAND",
    "TRIPLE": "TRIPLE ISLAND",
    "BONILLA": "BONILLA ISLAND",
    "LANGARA": "LANGARA ISLAND",
    "BOAT BLUFF": "BOAT BLUFF",
    "MCINNES": "MCINNES ISLAND",
    "IVORY": "IVORY ISLAND",
    "DRYAD": "DRYAD POINT",
    "ADDENBROKE": "ADDENBROKE ISLAND",
    # SXCN25 — WCVI South / Tofino
    "NOOTKA": "NOOTKA",
    "ESTEVAN": "ESTEVAN POINT",
    "LENNARD": "LENNARD ISLAND",
    "CAPE BEALE": "CAPE BEALE",
    # SXCN26 — Georgia Strait / S. Coast
    "CHROME": "CHROME ISLAND",
    "MERRY": "MERRY ISLAND",
    "ENTRANCE": "ENTRANCE ISLAND",
    "TRIAL IS": "TRIAL ISLAND",
}

# SXCN bulletin number → region
SXCN_REGIONS = {
    "23": "HECATE STRAIT",
    "25": "WEST COAST VANCOUVER ISLAND",
    "26": "STRAIT OF GEORGIA",
}

# SXCN sea condition abbreviations → full names (matching FPCN61)
SXCN_SEA_CONDITIONS = {
    "CHP": "CHOP",
    "MOD": "MODERATE",
    "MDT": "MODERATE",
    "RGH": "ROUGH",
    "RPLD": "RIPPLED",
}

# SXCN wind direction abbreviations
SXCN_WIND_DIRS = {
    "N": "NORTH", "NE": "NORTHEAST", "E": "EAST", "SE": "SOUTHEAST",
    "S": "SOUTH", "SW": "SOUTHWEST", "W": "WEST", "NW": "NORTHWEST",
}

# SXCN swell direction abbreviations
SXCN_SWELL_DIRS = {
    "N": "NORTHERLY", "NE": "NORTHEASTERLY", "E": "EASTERLY", "SE": "SOUTHEASTERLY",
    "S": "SOUTHERLY", "SW": "SOUTHWESTERLY", "W": "WESTERLY", "NW": "NORTHWESTERLY",
}


def extract_observation_day(report_time_line):
    """
    Extract the day-of-week from a report time line like "4 PM Sunday".

    Returns:
        Day name string (e.g., "Sunday") or None if not parseable.
    """
    match = re.match(r"\d+\s+(?:AM|PM)\s+(\w+day)", report_time_line)
    if match:
        return match.group(1)
    return None


def is_stale_retransmission(header_line, report_time_line, reference_time=None):
    """
    Check if a report is a stale retransmission by comparing the
    day-of-week in the header date against the observation time line.

    The header stamp is UTC (DDHHMM) but the observation line names a *local*
    day ("5 PM Thursday"), so the two must be compared in local time. Comparing
    the raw UTC day name rejected every bulletin issued between 00:00 and 07:00
    UTC — the 00/03/06 UTC slots, which are 5/8/11 PM the previous day in
    Pacific time. That silently discarded 3 of the 8 daily bulletins.

    Args:
        header_line: "FPCN61 CWVR 301510"
        report_time_line: "8 AM Monday"
        reference_time: Optional datetime for resolving year/month (for testing)

    Returns:
        True if stale (day-of-week mismatch), False if current,
        None if unable to determine (caller should parse anyway).
    """
    match = re.search(r"FPCN61\s+CWVR\s+(\d{6})", header_line)
    if not match:
        return None

    ddhhmm = match.group(1)
    day, hour, minute = int(ddhhmm[0:2]), int(ddhhmm[2:4]), int(ddhhmm[4:6])

    now_utc = reference_time or datetime.now(timezone.utc)

    # The stamp carries no month or year, so try the current month first and
    # walk back until the day exists and isn't in the future.
    dt = None
    year, month = now_utc.year, now_utc.month
    for _ in range(3):
        try:
            candidate = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except ValueError:
            candidate = None
        if candidate is not None and candidate <= now_utc + timedelta(hours=1):
            dt = candidate
            break
        month -= 1
        if month == 0:
            month, year = 12, year - 1
    if dt is None:
        return None

    # The bulletin's own clock: the UTC stamp, hour included, rendered in the
    # zone its text names. Dropping the hour here would misdate every bulletin.
    header_day_name = dt.astimezone(BULLETIN_TZ).strftime("%A")
    obs_day_name = extract_observation_day(report_time_line)
    if obs_day_name is None:
        return None

    return header_day_name != obs_day_name


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
        match = re.search(r"FPCN61\s+CWVR\s+(\d{6})", header_line)
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

        # Sanity check: if report date is in the future AT ALL, assume it's from last month
        # (Reports are current observations, NEVER legitimately in the future)
        # Allow small buffer (1 hour) for clock drift/timezone edge cases
        if dt > now_utc + timedelta(hours=1):
            month -= 1
            if month == 0:
                month = 12
                year -= 1
            try:
                dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
            except ValueError:
                # Day doesn't exist in previous month (e.g., day=31 rolled back to February).
                # Roll back one more month.
                month -= 1
                if month == 0:
                    month = 12
                    year -= 1
                try:
                    dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                except ValueError:
                    logger.warning(f"Cannot determine valid observation date for day={day}, giving up")
                    return None

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
    if not re.match(r"^[A-Z][A-Z\s]+\.", line):
        return None

    # Extract station name (everything before first period)
    station_name = line.split(".")[0].strip()

    # Initialize data dict
    data = {
        "station_name": station_name,
        "region": region,
        "wind_speed_kt": None,
        "wind_direction": None,
        "wind_gusting": 0,
        "wind_calm": 0,
        "wind_estimated": 0,
        "sea_height_ft": None,
        "sea_condition": None,
        "swell_intensity": None,
        "swell_direction": None,
    }

    # Check for WIND CALM
    if "WIND CALM" in line:
        data["wind_calm"] = 1
    else:
        # Check for ESTIMATED WIND
        if "ESTIMATED WIND" in line:
            data["wind_estimated"] = 1

        # Extract wind direction and speed
        # Pattern: "WIND SOUTHEAST 27 KNOTS"
        wind_match = re.search(r"WIND\s+([A-Z]+)\s+(\d+)\s+KNOTS?", line)
        if wind_match:
            data["wind_direction"] = wind_match.group(1)
            data["wind_speed_kt"] = float(wind_match.group(2))

        # Check for "AND GUSTING"
        if "AND GUSTING" in line or "GUSTING" in line:
            data["wind_gusting"] = 1

    # Check for SEAS RIPPLED
    if "SEAS RIPPLED" in line or "SEA RIPPLED" in line:
        data["sea_condition"] = "RIPPLED"
        data["sea_height_ft"] = 0  # Calm/rippled
    else:
        # Extract sea height and condition
        # Pattern: "SEAS 4 FEET MODERATE" or "SEAS 2 FOOT CHOP"
        seas_match = re.search(r"SEAS?\s+(\d+)\s+FEET?\s+([A-Z]+)", line)
        if seas_match:
            data["sea_height_ft"] = float(seas_match.group(1))
            data["sea_condition"] = seas_match.group(2)

    # Extract swell information
    # Pattern: "LOW SOUTHERLY SWELL" or "MODERATE SOUTHWESTERLY SWELL"
    swell_match = re.search(r"(LOW|MODERATE|HEAVY)\s+([A-Z]+)\s+SWELL", line)
    if swell_match:
        data["swell_intensity"] = swell_match.group(1)
        data["swell_direction"] = swell_match.group(2)
    # Also check for intensity without direction: "LOW TO MODERATE ... SWELL"
    elif re.search(r"(LOW|MODERATE|HEAVY).*SWELL", line):
        intensity_match = re.search(r"(LOW|MODERATE|HEAVY)", line)
        if intensity_match:
            data["swell_intensity"] = intensity_match.group(1)

    return data


def parse_report_file(filepath):
    """
    Parse a complete FPCN61 report file.

    Returns:
        list of dicts with station observations
    """
    try:
        text = filepath.read_text(encoding="utf-8", errors="ignore")
        lines = text.split("\n")

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
            if re.match(r"\d+\s+(AM|PM)\s+\w+", line):
                report_time_str = line
                if observation_time is None:
                    # Check for stale retransmission before parsing
                    stale = is_stale_retransmission(header_line, line)
                    if stale:
                        logger.info(
                            f"Skipping stale retransmission {filepath.name}: "
                            f"header date does not match '{line}'"
                        )
                        return []
                    observation_time = parse_report_time(header_line, line)
                continue

            # Check if this is a regional header
            if line.rstrip(".") in REGIONS:
                current_region = line.rstrip(".")
                continue

            # Skip other metadata lines
            if any(skip in line for skip in ["FPCN61", "CURRENT OBSERVATIONS", "YBL", "YZT", "YAZ", "YCD"]):
                continue

            # Try to parse as station entry
            if current_region:
                station_data = parse_station_entry(line, current_region)
                if station_data:
                    station_data["observation_time"] = observation_time
                    station_data["report_time_str"] = report_time_str
                    station_data["source_file"] = filepath.name
                    observations.append(station_data)

        logger.info(f"Parsed {len(observations)} station observations from {filepath.name}")
        return observations

    except Exception as e:
        logger.error(f"Error parsing {filepath}: {e}")
        return []


def parse_sxcn_time(header_line):
    """
    Parse observation time from SXCN header like "SXCN25 CWVR 112340".

    Returns:
        Unix timestamp (int) or None
    """
    match = re.search(r"SXCN\d+\s+CWVR\s+(\d{6})", header_line)
    if not match:
        return None

    ddhhmm = match.group(1)
    day = int(ddhhmm[0:2])
    hour = int(ddhhmm[2:4])
    minute = int(ddhhmm[4:6])

    now_utc = datetime.now(timezone.utc)
    year, month = now_utc.year, now_utc.month

    try:
        dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None

    # If in the future, roll back to previous month
    if dt > now_utc + timedelta(hours=1):
        month -= 1
        if month == 0:
            month, year = 12, year - 1
        try:
            dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except ValueError:
            return None

    return int(dt.timestamp())


def parse_sxcn_station_line(line, region):
    """
    Parse a single SXCN compact observation line.

    Examples:
        "NOOTKA        CLDY 15 SW06E 1FT CHP LO SW"
        "ESTEVAN       CLDY 15 SW05E 1FT CHP LO SW 1007.9R"
        "LANGARA       PC 15 NW16 3FT MOD LO W"
        "BOAT BLUFF    PC 15 CLM RPLD 2FT CHP IN FINLAYSON CHANNEL"
        "TRIPLE        N/A"
        "TRIAL IS      OVC 12R- SW18E3FT MDT FBNK DSTNT E-W"

    Returns:
        dict with parsed fields or None
    """
    line = line.strip()
    if not line:
        return None

    # Split station name from observation data
    # Station names are left-padded, data starts after whitespace gap
    match = re.match(r"^([A-Z][A-Z\s]*?)\s{2,}(.+)$", line)
    if not match:
        return None

    raw_name = match.group(1).strip()
    obs_text = match.group(2).strip()

    # Skip N/A stations
    if obs_text == "N/A":
        return None

    # Map abbreviated name to full name
    station_name = SXCN_STATION_NAMES.get(raw_name, raw_name)

    data = {
        "station_name": station_name,
        "region": region,
        "wind_speed_kt": None,
        "wind_direction": None,
        "wind_gusting": 0,
        "wind_calm": 0,
        "wind_estimated": 0,
        "sea_height_ft": None,
        "sea_condition": None,
        "swell_intensity": None,
        "swell_direction": None,
    }

    # Wind: "SW06E", "NW16", "CLM", "E14E"
    # May be jammed against sea height: "SW18E3FT"
    if " CLM " in f" {obs_text} ":
        data["wind_calm"] = 1
    else:
        wind_match = re.search(r"([NESW]{1,2})(\d{2,3})(E)?", obs_text)
        if wind_match:
            direction_abbr = wind_match.group(1)
            data["wind_direction"] = SXCN_WIND_DIRS.get(direction_abbr, direction_abbr)
            data["wind_speed_kt"] = float(wind_match.group(2))
            if wind_match.group(3):
                data["wind_estimated"] = 1

    # Seas: "1FT CHP", "3FT MOD", "RPLD", "3FT MDT"
    if "RPLD" in obs_text:
        data["sea_height_ft"] = 0
        data["sea_condition"] = "RIPPLED"
    else:
        seas_match = re.search(r"(\d+)FT\s+(\w+)", obs_text)
        if seas_match:
            data["sea_height_ft"] = float(seas_match.group(1))
            condition_abbr = seas_match.group(2)
            data["sea_condition"] = SXCN_SEA_CONDITIONS.get(condition_abbr, condition_abbr)

    # Swell: "LO SW", "LO S", "LO W", "MOD NW"
    swell_match = re.search(r"\b(LO|MOD|HVY)\s+([NESW]{1,2})\b", obs_text)
    if swell_match:
        intensity_map = {"LO": "LOW", "MOD": "MODERATE", "HVY": "HEAVY"}
        data["swell_intensity"] = intensity_map.get(swell_match.group(1))
        data["swell_direction"] = SXCN_SWELL_DIRS.get(swell_match.group(2))

    return data


def parse_sxcn_file(filepath):
    """
    Parse a complete SXCN bulletin file (SXCN23/25/26).

    Returns:
        list of dicts with station observations
    """
    try:
        text = filepath.read_text(encoding="utf-8", errors="ignore")
        lines = text.split("\n")

        header_line = lines[0] if lines else ""

        # Extract bulletin number for region mapping
        bulletin_match = re.search(r"SXCN(\d+)", header_line)
        if not bulletin_match:
            logger.warning(f"Could not parse SXCN header: {header_line}")
            return []

        bulletin_num = bulletin_match.group(1)
        region = SXCN_REGIONS.get(bulletin_num)
        if not region:
            logger.info(f"Skipping unsupported bulletin SXCN{bulletin_num}")
            return []

        observation_time = parse_sxcn_time(header_line)
        if not observation_time:
            logger.warning(f"Could not parse time from {filepath.name}")
            return []

        observations = []
        for line in lines[2:]:  # Skip header and VAE/VAJ/VAI line
            # Stop at supplementary section
            if "SUPPLEMENTARY" in line:
                break

            station_data = parse_sxcn_station_line(line, region)
            if station_data:
                station_data["observation_time"] = observation_time
                station_data["report_time_str"] = header_line.strip()
                station_data["source_file"] = filepath.name
                observations.append(station_data)

        if observations:
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
            cur.execute(
                """
                INSERT INTO lightstation_observation (
                    station_name, region, observation_time, report_time_str,
                    wind_speed_kt, wind_direction, wind_gusting, wind_calm, wind_estimated,
                    sea_height_ft, sea_condition, swell_intensity, swell_direction,
                    source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    obs["station_name"],
                    obs["region"],
                    obs["observation_time"],
                    obs["report_time_str"],
                    obs["wind_speed_kt"],
                    obs["wind_direction"],
                    obs["wind_gusting"],
                    obs["wind_calm"],
                    obs["wind_estimated"],
                    obs["sea_height_ft"],
                    obs["sea_condition"],
                    obs["swell_intensity"],
                    obs["swell_direction"],
                    obs["source_file"],
                ),
            )
            inserted += 1

        except sqlite3.IntegrityError:
            # Duplicate entry (same station + time already exists)
            skipped += 1

        except Exception as e:
            logger.warning(f"Failed to insert {obs['station_name']}: {e}")

    conn.commit()
    conn.close()

    logger.info(f"Inserted {inserted} new observations, skipped {skipped} duplicates")


def purge_old_data():
    """Remove observations and raw files older than the retention window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=LIGHTSTATION_RETENTION_DAYS)
    cutoff_epoch = int(cutoff.timestamp())

    # Purge DB rows
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM lightstation_observation WHERE observation_time < ?", (cutoff_epoch,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    if deleted:
        logger.info(f"Purged {deleted} observations older than {LIGHTSTATION_RETENTION_DAYS} days")

    # Purge raw FPCN61 files
    if FPCN61_DATA_DIR.exists():
        removed = 0
        for filepath in FPCN61_DATA_DIR.glob("FPCN61_CWVR_*"):
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                filepath.unlink()
                removed += 1
        if removed:
            logger.info(f"Removed {removed} raw FPCN61 files older than {LIGHTSTATION_RETENTION_DAYS} days")

    # Purge raw SXCN files
    if SXCN_DATA_DIR.exists():
        removed = 0
        for filepath in SXCN_DATA_DIR.glob("SXCN*_CWVR_*"):
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                filepath.unlink()
                removed += 1
        if removed:
            logger.info(f"Removed {removed} raw SXCN files older than {LIGHTSTATION_RETENTION_DAYS} days")


def main():
    logger.info("=== Parsing BC Lightstation Reports ===")

    all_observations = []

    # Parse FPCN61 reports
    if FPCN61_DATA_DIR.exists():
        fpcn_files = sorted(FPCN61_DATA_DIR.glob("FPCN61_CWVR_*"))
        if fpcn_files:
            logger.info(f"Found {len(fpcn_files)} FPCN61 file(s)")
            for filepath in fpcn_files:
                all_observations.extend(parse_report_file(filepath))

    # Parse SXCN bulletins (23/25/26 — lightstation obs)
    if SXCN_DATA_DIR.exists():
        sxcn_files = sorted(SXCN_DATA_DIR.glob("SXCN2[3-6]_CWVR_*"))
        if sxcn_files:
            logger.info(f"Found {len(sxcn_files)} SXCN file(s)")
            for filepath in sxcn_files:
                all_observations.extend(parse_sxcn_file(filepath))

    # Insert into database
    if all_observations:
        insert_observations(all_observations)
        logger.info(f"✓ Processing complete! Total: {len(all_observations)} observations")
    else:
        logger.warning("No observations parsed")

    # Purge old data
    purge_old_data()

    logger.info("=== Parse complete ===")


if __name__ == "__main__":
    main()
