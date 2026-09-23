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
from lib.config import (
    LIGHTSTATION_RAW_RETENTION_DAYS,
    LIGHTSTATION_RETENTION_DAYS,
    PROJECT_ROOT,
)
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
    # SXCN24 — Central Coast / N. Island. Subscribed 2026-09-07 with the
    # abbreviations guessed from the other bulletins' rule (drop a generic
    # ISLAND/POINT suffix); checked against real bulletins 2026-09-23, which
    # showed the rule does not hold here: PINE ISLAND and EGG ISLAND keep
    # their suffix, and CAPE MUDGE (also in FPCN61; the cross-bulletin merge
    # collapses the pair) was missing. Two weeks of Pine Island and Cape Mudge
    # readings were skipped before the warnings were read.
    "CHATHAM": "CHATHAM POINT",
    "CAPE MUDGE": "CAPE MUDGE",
    "PULTENEY": "PULTENEY POINT",
    "SCARLETT": "SCARLETT POINT",
    "PINE ISLAND": "PINE ISLAND",
    "EGG ISLAND": "EGG ISLAND",
    "CAPE SCOTT": "CAPE SCOTT",
    "QUATSINO": "QUATSINO",
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

# Entries that mean "no observation this cycle"
SXCN_UNAVAILABLE = {"N/A", "NA", "UNAVAILABLE"}

# SXCN bulletin number → region
SXCN_REGIONS = {
    "23": "HECATE STRAIT",
    "24": "CENTRAL COAST",
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
    "N": "NORTH",
    "NE": "NORTHEAST",
    "E": "EAST",
    "SE": "SOUTHEAST",
    "S": "SOUTH",
    "SW": "SOUTHWEST",
    "W": "WEST",
    "NW": "NORTHWEST",
}

# SXCN swell intensity abbreviations (FPCN61 spells them out)
SXCN_SWELL_INTENSITIES = {"LO": "LOW", "MOD": "MODERATE", "MDT": "MODERATE", "HVY": "HEAVY"}

# SXCN swell direction abbreviations
SXCN_SWELL_DIRS = {
    "N": "NORTHERLY",
    "NE": "NORTHEASTERLY",
    "E": "EASTERLY",
    "SE": "SOUTHEASTERLY",
    "S": "SOUTHERLY",
    "SW": "SOUTHWESTERLY",
    "W": "WESTERLY",
    "NW": "NORTHWESTERLY",
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

    dt = resolve_ddhhmm(match.group(1), reference_time)
    if dt is None:
        return None

    # The bulletin's own clock: the UTC stamp, hour included, rendered in the
    # zone its text names. Dropping the hour here would misdate every bulletin.
    header_day_name = dt.astimezone(BULLETIN_TZ).strftime("%A")
    obs_day_name = extract_observation_day(report_time_line)
    if obs_day_name is None:
        return None

    return header_day_name != obs_day_name


def resolve_ddhhmm(ddhhmm, now_utc=None):
    """UTC datetime for a WMO DDHHMM stamp, which carries no month or year.

    Tries the current month first and walks back until the day exists and is
    not in the future. The walk-back is the point: on the 1st of April a stamp
    from 31 March must not be tried as 31 April — that raised, the bulletin
    was dropped, and it was re-dropped on every run until it aged off disk.

    Returns:
        datetime (UTC) or None
    """
    day, hour, minute = int(ddhhmm[0:2]), int(ddhhmm[2:4]), int(ddhhmm[4:6])
    now_utc = now_utc or datetime.now(timezone.utc)
    year, month = now_utc.year, now_utc.month
    for _ in range(3):
        try:
            candidate = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except ValueError:
            candidate = None
        # One hour of grace for clock drift: a bulletin is never from the future.
        if candidate is not None and candidate <= now_utc + timedelta(hours=1):
            return candidate
        month -= 1
        if month == 0:
            month, year = 12, year - 1
    return None


def parse_report_time(header_line, report_time_line, now_utc=None):
    """
    Parse the report timestamp from the FPCN61 header.

    Args:
        header_line: "FPCN61 CWVR 251810" (DDHHMM format)
        report_time_line: "10 AM Tuesday" (unused; kept for the call site)
        now_utc: Optional reference time (for testing)

    Returns:
        Unix timestamp (int) or None
    """
    match = re.search(r"FPCN61\s+CWVR\s+(\d{6})", header_line)
    if not match:
        logger.warning(f"Could not parse header: {header_line}")
        return None
    dt = resolve_ddhhmm(match.group(1), now_utc)
    if dt is None:
        logger.warning(f"Cannot determine valid observation date for {header_line!r}, giving up")
        return None
    return int(dt.timestamp())


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
        # EC writes FOOT for 1-3 ft and FEET for 4 ft and up, so both spellings
        # need matching explicitly: FEET? matches FEE/FEET, never FOOT.
        seas_match = re.search(r"SEAS?\s+(\d+)\s+(?:FOOT|FEET|FT)\s+([A-Z]+)", line)
        if seas_match:
            data["sea_height_ft"] = float(seas_match.group(1))
            data["sea_condition"] = seas_match.group(2)

    # Extract swell information
    # Pattern: "LOW SOUTHERLY SWELL" or "MODERATE SOUTHWESTERLY SWELL"
    # A range is stored whole: "LOW TO MODERATE" used to be recorded as
    # MODERATE, because the search found the second word first-fit.
    swell_match = re.search(
        r"\b(LOW|MODERATE|HEAVY)(?:\s+TO\s+(LOW|MODERATE|HEAVY))?\s+([A-Z]+)\s+SWELL", line
    )
    if swell_match:
        low, high = swell_match.group(1), swell_match.group(2)
        data["swell_intensity"] = f"{low} TO {high}" if high else low
        data["swell_direction"] = swell_match.group(3)
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
                            f"Skipping stale retransmission {filepath.name}: " f"header date does not match '{line}'"
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


def parse_sxcn_time(header_line, now_utc=None):
    """
    Parse observation time from SXCN header like "SXCN25 CWVR 112340".

    Returns:
        Unix timestamp (int) or None
    """
    match = re.search(r"SXCN\d+\s+CWVR\s+(\d{6})", header_line)
    if not match:
        return None
    dt = resolve_ddhhmm(match.group(1), now_utc)
    return int(dt.timestamp()) if dt else None


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

    # Skip stations with no reading. The bulletins spell it several ways:
    # Egg Island has read "N/A", "NA" and "UNAVAILABLE" in one day of SXCN24.
    if obs_text.upper() in SXCN_UNAVAILABLE:
        return None

    # Map abbreviated name to full name.
    #
    # An unknown abbreviation is skipped, not stored under whatever the
    # bulletin called it. Falling through to `raw_name` would file the reading
    # under a station nothing else on the site knows — the page joins
    # observations to config/stations.json by name, so a phantom "EGG" would
    # render as a nameless card and never match the registry entry it belongs
    # to. Losing a row is recoverable once the warning is read; a phantom
    # station is not.
    station_name = SXCN_STATION_NAMES.get(raw_name)
    if station_name is None:
        logger.warning(
            f"Unmapped SXCN station abbreviation {raw_name!r}; skipping. "
            f"Add it to SXCN_STATION_NAMES if it is a station we carry."
        )
        return None

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
    # Speeds under 10 kt are sometimes written without the leading zero
    # ("NW8E", "SE5E"), and calm sometimes carries punctuation ("CLM,"). Both
    # used to fall through, leaving the reading with no wind at all.
    if re.search(r"\bCLM\b", obs_text):
        data["wind_calm"] = 1
    else:
        wind_match = re.search(r"\b([NESW]{1,2})(\d{1,3})(E)?", obs_text)
        if wind_match:
            direction_abbr = wind_match.group(1)
            data["wind_direction"] = SXCN_WIND_DIRS.get(direction_abbr, direction_abbr)
            data["wind_speed_kt"] = float(wind_match.group(2))
            if wind_match.group(3):
                data["wind_estimated"] = 1

    # Seas: "1FT CHP", "3FT MOD", "RPLD", "3FT MDT"
    seas_end = 0
    rpld = re.search(r"\bRPLD\b", obs_text)
    if rpld:
        data["sea_height_ft"] = 0
        data["sea_condition"] = "RIPPLED"
        seas_end = rpld.end()
    else:
        seas_match = re.search(r"(\d+)FT\s+(\w+)", obs_text)
        if seas_match:
            data["sea_height_ft"] = float(seas_match.group(1))
            condition_abbr = seas_match.group(2)
            data["sea_condition"] = SXCN_SEA_CONDITIONS.get(condition_abbr, condition_abbr)
            seas_end = seas_match.end()

    # Swell: "LO SW", "MOD NW", "MDT SW", "LO-MDT W". Searched only after the
    # seas group, so the sea state's own "MOD" is never read as a swell. MDT and
    # ranges used to be missed outright, dropping Quatsino, Cape Scott and Pine
    # Island's swell whenever it was moderate.
    swell_match = re.search(
        r"\b(LO|MOD|MDT|HVY)(?:-(LO|MOD|MDT|HVY))?\s+([NESW]{1,2})\b", obs_text[seas_end:]
    )
    if swell_match:
        low = SXCN_SWELL_INTENSITIES[swell_match.group(1)]
        high = swell_match.group(2) and SXCN_SWELL_INTENSITIES[swell_match.group(2)]
        data["swell_intensity"] = f"{low} TO {high}" if high else low
        data["swell_direction"] = SXCN_SWELL_DIRS.get(swell_match.group(3))

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


# ---------- Cross-bulletin merge ----------
#
# One observation, two bulletins. Nine of the 23 stations are carried by both
# FPCN61 and an SXCN bulletin, and each copy is timestamped from its own WMO
# header rather than from the observation, so the pair lands 30 minutes
# (SXCN26) or 40 minutes (SXCN23) apart and the unique index reads them as two
# readings. Neither feed can be dropped -- each states things the other cannot
# -- so the second copy is merged into the first instead.
#
# The window is a band around the offsets EC actually uses, not a plain
# tolerance, because Coast Guard SPECIAL reports arrive off-cycle: Addenbroke
# published one 60 minutes after its regular SXCN23 slot on 2026-09-06, and
# that is a real second observation, not a re-publication of the first.
PAIR_OFFSET_MIN_SEC = 20 * 60
PAIR_OFFSET_MAX_SEC = 50 * 60

# Both copies describe the same reading, so a merge only ever fills a gap:
# across the 464 pairs in the database when this was written, no value field
# disagreed where both bulletins stated one.
MERGE_VALUE_FIELDS = (
    "wind_speed_kt",
    "wind_direction",
    "sea_height_ft",
    "sea_condition",
    "swell_intensity",
    "swell_direction",
)

# Flags accumulate rather than fill, since absence is encoded as 0, not NULL.
# Gusting is the reason it matters: the coded format has no way to say it, so
# only FPCN61 ever sets it.
MERGE_FLAG_FIELDS = ("wind_gusting", "wind_calm", "wind_estimated")


def bulletin_family(source_file):
    """Which product a file belongs to: "FPCN61", "SXCN", or None."""
    if not source_file:
        return None
    if source_file.startswith("FPCN61"):
        return "FPCN61"
    if source_file.startswith("SXCN"):
        return "SXCN"
    return None


def is_correction(source_file):
    """True for a WMO correction bulletin, e.g. SXCN25_CWVR_231140_CCA__43418."""
    return bool(source_file and re.search(r"_CC[A-Z]_", source_file))


def partner_window(family, observation_time):
    """Epoch bounds where the other bulletin's copy of this reading would sit.

    FPCN61 re-publishes what SXCN already carried, so its stamp is the later
    of the two whichever order the files arrive in.
    """
    if observation_time is None:
        return None
    if family == "FPCN61":
        return observation_time - PAIR_OFFSET_MAX_SEC, observation_time - PAIR_OFFSET_MIN_SEC
    if family == "SXCN":
        return observation_time + PAIR_OFFSET_MIN_SEC, observation_time + PAIR_OFFSET_MAX_SEC
    return None


def find_existing_row(cur, obs):
    """Locate the row this observation belongs in, if there is one.

    Returns ``(row, keep_time)``: the row to merge into (None to insert a new
    one) and the timestamp the merged row should carry.
    """
    station = obs["station_name"]
    timestamp = obs["observation_time"]

    cur.execute(
        "SELECT * FROM lightstation_observation WHERE station_name = ? AND observation_time = ?",
        (station, timestamp),
    )
    row = cur.fetchone()
    if row is not None:
        return row, timestamp

    family = bulletin_family(obs.get("source_file"))
    window = partner_window(family, timestamp)
    if window is None:
        return None, timestamp

    # Match on family membership, not filename prefix: an already-merged row
    # names both products, and re-parsing either file must find it again.
    other = "%FPCN61%" if family == "SXCN" else "%SXCN%"
    cur.execute(
        """
        SELECT * FROM lightstation_observation
        WHERE station_name = ? AND observation_time BETWEEN ? AND ?
          AND source_file LIKE ?
        ORDER BY ABS(observation_time - ?) LIMIT 1
        """,
        (station, window[0], window[1], other, timestamp),
    )
    row = cur.fetchone()
    if row is None:
        return None, timestamp

    # SXCN's header minute is the observation minute -- its supplementary line
    # states it outright. FPCN61's is only when the prose went out, and the
    # hour its text names ("2 PM Thursday") is rounded. So the pair keeps the
    # SXCN time whichever copy arrived first.
    keep_time = timestamp if family == "SXCN" else row["observation_time"]
    return row, keep_time


def merge_observation(cur, row, obs, keep_time):
    """Fold an observation into an existing row. Returns True if it changed."""
    updates = {}

    # A correction bulletin (WMO "CCA", "CCB", ...) replaces what it states;
    # anything else only fills gaps. Without this a correction that arrived
    # after its original could never fix a wrong value, only a missing one.
    correcting = is_correction(obs.get("source_file"))

    for field in MERGE_VALUE_FIELDS:
        incoming = obs.get(field)
        if incoming is None:
            continue
        if row[field] is None or (correcting and row[field] != incoming):
            updates[field] = incoming

    # Flags only ever accumulate, corrections included: SXCN cannot say
    # "gusting" at all, so an SXCN correction clearing FPCN61's gust flag would
    # be erasing something it never had the words for.
    for field in MERGE_FLAG_FIELDS:
        if obs.get(field) and not row[field]:
            updates[field] = 1

    # region is deliberately not merged. The two products disagree about it --
    # SXCN can only name the whole area its bulletin covers, which files the
    # central-coast lights under Hecate Strait and Trial Island under the
    # Strait of Georgia -- but region is a property of the station, not of the
    # reading, and taking FPCN61's answer here would move a station between
    # groups on the page every time a merge landed. `config/stations.json`
    # already carries a per-station region; wiring the export to read it is
    # the fix, and it is its own change.

    if keep_time is not None and keep_time != row["observation_time"]:
        updates["observation_time"] = keep_time

    # report_time_str is deliberately left alone: it labels the timestamp the
    # row kept, and overwriting it with the other bulletin's wording would
    # describe a time the row no longer carries.
    sources = row["source_file"] or ""
    incoming = obs.get("source_file")
    if incoming and incoming not in sources:
        updates["source_file"] = f"{sources}+{incoming}" if sources else incoming

    if not updates:
        return False

    assignments = ", ".join(f"{field} = ?" for field in updates)
    cur.execute(
        f"UPDATE lightstation_observation SET {assignments} WHERE id = ?",  # noqa: S608 - keys are literals above
        (*updates.values(), row["id"]),
    )
    return True


def insert_observations(observations):
    """Store parsed observations, merging each reading's second copy.

    See the cross-bulletin merge notes above: a station in both FPCN61 and an
    SXCN bulletin publishes one observation twice, so the second copy fills
    gaps in the first rather than becoming a row of its own.
    """
    if not observations:
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    inserted = 0
    merged = 0
    skipped = 0

    for obs in observations:
        try:
            existing, keep_time = find_existing_row(cur, obs)
            if existing is not None:
                if merge_observation(cur, existing, obs, keep_time):
                    merged += 1
                else:
                    skipped += 1
                continue

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

    logger.info(
        f"Inserted {inserted} new observations, merged {merged} second copies, "
        f"skipped {skipped} already stored"
    )


def purge_old_data():
    """Remove observations and raw files older than their retention windows.

    The two windows differ: observations are kept long enough to describe each
    station's publishing schedule (see lib/lightstation_schedule.py), while a
    raw bulletin is disposable the moment it has been parsed.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LIGHTSTATION_RETENTION_DAYS)
    raw_cutoff = now - timedelta(days=LIGHTSTATION_RAW_RETENTION_DAYS)
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
            if mtime < raw_cutoff:
                filepath.unlink()
                removed += 1
        if removed:
            logger.info(f"Removed {removed} raw FPCN61 files older than {LIGHTSTATION_RAW_RETENTION_DAYS} day(s)")

    # Purge raw SXCN files
    if SXCN_DATA_DIR.exists():
        removed = 0
        for filepath in SXCN_DATA_DIR.glob("SXCN*_CWVR_*"):
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
            if mtime < raw_cutoff:
                filepath.unlink()
                removed += 1
        if removed:
            logger.info(f"Removed {removed} raw SXCN files older than {LIGHTSTATION_RAW_RETENTION_DAYS} day(s)")


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
