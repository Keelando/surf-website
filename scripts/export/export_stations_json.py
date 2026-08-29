#!/usr/bin/env python3
"""
Export stations.json metadata from backend to frontend.

Copies and validates the stations registry file from the backend config
directory to the frontend data directory for website consumption.

Source: ~/envcan_wave/config/stations.json
Output: ~/site/data/stations.json

This ensures the frontend always has the latest station metadata including
coordinates, names, types, and data sources for all buoys, tide stations,
and wind stations.
"""

import json

from lib.config import EXPORT_DIR, PROJECT_ROOT
from lib.logging_config import setup_logging

logger = setup_logging("stations_json_export")

# ---------- Config ----------
BACKEND_STATIONS = PROJECT_ROOT / "config" / "stations.json"
FRONTEND_STATIONS = EXPORT_DIR / "stations.json"

# ---------- Public field allowlist ----------
#
# site/data/stations.json is served to the world twice: directly at
# /data/stations.json, and as the /api/v1/stations endpoint. This export used
# to copy config/stations.json wholesale, so every field ever added to the
# registry was published on the next hourly run - including internal
# FlowWorks channel maps and operator notes.
#
# This is an ALLOWLIST, not a denylist: a new field in the registry stays
# private until it is named here deliberately. See the "two public surfaces"
# note in CLAUDE.md and docs/PUBLIC_API.md.
#
# Deliberately NOT published:
#   channels, fallback_channels, flowworks_site_id
#       internal FlowWorks sensor plumbing; meaningless without our credentials
#   url
#       the upstream endpoint we poll. source_url (the human-facing station
#       page) IS published; this one is a fetch target and stays private so
#       consumers bind to our contract rather than to our upstreams.
_COMMON = {"id", "name", "short_name", "location", "lat", "lon", "source", "type"}

PUBLIC_STATION_FIELDS = {
    "buoys": _COMMON
    | {
        "data_types",
        "update_frequency_minutes",
        "flag",
        "source_url",
        "caveat",
        "note",
        "wave_display",
        "wave_display_note",
    },
    "tides": _COMMON | {"data_types", "update_frequency_minutes", "code", "series", "note"},
    "wind": _COMMON
    | {
        "data_types",
        "update_frequency_minutes",
        "flag",
        "source_url",
        "elevation_m",
        "is_buoy",
        "caveat",
        "note",
    },
    "lightstations": _COMMON
    | {
        "update_frequency_hours",
        "region",
        "established",
        "icao",
        "notes",
        "reporting",
        "reporting_note",
    },
    "webcams": _COMMON | {"page_url", "update_frequency_minutes", "stream_delay_minutes"},
}

# _metadata is hand-written prose, so it gets its own allowlist. `notes`
# is filtered separately: notes.flowworks_api documents the upstream API's
# auth scheme and credentials, which is operator documentation and has no
# place in a public marine-data payload.
PUBLIC_METADATA_FIELDS = {
    "version",
    "updated",
    "description",
    "sources",
    "coordinate_system",
    "units",
    "notes",
}
PUBLIC_METADATA_NOTES = {"tide_series", "buoy_sources"}


def filter_public_fields(data):
    """Reduce the canonical registry to the fields that may be published.

    Returns a new dict; the input is not modified.
    """
    out = {}
    for section, entries in data.items():
        if section == "_metadata":
            meta = {k: v for k, v in entries.items() if k in PUBLIC_METADATA_FIELDS}
            if isinstance(meta.get("notes"), dict):
                meta["notes"] = {k: v for k, v in meta["notes"].items() if k in PUBLIC_METADATA_NOTES}
            out[section] = meta
            continue

        allowed = PUBLIC_STATION_FIELDS.get(section)
        if allowed is None:
            # An unrecognised section is withheld rather than guessed at, so
            # adding one to the registry cannot silently publish it.
            logger.warning(f"Section '{section}' has no public field allowlist; not exported")
            continue

        out[section] = {
            sid: {k: v for k, v in entry.items() if k in allowed} if isinstance(entry, dict) else entry
            for sid, entry in entries.items()
        }
    return out


def validate_stations_json(file_path):
    """
    Validate that stations.json is valid JSON and has expected structure.

    Returns: (is_valid, error_message)
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Check for expected top-level keys
        required_keys = ["buoys", "tides", "wind"]
        optional_keys = ["lightstations", "webcams"]

        missing_keys = [key for key in required_keys if key not in data]

        if missing_keys:
            return False, f"Missing required keys: {missing_keys}"

        # Check that each section is a dict
        all_keys = required_keys + [k for k in optional_keys if k in data]
        for key in all_keys:
            if not isinstance(data[key], dict):
                return False, f"Key '{key}' must be a dictionary, got {type(data[key])}"

        # Count stations
        counts = []
        counts.append(f"{len(data['buoys'])} buoys")
        counts.append(f"{len(data['tides'])} tides")
        counts.append(f"{len(data['wind'])} wind stations")

        if "lightstations" in data:
            counts.append(f"{len(data['lightstations'])} lightstations")
        if "webcams" in data:
            counts.append(f"{len(data['webcams'])} webcams")

        logger.info(f"Validated stations.json: {', '.join(counts)}")

        return True, None

    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"
    except Exception as e:
        return False, f"Validation error: {e}"


def export_stations():
    """Export stations.json from backend to frontend with warning header."""
    try:
        # Check if backend file exists
        if not BACKEND_STATIONS.exists():
            logger.error(f"Backend stations.json not found: {BACKEND_STATIONS}")
            return False

        # Validate backend file before copying
        is_valid, error = validate_stations_json(BACKEND_STATIONS)
        if not is_valid:
            logger.error(f"Backend stations.json validation failed: {error}")
            return False

        # Load backend data
        with open(BACKEND_STATIONS, "r") as f:
            data = json.load(f)

        # Add warning header (ordered dict to put it at the top)
        from collections import OrderedDict

        output = OrderedDict()
        output["_DO_NOT_EDIT"] = {
            "warning": "This file is AUTO-GENERATED by export_stations_json.py",
            "source": "~/envcan_wave/config/stations.json",
            "instructions": "To add/edit stations, edit the backend source file and re-run the export script",
            "last_generated": None,  # Will be set below
        }

        # Add timestamp
        from datetime import datetime, timezone

        output["_DO_NOT_EDIT"]["last_generated"] = datetime.now(timezone.utc).isoformat()

        # Add station data, reduced to the public field allowlist
        for key, value in filter_public_fields(data).items():
            output[key] = value

        # Write to frontend with warning
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        with open(FRONTEND_STATIONS, "w") as f:
            json.dump(output, f, indent=2)

        logger.info(f"Exported stations.json: {BACKEND_STATIONS} → {FRONTEND_STATIONS}")

        # Verify frontend file (skip _DO_NOT_EDIT key in validation)
        is_valid, error = validate_stations_json(FRONTEND_STATIONS)
        if not is_valid:
            logger.error(f"Frontend stations.json validation failed after copy: {error}")
            return False

        return True

    except Exception as e:
        logger.error(f"Error exporting stations.json: {e}")
        return False


if __name__ == "__main__":
    success = export_stations()
    exit(0 if success else 1)
