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
import shutil
from pathlib import Path

from config import PROJECT_ROOT, EXPORT_DIR
from logging_config import setup_logging

logger = setup_logging('stations_json_export')

# ---------- Config ----------
BACKEND_STATIONS = PROJECT_ROOT / "config" / "stations.json"
FRONTEND_STATIONS = EXPORT_DIR / "stations.json"


def validate_stations_json(file_path):
    """
    Validate that stations.json is valid JSON and has expected structure.

    Returns: (is_valid, error_message)
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Check for expected top-level keys
        required_keys = ['buoys', 'tides', 'wind']
        missing_keys = [key for key in required_keys if key not in data]

        if missing_keys:
            return False, f"Missing required keys: {missing_keys}"

        # Check that each section is a dict
        for key in required_keys:
            if not isinstance(data[key], dict):
                return False, f"Key '{key}' must be a dictionary, got {type(data[key])}"

        # Count stations
        buoy_count = len(data['buoys'])
        tide_count = len(data['tides'])
        wind_count = len(data['wind'])

        logger.info(f"Validated stations.json: {buoy_count} buoys, {tide_count} tides, {wind_count} wind stations")

        return True, None

    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"
    except Exception as e:
        return False, f"Validation error: {e}"


def export_stations():
    """Export stations.json from backend to frontend."""
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

        # Copy to frontend
        shutil.copy2(BACKEND_STATIONS, FRONTEND_STATIONS)
        logger.info(f"Exported stations.json: {BACKEND_STATIONS} → {FRONTEND_STATIONS}")

        # Verify frontend file
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
