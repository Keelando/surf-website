#!/usr/bin/env python3
"""
Centralized configuration for marine weather monitoring system.

This module provides database paths, export directories, and constants used
across all data processing scripts. Centralizing these values makes it easier
to maintain and update the system.

Usage:
    from config import BUOY_DATABASE, TIDE_DATABASE, EXPORT_DIR
    from config import BUOY_FRESHNESS_WINDOW, STATIONS_FILE

    # Open database
    conn = sqlite3.connect(BUOY_DATABASE)

    # Use freshness window
    cutoff_time = int(time.time()) - BUOY_FRESHNESS_WINDOW
"""

import json
from pathlib import Path

# =============================================================================
# Directory Paths
# =============================================================================

# Data storage directory (SQLite databases)
DATA_DIR = Path("~/.local/share").expanduser()

# Project root directory (lib/config.py -> lib/ -> project root)
PROJECT_ROOT = Path(__file__).parent.parent

# Export directory (JSON files for website)
EXPORT_DIR = PROJECT_ROOT / "site" / "data"

# =============================================================================
# Database Paths
# =============================================================================

# Buoy observations database (Environment Canada + NOAA)
BUOY_DATABASE = DATA_DIR / "buoy_data.sqlite"

# Tide observations and predictions database (DFO IWLS)
TIDE_DATABASE = DATA_DIR / "tide_data.sqlite"

# Storm surge forecast database (GDSPS)
STORM_SURGE_DATABASE = DATA_DIR / "storm_surge_forecast.sqlite"

# Wind observations database (Environment Canada land stations)
WIND_DATABASE = DATA_DIR / "wind_data.sqlite"

# Lightstation observations database (DFO lightstation reports)
LIGHTSTATION_DATABASE = DATA_DIR / "lightstation_data.sqlite"

# White Rock weather database (whiterock_east station)
WEATHER_DATABASE = DATA_DIR / "weather_data.sqlite"

# =============================================================================
# Configuration Files
# =============================================================================

# Master station registry (unified buoys and tide stations)
STATIONS_FILE = PROJECT_ROOT / "config" / "stations.json"

# =============================================================================
# Data Retention Periods (days)
# =============================================================================

# How long to keep buoy observations
BUOY_RETENTION_DAYS = 30

# How long to keep wind observations
WIND_RETENTION_DAYS = 30

# How long to keep tide observations (used for storm surge calculation)
TIDE_OBS_RETENTION_DAYS = 11

# How long to keep tide predictions
TIDE_PRED_RETENTION_DAYS = 3

# How long to keep high/low tide events
TIDE_HIGHLOW_RETENTION_DAYS = 3

# How long to keep storm surge forecasts (for hindcast analysis)
STORM_SURGE_RETENTION_DAYS = 30

# How long to keep lightstation observations
LIGHTSTATION_RETENTION_DAYS = 7

# =============================================================================
# Freshness Windows (seconds)
# =============================================================================

# How recent data must be to be considered "fresh" for exports
# 2 hours = allows for occasional reporting gaps while catching stale data
BUOY_FRESHNESS_WINDOW = 7200  # 2 hours

# Tide observations update every 30 minutes
TIDE_FRESHNESS_WINDOW = 7200  # 2 hours (allows 4 missed updates)

# Marine forecasts update 2-4 times daily
MARINE_FORECAST_FRESHNESS_WINDOW = 86400  # 24 hours

# =============================================================================
# Update Frequencies (minutes)
# =============================================================================

# How often scripts typically run (for reference/documentation)
UPDATE_FREQ = {
    "buoy_observations": 1,  # Every minute
    "noaa_buoy": 20,  # Every 20 minutes
    "tide_observations": 30,  # Every 30 minutes
    "tide_predictions": 1440,  # Daily (at midnight)
    "tide_highlow": 1440,  # Daily (at midnight)
    "storm_surge": 360,  # Every 6 hours
    "marine_forecast": 30,  # Every 30 minutes
    "json_exports": 1,  # Every minute (latest snapshot)
    "timeseries_exports": 5,  # Every 5 minutes
    "tide_json": 5,  # Every 5 minutes
    "mqtt": 1,  # Every minute
}

# =============================================================================
# External Service URLs
# =============================================================================

# DFO IWLS API base URL
DFO_IWLS_API_BASE = "https://api-iwls.dfo-mpo.gc.ca/api/v1"

# Environment Canada GeoMet WMS base URL (for GDSPS storm surge)
GEOMET_WMS_BASE = "https://geo.weather.gc.ca/geomet"

# NOAA NDBC data base URL
NOAA_NDBC_BASE = "https://www.ndbc.noaa.gov/data/realtime2"

# =============================================================================
# Data Quality Thresholds
# =============================================================================

# Maximum reasonable values (for sanity checking)
MAX_WAVE_HEIGHT = 20.0  # meters
MAX_WIND_SPEED = 200.0  # km/h
MAX_WATER_TEMP = 30.0  # °C
MIN_WATER_TEMP = -5.0  # °C (can be below freezing due to salinity)

# NOAA data quality: Minimum valid pressure (999 hPa is valid low pressure!)
MIN_VALID_PRESSURE = 900.0  # hPa

# =============================================================================
# Display Constants
# =============================================================================

# Number of decimal places for various measurements
DECIMALS = {
    "wave_height": 1,  # meters
    "wind_speed": 1,  # km/h or knots
    "water_temp": 1,  # °C
    "pressure": 1,  # hPa
    "tide_level": 2,  # meters
    "storm_surge": 3,  # meters
}

# =============================================================================
# Helper Functions
# =============================================================================


def ensure_directories():
    """
    Create necessary directories if they don't exist.

    Call this at the start of scripts that write to databases or export files.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "config").mkdir(parents=True, exist_ok=True)


def get_database_info():
    """
    Return dictionary of database paths and their existence status.

    Useful for diagnostics and troubleshooting.
    """
    return {
        "buoy_database": {
            "path": str(BUOY_DATABASE),
            "exists": BUOY_DATABASE.exists(),
            "size_mb": BUOY_DATABASE.stat().st_size / 1024 / 1024 if BUOY_DATABASE.exists() else 0,
        },
        "tide_database": {
            "path": str(TIDE_DATABASE),
            "exists": TIDE_DATABASE.exists(),
            "size_mb": TIDE_DATABASE.stat().st_size / 1024 / 1024 if TIDE_DATABASE.exists() else 0,
        },
        "storm_surge_database": {
            "path": str(STORM_SURGE_DATABASE),
            "exists": STORM_SURGE_DATABASE.exists(),
            "size_mb": STORM_SURGE_DATABASE.stat().st_size / 1024 / 1024 if STORM_SURGE_DATABASE.exists() else 0,
        },
        "wind_database": {
            "path": str(WIND_DATABASE),
            "exists": WIND_DATABASE.exists(),
            "size_mb": WIND_DATABASE.stat().st_size / 1024 / 1024 if WIND_DATABASE.exists() else 0,
        },
    }


def safe_json_write(path: Path, data: dict, sort_keys: bool = False, indent: int = 2):
    """
    Atomic JSON write: temp file + rename to avoid partial writes.

    This prevents race conditions where a reader might get incomplete JSON
    during the write operation. The temp file is written completely, then
    atomically renamed to the target path.

    Args:
        path: Target file path (will be created/overwritten)
        data: Dictionary to serialize as JSON
        sort_keys: Whether to sort dictionary keys (default False)
        indent: JSON indentation spaces (default 2)

    Example:
        from config import safe_json_write, EXPORT_DIR

        data = {"station": "Point_Atkinson", "wave_height": 1.5}
        safe_json_write(EXPORT_DIR / "latest.json", data)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=indent, sort_keys=sort_keys))
    tmp.replace(path)


# Test/demonstration code
if __name__ == "__main__":
    print("Marine Weather Monitoring Configuration")
    print("=" * 70)

    print("\n📂 Directory Paths:")
    print(f"  Data directory: {DATA_DIR}")
    print(f"  Export directory: {EXPORT_DIR}")
    print(f"  Project root: {PROJECT_ROOT}")

    print("\n💾 Database Paths:")
    print(f"  Buoy database: {BUOY_DATABASE}")
    print(f"    Exists: {BUOY_DATABASE.exists()}")
    print(f"  Tide database: {TIDE_DATABASE}")
    print(f"    Exists: {TIDE_DATABASE.exists()}")
    print(f"  Storm surge database: {STORM_SURGE_DATABASE}")
    print(f"    Exists: {STORM_SURGE_DATABASE.exists()}")

    print("\n📄 Configuration Files:")
    print(f"  Stations registry: {STATIONS_FILE}")
    print(f"    Exists: {STATIONS_FILE.exists()}")

    print("\n⏱️  Freshness Windows:")
    print(f"  Buoy data: {BUOY_FRESHNESS_WINDOW / 3600:.1f} hours")
    print(f"  Tide data: {TIDE_FRESHNESS_WINDOW / 3600:.1f} hours")
    print(f"  Marine forecast: {MARINE_FORECAST_FRESHNESS_WINDOW / 3600:.1f} hours")

    print("\n🗑️  Retention Periods:")
    print(f"  Buoy observations: {BUOY_RETENTION_DAYS} days")
    print(f"  Tide observations: {TIDE_OBS_RETENTION_DAYS} days")
    print(f"  Tide predictions: {TIDE_PRED_RETENTION_DAYS} days")
    print(f"  Storm surge forecasts: {STORM_SURGE_RETENTION_DAYS} days")

    print("\n📊 Database Info:")
    for db_name, info in get_database_info().items():
        print(f"  {db_name}:")
        print(f"    Path: {info['path']}")
        print(f"    Exists: {info['exists']}")
        if info["exists"]:
            print(f"    Size: {info['size_mb']:.2f} MB")

    print("\n✅ Configuration loaded successfully")
