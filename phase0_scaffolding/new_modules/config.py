#!/usr/bin/env python3
"""
Centralized configuration for marine weather monitoring pipeline.

This module provides a single source of truth for:
- Database paths
- Export directories
- Configuration file locations
- Freshness policies
- Field definitions

All paths use environment variables when available, with sensible defaults.
"""

from pathlib import Path
import os

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

# Allow override via environment variables
DATA_DIR = Path(os.getenv("MARINE_DATA_DIR", "~/.local/share")).expanduser()
SITE_DIR = Path(os.getenv("SITE_DIR", "~/site")).expanduser()

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
SITE_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATABASE PATHS
# =============================================================================

BUOY_DATABASE = DATA_DIR / "buoy_data.sqlite"
TIDE_DATABASE = DATA_DIR / "tide_data.sqlite"
STORM_SURGE_DATABASE = DATA_DIR / "storm_surge_forecast.sqlite"

# Future databases (uncomment when implemented)
# WIND_DATABASE = DATA_DIR / "wind_data.sqlite"
# MARINE_TRAFFIC_DATABASE = DATA_DIR / "marine_traffic.sqlite"

# =============================================================================
# CONFIGURATION FILE PATHS
# =============================================================================

# Repository root (calculated from this file's location when in src/core/)
# For Phase 0 (root): just use Path.cwd()
# After Phase 2 (src/core/): use Path(__file__).parent.parent.parent
REPO_ROOT = Path.cwd()  # Will update in Phase 2

# Configuration directory
CONFIG_DIR = REPO_ROOT / "config"

# Station registry (master metadata)
STATIONS_JSON = CONFIG_DIR / "stations.json"

# sr3 (Sarracenia) configuration directory
SR3_CONFIG_DIR = CONFIG_DIR / "sr3"

# =============================================================================
# EXPORT PATHS
# =============================================================================

# Website data directory
SITE_DATA_DIR = SITE_DIR / "data"
SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Buoy data exports
BUOY_LATEST_JSON = SITE_DATA_DIR / "latest_buoy_v2.json"
BUOY_TIMESERIES_JSON = SITE_DATA_DIR / "buoy_timeseries_24h.json"

# Tide data exports
TIDE_DATA_DIR = SITE_DATA_DIR / "tide"
TIDE_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Storm surge exports
STORM_SURGE_DIR = SITE_DATA_DIR / "storm_surge"
STORM_SURGE_DIR.mkdir(parents=True, exist_ok=True)

# Marine forecast export
MARINE_FORECAST_JSON = SITE_DATA_DIR / "marine_forecast.json"

# =============================================================================
# FRESHNESS POLICIES
# =============================================================================

# How old can buoy data be before we stop displaying it?
BUOY_FRESHNESS_WINDOW = 2 * 3600  # 2 hours in seconds

# How old can tide observations be before considered stale?
TIDE_OBS_FRESHNESS_WINDOW = 3 * 3600  # 3 hours in seconds

# How old can storm surge forecasts be?
STORM_SURGE_FRESHNESS_WINDOW = 8 * 3600  # 8 hours in seconds

# =============================================================================
# FIELD DEFINITIONS
# =============================================================================

# All available buoy observation fields
BUOY_FIELDS = [
    # Wave metrics
    "wave_height_sig",
    "wave_height_peak",
    "wave_period_sig",
    "wave_period_avg",
    "wave_period_peak",
    "wave_direction_avg",
    "wave_direction_peak",

    # Spectral wave data (NOAA only)
    "swell_height",
    "swell_period",
    "swell_direction",
    "wind_wave_height",
    "wind_wave_period",
    "wind_wave_direction",

    # Wind metrics
    "wind_speed",
    "wind_gust",
    "wind_direction",

    # Temperature and pressure
    "air_temp",
    "sea_temp",
    "pressure",
]

# Field metadata for exports (name, unit, display info)
FIELD_METADATA = {
    # Wave fields
    "wave_height_sig": {
        "name": "Significant Wave Height",
        "unit": "m",
        "description": "Average height of highest 1/3 of waves",
    },
    "wave_height_peak": {
        "name": "Peak Wave Height",
        "unit": "m",
        "description": "Maximum wave height",
    },
    "wave_period_sig": {
        "name": "Significant Wave Period",
        "unit": "s",
        "description": "Period of significant waves",
    },
    "wave_period_avg": {
        "name": "Average Wave Period",
        "unit": "s",
        "description": "Mean time between wave crests",
    },
    "wave_period_peak": {
        "name": "Peak Wave Period",
        "unit": "s",
        "description": "Period of most energetic waves",
    },
    "wave_direction_avg": {
        "name": "Average Wave Direction",
        "unit": "°",
        "description": "Mean direction waves are coming from",
    },
    "wave_direction_peak": {
        "name": "Peak Wave Direction",
        "unit": "°",
        "description": "Direction of most energetic waves",
    },

    # Spectral wave fields (NOAA only)
    "swell_height": {
        "name": "Swell Height",
        "unit": "m",
        "description": "Long-period ocean waves from distant storms",
    },
    "swell_period": {
        "name": "Swell Period",
        "unit": "s",
        "description": "Period of swell waves",
    },
    "swell_direction": {
        "name": "Swell Direction",
        "unit": "°",
        "description": "Direction swell is coming from",
    },
    "wind_wave_height": {
        "name": "Wind Wave Height",
        "unit": "m",
        "description": "Short-period locally-generated waves",
    },
    "wind_wave_period": {
        "name": "Wind Wave Period",
        "unit": "s",
        "description": "Period of wind waves",
    },
    "wind_wave_direction": {
        "name": "Wind Wave Direction",
        "unit": "°",
        "description": "Direction wind waves are coming from",
    },

    # Wind fields
    "wind_speed": {
        "name": "Wind Speed",
        "unit": "kt",
        "description": "Sustained wind speed (display unit)",
        "storage_unit": "km/h",  # Stored as km/h, displayed as knots
    },
    "wind_gust": {
        "name": "Wind Gust",
        "unit": "kt",
        "description": "Peak wind gust (display unit)",
        "storage_unit": "km/h",
    },
    "wind_direction": {
        "name": "Wind Direction",
        "unit": "°",
        "description": "Direction wind is coming from",
    },

    # Temperature fields
    "air_temp": {
        "name": "Air Temperature",
        "unit": "°C",
        "description": "Ambient air temperature",
    },
    "sea_temp": {
        "name": "Sea Temperature",
        "unit": "°C",
        "description": "Sea surface temperature",
    },

    # Pressure field
    "pressure": {
        "name": "Atmospheric Pressure",
        "unit": "hPa",
        "description": "Barometric pressure (999 hPa is valid low pressure!)",
    },
}

# =============================================================================
# TIDE STATION CONFIGURATION
# =============================================================================

# Tide prediction window (how many days ahead to fetch)
TIDE_PREDICTION_DAYS = 3  # Extended from 2 to support 2-day water level forecasts

# Tide high/low event window
TIDE_HIGHLOW_HOURS = 48  # Fetch next 48 hours of high/low events

# =============================================================================
# STORM SURGE CONFIGURATION
# =============================================================================

# GDSPS (Global Deterministic Surge Prediction System) configuration
GDSPS_MODEL = "global-surge"
GDSPS_FORECAST_HOURS = 240  # 10 days

# Which forecast run to archive for hindcast analysis
# 18Z run is closest to noon Pacific time (11 AM PST / 10 AM PDT)
HINDCAST_ARCHIVE_RUN = "18"  # Archive 18Z run to database

# Hindcast export range (hours 38-61 = full calendar day 2 days ahead)
HINDCAST_START_HOUR = 38
HINDCAST_END_HOUR = 61

# =============================================================================
# NOAA CONFIGURATION
# =============================================================================

# NOAA NDBC buoy data URLs
NOAA_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2"

# Data retention (how many days to keep in database before purging)
BUOY_DATA_RETENTION_DAYS = 7
TIDE_DATA_RETENTION_DAYS = 30
STORM_SURGE_RETENTION_DAYS = 14

# =============================================================================
# MQTT CONFIGURATION (Home Assistant Integration)
# =============================================================================

# MQTT topics (optional integration)
MQTT_BASE_TOPIC = "marine_weather"
MQTT_BUOY_TOPIC = f"{MQTT_BASE_TOPIC}/buoys"
MQTT_TIDE_TOPIC = f"{MQTT_BASE_TOPIC}/tides"
MQTT_STORM_SURGE_TOPIC = f"{MQTT_BASE_TOPIC}/storm_surge"

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

# Log directory (for future centralized logging)
LOG_DIR = REPO_ROOT / "logs"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_database_path(db_type):
    """
    Get database path by type.

    Args:
        db_type: One of 'buoy', 'tide', 'storm_surge', 'wind'

    Returns:
        Path object for database

    Raises:
        ValueError: If db_type not recognized
    """
    db_map = {
        'buoy': BUOY_DATABASE,
        'tide': TIDE_DATABASE,
        'storm_surge': STORM_SURGE_DATABASE,
        # 'wind': WIND_DATABASE,  # Uncomment when wind database added
    }

    if db_type not in db_map:
        raise ValueError(f"Unknown database type: {db_type}")

    return db_map[db_type]


def get_export_path(export_type):
    """
    Get export JSON path by type.

    Args:
        export_type: One of 'buoy_latest', 'buoy_timeseries', 'marine_forecast'

    Returns:
        Path object for export file

    Raises:
        ValueError: If export_type not recognized
    """
    export_map = {
        'buoy_latest': BUOY_LATEST_JSON,
        'buoy_timeseries': BUOY_TIMESERIES_JSON,
        'marine_forecast': MARINE_FORECAST_JSON,
    }

    if export_type not in export_map:
        raise ValueError(f"Unknown export type: {export_type}")

    return export_map[export_type]


def validate_config():
    """
    Validate configuration paths and directories exist.

    Returns:
        bool: True if all critical paths exist, False otherwise
    """
    critical_paths = [
        DATA_DIR,
        SITE_DIR,
        SITE_DATA_DIR,
    ]

    all_exist = True
    for path in critical_paths:
        if not path.exists():
            print(f"⚠️  Warning: Path does not exist: {path}")
            all_exist = False

    return all_exist


if __name__ == "__main__":
    # Configuration self-test and display
    print("Marine Weather Pipeline Configuration")
    print("=" * 60)

    print("\n📁 DATA DIRECTORIES:")
    print(f"  Data directory:       {DATA_DIR}")
    print(f"  Site directory:       {SITE_DIR}")
    print(f"  Buoy database:        {BUOY_DATABASE}")
    print(f"  Tide database:        {TIDE_DATABASE}")
    print(f"  Storm surge database: {STORM_SURGE_DATABASE}")

    print("\n📄 EXPORT PATHS:")
    print(f"  Buoy latest:          {BUOY_LATEST_JSON}")
    print(f"  Buoy timeseries:      {BUOY_TIMESERIES_JSON}")
    print(f"  Marine forecast:      {MARINE_FORECAST_JSON}")
    print(f"  Tide data dir:        {TIDE_DATA_DIR}")
    print(f"  Storm surge dir:      {STORM_SURGE_DIR}")

    print("\n⏱️  FRESHNESS WINDOWS:")
    print(f"  Buoy data:            {BUOY_FRESHNESS_WINDOW // 3600} hours")
    print(f"  Tide observations:    {TIDE_OBS_FRESHNESS_WINDOW // 3600} hours")
    print(f"  Storm surge forecast: {STORM_SURGE_FRESHNESS_WINDOW // 3600} hours")

    print("\n📊 FIELD COUNTS:")
    print(f"  Total buoy fields:    {len(BUOY_FIELDS)}")
    print(f"  Field metadata:       {len(FIELD_METADATA)}")

    print("\n🔍 CONFIGURATION VALIDATION:")
    if validate_config():
        print("  ✅ All critical paths exist")
    else:
        print("  ⚠️  Some paths missing (will be created on first run)")

    print("\n✅ Configuration loaded successfully!")
