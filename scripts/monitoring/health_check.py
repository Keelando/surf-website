#!/usr/bin/env python3
"""
Health monitoring script for Salish Sea marine weather system.

Checks:
- Data freshness (buoys, wind, tides, lightstations)
- Database integrity (size, WAL mode, recent writes)
- Export file freshness and validity
- Basic cron job monitoring

Output: /home/keelando/site/data/system_health.json

Usage:
    python3 health_check.py [--verbose]
"""

import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from lib.config import EXPORT_DIR
from lib.logging_config import setup_logging

# Add lib to path for imports
from lib.stations import get_all_buoys, get_all_lightstations, get_all_tides, get_all_wind

# Setup logging (console disabled for cron, file only)
logger = setup_logging("health_check", console=False)

# Database paths
DB_DIR = Path.home() / ".local" / "share"
BUOY_DB = DB_DIR / "buoy_data.sqlite"
WIND_DB = DB_DIR / "wind_data.sqlite"
TIDE_DB = DB_DIR / "tide_data.sqlite"
LIGHTSTATION_DB = DB_DIR / "lightstation_data.sqlite"
STORM_SURGE_DB = DB_DIR / "storm_surge_forecast.sqlite"
WEATHER_DB = DB_DIR / "weather_data.sqlite"

# Export paths
SITE_DATA = EXPORT_DIR
OUTPUT_FILE = SITE_DATA / "system_health.json"

# Freshness thresholds (in hours)
THRESHOLDS = {
    "buoy": {"warning": 2, "error": 4},
    "wind": {"warning": 2, "error": 4},
    "tide": {"warning": 2, "error": 4},
    "lightstation": {"warning": 6, "error": 12},
    "webcam": {"warning": 2, "error": 24},  # 24h error threshold for daylight-only cams
}

# Stations known to be intermittent (report but don't flag as critical)
INTERMITTENT_STATIONS = {
    "TRIAL_ISLAND": "Lightstation - reports intermittently, extended gaps are normal",
}

VERBOSE = False


def log(msg: str):
    """Print if verbose mode enabled, also log at debug level."""
    logger.debug(msg)  # Always log to file at DEBUG level
    if VERBOSE:
        print(msg)  # Print to console only if verbose


def check_data_freshness() -> Dict:
    """Check all data sources for stale data."""
    log("\n=== Checking Data Freshness ===")

    stale_stations = []
    total_checked = 0
    status = "ok"

    # Check buoys
    log("Checking buoys...")
    buoy_stale = check_buoy_freshness()
    stale_stations.extend(buoy_stale)
    total_checked += len(get_all_buoys())

    # Check wind stations
    log("Checking wind stations...")
    wind_stale = check_wind_freshness()
    stale_stations.extend(wind_stale)
    total_checked += len(get_all_wind())

    # Check tide stations
    log("Checking tide stations...")
    tide_stale = check_tide_freshness()
    stale_stations.extend(tide_stale)
    total_checked += len(get_all_tides())

    # Check lightstations
    log("Checking lightstations...")
    lightstation_stale = check_lightstation_freshness()
    stale_stations.extend(lightstation_stale)
    total_checked += len(get_all_lightstations())

    # Check webcams
    log("Checking webcams...")
    webcam_stale = check_webcam_freshness()
    stale_stations.extend(webcam_stale)
    total_checked += len(_load_webcam_config())

    # Determine overall status (ignore 'info' severity for intermittent stations)
    error_stations = [s for s in stale_stations if s["severity"] == "error"]
    warning_stations = [s for s in stale_stations if s["severity"] == "warning"]

    if error_stations:
        status = "error"
    elif warning_stations:
        status = "warning"

    return {
        "status": status,
        "total_stations": total_checked,
        "stale_count": len(stale_stations),
        "stale_stations": stale_stations,
    }


def check_buoy_freshness() -> List[Dict]:
    """Check buoy data freshness."""
    stale = []
    now = datetime.now(timezone.utc)

    if not BUOY_DB.exists():
        log(f"  ⚠️  Database not found: {BUOY_DB}")
        return stale

    try:
        conn = sqlite3.connect(BUOY_DB)
        cursor = conn.cursor()

        for buoy_id, metadata in get_all_buoys().items():
            # Get most recent observation time for this buoy
            cursor.execute(
                """
                SELECT MAX(observation_time)
                FROM buoy_observation
                WHERE buoy_id = ?
            """,
                (buoy_id,),
            )

            result = cursor.fetchone()
            if not result or not result[0]:
                log(f"  ❌ {metadata['name']}: No data found")
                stale.append(
                    {
                        "id": buoy_id,
                        "name": metadata["name"],
                        "type": "buoy",
                        "age_hours": None,
                        "severity": "error",
                        "message": "No data",
                    }
                )
                continue

            last_obs = datetime.fromtimestamp(result[0], tz=timezone.utc)
            age = (now - last_obs).total_seconds() / 3600

            if age > THRESHOLDS["buoy"]["error"]:
                severity = "error"
                log(f"  ❌ {metadata['name']}: {age:.1f}h old (ERROR)")
            elif age > THRESHOLDS["buoy"]["warning"]:
                severity = "warning"
                log(f"  ⚠️  {metadata['name']}: {age:.1f}h old (WARNING)")
            else:
                log(f"  ✅ {metadata['name']}: {age:.1f}h old (OK)")
                continue

            stale.append(
                {
                    "id": buoy_id,
                    "name": metadata["name"],
                    "type": "buoy",
                    "age_hours": round(age, 1),
                    "last_observation": last_obs.isoformat(),
                    "severity": severity,
                }
            )

        conn.close()
    except Exception as e:
        log(f"  ❌ Error checking buoys: {e}")

    return stale


def check_wind_freshness() -> List[Dict]:
    """Check wind station data freshness."""
    stale = []
    now = datetime.now(timezone.utc)

    if not WIND_DB.exists():
        log(f"  ⚠️  Database not found: {WIND_DB}")
        return stale

    try:
        conn = sqlite3.connect(WIND_DB)
        cursor = conn.cursor()

        # Also check weather database for whiterock_east
        weather_conn = None
        weather_cursor = None
        if WEATHER_DB.exists():
            weather_conn = sqlite3.connect(WEATHER_DB)
            weather_cursor = weather_conn.cursor()

        for station_id, metadata in get_all_wind().items():
            # Special case: whiterock_east is in weather_data.sqlite
            if station_id == "whiterock_east" and weather_cursor:
                weather_cursor.execute("""
                    SELECT MAX(observation_time)
                    FROM whiterock_weather
                """)
                result = weather_cursor.fetchone()
            else:
                cursor.execute(
                    """
                    SELECT MAX(observation_time)
                    FROM wind_observation
                    WHERE station_id = ?
                """,
                    (station_id,),
                )
                result = cursor.fetchone()
            if not result or not result[0]:
                log(f"  ❌ {metadata['name']}: No data found")
                stale.append(
                    {
                        "id": station_id,
                        "name": metadata["name"],
                        "type": "wind",
                        "age_hours": None,
                        "severity": "error",
                        "message": "No data",
                    }
                )
                continue

            last_obs = datetime.fromtimestamp(result[0], tz=timezone.utc)
            age = (now - last_obs).total_seconds() / 3600

            if age > THRESHOLDS["wind"]["error"]:
                severity = "error"
                log(f"  ❌ {metadata['name']}: {age:.1f}h old (ERROR)")
            elif age > THRESHOLDS["wind"]["warning"]:
                severity = "warning"
                log(f"  ⚠️  {metadata['name']}: {age:.1f}h old (WARNING)")
            else:
                log(f"  ✅ {metadata['name']}: {age:.1f}h old (OK)")
                continue

            stale.append(
                {
                    "id": station_id,
                    "name": metadata["name"],
                    "type": "wind",
                    "age_hours": round(age, 1),
                    "last_observation": last_obs.isoformat(),
                    "severity": severity,
                }
            )

        conn.close()
        if weather_conn:
            weather_conn.close()
    except Exception as e:
        log(f"  ❌ Error checking wind stations: {e}")

    return stale


def check_tide_freshness() -> List[Dict]:
    """Check tide station observation freshness."""
    stale = []
    now = datetime.now(timezone.utc)

    if not TIDE_DB.exists():
        log(f"  ⚠️  Database not found: {TIDE_DB}")
        return stale

    try:
        conn = sqlite3.connect(TIDE_DB)
        cursor = conn.cursor()

        # Only check stations that should have observations (series contains 'wlo')
        for station_key, metadata in get_all_tides().items():
            if "wlo" not in metadata.get("series", []):
                log(f"  ⏭️  {metadata['name']}: No observations expected (predictions only)")
                continue

            # Map station_key to station_id (e.g., 'point_atkinson' -> '07795')
            station_id = metadata.get("id")
            if not station_id:
                continue

            cursor.execute(
                """
                SELECT MAX(observation_time)
                FROM tide_observation
                WHERE station_id = ?
            """,
                (station_id,),
            )

            result = cursor.fetchone()
            if not result or not result[0]:
                log(f"  ❌ {metadata['name']}: No data found")
                stale.append(
                    {
                        "id": station_key,
                        "name": metadata["name"],
                        "type": "tide",
                        "age_hours": None,
                        "severity": "error",
                        "message": "No data",
                    }
                )
                continue

            last_obs = datetime.fromtimestamp(result[0], tz=timezone.utc)
            age = (now - last_obs).total_seconds() / 3600

            if age > THRESHOLDS["tide"]["error"]:
                severity = "error"
                log(f"  ❌ {metadata['name']}: {age:.1f}h old (ERROR)")
            elif age > THRESHOLDS["tide"]["warning"]:
                severity = "warning"
                log(f"  ⚠️  {metadata['name']}: {age:.1f}h old (WARNING)")
            else:
                log(f"  ✅ {metadata['name']}: {age:.1f}h old (OK)")
                continue

            stale.append(
                {
                    "id": station_key,
                    "name": metadata["name"],
                    "type": "tide",
                    "age_hours": round(age, 1),
                    "last_observation": last_obs.isoformat(),
                    "severity": severity,
                }
            )

        conn.close()
    except Exception as e:
        log(f"  ❌ Error checking tide stations: {e}")

    return stale


def check_lightstation_freshness() -> List[Dict]:
    """Check lightstation data freshness against the station registry."""
    stale = []
    now = datetime.now(timezone.utc)

    if not LIGHTSTATION_DB.exists():
        log(f"  ⚠️  Database not found: {LIGHTSTATION_DB}")
        return stale

    try:
        conn = sqlite3.connect(LIGHTSTATION_DB)
        cursor = conn.cursor()

        # Build lookup of latest observation per station from DB
        cursor.execute("""
            SELECT station_name, MAX(observation_time) as last_obs
            FROM lightstation_observation
            GROUP BY station_name
        """)
        db_stations = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()

        # Check every station in the registry
        for station_id, metadata in get_all_lightstations().items():
            station_name = metadata["name"].upper()

            last_obs_timestamp = db_stations.get(station_name)

            if not last_obs_timestamp:
                log(f"  ❌ {metadata['name']}: No data found")
                stale.append(
                    {
                        "id": station_id,
                        "name": metadata["name"],
                        "type": "lightstation",
                        "age_hours": None,
                        "severity": "error",
                        "message": "No data",
                    }
                )
                continue

            last_obs = datetime.fromtimestamp(last_obs_timestamp, tz=timezone.utc)
            age = (now - last_obs).total_seconds() / 3600

            if age > THRESHOLDS["lightstation"]["error"]:
                severity = "error"
                if station_id in INTERMITTENT_STATIONS:
                    severity = "info"
                    log(f"  ℹ️  {metadata['name']}: {age:.1f}h old (INTERMITTENT - expected)")
                else:
                    log(f"  ❌ {metadata['name']}: {age:.1f}h old (ERROR)")
            elif age > THRESHOLDS["lightstation"]["warning"]:
                severity = "warning"
                log(f"  ⚠️  {metadata['name']}: {age:.1f}h old (WARNING)")
            else:
                log(f"  ✅ {metadata['name']}: {age:.1f}h old (OK)")
                continue

            stale_entry = {
                "id": station_id,
                "name": metadata["name"],
                "type": "lightstation",
                "age_hours": round(age, 1),
                "last_observation": last_obs.isoformat(),
                "severity": severity,
            }

            if station_id in INTERMITTENT_STATIONS:
                stale_entry["note"] = INTERMITTENT_STATIONS[station_id]

            stale.append(stale_entry)

    except Exception as e:
        log(f"  ❌ Error checking lightstations: {e}")

    return stale


def _load_webcam_config() -> Dict[str, Dict]:
    """Load webcam config from config/webcams.json (single source of truth).

    Translates the on-disk schema (interval_minutes, check_daylight, website_dir)
    into the fields this module uses. Returns {} if config is missing.
    """
    config_path = Path(__file__).resolve().parents[2] / "config" / "webcams.json"
    if not config_path.exists():
        log(f"  ⚠️  webcam config not found at {config_path}")
        return {}

    with open(config_path) as f:
        raw = json.load(f)

    project_root = Path(__file__).resolve().parents[2]
    webcams = {}
    for cam_id, cam in raw.items():
        # Skip `_`-prefixed meta keys (e.g. `_schedule_note`), matching the
        # loaders in fetch_webcam.py and storage_metrics_to_mqtt.py.
        if cam_id.startswith("_"):
            continue
        webcams[cam_id] = {
            "name": cam["name"],
            "path": project_root / cam["website_dir"] / "latest.json",
            "interval": cam.get("interval_minutes", 10),
            "daylight_only": cam.get("check_daylight", False),
            "disabled": cam.get("disabled_in_cron", False),
        }
    return webcams


def check_webcam_freshness() -> List[Dict]:
    """Check webcam image freshness."""
    stale = []
    now = datetime.now(timezone.utc)

    webcams = _load_webcam_config()

    for webcam_id, webcam_meta in webcams.items():
        if webcam_meta.get("disabled"):
            log(f"  ⏭️  {webcam_meta['name']}: Disabled in cron")
            continue

        if not webcam_meta["path"].exists():
            log(f"  ❌ {webcam_meta['name']}: Metadata file not found")
            stale.append(
                {
                    "id": webcam_id,
                    "name": webcam_meta["name"],
                    "type": "webcam",
                    "age_hours": None,
                    "severity": "error",
                    "message": "Metadata file not found",
                }
            )
            continue

        try:
            with open(webcam_meta["path"], "r") as f:
                metadata = json.load(f)

            last_update_str = metadata.get("timestamp")
            if not last_update_str:
                log(f"  ❌ {webcam_meta['name']}: No timestamp in metadata")
                stale.append(
                    {
                        "id": webcam_id,
                        "name": webcam_meta["name"],
                        "type": "webcam",
                        "age_hours": None,
                        "severity": "error",
                        "message": "No timestamp",
                    }
                )
                continue

            # Parse ISO timestamp
            last_update = datetime.fromisoformat(last_update_str.replace("Z", "+00:00"))
            age = (now - last_update).total_seconds() / 3600

            # Daylight-only cams: use relaxed thresholds
            if webcam_meta.get("daylight_only"):
                # Warning at 2x interval, error at 24h (full day missed)
                warning_threshold = max(webcam_meta["interval"] / 60 * 2, THRESHOLDS["webcam"]["warning"])
                error_threshold = THRESHOLDS["webcam"]["error"]
            else:
                # 24/7 cams: use stricter thresholds
                warning_threshold = webcam_meta["interval"] / 60 * 2  # 2x interval
                error_threshold = webcam_meta["interval"] / 60 * 4  # 4x interval

            if age > error_threshold:
                severity = "error"
                log(f"  ❌ {webcam_meta['name']}: {age:.1f}h old (ERROR)")
            elif age > warning_threshold:
                severity = "warning"
                log(f"  ⚠️  {webcam_meta['name']}: {age:.1f}h old (WARNING)")
            else:
                log(f"  ✅ {webcam_meta['name']}: {age:.1f}h old (OK)")
                continue

            stale.append(
                {
                    "id": webcam_id,
                    "name": webcam_meta["name"],
                    "type": "webcam",
                    "age_hours": round(age, 1),
                    "last_update": last_update.isoformat(),
                    "severity": severity,
                    "daylight_only": webcam_meta.get("daylight_only", False),
                }
            )

        except Exception as e:
            log(f"  ❌ {webcam_meta['name']}: Error reading metadata - {e}")
            stale.append(
                {
                    "id": webcam_id,
                    "name": webcam_meta["name"],
                    "type": "webcam",
                    "age_hours": None,
                    "severity": "error",
                    "message": str(e),
                }
            )

    return stale


def check_storage_mount() -> Dict:
    """Check if external storage drive is mounted and report disk usage."""
    log("\n=== Checking Storage Mount ===")

    mount_path = Path("/mnt/storage")
    mounted = os.path.ismount(mount_path)

    if not mounted:
        log("  ❌ /mnt/storage is NOT mounted — webcam archiving is disabled")
        return {
            "status": "error",
            "mounted": False,
            "message": "External storage drive is not mounted",
        }

    try:
        usage = shutil.disk_usage(mount_path)
        usage_percent = (usage.used / usage.total) * 100
        total_gb = usage.total / (1024**3)
        used_gb = usage.used / (1024**3)
        free_gb = usage.free / (1024**3)

        status = "ok"
        if usage_percent > 90:
            status = "error"
            log(f"  ❌ /mnt/storage: {usage_percent:.1f}% full (CRITICAL)")
        elif usage_percent > 80:
            status = "warning"
            log(f"  ⚠️  /mnt/storage: {usage_percent:.1f}% full (WARNING)")
        else:
            log(f"  ✅ /mnt/storage: {usage_percent:.1f}% full ({free_gb:.1f}GB free)")

        return {
            "status": status,
            "mounted": True,
            "total_gb": round(total_gb, 1),
            "used_gb": round(used_gb, 1),
            "free_gb": round(free_gb, 1),
            "usage_percent": round(usage_percent, 1),
        }
    except Exception as e:
        log(f"  ❌ /mnt/storage: Error reading disk usage - {e}")
        return {
            "status": "error",
            "mounted": True,
            "error": str(e),
        }


def check_database_integrity() -> Dict:
    """Check database health: size, WAL mode, recent writes."""
    log("\n=== Checking Database Integrity ===")

    databases = {
        "buoy_data": BUOY_DB,
        "wind_data": WIND_DB,
        "weather_data": WEATHER_DB,
        "tide_data": TIDE_DB,
        "lightstation_data": LIGHTSTATION_DB,
        "storm_surge": STORM_SURGE_DB,
    }

    status = "ok"
    db_status = {}

    for name, db_path in databases.items():
        if not db_path.exists():
            log(f"  ⚠️  {name}: Database not found")
            db_status[name] = {"exists": False, "status": "warning"}
            status = "warning"
            continue

        try:
            # Get file size
            size_mb = db_path.stat().st_size / (1024 * 1024)

            # Check WAL mode
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode;")
            journal_mode = cursor.fetchone()[0]

            # Count recent inserts (last hour)
            one_hour_ago = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp())

            # Try to find a table with observation_time
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

            recent_count = 0
            for table in tables:
                if "observation" in table.lower():
                    try:
                        cursor.execute(
                            f"""
                            SELECT COUNT(*) FROM {table}
                            WHERE observation_time > ?
                        """,
                            (one_hour_ago,),
                        )
                        recent_count += cursor.fetchone()[0]
                    except sqlite3.Error:
                        pass  # Table might not have observation_time column

            conn.close()

            log(f"  ✅ {name}: {size_mb:.1f}MB, {journal_mode} mode, {recent_count} recent writes")

            db_status[name] = {
                "exists": True,
                "size_mb": round(size_mb, 1),
                "journal_mode": journal_mode,
                "recent_writes_1h": recent_count,
                "status": "ok",
            }

        except Exception as e:
            log(f"  ❌ {name}: Error - {e}")
            db_status[name] = {"exists": True, "status": "error", "error": str(e)}
            status = "error"

    return {"status": status, "databases": db_status}


def check_export_freshness() -> Dict:
    """Check that JSON exports are recent and valid."""
    log("\n=== Checking Export Files ===")

    exports = {
        "latest_buoy": SITE_DATA / "latest_buoy_v2.json",
        "latest_wind": SITE_DATA / "latest_wind.json",
        "tide_latest": SITE_DATA / "tide-latest.json",
        "latest_lightstation": SITE_DATA / "latest_lightstation.json",
        "buoy_48hr": SITE_DATA / "buoy_timeseries_48h.json",
        "wind_48hr": SITE_DATA / "wind_timeseries_48hr.json",
        "lightstation_24hr": SITE_DATA / "lightstation_timeseries_24hr.json",
        "tide_timeseries": SITE_DATA / "tide-timeseries.json",
        "marine_forecast": SITE_DATA / "marine_forecast.json",
        "stations": SITE_DATA / "stations.json",
    }

    status = "ok"
    export_status = {}

    for name, path in exports.items():
        if not path.exists():
            log(f"  ⚠️  {name}: File not found")
            export_status[name] = {"exists": False, "status": "warning"}
            status = "warning"
            continue

        try:
            # Check file age
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            age_minutes = (datetime.now(timezone.utc) - mtime).total_seconds() / 60

            # Check file size
            size_kb = path.stat().st_size / 1024

            # Validate JSON
            with open(path, "r") as f:
                json.load(f)

            # Flag if file is stale (>1 hour old)
            file_status = "ok"
            if age_minutes > 60:
                file_status = "warning"
                status = "warning"
                log(f"  ⚠️  {name}: {age_minutes:.0f}min old (stale), {size_kb:.1f}KB")
            else:
                log(f"  ✅ {name}: {age_minutes:.0f}min old, {size_kb:.1f}KB")

            export_status[name] = {
                "exists": True,
                "age_minutes": round(age_minutes, 1),
                "size_kb": round(size_kb, 1),
                "valid_json": True,
                "status": file_status,
            }

        except json.JSONDecodeError as e:
            log(f"  ❌ {name}: Invalid JSON - {e}")
            export_status[name] = {"exists": True, "status": "error", "error": "Invalid JSON"}
            status = "error"
        except Exception as e:
            log(f"  ❌ {name}: Error - {e}")
            export_status[name] = {"exists": True, "status": "error", "error": str(e)}
            status = "error"

    return {"status": status, "exports": export_status}


def main():
    """Run all health checks and generate report."""
    global VERBOSE

    if "--verbose" in sys.argv:
        VERBOSE = True

    logger.info("Health Check Started")
    log("🏥 Health Check Started")
    log(f"Time: {datetime.now(timezone.utc).isoformat()}")

    # Run all checks
    storage_mount = check_storage_mount()
    data_freshness = check_data_freshness()
    db_integrity = check_database_integrity()
    export_freshness = check_export_freshness()

    # Determine overall status
    statuses = [storage_mount["status"], data_freshness["status"], db_integrity["status"], export_freshness["status"]]

    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"
    else:
        overall_status = "ok"

    # Build report
    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "checks": {
            "storage_mount": storage_mount,
            "data_freshness": data_freshness,
            "database_integrity": db_integrity,
            "export_freshness": export_freshness,
        },
    }

    # Write to output file
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    # Log summary at INFO level
    logger.info(
        "Health check complete: "
        f"{overall_status.upper()} | "
        f"Storage: {storage_mount['status']} | "
        f"Data: {data_freshness['status']} "
        f"({data_freshness['stale_count']} stale/{data_freshness['total_stations']} total) | "
        f"DB: {db_integrity['status']} | "
        f"Exports: {export_freshness['status']}"
    )

    # Log any issues
    if overall_status != "ok":
        stale_critical = [s for s in data_freshness["stale_stations"] if s["severity"] in ["error", "warning"]]
        if stale_critical:
            logger.warning(f"Stale stations: {', '.join([s['name'] for s in stale_critical])}")

    log(f"\n✅ Health check complete: {overall_status.upper()}")
    log(f"Report saved to: {OUTPUT_FILE}")

    # Exit with appropriate code
    if overall_status == "error":
        logger.error("Health check failed with errors")
        sys.exit(2)
    elif overall_status == "warning":
        logger.warning("Health check completed with warnings")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
