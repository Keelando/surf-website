#!/usr/bin/env python3
"""
Wave Forecast Fetcher for Surf Server
Fetches RDWPS (national 2.5 km) wave forecasts from Environment Canada GeoMet
WMS via point extraction at buoy locations, storing every run to the database
for model-vs-buoy validation (bias/RMSE scoring).

Verified 2026-08-15: GetFeatureInfo values are bit-identical to the raw GRIB2
files, so nothing is lost by extracting points over WMS instead of downloading
grids. Full parameter inventory and field-selection rationale in
docs/project/RDWPS_PARAMETERS.md.
"""

import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from lib.config import (
    EXPORT_DIR,
    GEOMET_WMS_BASE,
    WAVE_FORECAST_DATABASE,
    WAVE_FORECAST_RETENTION_DAYS,
    safe_json_write,
)
from lib.logging_config import setup_logging
from lib.stations import get_buoy

logger = setup_logging("wave_forecast")

# GeoMet layers to fetch, keyed by our field names.
# Heights in metres, periods in seconds, directions degrees true coming FROM
# (matches the site-wide meteorological convention — verified against the
# model's own wind field, see docs/project/RDWPS_PARAMETERS.md).
VARIABLES = {
    "wave_height": "RDWPS_2.5km_SignificantWaveHeight",
    "peak_period": "RDWPS_2.5km_PeakWavePeriod",
    "wave_direction": "RDWPS_2.5km_MeanWaveDir",
    "wind_wave_height": "RDWPS_2.5km_WindWavesSignificantHeight",
}

UNITS = {
    "wave_height": "m",
    "peak_period": "s",
    "wave_direction": "degrees_true_from",
    "wind_wave_height": "m",
}

# Buoys to extract at (ids from config/stations.json). Halibut Bank first;
# add the other EC buoys once the validation run looks sane.
BUOY_IDS = ["4600146"]

OUTPUT_DIR = EXPORT_DIR / "wave_forecast"
LOCKFILE = Path("/tmp/wave_forecast_fetch.lock")

# WMS 1.3.0 + EPSG:4326 uses lat,lon axis order. We ask for a small box
# centred on the buoy and query the centre pixel.
BBOX_OFFSET = 0.02  # degrees
FETCH_DELAY = 0.5  # seconds between requests (rate limiting)
REQUEST_TIMEOUT = 60  # seconds


def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        age = time.time() - LOCKFILE.stat().st_mtime
        if age > 900:  # 15 minutes (a full fetch takes a few minutes)
            logger.info("⚠️  Removing stale lock file")
            LOCKFILE.unlink()
        else:
            logger.info(f"⚠️  Another instance is running (lock age: {age:.0f}s), exiting")
            return False
    LOCKFILE.touch()
    return True


def release_lock():
    """Remove lock file."""
    if LOCKFILE.exists():
        LOCKFILE.unlink()


def ensure_db_schema(conn):
    """Create forecast storage table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wave_forecast (
            station_id TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            valid_time INTEGER NOT NULL,
            value REAL NOT NULL,
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wave_forecast_station_run
        ON wave_forecast(station_id, forecast_run_time DESC)
    """)
    conn.commit()


def get_time_steps(layer):
    """Read the layer's time dimension from a single-layer GetCapabilities.

    GeoMet supports filtering capabilities to one layer (multi-layer filters
    are rejected), which keeps the response tiny. Returns a list of datetimes.
    """
    resp = requests.get(
        GEOMET_WMS_BASE,
        params={
            "SERVICE": "WMS",
            "VERSION": "1.3.0",
            "REQUEST": "GetCapabilities",
            "LAYERS": layer,
        },
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    match = re.search(
        r'<Dimension name="time"[^>]*>([^<]+)</Dimension>',
        resp.text,
    )
    if not match:
        raise ValueError(f"No time dimension found for layer {layer}")

    start_str, end_str, interval_str = match.group(1).split("/")
    iso_format = "%Y-%m-%dT%H:%M:%SZ"
    start_time = datetime.strptime(start_str, iso_format).replace(tzinfo=timezone.utc)
    end_time = datetime.strptime(end_str, iso_format).replace(tzinfo=timezone.utc)
    interval_hours = int(re.sub(r"\D", "", interval_str))

    steps = [start_time]
    while steps[-1] < end_time:
        steps.append(steps[-1] + timedelta(hours=interval_hours))
    return steps


def fetch_point(layer, lat, lon, timestamp):
    """Fetch one value at one location and time.

    Returns (value, model_run_time) — both None if the cell is masked
    (land, or a swell partition that doesn't exist there).
    """
    time_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = requests.get(
            GEOMET_WMS_BASE,
            params={
                "SERVICE": "WMS",
                "VERSION": "1.3.0",
                "REQUEST": "GetFeatureInfo",
                "LAYERS": layer,
                "QUERY_LAYERS": layer,
                "CRS": "EPSG:4326",
                "BBOX": f"{lat - BBOX_OFFSET},{lon - BBOX_OFFSET},{lat + BBOX_OFFSET},{lon + BBOX_OFFSET}",
                "WIDTH": 10,
                "HEIGHT": 10,
                "I": 5,
                "J": 5,
                "INFO_FORMAT": "application/json",
                "TIME": time_str,
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        features = resp.json().get("features", [])
        if not features:
            return None, None
        props = features[0]["properties"]
        value = float(props["value"])
        # GeoMet returns the GRIB missing-value sentinel (9999.0) for masked
        # cells — e.g. wind-wave/swell partitions that don't exist at that
        # hour — rather than an empty feature list.
        if value >= 9000:
            return None, None
        return value, props.get("dim_reference_time")
    except Exception as e:
        logger.info(f"    ⚠️  Error fetching {layer} at {time_str}: {e}")
        return None, None


def fetch_station_forecast(station_id, station_info, time_steps):
    """Fetch all variables for all timesteps at one station.

    Returns (forecast, run_time) where forecast maps ISO valid time ->
    {field: value} and run_time is the model run the values came from.
    """
    logger.info(f"\n📍 Fetching {station_info['name']}...")
    lat, lon = station_info["lat"], station_info["lon"]

    forecast = {}
    run_times = set()
    successful = 0
    failed = 0

    for field, layer in VARIABLES.items():
        logger.info(f"  🌊 {field} ({layer})")
        for timestamp in time_steps:
            value, run_time = fetch_point(layer, lat, lon, timestamp)
            if value is not None:
                time_key = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                forecast.setdefault(time_key, {})[field] = round(value, 3)
                if run_time:
                    run_times.add(run_time)
                successful += 1
            else:
                failed += 1
            time.sleep(FETCH_DELAY)

    total = len(VARIABLES) * len(time_steps)
    logger.info(f"    ✅ Retrieved {successful}/{total} values (failed/masked: {failed})")

    if len(run_times) > 1:
        logger.info(f"    ⚠️  Mixed model runs in one fetch: {sorted(run_times)}")
    run_time = max(run_times) if run_times else None
    return forecast, run_time


def store_forecast_to_db(station_id, forecast, run_time):
    """Store one station's forecast, keyed by model run for validation."""
    if not run_time:
        logger.info("    ⚠️  No model run time in responses, skipping DB storage")
        return

    iso_format = "%Y-%m-%dT%H:%M:%SZ"
    run_epoch = int(datetime.strptime(run_time, iso_format).replace(tzinfo=timezone.utc).timestamp())

    WAVE_FORECAST_DATABASE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(WAVE_FORECAST_DATABASE)
    ensure_db_schema(conn)
    cur = conn.cursor()

    stored = 0
    for time_key, fields in forecast.items():
        valid_epoch = int(datetime.strptime(time_key, iso_format).replace(tzinfo=timezone.utc).timestamp())
        for field, value in fields.items():
            cur.execute(
                """
                INSERT OR REPLACE INTO wave_forecast
                (station_id, variable, forecast_run_time, valid_time, value)
                VALUES (?, ?, ?, ?, ?)
                """,
                (station_id, field, run_epoch, valid_epoch, value),
            )
            stored += 1
    conn.commit()

    cutoff = int(time.time()) - WAVE_FORECAST_RETENTION_DAYS * 86400
    cur.execute("DELETE FROM wave_forecast WHERE forecast_run_time < ?", (cutoff,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()

    logger.info(f"    💾 Stored {stored} values to database (run {run_time})")
    if deleted > 0:
        logger.info(f"    🗑️  Purged {deleted} records older than {WAVE_FORECAST_RETENTION_DAYS} days")


def save_forecast(station_id, station_info, forecast, run_time):
    """Save forecast JSON with an explicit allowlist of fields (site/data is public)."""
    output_data = {
        "station_id": station_id,
        "station_name": station_info["name"],
        "location": {"lat": station_info["lat"], "lon": station_info["lon"]},
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "model": "RDWPS national 2.5km",
        "model_run_time": run_time,
        "units": UNITS,
        "forecast": forecast,
    }
    output_file = OUTPUT_DIR / f"{station_id}.json"
    safe_json_write(output_file, output_data)
    logger.info(f"    💾 Saved to {output_file}")


def main():
    logger.info("🌊 Wave Forecast Fetcher (RDWPS)")
    logger.info("=" * 50)

    if not acquire_lock():
        return 1

    try:
        first_layer = next(iter(VARIABLES.values()))
        time_steps = get_time_steps(first_layer)
        logger.info(
            f"📅 Forecast period: {time_steps[0].strftime('%Y-%m-%d %H:%M')} to "
            f"{time_steps[-1].strftime('%Y-%m-%d %H:%M')} UTC ({len(time_steps)} timesteps)"
        )

        total_minutes = len(time_steps) * len(VARIABLES) * len(BUOY_IDS) * FETCH_DELAY / 60
        logger.info(f"⏰ Estimated fetch time: ~{total_minutes:.1f} minutes")

        failures = 0
        for buoy_id in BUOY_IDS:
            station_info = get_buoy(buoy_id)
            if not station_info:
                logger.info(f"❌ Buoy {buoy_id} not found in stations.json")
                failures += 1
                continue

            forecast, run_time = fetch_station_forecast(buoy_id, station_info, time_steps)
            if forecast:
                store_forecast_to_db(buoy_id, forecast, run_time)
                save_forecast(buoy_id, station_info, forecast, run_time)
            else:
                logger.info(f"    ❌ No data retrieved for {buoy_id}")
                failures += 1

        if failures:
            logger.info(f"\n⚠️  Completed with {failures} station failure(s)")
            return 1
        logger.info("\n✅ Wave forecast update complete!")
        return 0

    except Exception as e:
        logger.info(f"\n❌ Fatal error: {e}")
        return 1

    finally:
        release_lock()


if __name__ == "__main__":
    exit(main())
