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

MODEL_NAME = "RDWPS national 2.5km"

OUTPUT_DIR = EXPORT_DIR / "wave_forecast"
LOCKFILE = Path("/tmp/wave_forecast_fetch.lock")

# WMS 1.3.0 + EPSG:4326 uses lat,lon axis order. We ask for a small box
# centred on the buoy and query the centre pixel.
BBOX_OFFSET = 0.02  # degrees
# Rate limiting. MSC's guidance is "about 1 request per second"; with ~0.45 s
# of network per request, a 0.5 s delay puts a burst at 1.05 req/s — right at
# that line. 1.5 s gives 0.51 req/s, and a 4-minute run is nothing for a job
# that goes 4x/day. Daily totals were never the risk here; the burst rate was.
FETCH_DELAY = 1.5  # seconds between requests
REQUEST_TIMEOUT = 60  # seconds

# The model publishes 49 hourly steps (0–48 h), but we don't fetch them all:
# one WMS request buys one (variable, hour), so the time axis is what drives
# our request count. Hourly detail matters for planning a day on the water;
# past 24 h the model's skill is soft enough that 3-hourly is plenty.
# 25 + 8 = 33 steps → 133 requests/run, 532/day at 4 runs — 0.6% of MSC's
# 86,400/day guidance (https://eccc-msc.github.io/open-data/usage-policy/).
FINE_HORIZON_HOURS = 24  # hourly out to here
COARSE_STEP_HOURS = 3  # then this spacing to the end of the run

# Session reuse: keep-alive cuts ~120 ms off each request (493 ms cold vs
# 376 ms warm, measured 2026-08-15), which is ~15 s over a full run.
SESSION = requests.Session()


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
    """Create forecast storage tables if they don't exist.

    Forecast data is 3-dimensional (run x valid time x variable) where
    observations are 2-D, and it has a verification lifecycle observations
    don't — hence the extra tables here rather than a single flat one.
    """
    cur = conn.cursor()
    # `value` is nullable and `status` says why: 'ok' or 'masked'. A masked
    # cell is information, not absence — RDWPS masks wind-wave height when
    # there is no wind sea (all 22 masked steps on 2026-08-15 had Hs <=
    # 0.104 m). Storing a row either way also means a missing row can only
    # mean a failed fetch.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wave_forecast (
            station_id TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            valid_time INTEGER NOT NULL,
            value REAL,
            status TEXT NOT NULL DEFAULT 'ok',
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wave_forecast_station_run
        ON wave_forecast(station_id, forecast_run_time DESC)
    """)
    # Lead time is the axis every skill query groups by (bias and RMSE both
    # degrade with it), so index it rather than deriving it per query.
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wave_forecast_lead
        ON wave_forecast(station_id, variable, (valid_time - forecast_run_time))
    """)
    # Which runs we actually captured, and how cleanly. `model` is provenance:
    # an RDWPS version change must not silently blend into the archive.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wave_forecast_run (
            station_id TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            fetched_at INTEGER NOT NULL,
            model TEXT NOT NULL,
            n_ok INTEGER NOT NULL DEFAULT 0,
            n_masked INTEGER NOT NULL DEFAULT 0,
            n_failed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (station_id, forecast_run_time)
        )
    """)
    # Forecast/observation pairs, written once a valid_time has passed.
    # Deliberately NOT subject to WAVE_FORECAST_RETENTION_DAYS: the raw runs
    # are bulky and disposable, but these triples are the skill history, and
    # this summer is too calm to conclude anything from. Pairing at write time
    # also freezes the observation as it was, so a later buoy backfill can't
    # retroactively rewrite past scores.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wave_forecast_verification (
            station_id TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            valid_time INTEGER NOT NULL,
            lead_hours INTEGER NOT NULL,
            forecast_value REAL,
            observed_value REAL,
            -- The buoy reading at (or nearest) the model run hour: the
            -- persistence baseline. Bias and RMSE alone say how wrong the
            -- model is; a skill score says whether it beats "conditions stay
            -- as they are", which is the question that decides how far out
            -- the forecast is worth displaying at all. Captured here because
            -- it is cheap now and needs re-deriving every past t0 later.
            reference_value REAL,
            obs_offset_seconds INTEGER,
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
        )
    """)
    conn.commit()


def migrate_db_schema(conn):
    """Bring pre-2026-08-15 databases up to the current schema.

    The original table had `value REAL NOT NULL`, which now has to accept NULL
    so masked steps can be recorded. SQLite cannot relax a column constraint
    in place, so the table is rebuilt — ALTER TABLE ... ADD COLUMN alone
    leaves the old NOT NULL and every masked insert fails.
    """
    ver_cols = {row[1] for row in conn.execute("PRAGMA table_info(wave_forecast_verification)")}
    if ver_cols and "reference_value" not in ver_cols:
        conn.execute("ALTER TABLE wave_forecast_verification ADD COLUMN reference_value REAL")
        conn.commit()
        logger.info("    🔧 Migrated wave_forecast_verification: added reference_value")

    info = list(conn.execute("PRAGMA table_info(wave_forecast)"))
    if not info:
        return
    cols = {row[1] for row in info}
    value_not_null = any(row[1] == "value" and row[3] for row in info)

    if not value_not_null:
        if "status" not in cols:
            conn.execute("ALTER TABLE wave_forecast ADD COLUMN status TEXT NOT NULL DEFAULT 'ok'")
            conn.commit()
            logger.info("    🔧 Migrated wave_forecast: added status column")
        return

    logger.info("    🔧 Rebuilding wave_forecast to allow NULL values (masked steps)...")
    carried = "status" in cols
    conn.executescript(f"""
        PRAGMA foreign_keys=off;
        BEGIN;
        CREATE TABLE wave_forecast_new (
            station_id TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            valid_time INTEGER NOT NULL,
            value REAL,
            status TEXT NOT NULL DEFAULT 'ok',
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
        );
        INSERT INTO wave_forecast_new
            (station_id, variable, forecast_run_time, valid_time, value, status, created_at)
        SELECT station_id, variable, forecast_run_time, valid_time, value,
               {"status" if carried else "'ok'"}, created_at
        FROM wave_forecast;
        DROP TABLE wave_forecast;
        ALTER TABLE wave_forecast_new RENAME TO wave_forecast;
        COMMIT;
        PRAGMA foreign_keys=on;
    """)
    # Indexes lived on the dropped table — recreate them.
    ensure_db_schema(conn)
    rows = conn.execute("SELECT COUNT(*) FROM wave_forecast").fetchone()[0]
    logger.info(f"    🔧 Rebuild complete, {rows} rows preserved")


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


def taper_time_steps(steps):
    """Thin the model's hourly steps: hourly to FINE_HORIZON_HOURS, then
    every COARSE_STEP_HOURS. Keeps the last step so the horizon is unchanged.

    Driven off the first step (the run hour) rather than wall-clock, so a late
    fetch still tapers at the same lead times.
    """
    if not steps:
        return steps

    run_start = steps[0]
    kept = []
    for step in steps:
        lead = (step - run_start).total_seconds() / 3600
        if lead <= FINE_HORIZON_HOURS or lead % COARSE_STEP_HOURS == 0:
            kept.append(step)
    if kept[-1] != steps[-1]:
        kept.append(steps[-1])
    return kept


def fetch_point(layer, lat, lon, timestamp):
    """Fetch one value at one location and time.

    Returns (value, model_run_time, status) where status is one of:
      'ok'     — value is a real number
      'masked' — the model has no value here (land, or a partition/wind sea
                 that doesn't exist at that hour). value is None.
      'failed' — the request errored. value is None.

    'masked' and 'failed' are kept apart deliberately: the first is a
    statement by the model, the second is a gap in our record.
    """
    time_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = SESSION.get(
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
            return None, None, "masked"
        props = features[0]["properties"]
        value = float(props["value"])
        run_time = props.get("dim_reference_time")
        # GeoMet returns the GRIB missing-value sentinel (9999.0) for masked
        # cells — e.g. wind-wave/swell partitions that don't exist at that
        # hour — rather than an empty feature list. Keep the run time: the
        # response is valid, it's the cell that has no value.
        if value >= 9000:
            return None, run_time, "masked"
        return value, run_time, "ok"
    except Exception as e:
        logger.info(f"    ⚠️  Error fetching {layer} at {time_str}: {e}")
        return None, None, "failed"


def fetch_station_forecast(station_id, station_info, time_steps):
    """Fetch all variables for all timesteps at one station.

    Returns (forecast, run_time) where forecast maps ISO valid time ->
    {field: value} and run_time is the model run the values came from.
    """
    logger.info(f"\n📍 Fetching {station_info['name']}...")
    lat, lon = station_info["lat"], station_info["lon"]

    forecast = {}
    readings = []  # (field, timestamp, value, status) for every step attempted
    run_times = set()
    counts = {"ok": 0, "masked": 0, "failed": 0}

    for field, layer in VARIABLES.items():
        logger.info(f"  🌊 {field} ({layer})")
        for timestamp in time_steps:
            value, run_time, status = fetch_point(layer, lat, lon, timestamp)
            counts[status] += 1
            if run_time:
                run_times.add(run_time)
            if status != "failed":
                readings.append((field, timestamp, value, status))
            if value is not None:
                time_key = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                forecast.setdefault(time_key, {})[field] = round(value, 3)
            time.sleep(FETCH_DELAY)

    total = len(VARIABLES) * len(time_steps)
    logger.info(
        f"    ✅ Retrieved {counts['ok']}/{total} values "
        f"(masked: {counts['masked']}, failed: {counts['failed']})"
    )

    if len(run_times) > 1:
        logger.info(f"    ⚠️  Mixed model runs in one fetch: {sorted(run_times)}")
    run_time = max(run_times) if run_times else None
    return forecast, readings, counts, run_time


def store_forecast_to_db(station_id, readings, counts, run_time):
    """Store one station's forecast, keyed by model run for validation.

    Writes a row for every step we got an answer for — masked included, with
    value NULL — so an absent row unambiguously means the fetch failed.
    """
    if not run_time:
        logger.info("    ⚠️  No model run time in responses, skipping DB storage")
        return

    iso_format = "%Y-%m-%dT%H:%M:%SZ"
    run_epoch = int(datetime.strptime(run_time, iso_format).replace(tzinfo=timezone.utc).timestamp())

    WAVE_FORECAST_DATABASE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(WAVE_FORECAST_DATABASE)
    ensure_db_schema(conn)
    migrate_db_schema(conn)
    cur = conn.cursor()

    stored = 0
    for field, timestamp, value, status in readings:
        valid_epoch = int(timestamp.timestamp())
        cur.execute(
            """
            INSERT OR REPLACE INTO wave_forecast
            (station_id, variable, forecast_run_time, valid_time, value, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (station_id, field, run_epoch, valid_epoch, value, status),
        )
        stored += 1

    cur.execute(
        """
        INSERT OR REPLACE INTO wave_forecast_run
        (station_id, forecast_run_time, fetched_at, model, n_ok, n_masked, n_failed)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            station_id,
            run_epoch,
            int(time.time()),
            MODEL_NAME,
            counts["ok"],
            counts["masked"],
            counts["failed"],
        ),
    )
    conn.commit()

    # Raw runs are bulky and disposable; wave_forecast_verification is not
    # pruned here — it is the long-term skill record (see ensure_db_schema).
    cutoff = int(time.time()) - WAVE_FORECAST_RETENTION_DAYS * 86400
    cur.execute("DELETE FROM wave_forecast WHERE forecast_run_time < ?", (cutoff,))
    deleted = cur.rowcount
    cur.execute("DELETE FROM wave_forecast_run WHERE forecast_run_time < ?", (cutoff,))
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
        "model": MODEL_NAME,
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
        published_steps = get_time_steps(first_layer)
        time_steps = taper_time_steps(published_steps)
        logger.info(
            f"📅 Forecast period: {time_steps[0].strftime('%Y-%m-%d %H:%M')} to "
            f"{time_steps[-1].strftime('%Y-%m-%d %H:%M')} UTC "
            f"({len(time_steps)} of {len(published_steps)} published steps — "
            f"hourly to {FINE_HORIZON_HOURS}h, then {COARSE_STEP_HOURS}-hourly)"
        )

        requests_planned = len(time_steps) * len(VARIABLES) * len(BUOY_IDS)
        # ~0.45 s of network per request on top of our own delay (measured
        # 2026-08-15); the old estimate counted only the sleep and came in at
        # half the real runtime.
        total_minutes = requests_planned * (FETCH_DELAY + 0.45) / 60
        logger.info(
            f"⏰ {requests_planned} requests planned, estimated ~{total_minutes:.1f} minutes"
        )

        failures = 0
        for buoy_id in BUOY_IDS:
            station_info = get_buoy(buoy_id)
            if not station_info:
                logger.info(f"❌ Buoy {buoy_id} not found in stations.json")
                failures += 1
                continue

            forecast, readings, counts, run_time = fetch_station_forecast(
                buoy_id, station_info, time_steps
            )
            if forecast:
                store_forecast_to_db(buoy_id, readings, counts, run_time)
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
