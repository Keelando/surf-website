#!/usr/bin/env python3
"""
Wave and Wind Forecast Fetcher for Surf Server
Fetches RDWPS (national 2.5 km) wave forecasts and HRDPS (continental 2.5 km)
10 m wind forecasts from Environment Canada GeoMet WMS via point extraction at
buoy locations, storing every run to the database for model-vs-buoy validation
(bias/RMSE scoring).

Verified 2026-08-15: GetFeatureInfo values are bit-identical to the raw GRIB2
files, so nothing is lost by extracting points over WMS instead of downloading
grids. Full parameter inventory and field-selection rationale in
docs/project/RDWPS_PARAMETERS.md.

Wind comes from HRDPS rather than RDWPS (added 2026-08-17) because RDWPS's own
forcing wind (UGRD/VGRD) exists only in the GRIB2 files — it is not published
as a WMS layer, and HRDPS is the better wind source regardless. Two models in
one fetch is why every row carries its own `model`: an RDWPS run and an HRDPS
run share the same 00/06/12/18Z hours but are not the same forecast, and
blending them would quietly corrupt the verification archive.
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
from lib.forecast_steps import taper_time_steps
from lib.logging_config import setup_logging
from lib.stations import get_buoy

logger = setup_logging("wave_forecast")

MODEL_NAME = "RDWPS national 2.5km"
WIND_MODEL_NAME = "HRDPS continental 2.5km"

# GeoMet layers to fetch, keyed by our field names, grouped by the model that
# publishes them. Field names are globally unique across models on purpose:
# `variable` alone identifies the source, which is what lets the storage tables
# keep their existing primary keys (see test_field_names_are_unique_across_models).
#
# Heights in metres, periods in seconds, directions degrees true coming FROM
# (matches the site-wide meteorological convention — waves verified against the
# model's own wind field, wind direction verified 2026-08-17 against RDWPS
# MeanWaveDir on a 100 % wind-sea hour: WD 276.2° vs 275.7°. See
# docs/project/RDWPS_PARAMETERS.md).
SOURCES = {
    MODEL_NAME: {
        "wave_height": "RDWPS_2.5km_SignificantWaveHeight",
        "peak_period": "RDWPS_2.5km_PeakWavePeriod",
        "wave_direction": "RDWPS_2.5km_MeanWaveDir",
        "wind_wave_height": "RDWPS_2.5km_WindWavesSignificantHeight",
    },
    WIND_MODEL_NAME: {
        "wind_speed": "HRDPS.CONTINENTAL_WSPD",
        "wind_direction": "HRDPS.CONTINENTAL_WD",
        # WGX (gust maximum), not WGE (gust estimate): sampled 2026-08-17, WGE
        # and WGN were identical to each other and WGX was the only one above
        # the sustained wind. All three are masked at most hours — the model
        # only diagnoses a gust where there is one to diagnose, so a gust row
        # existing at all is itself the signal (8 of 9 sampled hours masked;
        # the one that wasn't is the 20.8 kt peak over a 16.8 kt sustained).
        "wind_gust": "HRDPS.CONTINENTAL_WGX",
    },
}

# Flat field -> layer view of SOURCES, for callers that don't care which model
# a field came from.
VARIABLES = {field: layer for layers in SOURCES.values() for field, layer in layers.items()}

UNITS = {
    "wave_height": "m",
    "peak_period": "s",
    "wave_direction": "degrees_true_from",
    "wind_wave_height": "m",
    "wind_speed": "km/h",
    "wind_direction": "degrees_true_from",
    "wind_gust": "km/h",
}

# GeoMet serves wind in m/s; the site stores km/h everywhere and converts to
# knots for display. Convert on the way in so the database never holds two
# units for the same quantity.
MS_TO_KMH = 3.6
CONVERSIONS = {
    "wind_speed": lambda value: value * MS_TO_KMH,
    "wind_gust": lambda value: value * MS_TO_KMH,
}

# Points to extract at (ids from config/stations.json). Halibut Bank is the
# reference station — it has an EC buoy reporting the same spot, which is what
# the verification archive scores against.
#
# Crescent Beach Ocean (added 2026-08-18) is the second point for the same
# reason: Surrey's CRPILE sensor sits there, so the forecast can be checked
# against something measured.
#
# The bay is better resolved than first assumed. Boundary Bay is ~22 km across
# the mouth (Point Roberts to White Rock) and ~10 km deep, and a W->E transect
# at 49.00N on 2026-08-18 returned seven distinct wet cells with genuinely
# varying values (0.055-0.066 m), not one cell repeated — so RDWPS has real
# internal structure here. CRPILE is 6 km offshore of Crescent Beach, in open
# water, not on the drying flats.
#
# What is still worth watching: the water is shallow and the fetch is short, so
# the sea state is depth-limited chop (the model's own 1.7 s periods say as
# much), and that is the regime a model tuned for offshore sea state is most
# likely to get wrong. Having observations is how we find out.
#
# The remaining candidates (Hein Bank, Sombrio Beach, Long Beach) are open
# water where the model has more to say, but none has a co-located sensor and
# none is in stations.json — they are not instruments, so adding them under
# `buoys` would put phantom stations on the map. They need a forecast-only
# registry section first.
BUOY_IDS = ["4600146", "CRPILE"]

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

# Both models publish 49 hourly steps (0–48 h), but we don't fetch them all:
# one WMS request buys one (variable, hour), so the time axis is what drives
# our request count. Hourly detail matters for planning a day on the water;
# past 24 h the model's skill is soft enough that 3-hourly is plenty.
# 25 + 8 = 33 steps × 7 variables = 231 requests per station per run, plus 2
# GetCapabilities shared across stations. At 2 stations that is 464/run and
# 1,856/day — ~2.1% of MSC's 86,400/day guidance
# (https://eccc-msc.github.io/open-data/usage-policy/). Note the guidance is a
# *rate*: each added station leaves FETCH_DELAY untouched, so it lengthens the
# burst (~7.6 min per station) without raising the 0.51 req/s it runs at. The
# run has ~6 h of headroom before the next one, so runtime is not the binding
# constraint either — but the flock guard is what makes that safe to rely on.
FINE_HORIZON_HOURS = 24  # hourly out to here
COARSE_STEP_HOURS = 3  # then this spacing to the end of the run

# Session reuse: keep-alive cuts ~120 ms off each request (493 ms cold vs
# 376 ms warm, measured 2026-08-15), which is ~15 s over a full run.
SESSION = requests.Session()


def acquire_lock():
    """Simple file-based lock to prevent concurrent runs."""
    if LOCKFILE.exists():
        age = time.time() - LOCKFILE.stat().st_mtime
        # Must stay comfortably above the real runtime, which scales with
        # BUOY_IDS: ~7.6 min per station, so two stations already exceed the
        # old 15-minute threshold and a live fetch would be declared stale by
        # anything that ran alongside it. An hour is still well inside the 6 h
        # gap between runs, so a genuinely wedged process is cleared before the
        # next one is due.
        if age > 3600:
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
    # 0.104 m), and HRDPS masks gusts at every hour it doesn't diagnose one.
    # Storing a row either way also means a missing row can only mean a failed
    # fetch.
    #
    # `model` is carried per row, not just per run: two models are fetched in
    # one pass and they publish runs at the same 00/06/12/18Z hours, so
    # forecast_run_time alone does not say which forecast a value came from.
    # It stays out of the primary key because `variable` already identifies the
    # model (field names are unique across SOURCES) — the column is there so a
    # verification query never has to encode that mapping.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wave_forecast (
            station_id TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            valid_time INTEGER NOT NULL,
            value REAL,
            status TEXT NOT NULL DEFAULT 'ok',
            model TEXT NOT NULL DEFAULT '',
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
    # a model version change must not silently blend into the archive, and it
    # is part of the primary key because RDWPS and HRDPS both publish an 00Z
    # run — keyed on (station, run) alone, the second model written would
    # overwrite the first model's row for the same hour.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wave_forecast_run (
            station_id TEXT NOT NULL,
            model TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            fetched_at INTEGER NOT NULL,
            n_ok INTEGER NOT NULL DEFAULT 0,
            n_masked INTEGER NOT NULL DEFAULT 0,
            n_failed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (station_id, model, forecast_run_time)
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
            model TEXT,
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
        )
    """)
    conn.commit()


def _columns(conn, table):
    """PRAGMA table_info as {name: row}, empty when the table doesn't exist."""
    return {row[1]: row for row in conn.execute(f"PRAGMA table_info({table})")}


def migrate_db_schema(conn):
    """Bring older databases up to the current schema.

    Two rounds of change so far:

    1. **2026-08-15** — the original table had `value REAL NOT NULL`, which now
       has to accept NULL so masked steps can be recorded. SQLite cannot relax
       a column constraint in place, so the table is rebuilt: ALTER TABLE ...
       ADD COLUMN alone leaves the old NOT NULL and every masked insert fails.
    2. **2026-08-17** — a second model (HRDPS wind) joined the fetch, so every
       row carries a `model`, and `wave_forecast_run` needs it in the primary
       key. Both models publish an 00Z run; without the key change the second
       model written each pass would overwrite the first model's run row.
    """
    ver_cols = _columns(conn, "wave_forecast_verification")
    if ver_cols and "reference_value" not in ver_cols:
        conn.execute("ALTER TABLE wave_forecast_verification ADD COLUMN reference_value REAL")
        conn.commit()
        logger.info("    🔧 Migrated wave_forecast_verification: added reference_value")
    if ver_cols and "model" not in ver_cols:
        conn.execute("ALTER TABLE wave_forecast_verification ADD COLUMN model TEXT")
        conn.commit()
        logger.info("    🔧 Migrated wave_forecast_verification: added model")

    _migrate_forecast_table(conn)
    _migrate_run_table(conn)


def _migrate_forecast_table(conn):
    """Relax `value` to nullable (2026-08-15) and add `model` (2026-08-17)."""
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
        if "model" not in cols:
            conn.execute("ALTER TABLE wave_forecast ADD COLUMN model TEXT NOT NULL DEFAULT ''")
            _backfill_model(conn)
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
            model TEXT NOT NULL DEFAULT '',
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
    _backfill_model(conn)
    rows = conn.execute("SELECT COUNT(*) FROM wave_forecast").fetchone()[0]
    logger.info(f"    🔧 Rebuild complete, {rows} rows preserved")


def _backfill_model(conn):
    """Label pre-2026-08-17 rows, all of which are RDWPS waves.

    Matched on the field names rather than blanket-set, so a row whose variable
    we no longer recognise stays visibly unlabelled instead of being asserted
    into the wrong model.
    """
    for model, layers in SOURCES.items():
        placeholders = ",".join("?" * len(layers))
        conn.execute(
            f"UPDATE wave_forecast SET model = ? WHERE model = '' AND variable IN ({placeholders})",
            (model, *layers),
        )
    conn.commit()
    labelled = conn.execute("SELECT COUNT(*) FROM wave_forecast WHERE model != ''").fetchone()[0]
    orphans = conn.execute("SELECT COUNT(*) FROM wave_forecast WHERE model = ''").fetchone()[0]
    logger.info(f"    🔧 Migrated wave_forecast: labelled {labelled} rows with their model")
    if orphans:
        logger.info(f"    ⚠️  {orphans} rows have a variable not in SOURCES and stay unlabelled")


def _migrate_run_table(conn):
    """Put `model` in the run table's primary key (2026-08-17)."""
    cols = _columns(conn, "wave_forecast_run")
    if not cols or cols["model"][5]:  # row[5] is the column's position in the PK
        return

    logger.info("    🔧 Rebuilding wave_forecast_run to key runs by model...")
    conn.executescript("""
        PRAGMA foreign_keys=off;
        BEGIN;
        CREATE TABLE wave_forecast_run_new (
            station_id TEXT NOT NULL,
            model TEXT NOT NULL,
            forecast_run_time INTEGER NOT NULL,
            fetched_at INTEGER NOT NULL,
            n_ok INTEGER NOT NULL DEFAULT 0,
            n_masked INTEGER NOT NULL DEFAULT 0,
            n_failed INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (station_id, model, forecast_run_time)
        );
        INSERT INTO wave_forecast_run_new
            (station_id, model, forecast_run_time, fetched_at, n_ok, n_masked, n_failed)
        SELECT station_id, model, forecast_run_time, fetched_at, n_ok, n_masked, n_failed
        FROM wave_forecast_run;
        DROP TABLE wave_forecast_run;
        ALTER TABLE wave_forecast_run_new RENAME TO wave_forecast_run;
        COMMIT;
        PRAGMA foreign_keys=on;
    """)
    rows = conn.execute("SELECT COUNT(*) FROM wave_forecast_run").fetchone()[0]
    logger.info(f"    🔧 Rebuild complete, {rows} run rows preserved")


def to_utc(dt):
    """Normalise a forecast timestamp to UTC.

    Every timestamp in this module is a model valid time, which is always UTC:
    `get_time_steps` attaches UTC explicitly, and GeoMet only speaks Z. This
    guards the two places where that could silently go wrong — `strftime` on a
    non-UTC aware datetime would label local time as `Z`, and `.timestamp()` on
    a naive datetime would read it in the server's local zone, putting every
    valid_time 7–8 hours out. A naive datetime is therefore taken as UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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

    # RDWPS publishes PT1H, but read the unit rather than assuming it: stripping
    # the letters out of "PT30M" would step the axis 30 *hours* at a time and
    # quietly fetch three days of the wrong instants.
    unit_match = re.fullmatch(r"PT(\d+)([HM])", interval_str.strip())
    if not unit_match:
        raise ValueError(f"Unsupported time interval {interval_str!r} for layer {layer}")
    amount, unit = int(unit_match.group(1)), unit_match.group(2)
    interval = timedelta(hours=amount) if unit == "H" else timedelta(minutes=amount)

    steps = [start_time]
    while steps[-1] < end_time:
        steps.append(steps[-1] + interval)
    return steps


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
    time_str = to_utc(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
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


def fetch_station_forecast(station_id, station_info, layers, time_steps):
    """Fetch one model's variables for all timesteps at one station.

    `layers` is a {field: layer} map — one model's entry in SOURCES. Returns
    (forecast, readings, counts, run_time) where forecast maps ISO valid time
    -> {field: value} and run_time is the model run the values came from.
    """
    logger.info(f"\n📍 Fetching {station_info['name']}...")
    lat, lon = station_info["lat"], station_info["lon"]

    forecast = {}
    readings = []  # (field, timestamp, value, status) for every step attempted
    run_times = set()
    counts = {"ok": 0, "masked": 0, "failed": 0}

    for field, layer in layers.items():
        logger.info(f"  🌊 {field} ({layer})")
        convert = CONVERSIONS.get(field)
        for timestamp in time_steps:
            value, run_time, status = fetch_point(layer, lat, lon, timestamp)
            # Convert before anything stores or exports it, so km/h is the only
            # unit this field ever has downstream.
            if value is not None and convert:
                value = convert(value)
            counts[status] += 1
            if run_time:
                run_times.add(run_time)
            if status != "failed":
                readings.append((field, timestamp, value, status))
            if value is not None:
                time_key = to_utc(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
                forecast.setdefault(time_key, {})[field] = round(value, 3)
            time.sleep(FETCH_DELAY)

    total = len(layers) * len(time_steps)
    logger.info(
        f"    ✅ Retrieved {counts['ok']}/{total} values "
        f"(masked: {counts['masked']}, failed: {counts['failed']})"
    )

    if len(run_times) > 1:
        logger.info(f"    ⚠️  Mixed model runs in one fetch: {sorted(run_times)}")
    run_time = max(run_times) if run_times else None
    return forecast, readings, counts, run_time


def store_forecast_to_db(station_id, model, readings, counts, run_time):
    """Store one station's forecast for one model, keyed by run for validation.

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
        valid_epoch = int(to_utc(timestamp).timestamp())
        cur.execute(
            """
            INSERT OR REPLACE INTO wave_forecast
            (station_id, variable, forecast_run_time, valid_time, value, status, model)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (station_id, field, run_epoch, valid_epoch, value, status, model),
        )
        stored += 1

    cur.execute(
        """
        INSERT OR REPLACE INTO wave_forecast_run
        (station_id, model, forecast_run_time, fetched_at, n_ok, n_masked, n_failed)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            station_id,
            model,
            run_epoch,
            int(time.time()),
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

    logger.info(f"    💾 Stored {stored} {model} values to database (run {run_time})")
    if deleted > 0:
        logger.info(f"    🗑️  Purged {deleted} records older than {WAVE_FORECAST_RETENTION_DAYS} days")


def save_forecast(station_id, station_info, forecast, run_times):
    """Save forecast JSON with an explicit allowlist of fields (site/data is public).

    `run_times` maps model name -> that model's run. `model`/`model_run_time`
    stay at the top level describing the wave model, because that is what the
    page reads today; `models` carries the full per-model provenance, which is
    what a reader needs once wind from a second model shares the same series.
    """
    output_data = {
        "station_id": station_id,
        "station_name": station_info["name"],
        "location": {"lat": station_info["lat"], "lon": station_info["lon"]},
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "model": MODEL_NAME,
        "model_run_time": run_times.get(MODEL_NAME),
        "models": [
            {
                "name": model,
                "run_time": run_times.get(model),
                "variables": sorted(SOURCES[model]),
            }
            for model in SOURCES
        ],
        "units": UNITS,
        "forecast": forecast,
    }
    output_file = OUTPUT_DIR / f"{station_id}.json"
    safe_json_write(output_file, output_data)
    logger.info(f"    💾 Saved to {output_file}")


def save_index(stations):
    """Write the list of stations that have a forecast file.

    The page needs to know which stations to offer before it can load one, and
    hardcoding that list in JavaScript would make BUOY_IDS a second source of
    truth for something this script already knows. Written from the stations
    that actually produced data, so a station whose fetch failed is absent from
    the picker rather than offered as a broken option.

    Allowlisted fields only — site/data is served straight to the public.
    """
    output_data = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "stations": [
            {
                "station_id": station_id,
                "name": info["name"],
                "lat": info["lat"],
                "lon": info["lon"],
            }
            for station_id, info in stations
        ],
    }
    safe_json_write(OUTPUT_DIR / "index.json", output_data)
    logger.info(f"💾 Saved station index ({len(stations)} stations)")


def plan_time_steps():
    """Read each model's published time axis and taper it.

    Read per model rather than once: RDWPS and HRDPS publish on the same
    00/06/12/18Z cadence but not at the same minute, so a fetch can legitimately
    catch different runs, and their step lists must be allowed to differ.
    """
    plan = {}
    for model, layers in SOURCES.items():
        published = get_time_steps(next(iter(layers.values())))
        steps = taper_time_steps(published, FINE_HORIZON_HOURS, COARSE_STEP_HOURS)
        plan[model] = steps
        logger.info(
            f"📅 {model}: {steps[0].strftime('%Y-%m-%d %H:%M')} to "
            f"{steps[-1].strftime('%Y-%m-%d %H:%M')} UTC "
            f"({len(steps)} of {len(published)} published steps — "
            f"hourly to {FINE_HORIZON_HOURS}h, then {COARSE_STEP_HOURS}-hourly)"
        )
    return plan


def main():
    logger.info("🌊 Wave + Wind Forecast Fetcher (RDWPS waves, HRDPS wind)")
    logger.info("=" * 50)

    if not acquire_lock():
        return 1

    try:
        steps_by_model = plan_time_steps()

        requests_planned = sum(
            len(steps) * len(SOURCES[model]) for model, steps in steps_by_model.items()
        ) * len(BUOY_IDS)
        # ~0.45 s of network per request on top of our own delay (measured
        # 2026-08-15); the old estimate counted only the sleep and came in at
        # half the real runtime.
        total_minutes = requests_planned * (FETCH_DELAY + 0.45) / 60
        logger.info(
            f"⏰ {requests_planned} requests planned, estimated ~{total_minutes:.1f} minutes"
        )

        failures = 0
        published = []  # (station_id, info) for stations that produced a file
        for buoy_id in BUOY_IDS:
            station_info = get_buoy(buoy_id)
            if not station_info:
                logger.info(f"❌ Buoy {buoy_id} not found in stations.json")
                failures += 1
                continue

            # One merged series per station across models — they share a valid
            # time axis — but each model stored and labelled separately.
            merged = {}
            run_times = {}
            for model, layers in SOURCES.items():
                forecast, readings, counts, run_time = fetch_station_forecast(
                    buoy_id, station_info, layers, steps_by_model[model]
                )
                if not forecast:
                    logger.info(f"    ❌ No {model} data retrieved for {buoy_id}")
                    failures += 1
                    continue
                store_forecast_to_db(buoy_id, model, readings, counts, run_time)
                run_times[model] = run_time
                for time_key, values in forecast.items():
                    merged.setdefault(time_key, {}).update(values)

            if merged:
                save_forecast(buoy_id, station_info, merged, run_times)
                published.append((buoy_id, station_info))
            else:
                logger.info(f"    ❌ No data retrieved for {buoy_id}")

        if published:
            save_index(published)

        if failures:
            logger.info(f"\n⚠️  Completed with {failures} fetch failure(s)")
            return 1
        logger.info("\n✅ Wave + wind forecast update complete!")
        return 0

    except Exception as e:
        logger.info(f"\n❌ Fatal error: {e}")
        return 1

    finally:
        release_lock()


if __name__ == "__main__":
    exit(main())
