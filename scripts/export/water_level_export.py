#!/usr/bin/env python3
"""Consolidated water-level exporter.

Replaces ``export_combined_water_level.py`` and ``export_observed_storm_surge.py``
with a single Python process that:

  - reads the tide SQLite database once
  - reads the storm-surge forecast JSON once
  - emits ``site/data/combined-water-level.json`` (forecast)
  - emits ``site/data/storm_surge/observed_surge.json`` (hindcast)
  - patches ``site/data/tide-latest.json`` with current surge values

Cron should invoke this from a single ``*/10`` line, plus an extra run a few
minutes after each storm-surge fetch so the combined forecast picks up fresh
surge data immediately.
"""

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from lib.config import EXPORT_DIR, TIDE_DATABASE
from lib.logging_config import setup_logging
from lib.stations import STATIONS
from lib.water_level_stations import WATER_LEVEL_STATIONS

logger = setup_logging("water_level_export")

# ---------- Paths ----------
SURGE_DIR = EXPORT_DIR / "storm_surge"
COMBINED_OUTPUT = EXPORT_DIR / "combined-water-level.json"
OBSERVED_OUTPUT = SURGE_DIR / "observed_surge.json"
LATEST_TIDE = EXPORT_DIR / "tide-latest.json"

# Test-mode paths (mirror the old export_combined_water_level.py layout)
_TESTS_DIR = Path(__file__).parent / "tests"
TEST_TIDE_DB = _TESTS_DIR / "databases" / "tide_data_test.sqlite"
TEST_SURGE_DIR = _TESTS_DIR / "data" / "storm_surge"
TEST_COMBINED_OUTPUT = _TESTS_DIR / "data" / "combined-water-level.json"
TEST_OBSERVED_OUTPUT = _TESTS_DIR / "data" / "observed_surge.json"

# ---------- Tunables ----------
FORECAST_DAYS = 3
OBSERVED_DAYS_BACK = 10
DOWNSAMPLE_MINUTES = 15

# ---------- Combined-output metadata (verbatim from the previous script) ----------
IMPORTANT_NOTES = {
    "wave_effects_not_included": (
        "These predictions do NOT include wave setup, wave runup, or wave overtopping effects. "
        "During storms with large waves, actual water levels at the shore can be significantly "
        "higher due to breaking waves pushing water inland (wave setup ~0.2-0.5m, wave runup "
        "~1-3m+ depending on location and wave height)."
    ),
    "what_is_storm_surge": (
        "Storm surge is the static elevation of water level caused by meteorological forcing "
        "(wind stress and atmospheric pressure). GDSPS predicts this component using ocean "
        "circulation models driven by weather forecasts."
    ),
    "total_flooding_risk": (
        "For complete coastal flooding assessment, wave effects must be added separately. "
        "Wave runup is often the dominant factor during major storms on exposed coasts."
    ),
    "inland_vs_exposed": (
        "Wave effects are minimal in protected areas (e.g., Vancouver Harbour) but can dominate "
        "on exposed outer coasts (e.g., Tofino, Neah Bay)."
    ),
}


# ---------- Helpers ----------
def _is_downsample_tick(dt: datetime) -> bool:
    return dt.minute % DOWNSAMPLE_MINUTES == 0 and dt.second == 0


def load_surge_forecasts(surge_dir: Path) -> dict:
    """Return ``{surge_station_id: {unix_ts: surge_m}}`` from combined_forecast.json."""
    combined_file = surge_dir / "combined_forecast.json"
    if not combined_file.exists():
        logger.warning(f"Storm surge forecast not found: {combined_file}")
        return {}

    with open(combined_file) as f:
        data = json.load(f)

    out: dict[str, dict[int, float]] = {}
    for station_id, station_data in data.get("stations", {}).items():
        forecasts: dict[int, float] = {}
        for time_str, surge_value in station_data.get("forecast", {}).items():
            ts = int(datetime.fromisoformat(time_str.replace("Z", "+00:00")).timestamp())
            forecasts[ts] = surge_value
        if forecasts:
            out[station_id] = forecasts

    logger.info(f"Loaded storm surge forecasts for {len(out)} stations")
    return out


def interpolate_surge(surge_forecasts: dict, target_ts: int):
    """Linearly interpolate surge at ``target_ts``; return None if out of range."""
    if not surge_forecasts:
        return None
    if target_ts in surge_forecasts:
        return surge_forecasts[target_ts]

    before = after = None
    for ts in sorted(surge_forecasts.keys()):
        if ts < target_ts:
            before = ts
        elif ts > target_ts:
            after = ts
            break

    if before is None or after is None:
        return None

    v1 = surge_forecasts[before]
    v2 = surge_forecasts[after]
    return v1 + (v2 - v1) * (target_ts - before) / (after - before)


# ---------- Forecast view ----------
def build_forecast_for_station(conn, station, surge_forecasts, start_ts, end_ts):
    """Combine astronomical tide predictions with the surge forecast."""
    if station.surge_source not in surge_forecasts:
        logger.warning(f"  no surge forecast for {station.surge_source}")
        return None

    surge = surge_forecasts[station.surge_source]

    cur = conn.cursor()
    cur.execute(
        """
        SELECT prediction_time, water_level
        FROM tide_prediction
        WHERE station_name = ?
          AND prediction_time >= ?
          AND prediction_time <= ?
          AND water_level IS NOT NULL
        ORDER BY prediction_time ASC
        """,
        (station.tide_key, start_ts, end_ts),
    )
    tide_preds = cur.fetchall()
    if not tide_preds:
        logger.warning(f"  no tide predictions for {station.tide_key}")
        return None

    combined: list[dict] = []
    peak_total = None
    peak_entry = None

    for ts, tide_level in tide_preds:
        surge_value = interpolate_surge(surge, ts)
        if surge_value is None:
            continue

        total = tide_level + surge_value
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)

        if peak_total is None or total > peak_total:
            peak_total = total
            local_dt = dt.astimezone(ZoneInfo("America/Vancouver"))
            peak_entry = {
                "time": dt.isoformat(),
                "astronomical_tide_m": round(tide_level, 3),
                "storm_surge_m": round(surge_value, 3),
                "total_water_level_m": round(total, 3),
                "description": (f"Peak occurs at {local_dt.strftime('%Y-%m-%d %I:%M %p PST')}"),
            }

        if _is_downsample_tick(dt):
            combined.append(
                {
                    "time": dt.isoformat(),
                    "astronomical_tide_m": round(tide_level, 3),
                    "storm_surge_m": round(surge_value, 3),
                    "total_water_level_m": round(total, 3),
                }
            )

    return {"forecast": combined, "peak": peak_entry}


def _without_timestamp(doc):
    """A copy of `doc` with its generation timestamp removed.

    Handles both shapes this module writes: a top-level `generated_utc`
    (observed surge) and a nested `_meta.generated_utc` (combined levels).
    """
    if not isinstance(doc, dict):
        return doc
    trimmed = {k: v for k, v in doc.items() if k != "generated_utc"}
    meta = trimmed.get("_meta")
    if isinstance(meta, dict):
        trimmed["_meta"] = {k: v for k, v in meta.items() if k != "generated_utc"}
    return trimmed


def _write_json_if_changed(payload, output_path, **dump_kwargs):
    """Write `payload` atomically, but leave the file untouched if only the
    timestamp would change.

    This export runs every 10 minutes, while the tide observations behind it
    arrive every 15 (hourly at Crescent Beach). Roughly one run in three
    therefore has nothing new to say. Rewriting anyway bumps the file's mtime,
    Caddy derives its ETag from mtime and size, and a changed ETag makes every
    conditional request from an API consumer re-download bytes it already has
    -- so the /api/v1 304s are defeated by a file that did not actually move.
    See docs/PUBLIC_API.md.

    `generated_utc` is excluded from the comparison because it changes on
    every run by definition; what matters is whether the DATA moved.

    Returns True if the file was written, False if it was already current.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, **dump_kwargs)

    if output_path.exists():
        try:
            existing = json.loads(output_path.read_text())
        except (OSError, json.JSONDecodeError):
            existing = None  # unreadable or corrupt: rewrite it
        if existing is not None and _without_timestamp(existing) == _without_timestamp(payload):
            logger.info(f"{output_path.name} unchanged; leaving mtime and ETag alone")
            return False

    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp.write_text(serialized)
    tmp.replace(output_path)
    return True


def write_combined(stations_data, today_local, forecast_end_local, output_path):
    output = {
        "_meta": {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "type": "combined_water_level",
            "description": "Total water level = astronomical tide + storm surge forecast",
            "forecast_start": today_local.isoformat(),
            "forecast_end": forecast_end_local.isoformat(),
            "timezone": "America/Vancouver",
            "forecast_days": FORECAST_DAYS,
            "units": "meters",
            "data_sources": {
                "astronomical_tide": "DFO IWLS (Integrated Water Level System)",
                "storm_surge": ("Environment Canada GDSPS (Global Deterministic Storm Surge Prediction System)"),
            },
            "important_notes": IMPORTANT_NOTES,
        },
        "stations": stations_data,
    }
    _write_json_if_changed(output, output_path, indent=2, sort_keys=True)


def update_latest_with_surge(surge_dir, latest_path):
    """Inject current surge + total water level into tide-latest.json."""
    if not latest_path.exists():
        logger.warning(f"tide-latest.json not found: {latest_path}")
        return

    combined_file = surge_dir / "combined_forecast.json"
    if not combined_file.exists():
        logger.warning("No storm surge data for latest update")
        return

    with open(latest_path) as f:
        tide_latest = json.load(f)
    with open(combined_file) as f:
        surge_data = json.load(f)

    surge_stations = surge_data.get("stations", {})
    if not surge_stations:
        return

    now = datetime.now(timezone.utc)
    updated = 0
    for station in WATER_LEVEL_STATIONS:
        if station.surge_source is None:
            continue
        if station.tide_key not in tide_latest.get("stations", {}):
            continue
        if station.surge_source not in surge_stations:
            continue

        surge_forecast = surge_stations[station.surge_source].get("forecast", {})
        if not surge_forecast:
            continue

        closest_surge = None
        min_diff = float("inf")
        for iso_time, surge_value in surge_forecast.items():
            dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
            diff = abs((dt - now).total_seconds())
            if diff < min_diff:
                min_diff = diff
                closest_surge = surge_value

        if closest_surge is None:
            continue

        station_data = tide_latest["stations"][station.tide_key]
        if "prediction_now" in station_data:
            tide_value = station_data["prediction_now"]["value"]
            station_data["prediction_now"]["surge"] = round(closest_surge, 3)
            station_data["prediction_now"]["total_water_level"] = round(tide_value + closest_surge, 3)
        if "observation" in station_data:
            tide_value = station_data["observation"]["value"]
            station_data["observation"]["surge"] = round(closest_surge, 3)
            station_data["observation"]["total_water_level"] = round(tide_value + closest_surge, 3)
        updated += 1

    if updated > 0:
        tide_latest.setdefault("_meta", {})["updated_with_surge"] = datetime.now(timezone.utc).isoformat()
        tmp = latest_path.with_suffix(latest_path.suffix + ".tmp")
        tmp.write_text(json.dumps(tide_latest, indent=2, sort_keys=True))
        tmp.replace(latest_path)
        logger.info(f"Updated {updated} stations in tide-latest.json with surge data")


# ---------- Observed view ----------
def build_observed_for_station(conn, station, registry_entry, start_ts):
    """Calculate observed surge (observation - prediction) for the past N days."""
    if station.observed_key is None:
        return None

    station_id = registry_entry["id"]
    station_name = registry_entry["name"]
    cur = conn.cursor()

    if station.is_surrey:
        cur.execute(
            """
            SELECT observation_time, tidal_residual
            FROM surrey_geodetic_data
            WHERE station_id = ?
              AND observation_time >= ?
              AND tidal_residual IS NOT NULL
            ORDER BY observation_time ASC
            """,
            (station_id, start_ts),
        )
        rows = cur.fetchall()
        if not rows:
            logger.warning(f"  no surrey residual data for {station_name}")
            return None

        data = []
        for ts, residual in rows:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            if not _is_downsample_tick(dt):
                continue
            data.append(
                {
                    "time": dt.isoformat(),
                    "observed_surge_m": round(float(residual), 4),
                    "source": "surrey_calculated",
                }
            )
    else:
        cur.execute(
            """
            SELECT observation_time, water_level, quality
            FROM tide_observation
            WHERE station_id = ? AND observation_time >= ?
            ORDER BY observation_time ASC
            """,
            (station_id, start_ts),
        )
        observations = cur.fetchall()
        cur.execute(
            """
            SELECT prediction_time, water_level
            FROM tide_prediction
            WHERE station_id = ? AND prediction_time >= ?
            ORDER BY prediction_time ASC
            """,
            (station_id, start_ts),
        )
        predictions = cur.fetchall()

        if not observations:
            logger.warning(f"  no observations for {station_name}")
            return None
        if not predictions:
            logger.warning(f"  no predictions for {station_name}")
            return None

        pred_dict = {int(t): float(v) for t, v in predictions}
        data = []
        for obs_ts, obs_level, quality in observations:
            obs_ts = int(obs_ts)
            dt = datetime.fromtimestamp(obs_ts, tz=timezone.utc)
            if not _is_downsample_tick(dt):
                continue

            pred_level = None
            for offset in (0, 60, -60, 120, -120, 180, -180, 240, -240, 300, -300):
                check = obs_ts + offset
                if check in pred_dict:
                    pred_level = pred_dict[check]
                    break
            if pred_level is None:
                continue

            data.append(
                {
                    "time": dt.isoformat(),
                    "observed_surge_m": round(float(obs_level) - pred_level, 4),
                    "observation_m": round(float(obs_level), 3),
                    "prediction_m": round(pred_level, 3),
                    "quality": quality,
                }
            )

    if not data:
        logger.warning(f"  no matched observed-surge points for {station_name}")
        return None

    return {
        "station_name": station_name,
        "tide_station_id": station.tide_key,
        "location": {"lat": registry_entry["lat"], "lon": registry_entry["lon"]},
        "data": data,
        "count": len(data),
        "time_range": {"start": data[0]["time"], "end": data[-1]["time"]},
    }


def write_observed(stations_data, output_path):
    output = {
        "description": "Observed storm surge calculated from tide observations minus predictions",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "days_back": OBSERVED_DAYS_BACK,
        "units": "meters",
        "calculation": "observed_surge = tide_observation - tide_prediction",
        "stations": stations_data,
    }
    _write_json_if_changed(output, output_path, indent=2)


# ---------- Orchestration ----------
def run(tide_db_path, surge_dir, combined_out, observed_out, latest_path):
    if not tide_db_path.exists():
        logger.error(f"Tide database not found: {tide_db_path}")
        return 1

    tide_registry = STATIONS.tides

    pacific = ZoneInfo("America/Vancouver")
    now_pacific = datetime.now(pacific)
    today_local = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    forecast_end_local = today_local + timedelta(days=FORECAST_DAYS)
    forecast_start_ts = int(today_local.astimezone(timezone.utc).timestamp())
    forecast_end_ts = int(forecast_end_local.astimezone(timezone.utc).timestamp())

    observed_start_local = today_local - timedelta(days=OBSERVED_DAYS_BACK - 1)
    observed_start_ts = int(observed_start_local.astimezone(timezone.utc).timestamp())

    surge_forecasts = load_surge_forecasts(surge_dir)
    if not surge_forecasts:
        logger.warning("No surge forecasts available — combined export will be empty")

    combined_data: dict[str, dict] = {}
    observed_data: dict[str, dict] = {}

    conn = sqlite3.connect(tide_db_path)
    try:
        for station in WATER_LEVEL_STATIONS:
            registry_entry = tide_registry.get(station.tide_key)
            if not registry_entry:
                logger.warning(f"Station {station.tide_key} missing from registry")
                continue

            if station.surge_source is not None and surge_forecasts:
                logger.info(f"forecast: {station.tide_key} + {station.surge_source}")
                fc = build_forecast_for_station(conn, station, surge_forecasts, forecast_start_ts, forecast_end_ts)
                if fc and fc["forecast"]:
                    combined_data[station.tide_key] = {
                        "tide_station_id": station.tide_key,
                        "surge_station_id": station.surge_source,
                        "forecast": fc["forecast"],
                        "peak": fc["peak"],
                    }
                    logger.info(
                        f"  {len(fc['forecast'])} combined predictions"
                        + (f"; peak {fc['peak']['total_water_level_m']}m" if fc["peak"] else "")
                    )

            if station.observed_key is not None:
                logger.info(f"observed: {station.tide_key} → {station.observed_key}")
                ob = build_observed_for_station(conn, station, registry_entry, observed_start_ts)
                if ob:
                    observed_data[station.observed_key] = ob
                    logger.info(f"  {ob['count']} observed surge points")
    finally:
        conn.close()

    write_combined(combined_data, today_local, forecast_end_local, combined_out)
    write_observed(observed_data, observed_out)

    if surge_forecasts and latest_path is not None:
        update_latest_with_surge(surge_dir, latest_path)

    logger.info(f"Done. forecast_stations={len(combined_data)} observed_stations={len(observed_data)}")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Export water-level forecast + hindcast JSON")
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Use test fixtures instead of production paths (skips tide-latest patch)",
    )
    parser.add_argument(
        "--combined-out",
        type=Path,
        default=None,
        help="Override combined-water-level.json output path",
    )
    parser.add_argument(
        "--observed-out",
        type=Path,
        default=None,
        help="Override observed_surge.json output path",
    )
    parser.add_argument(
        "--skip-latest-patch",
        action="store_true",
        help="Do not write to tide-latest.json (use when validating outputs)",
    )
    args = parser.parse_args()

    if args.test_mode:
        logger.info("[TEST MODE]")
        tide_db = TEST_TIDE_DB
        surge_dir = TEST_SURGE_DIR
        combined_out = args.combined_out or TEST_COMBINED_OUTPUT
        observed_out = args.observed_out or TEST_OBSERVED_OUTPUT
        latest_path = None
    else:
        tide_db = TIDE_DATABASE
        surge_dir = SURGE_DIR
        combined_out = args.combined_out or COMBINED_OUTPUT
        observed_out = args.observed_out or OBSERVED_OUTPUT
        latest_path = None if args.skip_latest_patch else LATEST_TIDE

    try:
        return run(tide_db, surge_dir, combined_out, observed_out, latest_path)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Error: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
