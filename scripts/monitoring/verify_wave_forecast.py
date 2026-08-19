#!/usr/bin/env python3
"""
Forecast verification writer — pairs archived model forecasts with what the
buoys actually measured, so the site can say how much the forecast is worth.

`scripts/fetch/fetch_wave_forecast.py` stores every RDWPS/HRDPS run it fetches,
keyed by (station, variable, run, valid time) so a later run never overwrites
an earlier run's prediction for the same hour. That archive is the raw
material; this script is the other half. Once a valid time has passed it looks
up the observation for that hour and writes one row to
`wave_forecast_verification`: what the model said, what happened, and what
"nothing changes from now" would have said (the persistence baseline).

Three things this deliberately does NOT do:

- **It does not loosen the time match to flatter the model.** A forecast that
  gets a gale's size right but its timing wrong is penalised twice by exact
  point matching — once for the peak that didn't happen, once for the peak it
  missed. That is a real limitation, and the fix is a separate peak-timing
  metric (predicted peak time vs observed peak time, scored apart from
  magnitude), not a fuzzy valid-time join here. Fuzzy matching would quietly
  reward a forecast that is wrong at every specific hour, which is exactly the
  question the 3-hourly rows past +24 h exist to answer.
- **It does not filter out small waves.** Sub-0.5 m rows are still paired and
  stored; the 0.5 m event gate is applied when *reading* the archive (see
  EVENT_THRESHOLDS). Storing them keeps false alarms and misses in the record —
  a 0.7 m forecast that never materialised is a scoring event, and dropping it
  at write time would make the model look better the worse it got.
- **It does not revise a pair once written.** Rows are INSERT OR IGNORE, so a
  late buoy backfill cannot retroactively rewrite a past score. If the
  observation has not arrived yet, no row is written and the pair is retried on
  the next run until it falls out of LOOKBACK_DAYS.

Usage:
    verify_wave_forecast.py             # pair what's ready, then summarise
    verify_wave_forecast.py --summary   # summarise only, write nothing
    verify_wave_forecast.py --lookback-days 60   # backfill the whole archive
"""

import argparse
import bisect
import sqlite3
import sys
import time
from pathlib import Path

# Installed as a package this would be an import; `pyproject.toml` maps `lib`
# only, so the repo root goes on sys.path the way scripts/pipelines/ does it.
# (scripts/monitoring/this_file → parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import BUOY_DATABASE, WAVE_FORECAST_DATABASE  # noqa: E402
from lib.directions import circular_difference  # noqa: E402
from lib.logging_config import setup_logging  # noqa: E402
from scripts.fetch.fetch_wave_forecast import (  # noqa: E402
    UNITS,
    ensure_db_schema,
    migrate_db_schema,
)

logger = setup_logging("wave_forecast_verify")

# Forecast field -> the buoy_observation column that measures the same thing.
#
# Both forecast stations live in buoy_data.sqlite: 4600146 is the EC buoy at
# Halibut Bank, CRPILE is Surrey's Crescent Beach sensor, which the Surrey
# fetch writes into the same table. So there is one observation source here,
# not two.
#
# `wind_wave_height` maps to None on purpose. The column exists in
# buoy_observation but neither station has ever populated it (0 of 720 rows at
# 4600146, 0 of 4312 at CRPILE, checked 2026-08-19) — the wind-sea/swell
# partition is a model diagnostic that our instruments don't report. Mapping it
# to a column that is always NULL would look like a data outage forever; naming
# it here as unverifiable says the real thing.
#
# Directions are absent at CRPILE too (wave_direction_avg is never populated
# there), but that is per-station rather than per-variable, and needs no
# special case: no observation simply means no pair.
OBSERVATION_COLUMNS = {
    "wave_height": "wave_height_sig",
    "peak_period": "wave_period_peak",
    "wave_direction": "wave_direction_avg",
    "wind_wave_height": None,  # no instrument measures this — see above
    "wind_speed": "wind_speed",
    "wind_direction": "wind_direction",
    "wind_gust": "wind_gust",
}

# Bearings, not scalars: their errors must go through circular_difference().
CIRCULAR_VARIABLES = {"wave_direction", "wind_direction"}

# Event gate for the headline scores, applied at read time (user's call,
# 2026-08-19). Two reasons, both about signal rather than tidiness: the EC buoy
# quantises Hs to 0.1 m, so a summer of 0.2-0.4 m readings is mostly bucket
# noise; and a "how good is the forecast" number should describe the conditions
# anyone is making a decision about. A row counts as an event if EITHER side
# crosses — gating on the observation alone would discard false alarms, gating
# on the forecast alone would discard misses, and those are the two failures
# the score most needs to catch.
#
# Only wave heights are gated. A wind threshold is defensible too but would be
# a different number picked for a different reason, so it stays unset rather
# than guessed; variables absent from this dict are scored on every pair.
EVENT_THRESHOLDS = {
    "wave_height": 0.5,
    "wind_wave_height": 0.5,
}

# How long after a valid time to wait before trying to pair it. Halibut Bank
# reports hourly and lands in the database within minutes, but a settle window
# costs nothing and keeps a transient upstream delay from burning the pair.
SETTLE_HOURS = 3

# How far back to look for unpaired rows. Bounds the work per run and, more
# importantly, bounds the retry: a valid time whose observation never arrives
# is abandoned once it falls out of this window rather than being re-queried
# forever. Comfortably longer than any plausible buoy outage.
LOOKBACK_DAYS = 7

# How far from the forecast's valid time an observation may sit and still count
# as measuring that hour. Forecast valid times are exactly on the hour; 4600146
# reports at :05 and CRPILE every 10 minutes, so 30 minutes matches the
# intended hour and nothing else. The actual gap is stored per row
# (obs_offset_seconds) so a loose match stays visible rather than averaging in
# as if it were exact.
MATCH_TOLERANCE_SECONDS = 1800

# Display buckets for the summary. Skill degrades with lead time — that curve
# is the whole point — but 33 separate lead hours is a wall of numbers, and the
# early buckets carry many more samples than the 3-hourly tail.
LEAD_BUCKETS = [(0, 6), (7, 12), (13, 24), (25, 48)]


def error_of(variable, forecast_value, observed_value):
    """Signed forecast error, circular-aware for bearings."""
    if forecast_value is None or observed_value is None:
        return None
    if variable in CIRCULAR_VARIABLES:
        return circular_difference(forecast_value, observed_value)
    return forecast_value - observed_value


def is_event(variable, forecast_value, observed_value):
    """Whether a pair clears the event gate for its variable."""
    threshold = EVENT_THRESHOLDS.get(variable)
    if threshold is None:
        return True
    return any(v is not None and v >= threshold for v in (forecast_value, observed_value))


# ── observation lookup ──────────────────────────────────────────────


class ObservationSeries:
    """Nearest-in-time lookup over one station's observations.

    Built once per station and queried thousands of times, so the whole window
    is read up front and bisected rather than issuing a query per forecast row.

    One series per column, holding only the timestamps where that column
    actually has a value: "nearest observation" has to mean nearest reading of
    *this* variable, not nearest row. Otherwise a station that reports wind
    every 10 minutes but waves hourly would match a wave forecast to a row
    whose wave field is NULL and call it a miss.
    """

    def __init__(self, rows, columns):
        # rows: (observation_time, *column values) ordered by time.
        # Keyed by timestamp first because buoy_observation has no uniqueness
        # constraint — a re-parsed file can leave two rows for one instant, and
        # the later one wins.
        by_time = {}
        for row in rows:
            by_time[row[0]] = row[1:]

        self._times = {}
        self._values = {}
        for index, column in enumerate(columns):
            times, values = [], []
            for observation_time in sorted(by_time):
                value = by_time[observation_time][index]
                if value is not None:
                    times.append(observation_time)
                    values.append(value)
            self._times[column] = times
            self._values[column] = values

    def nearest(self, column, target_time, tolerance):
        """Return (value, offset_seconds) for the reading closest to target_time.

        offset_seconds is signed (observation - target), so a systematic lag
        shows up as a consistent sign rather than averaging away. Returns
        (None, None) when nothing falls inside the tolerance.
        """
        times = self._times.get(column) or []
        if not times:
            return None, None

        index = bisect.bisect_left(times, target_time)
        best = None
        # The insertion point and the entry before it are the only two
        # candidates for nearest in a sorted list.
        for candidate in (index - 1, index):
            if 0 <= candidate < len(times):
                offset = times[candidate] - target_time
                if abs(offset) <= tolerance and (best is None or abs(offset) < abs(best[1])):
                    best = (self._values[column][candidate], offset)
        return best if best else (None, None)


def load_observations(station_id, start_time, end_time):
    """Read one station's observations for every column we verify against."""
    columns = sorted({column for column in OBSERVATION_COLUMNS.values() if column})
    if not BUOY_DATABASE.exists():
        logger.info(f"⚠️  No observation database at {BUOY_DATABASE}")
        return ObservationSeries([], columns)

    with sqlite3.connect(f"file:{BUOY_DATABASE}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            f"""
            SELECT observation_time, {", ".join(columns)}
            FROM buoy_observation
            WHERE buoy_id = ? AND observation_time BETWEEN ? AND ?
            ORDER BY observation_time
            """,
            (station_id, start_time, end_time),
        ).fetchall()
    return ObservationSeries(rows, columns)


# ── pairing ─────────────────────────────────────────────────────────


def unpaired_forecasts(conn, window_start, window_end):
    """Archived forecast values whose hour has passed and that aren't yet scored.

    Masked and failed steps are excluded: a masked step is the model declining
    to give a number, which is not a prediction to score.
    """
    return conn.execute(
        """
        SELECT f.station_id, f.variable, f.forecast_run_time, f.valid_time,
               f.value, f.model
        FROM wave_forecast f
        LEFT JOIN wave_forecast_verification v
               ON v.station_id = f.station_id
              AND v.variable = f.variable
              AND v.forecast_run_time = f.forecast_run_time
              AND v.valid_time = f.valid_time
        WHERE f.status = 'ok'
          AND f.value IS NOT NULL
          AND f.valid_time BETWEEN ? AND ?
          AND v.station_id IS NULL
        ORDER BY f.station_id, f.valid_time
        """,
        (window_start, window_end),
    ).fetchall()


def write_verification_pairs(conn, lookback_days=LOOKBACK_DAYS, now=None):
    """Pair every ready forecast row with its observation. Returns a count dict."""
    now = int(time.time()) if now is None else int(now)
    window_end = now - SETTLE_HOURS * 3600
    window_start = now - lookback_days * 86400

    pending = unpaired_forecasts(conn, window_start, window_end)
    counts = {"written": 0, "no_observation": 0, "unverifiable": 0, "no_reference": 0}
    if not pending:
        logger.info("✅ Nothing new to verify")
        return counts

    logger.info(f"🔍 {len(pending)} unpaired forecast values in the last {lookback_days} days")

    # Observations are loaded per station over a window wide enough for both
    # lookups: the valid time, and the run time behind it (up to 48 h earlier)
    # for the persistence baseline.
    series_by_station = {}
    for station_id in sorted({row[0] for row in pending}):
        series_by_station[station_id] = load_observations(
            station_id,
            window_start - 48 * 3600 - MATCH_TOLERANCE_SECONDS,
            window_end + MATCH_TOLERANCE_SECONDS,
        )

    for station_id, variable, run_time, valid_time, forecast_value, model in pending:
        column = OBSERVATION_COLUMNS.get(variable)
        if not column:
            counts["unverifiable"] += 1
            continue

        series = series_by_station[station_id]
        observed_value, offset = series.nearest(column, valid_time, MATCH_TOLERANCE_SECONDS)
        if observed_value is None:
            # Not written at all, so a late-arriving observation is still
            # picked up on a later run inside the lookback window.
            counts["no_observation"] += 1
            continue

        # Persistence baseline: what the buoy read when the model run started.
        # Nullable — an outage at t0 costs the skill score for that pair, not
        # the pair itself.
        reference_value, _ = series.nearest(column, run_time, MATCH_TOLERANCE_SECONDS)
        if reference_value is None:
            counts["no_reference"] += 1

        conn.execute(
            """
            INSERT OR IGNORE INTO wave_forecast_verification
            (station_id, variable, forecast_run_time, valid_time, lead_hours,
             forecast_value, observed_value, reference_value, obs_offset_seconds, model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                station_id,
                variable,
                run_time,
                valid_time,
                (valid_time - run_time) // 3600,
                forecast_value,
                observed_value,
                reference_value,
                offset,
                model,
            ),
        )
        counts["written"] += 1

    conn.commit()
    logger.info(
        f"    💾 Wrote {counts['written']} pairs "
        f"(no observation yet: {counts['no_observation']}, "
        f"unverifiable variable: {counts['unverifiable']}, "
        f"no persistence baseline: {counts['no_reference']})"
    )
    return counts


# ── scoring ─────────────────────────────────────────────────────────


def score(pairs, variable):
    """Bias, RMSE, and skill vs persistence over a set of (f, o, reference).

    The skill score is Murphy's: 1 - MSE(model)/MSE(persistence). Positive
    means the model beat "conditions stay as they are at run time", zero means
    it tied, negative means persistence would have served better. That is the
    number that decides how far out a forecast is worth showing at all — bias
    and RMSE alone say how wrong the model is, not whether it is useful.

    Skill is computed only over pairs that have a baseline, so it is never a
    comparison against a different sample than the one it claims.
    """
    errors = []
    model_squared, persistence_squared = [], []
    for forecast_value, observed_value, reference_value in pairs:
        error = error_of(variable, forecast_value, observed_value)
        if error is None:
            continue
        errors.append(error)
        baseline = error_of(variable, reference_value, observed_value)
        if baseline is not None:
            model_squared.append(error**2)
            persistence_squared.append(baseline**2)

    if not errors:
        return None

    mean_squared = sum(model_squared) / len(model_squared) if model_squared else None
    baseline_mean_squared = (
        sum(persistence_squared) / len(persistence_squared) if persistence_squared else None
    )
    skill = None
    if mean_squared is not None and baseline_mean_squared:
        skill = 1 - mean_squared / baseline_mean_squared

    return {
        "n": len(errors),
        "bias": sum(errors) / len(errors),
        "rmse": (sum(error**2 for error in errors) / len(errors)) ** 0.5,
        "skill_vs_persistence": skill,
        "n_skill": len(model_squared),
    }


def summarise(conn, events_only=True):
    """Per station/variable/lead-bucket scores, as a list of dicts."""
    rows = conn.execute(
        """
        SELECT station_id, variable, lead_hours, forecast_value, observed_value,
               reference_value
        FROM wave_forecast_verification
        ORDER BY station_id, variable, lead_hours
        """
    ).fetchall()

    grouped = {}
    for station_id, variable, lead_hours, forecast_value, observed_value, reference in rows:
        if events_only and not is_event(variable, forecast_value, observed_value):
            continue
        bucket = next(
            (b for b in LEAD_BUCKETS if b[0] <= lead_hours <= b[1]),
            None,
        )
        if bucket is None:
            continue
        key = (station_id, variable, bucket)
        grouped.setdefault(key, []).append((forecast_value, observed_value, reference))

    results = []
    for (station_id, variable, bucket), pairs in sorted(grouped.items()):
        stats = score(pairs, variable)
        if stats:
            results.append(
                {
                    "station_id": station_id,
                    "variable": variable,
                    "lead_bucket": f"{bucket[0]}-{bucket[1]}h",
                    **stats,
                }
            )
    return results


def log_summary(conn):
    """Print the scoreboard, gated and ungated, to the log."""
    for events_only in (True, False):
        label = "events only" if events_only else "all pairs"
        gates = ", ".join(f"{k} ≥ {v}" for k, v in sorted(EVENT_THRESHOLDS.items()))
        results = summarise(conn, events_only=events_only)
        logger.info(f"\n📊 Forecast skill — {label}" + (f" ({gates})" if events_only else ""))
        if not results:
            logger.info("    (no pairs yet)")
            continue
        logger.info(
            f"    {'station':<10} {'variable':<17} {'lead':<8} "
            f"{'n':>5} {'bias':>9} {'rmse':>9} {'skill':>7}"
        )
        for row in results:
            skill = row["skill_vs_persistence"]
            unit = UNITS.get(row["variable"], "")
            unit = "°" if unit.startswith("degrees") else unit
            logger.info(
                f"    {row['station_id']:<10} {row['variable']:<17} {row['lead_bucket']:<8} "
                f"{row['n']:>5} {row['bias']:>+8.2f}{unit:<1} {row['rmse']:>8.2f}{unit:<1} "
                f"{'  n/a' if skill is None else f'{skill:>+7.2f}'}"
            )


# ── entry point ─────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument(
        "--summary",
        action="store_true",
        help="only print the scoreboard; write no new pairs",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=LOOKBACK_DAYS,
        help=f"how far back to look for unpaired forecasts (default {LOOKBACK_DAYS})",
    )
    args = parser.parse_args()

    if not WAVE_FORECAST_DATABASE.exists():
        logger.info(f"⚠️  No forecast archive at {WAVE_FORECAST_DATABASE}, nothing to verify")
        return 0

    conn = sqlite3.connect(WAVE_FORECAST_DATABASE)
    try:
        ensure_db_schema(conn)
        migrate_db_schema(conn)
        if not args.summary:
            write_verification_pairs(conn, lookback_days=args.lookback_days)
        log_summary(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
