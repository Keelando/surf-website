"""Is a station reporting normally, late, or down? Judged by its own rhythm.

The exports have always carried `stale`: a flat "older than 3 hours". That
one number is wrong in both directions. A ten-minute station three hours
silent has missed eighteen reports and is plainly broken, while an hourly EC
buoy's newest reading is routinely an hour old, so at three hours it has
missed only two. `stale` stays as it is (it is part of the public API);
`status` is the per-station answer beside it:

    "ok"    reporting on its usual rhythm
    "late"  missed about two of its own expected reports
    "down"  silent for 12 hours or more (longer for a slow station; below)

RHYTHM, FROM ARRIVALS
    A station's rhythm is read from its own recent history, the
    `observation_time` and first-ingest `recorded_at` of each row (both
    writers keep `recorded_at` at first sighting; see lib/reporting_lag.py).
    Two numbers matter to a visitor, and both are about *arrivals*, not
    observation spacing:

    - cadence: how often new data reaches us. NOAA and Surrey deliver
      ten-minute readings in batches, so their observation spacing (10 min)
      says nothing about when the next number appears on the page (20 min).
      Rows landing within BATCH_SECONDS of each other are one delivery.
    - normal peak age: how old the newest reading usually gets just before
      the next delivery replaces it: cadence plus upstream delay (an hourly
      EC buoy: 60 + ~4 min; NOAA and Surrey carry far more delay). The 90th
      percentile, so the odd slow delivery does not set the bar.

    Late is two more missed deliveries past the normal peak:
    late_after = peak + 2 * cadence. Measured 2026-09-23: 3.1 h for the hourly
    EC buoys and wind stations, 1.7-2.4 h for NOAA and Surrey, 1 h (the floor)
    for the ten-minute stations.

FLOORS
    LATE_FLOOR_SECONDS: a five-minute station missing two polls is "late" by
    the rule and not by any reader's standard; a reading under an hour old
    is still current conditions. DOWN_FLOOR_SECONDS matches the cards'
    existing "STATION DOWN" at 12 h. A slow station whose late threshold is
    itself long gets down at twice that instead, so late never collapses
    into down.

FALLBACK
    Too little history (a new station, a long outage) and the legacy flat
    thresholds apply: late at 3 h, which is what `stale` has always meant,
    down at 12 h.
"""

from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

LOOKBACK_SECONDS = 7 * 86400
BATCH_SECONDS = 300
PEAK_PERCENTILE = 0.9
MIN_DELIVERIES = 6

LATE_FLOOR_SECONDS = 3600
DOWN_FLOOR_SECONDS = 12 * 3600
LEGACY_LATE_SECONDS = 3 * 3600


@dataclass(frozen=True)
class Rhythm:
    late_after_seconds: float
    down_after_seconds: float
    # None when the thresholds are the legacy fallback, not measured.
    cadence_seconds: Optional[float] = None
    peak_age_seconds: Optional[float] = None


LEGACY_RHYTHM = Rhythm(LEGACY_LATE_SECONDS, DOWN_FLOOR_SECONDS)


def rhythm_from_thresholds(late_after_seconds: float) -> Rhythm:
    """Rhythm for a source that already knows its late threshold (lightstations)."""
    late = max(LATE_FLOOR_SECONDS, late_after_seconds)
    return Rhythm(late, max(DOWN_FLOOR_SECONDS, 2 * late))


def infer_rhythm(rows: Iterable[tuple[float, Optional[float]]]) -> Rhythm:
    """Rhythm from (observation_time, arrived_at) pairs, epoch seconds.

    Rows without an arrival time are ignored. Falls back to LEGACY_RHYTHM when
    fewer than MIN_DELIVERIES deliveries can be seen.
    """
    arrivals = sorted((a, o) for o, a in rows if a is not None)
    newest = None
    deliveries: list[tuple[float, float]] = []  # (arrived_at, age of newest before it)
    for arrived, obs in arrivals:
        if newest is None:
            newest = obs
            continue
        if obs <= newest:
            continue  # a backfilled older reading: never the one on the page
        if deliveries and arrived - deliveries[-1][0] <= BATCH_SECONDS:
            newest = obs  # later row of the same delivery
            continue
        deliveries.append((arrived, arrived - newest))
        newest = obs

    if len(deliveries) < MIN_DELIVERIES:
        return LEGACY_RHYTHM

    times = [d[0] for d in deliveries]
    cadence = statistics.median(b - a for a, b in zip(times, times[1:]))
    peaks = sorted(d[1] for d in deliveries)
    peak = peaks[int(PEAK_PERCENTILE * (len(peaks) - 1))]
    late = max(LATE_FLOOR_SECONDS, peak + 2 * cadence)
    return Rhythm(
        late_after_seconds=late,
        down_after_seconds=max(DOWN_FLOOR_SECONDS, 2 * late),
        cadence_seconds=cadence,
        peak_age_seconds=peak,
    )


def _parse_recorded_at(value) -> Optional[float]:
    """`recorded_at` is SQLite's datetime('now'): 'YYYY-MM-DD HH:MM:SS', UTC."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def load_rhythms(conn: sqlite3.Connection, table: str, id_column: str, now: float) -> dict[str, Rhythm]:
    """Rhythm for every station with recent rows in `table`, in one query.

    `table` and `id_column` are code constants, never input.
    """
    by_station: dict[str, list[tuple[float, Optional[float]]]] = {}
    rows = conn.execute(
        f"SELECT {id_column}, observation_time, recorded_at FROM {table} "
        "WHERE observation_time > ? AND observation_time <= ?",
        (now - LOOKBACK_SECONDS, now),
    )
    for station_id, obs, recorded in rows:
        by_station.setdefault(str(station_id), []).append((obs, _parse_recorded_at(recorded)))
    return {sid: infer_rhythm(pairs) for sid, pairs in by_station.items()}


def report_status(age_seconds: float, rhythm: Rhythm) -> str:
    """Status of a latest reading `age_seconds` old: "ok", "late" or "down"."""
    if age_seconds > rhythm.down_after_seconds:
        return "down"
    if age_seconds > rhythm.late_after_seconds:
        return "late"
    return "ok"


def status_fields(age_seconds: float, rhythm: Rhythm) -> dict:
    """The export fields: status plus the thresholds it was judged against."""
    return {
        "status": report_status(age_seconds, rhythm),
        "late_after_minutes": round(rhythm.late_after_seconds / 60),
        "down_after_minutes": round(rhythm.down_after_seconds / 60),
    }
