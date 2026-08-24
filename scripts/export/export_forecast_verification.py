#!/usr/bin/env python3
"""
Forecast verification export — what the model said a day ahead, against what
actually happened, for the last 48 hours at each forecast location.

The forecast page already shows the model looking forward. This is the same
picture looking backward: two series on one time axis, so the reader can judge
the forecast by the only evidence that matters — how the last two days went.

**The stitched forecast is a 19-24 h lead band, not a flat 24 h.** For each past
hour this takes the newest run that was still at least ~19 h ahead, i.e. the
largest available lead not exceeding 24 h. It cycles 19→24 and drops back,
because runs are 6-hourly and the fetch taper (hourly to +24 h, then 3-hourly)
means leads 25 and 26 do not exist in the archive at all — so for a valid hour
one hour past a run boundary, the next candidate down is 19 h, not 25 h. Every
point therefore carries its own `lead_hours`: the sawtooth is real, consecutive
points can come from different runs, and a small step at a seam is the honest
rendering of that rather than a glitch to smooth away.

Capping at 24 h rather than taking the nearest lead to 24 h is deliberate — it
guarantees the curve never claims more notice than it actually had.

Forecast and observation are emitted as two independent series over a shared
time axis; nothing is paired here. Pairing is the verification writer's job
(`scripts/monitoring/verify_wave_forecast.py`), and it answers a different
question — a plot wants every observation it can draw, including the last few
hours the verifier's settle window deliberately holds back. Both read the same
`wave_forecast` archive, so the chart and the skill scores cannot disagree.

Output: site/data/wave_forecast/verification/{station_id}.json
"""

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# (scripts/export/this_file → parents[2]); see scripts/pipelines/ for the same
# bootstrap and why `pyproject.toml` mapping `lib` only makes it necessary.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import BUOY_DATABASE, EXPORT_DIR, WAVE_FORECAST_DATABASE, safe_json_write  # noqa: E402
from lib.logging_config import setup_logging  # noqa: E402
from lib.stations import get_buoy  # noqa: E402
from scripts.fetch.fetch_wave_forecast import STATIONS, UNITS  # noqa: E402
from scripts.monitoring.verify_wave_forecast import OBSERVATION_COLUMNS  # noqa: E402

logger = setup_logging("forecast_verification")

OUTPUT_DIR = EXPORT_DIR / "wave_forecast" / "verification"

# How far back the plot reaches.
WINDOW_HOURS = 48

# The lead band described above. MAX is the promise ("at least this much
# notice"); MIN is what the 6-hourly run cadence leaves at the far end of each
# cycle, and is published so the page can label the band honestly instead of
# calling the whole thing a 24-hour forecast.
LEAD_TARGET_HOURS = 24
LEAD_MIN_HOURS = 19


def iso(epoch):
    """Epoch seconds → ISO 8601 UTC, the site-wide JSON timestamp format."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stitched_forecast(conn, station_id, window_start, window_end):
    """Per valid hour, the newest forecast that was still <= LEAD_TARGET_HOURS out.

    Returns {variable: [{"time", "value", "lead_hours"}, ...]}.

    Masked steps are skipped rather than carried as nulls: for the wave model a
    masked wind-wave partition means "no wind sea at this hour", which is a gap
    in the series, not a zero.
    """
    rows = conn.execute(
        """
        WITH candidates AS (
            SELECT variable, valid_time, value,
                   (valid_time - forecast_run_time) / 3600 AS lead_hours
            FROM wave_forecast
            WHERE station_id = ?
              AND status = 'ok'
              AND value IS NOT NULL
              AND valid_time BETWEEN ? AND ?
        ),
        ranked AS (
            SELECT variable, valid_time, value, lead_hours,
                   ROW_NUMBER() OVER (
                       PARTITION BY variable, valid_time ORDER BY lead_hours DESC
                   ) AS rank
            FROM candidates
            WHERE lead_hours <= ?
        )
        SELECT variable, valid_time, value, lead_hours
        FROM ranked WHERE rank = 1
        ORDER BY variable, valid_time
        """,
        (station_id, window_start, window_end, LEAD_TARGET_HOURS),
    ).fetchall()

    series = {}
    for variable, valid_time, value, lead_hours in rows:
        series.setdefault(variable, []).append(
            {"time": iso(valid_time), "value": round(value, 3), "lead_hours": lead_hours}
        )
    return series


def observations(station_id, window_start, window_end):
    """The measured series for every variable that has an instrument behind it.

    Column mapping comes from the verification writer so there is one place
    that decides which instrument measures which forecast variable — including
    the ones nothing measures (`wind_wave_height`), which simply produce no
    observed series here.
    """
    verifiable = {
        variable: column for variable, column in OBSERVATION_COLUMNS.items() if column
    }
    if not BUOY_DATABASE.exists():
        logger.info(f"⚠️  No observation database at {BUOY_DATABASE}")
        return {}

    columns = sorted(set(verifiable.values()))
    with sqlite3.connect(f"file:{BUOY_DATABASE}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            f"""
            SELECT observation_time, {", ".join(columns)}
            FROM buoy_observation
            WHERE buoy_id = ? AND observation_time BETWEEN ? AND ?
            ORDER BY observation_time
            """,
            (station_id, window_start, window_end),
        ).fetchall()

    series = {}
    for variable, column in verifiable.items():
        index = columns.index(column) + 1  # +1 for observation_time
        points = [
            {"time": iso(row[0]), "value": round(row[index], 3)}
            for row in rows
            if row[index] is not None
        ]
        if points:
            series[variable] = points
    return series


def build_station_payload(conn, station_id, station_info, now):
    """Assemble one station's verification series, or None when there is nothing to draw."""
    window_start = now - WINDOW_HOURS * 3600
    forecast = stitched_forecast(conn, station_id, window_start, now)
    observed = observations(station_id, window_start, now)

    variables = sorted(set(forecast) | set(observed))
    if not variables:
        return None

    return {
        "station_id": station_id,
        "station_name": station_info["name"],
        "location": {"lat": station_info["lat"], "lon": station_info["lon"]},
        "generated_utc": iso(now),
        "window_hours": WINDOW_HOURS,
        # Published so the page can label the band rather than overclaim: the
        # curve is "how it looked about a day ahead", spanning these leads.
        "lead_band": {
            "target_hours": LEAD_TARGET_HOURS,
            "min_hours": LEAD_MIN_HOURS,
            "max_hours": LEAD_TARGET_HOURS,
        },
        # km/h for wind, matching the sibling forecast file this is charted
        # against. The page converts to knots at render time, as everywhere.
        "units": {variable: UNITS[variable] for variable in variables if variable in UNITS},
        "series": {
            variable: {
                "forecast": forecast.get(variable, []),
                "observed": observed.get(variable, []),
            }
            for variable in variables
        },
    }


def export_verification(now=None):
    """Write one verification file per forecast station. Returns the number written."""
    now = int(datetime.now(timezone.utc).timestamp()) if now is None else int(now)

    if not WAVE_FORECAST_DATABASE.exists():
        logger.info(f"⚠️  No forecast archive at {WAVE_FORECAST_DATABASE}, nothing to export")
        return 0

    written = 0
    with sqlite3.connect(f"file:{WAVE_FORECAST_DATABASE}?mode=ro", uri=True) as conn:
        for station_id in STATIONS:
            station_info = get_buoy(station_id)
            if not station_info:
                logger.info(f"❌ Station {station_id} not found in stations.json")
                continue

            payload = build_station_payload(conn, station_id, station_info, now)
            if not payload:
                logger.info(f"    ⚠️  No verification data for {station_id} yet")
                continue

            # site/data is served straight to the public — every field above is
            # written explicitly, never copied wholesale from a source row.
            safe_json_write(OUTPUT_DIR / f"{station_id}.json", payload)
            counts = ", ".join(
                f"{variable} {len(s['forecast'])}f/{len(s['observed'])}o"
                for variable, s in sorted(payload["series"].items())
            )
            logger.info(f"    💾 {station_id}: {counts}")
            written += 1

    logger.info(f"✅ Wrote {written} verification file(s) to {OUTPUT_DIR}")
    return written


def main():
    logger.info(f"📉 Forecast verification export (last {WINDOW_HOURS} h)")
    export_verification()
    return 0


if __name__ == "__main__":
    sys.exit(main())
