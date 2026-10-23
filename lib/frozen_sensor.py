"""
Detect a sensor that keeps reporting the same value — a stuck instrument.

A frozen sensor does not look stale: the station keeps publishing on time
with a fresh timestamp, and only the values stop moving. White Rock East
Beach did exactly this from 2026-10-04 — wind, humidity, dew point and
pressure all fixed while the temperature kept changing — so freshness checks
never caught it.

The test is a run of identical readings spanning longer than the quantity
could plausibly hold still at the instrument's resolution: wind to 0.1 kt
and 1° for two hours, pressure to 0.01 hPa or temperature to 0.1 °C for
three. Only the current run counts — a sensor that froze and recovered is
fine again.
"""

from datetime import datetime, timezone

HOUR = 3600

# White Rock East Beach: one check per sensor, each listing the columns
# it compares together and the columns it hides when frozen. Humidity has
# no check of its own (whole percent, it can sit at 100 % for hours in fog);
# it rides on dew point, which moves with temperature unless the humidity
# sensor has stopped.
WHITEROCK_CHECKS = {
    "wind": {
        "compare": ("wind_speed", "wind_direction", "wind_gust"),
        "hide": (
            "wind_speed",
            "wind_direction",
            "wind_direction_cardinal",
            "wind_gust",
            "wind_gust_direction",
            "wind_gust_direction_cardinal",
            "wind_chill",
        ),
        "min_span": 2 * HOUR,
    },
    "temperature": {
        "compare": ("temperature",),
        "hide": ("temperature", "heat_index", "wind_chill"),
        "min_span": 3 * HOUR,
    },
    "humidity": {
        "compare": ("dew_point",),
        "hide": ("dew_point", "humidity", "heat_index"),
        "min_span": 3 * HOUR,
    },
    "pressure": {
        "compare": ("pressure",),
        "hide": ("pressure",),
        "min_span": 3 * HOUR,
    },
}


def frozen_since(rows, min_span):
    """
    Start of the current run of identical readings, if it is a frozen one.

    Args:
        rows: (epoch, values) pairs, NEWEST FIRST; `values` is a tuple.
        min_span: Seconds a run must span to count as frozen.

    Returns:
        Epoch of the run's first reading, or None when the latest values are
        missing, the run is shorter than `min_span`, or the run is flat calm
        (all zeros), which real air does hold.
    """
    if not rows:
        return None
    newest_time, newest = rows[0]
    if any(v is None for v in newest) or all(v == 0 for v in newest):
        return None

    start = newest_time
    for epoch, values in rows[1:]:
        if values != newest:
            break
        start = epoch
    return start if newest_time - start >= min_span else None


def whiterock_frozen(conn, lookback=7 * 24 * HOUR):
    """
    Frozen sensors at White Rock East Beach, from `whiterock_weather`.

    Returns:
        {check name: epoch the freeze began}, e.g. {"wind": 1792588538};
        empty when every sensor is moving.
    """
    columns = sorted({c for check in WHITEROCK_CHECKS.values() for c in check["compare"]})
    cur = conn.execute(
        f"""
        SELECT observation_time, {", ".join(columns)}
        FROM whiterock_weather
        WHERE observation_time >= (SELECT MAX(observation_time) FROM whiterock_weather) - ?
        ORDER BY observation_time DESC
        """,
        (lookback,),
    )
    records = [(row[0], dict(zip(columns, row[1:]))) for row in cur.fetchall()]

    frozen = {}
    for name, check in WHITEROCK_CHECKS.items():
        rows = [(epoch, tuple(r[c] for c in check["compare"])) for epoch, r in records]
        since = frozen_since(rows, check["min_span"])
        if since is not None:
            frozen[name] = since
    return frozen


def hidden_columns(frozen):
    """Every column to withhold for the frozen checks in `frozen`."""
    return {c for name in frozen for c in WHITEROCK_CHECKS[name]["hide"]}


def frozen_iso(frozen):
    """`frozen` with ISO 8601 UTC times, for a JSON export."""
    return {name: datetime.fromtimestamp(since, tz=timezone.utc).isoformat() for name, since in frozen.items()}
