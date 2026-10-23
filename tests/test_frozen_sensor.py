"""Tests for lib/frozen_sensor.py: a stuck instrument reads as frozen, real air does not."""

import sqlite3

from lib.frozen_sensor import WHITEROCK_CHECKS, frozen_since, hidden_columns, whiterock_frozen

H = 3600
STEP = 600  # White Rock reports every 10 minutes


def run(values_newest_first, start=100_000):
    """(epoch, values) pairs, newest first, one per 10-minute report."""
    n = len(values_newest_first)
    return [(start + (n - 1 - i) * STEP, v) for i, v in enumerate(values_newest_first)]


def test_identical_readings_past_the_span_are_frozen():
    rows = run([(1.9, 166.0, 3.3)] * 13)  # two hours exactly
    assert frozen_since(rows, 2 * H) == rows[-1][0]


def test_identical_readings_short_of_the_span_are_not():
    rows = run([(1.9, 166.0, 3.3)] * 12)  # 1 h 50 min
    assert frozen_since(rows, 2 * H) is None


def test_only_the_current_run_counts():
    rows = run([(5.0, 200.0, 8.0)] + [(1.9, 166.0, 3.3)] * 30)
    assert frozen_since(rows, 2 * H) is None


def test_run_starts_at_its_first_identical_reading():
    rows = run([(1.9, 166.0, 3.3)] * 20 + [(7.5, 184.0, 14.2)])
    assert frozen_since(rows, 2 * H) == rows[-2][0]


def test_flat_calm_is_not_frozen():
    assert frozen_since(run([(0.0, 0.0)] * 40), 2 * H) is None


def test_missing_latest_value_is_not_frozen():
    assert frozen_since(run([(None,)] * 40), 2 * H) is None
    assert frozen_since([], 2 * H) is None


def make_db(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE whiterock_weather (observation_time INTEGER, temperature REAL,"
        " dew_point REAL, wind_speed REAL, wind_direction REAL, wind_gust REAL, pressure REAL)"
    )
    conn.executemany("INSERT INTO whiterock_weather VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    return conn


def test_whiterock_flags_stuck_sensors_but_not_the_moving_one():
    # The 2026-10 failure: temperature moves, everything else is fixed.
    rows = [(i * STEP, 9.0 + (i % 5) * 0.1, 7.2, 1.9, 166.0, 3.3, 1011.6) for i in range(40)]
    frozen = whiterock_frozen(make_db(rows))
    assert set(frozen) == {"wind", "humidity", "pressure"}
    hidden = hidden_columns(frozen)
    assert {"wind_speed", "humidity", "pressure"} <= hidden
    assert "temperature" not in hidden


def test_whiterock_moving_sensors_are_left_alone():
    rows = [
        (i * STEP, 9.0 + i * 0.1, 7.0 + i * 0.05, 5 + i % 3, 180 + i, 8 + i % 4, 1011 + i * 0.01) for i in range(40)
    ]
    assert whiterock_frozen(make_db(rows)) == {}


def test_every_check_hides_what_it_compares():
    for check in WHITEROCK_CHECKS.values():
        assert set(check["compare"]) <= set(check["hide"])
