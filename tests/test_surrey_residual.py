"""Surrey residual: the predicted channel's clock is measured, not assumed (lib/surrey_residual.py)."""

import math
import sqlite3

import pytest

from lib import surrey_residual as sr

T0 = 1_793_577_600  # 2026-11-02 00:00 UTC
PERIOD = 12.42 * 3600  # M2


def tide(t):
    return 2.0 * math.cos(2 * math.pi * (t - T0) / PERIOD)


def surge(t):
    # Slow, tide-unrelated surge so the true residual is not a constant.
    return 0.3 + 0.2 * math.sin(2 * math.pi * (t - T0) / (40 * 3600))


def observed(hours=30):
    return {t: tide(t) + surge(t) for t in range(T0, T0 + hours * 3600, 60)}


def predicted(stamp_error_s=0, hours=30):
    """20-minute predictions, each stamped ``stamp_error_s`` off its true time."""
    return {t - stamp_error_s: tide(t) for t in range(T0, T0 + hours * 3600, 1200)}


@pytest.mark.parametrize("error_h", [-1, 0, 1])
def test_detects_a_whole_hour_clock_error(error_h):
    shift, spreads = sr.detect_prediction_shift(observed(), predicted(error_h * 3600))
    assert shift == error_h * 3600
    assert spreads[shift] == min(spreads.values())


def test_the_2026_11_case_stamps_predictions_an_hour_early():
    # Observed in UTC-7, predicted in UTC-8 read as UTC-7: each prediction
    # lands an hour before its true time, so the fix adds an hour.
    shift, _ = sr.detect_prediction_shift(observed(), predicted(3600))
    assert shift == 3600


def test_too_little_overlap_is_undecided_not_zero():
    shift, _ = sr.detect_prediction_shift(observed(hours=6), predicted())
    assert shift is None


def test_a_small_nonhour_lag_is_not_corrected():
    # A 10-minute physical lag is not a clock error and must not be "fixed".
    shift, _ = sr.detect_prediction_shift(observed(), predicted(600))
    assert shift == 0


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE tide_observation (station_id, observation_time, water_level)")
    c.execute("CREATE TABLE tide_prediction (station_id, prediction_time, water_level)")
    c.execute("CREATE TABLE surrey_geodetic_data (station_id, observation_time, tidal_residual)")
    c.executemany("INSERT INTO tide_observation VALUES ('s', ?, ?)", observed().items())
    c.executemany("INSERT INTO tide_prediction VALUES ('s', ?, ?)", predicted().items())
    return c


def test_residual_is_the_surge_with_interpolated_predictions(conn):
    rows = sr.residual_series(conn, "s", T0, T0 + 6 * 3600)
    assert len(rows) > 300
    # Linear interpolation of a 4 m tide over 20 minutes errs < 1 cm.
    assert max(abs(r - surge(t)) for t, r in rows) < 0.01


def test_residual_skips_gaps_in_the_predictions(conn):
    conn.execute("DELETE FROM tide_prediction WHERE prediction_time BETWEEN ? AND ?", (T0 + 3600, T0 + 3 * 3600))
    times = {t for t, _ in sr.residual_series(conn, "s", T0, T0 + 6 * 3600)}
    assert T0 + 2 * 3600 not in times
    assert T0 + 5 * 3600 in times


def test_compare_with_surrey_reports_their_offset(conn):
    # Surrey's residual computed from hour-early predictions vs ours.
    conn.executemany(
        "INSERT INTO surrey_geodetic_data VALUES ('s', ?, ?)",
        [(t, tide(t) + surge(t) - tide(t + 3600)) for t in range(T0, T0 + 24 * 3600, 1200)],
    )
    n, _, max_diff = sr.compare_with_surrey(conn, "s", T0)
    assert n > 50
    assert max_diff > 0.5
