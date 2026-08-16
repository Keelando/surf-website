"""Tests for lib/forecast_steps.py — the shared time-step taper."""

from datetime import datetime, timedelta, timezone

from lib.forecast_steps import taper_time_steps

RUN = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


def hourly(count):
    return [RUN + timedelta(hours=h) for h in range(count)]


def leads(steps):
    return [int((s - RUN).total_seconds() // 3600) for s in steps]


def test_empty_input():
    assert taper_time_steps([], 24, 3) == []


def test_fine_window_kept_hourly():
    kept = leads(taper_time_steps(hourly(49), 24, 3))
    assert kept[:25] == list(range(25))


def test_coarse_tail_spacing():
    kept = leads(taper_time_steps(hourly(49), 24, 3))
    assert kept[24:] == [24, 27, 30, 33, 36, 39, 42, 45, 48]


def test_wave_shape_step_count():
    """RDWPS: 49 hourly steps, hourly to 24 h then 3-hourly → 33 steps."""
    assert len(taper_time_steps(hourly(49), 24, 3)) == 33


def test_surge_shape_step_count():
    """GDSPS: 241 hourly steps, hourly to 72 h then 3-hourly → 129 steps."""
    assert len(taper_time_steps(hourly(241), 72, 3)) == 129


def test_horizon_preserved():
    """The last published step survives even when it is off the coarse grid."""
    steps = hourly(50)  # lead 49 is not a multiple of 3
    kept = taper_time_steps(steps, 24, 3)
    assert kept[-1] == steps[-1]
    assert leads(kept)[-2:] == [48, 49]


def test_taper_is_relative_to_run_start_not_wall_clock():
    """A late fetch (steps starting mid-run) tapers off the first step."""
    late = [RUN + timedelta(hours=h) for h in range(5, 54)]
    kept = taper_time_steps(late, 24, 3)
    assert kept[0] == late[0]
    assert (kept[25] - late[0]) == timedelta(hours=27)
