"""Tests for lib/lightstation_schedule.py — inferring publishing cycles."""

from datetime import datetime, timedelta, timezone

from lib.lightstation_schedule import (
    DEFAULT_STALE_HOURS,
    infer_schedule,
    staleness_threshold_hours,
)

DAYS = 10


def _series(slots, days=DAYS, skip=()):
    """Build epoch timestamps for `slots` ("HH:MM" UTC) repeated over `days`.

    `skip` drops (day_index, slot) pairs, to model a station that misses one.
    """
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    out = []
    for day in range(days):
        for slot in slots:
            if (day, slot) in skip:
                continue
            hour, minute = (int(x) for x in slot.split(":"))
            moment = start + timedelta(days=day, hours=hour, minutes=minute)
            out.append(int(moment.timestamp()))
    return out


class TestEvenCycles:
    def test_three_hourly_cycle_is_recognised(self):
        slots = [f"{h:02d}:10" for h in range(0, 24, 3)]
        s = infer_schedule(_series(slots))
        assert s["reports_per_day"] == 8
        assert s["interval_hours"] == 3
        assert s["longest_gap_hours"] == 3
        assert s["confident"] is True

    def test_offset_cycle_keeps_its_minutes(self):
        """The :40 bulletin family must not be rounded onto the :10 family."""
        slots = [f"{h:02d}:40" for h in range(2, 24, 3)]
        s = infer_schedule(_series(slots))
        assert s["slots_utc"] == slots
        assert s["interval_hours"] == 3


class TestUnevenCycles:
    def test_daytime_only_station_is_not_called_evenly_spaced(self):
        """Cape Mudge's real shape: four reports, then a 15-hour silence."""
        s = infer_schedule(_series(["00:10", "15:10", "18:10", "21:10"]))
        assert s["reports_per_day"] == 4
        assert s["interval_hours"] is None, "4 reports in 9h then nothing is not 'every 3h'"
        assert s["longest_gap_hours"] == 15

    def test_overnight_gap_shows_as_the_longest_wait(self):
        """The 08:40 slot is never published, so 05:40 → 11:40 is a 6h wait."""
        slots = [f"{h:02d}:40" for h in (2, 5, 11, 14, 17, 20, 23)]
        s = infer_schedule(_series(slots))
        assert s["reports_per_day"] == 7
        assert s["interval_hours"] is None
        assert s["longest_gap_hours"] == 6


class TestRobustness:
    def test_stray_report_does_not_become_a_slot(self):
        slots = [f"{h:02d}:10" for h in range(0, 24, 3)]
        times = _series(slots)
        times.append(int(datetime(2026, 8, 3, 7, 22, tzinfo=timezone.utc).timestamp()))
        s = infer_schedule(times)
        assert "07:20" not in s["slots_utc"]
        assert s["reports_per_day"] == 8

    def test_near_miss_minutes_round_into_their_slot(self):
        """A 14:38 and a 14:41 are both the 14:40 slot, not two new ones."""
        start = datetime(2026, 8, 1, 14, tzinfo=timezone.utc)
        times = [int((start + timedelta(days=d, minutes=m)).timestamp()) for d in range(DAYS) for m in (38, 41)]
        s = infer_schedule(times)
        assert s["slots_utc"] == ["14:40"]

    def test_short_history_is_not_confident(self):
        slots = [f"{h:02d}:10" for h in range(0, 24, 3)]
        s = infer_schedule(_series(slots, days=1))
        assert s["confident"] is False

    def test_intermittent_station_establishes_no_slots(self):
        """Chrome Island: a handful of reports at scattered slots."""
        times = _series(["02:40"], days=10, skip=[(d, "02:40") for d in range(2, 10)])
        times += _series(["17:40"], days=10, skip=[(d, "17:40") for d in range(2, 10)])
        s = infer_schedule(times)
        assert s["confident"] is False

    def test_no_observations_returns_none(self):
        assert infer_schedule([]) is None


class TestRetentionSupportsInference:
    """The schedule is only as good as the history the database keeps."""

    def test_lookback_fits_inside_retention(self):
        from lib.config import LIGHTSTATION_RETENTION_DAYS
        from scripts.export.export_lightstation_json import SCHEDULE_LOOKBACK_DAYS

        assert (
            SCHEDULE_LOOKBACK_DAYS <= LIGHTSTATION_RETENTION_DAYS
        ), "the schedule inference asks for more history than the purge keeps"

    def test_retention_covers_a_twice_daily_station(self):
        """Chrome Island reports ~1.6x/day; a slot needs 60% of days to count.

        Under about two weeks there are too few samples for such a station to
        establish any slot, and the page falls back to calling it irregular.
        """
        from lib.config import LIGHTSTATION_RETENTION_DAYS

        assert LIGHTSTATION_RETENTION_DAYS >= 14


class TestStalenessThreshold:
    """Staleness measured against a station's own cadence.

    The flat 12 h this replaced was wrong at both ends. Cape Mudge, Chatham
    Point and Pulteney Point report four times a day in daylight only and are
    normally silent for 15 h overnight, so they were reported stale every
    night for behaving normally — 10 such gaps each in the 30 days to
    2026-09-06. Meanwhile the seven-a-day stations, whose longest normal gap
    is 6 h, got two full missed cycles before anything was said.
    """

    def test_regular_station_is_flagged_sooner_than_the_old_flat_value(self):
        threshold = staleness_threshold_hours({"confident": True, "longest_gap_hours": 6.0})
        assert threshold == 9.0
        assert threshold < DEFAULT_STALE_HOURS

    def test_daylight_only_station_survives_its_own_overnight_gap(self):
        """The bug this exists to prevent: 15 h of normal silence read as a fault."""
        threshold = staleness_threshold_hours({"confident": True, "longest_gap_hours": 15.0})
        assert threshold == 18.0
        assert 15.0 < threshold, "a normal overnight gap must not read as stale"

    def test_grace_is_one_reporting_cycle_not_a_multiplier(self):
        """A fixed grace, so a slow station does not get a proportionally huge
        allowance — 15 h + 3 h, not 15 h x 2."""
        slow = staleness_threshold_hours({"confident": True, "longest_gap_hours": 15.0})
        fast = staleness_threshold_hours({"confident": True, "longest_gap_hours": 6.0})
        assert slow - 15.0 == fast - 6.0 == 3.0

    def test_unconfident_schedule_falls_back_to_the_flat_value(self):
        for schedule in (
            None,
            {},
            {"confident": False, "longest_gap_hours": 15.0},
            {"confident": True, "longest_gap_hours": None},
        ):
            assert staleness_threshold_hours(schedule) == DEFAULT_STALE_HOURS

    def test_threshold_derived_from_a_real_inferred_schedule(self):
        """End to end from timestamps, not from a hand-built dict: a station on
        the 4x daily daylight cycle must come out above its own longest gap."""
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        stamps = []
        for day in range(14):
            for hour in (15, 18, 21, 24):  # 24 wraps to 00:10 the next day
                stamps.append(int((start + timedelta(days=day, hours=hour, minutes=10)).timestamp()))
        schedule = infer_schedule(stamps)
        assert schedule["reports_per_day"] == 4
        threshold = staleness_threshold_hours(schedule)
        assert threshold > schedule["longest_gap_hours"]
