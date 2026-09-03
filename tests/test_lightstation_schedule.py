"""Tests for lib/lightstation_schedule.py — inferring publishing cycles."""

from datetime import datetime, timedelta, timezone

from lib.lightstation_schedule import infer_schedule

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
