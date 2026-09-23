"""Tests for lib/report_status.py: per-station late/down judged by its own rhythm."""

import sqlite3

from lib.report_status import (
    DOWN_FLOOR_SECONDS,
    LATE_FLOOR_SECONDS,
    LEGACY_RHYTHM,
    infer_rhythm,
    load_rhythms,
    report_status,
    rhythm_from_thresholds,
    status_fields,
)

H = 3600
M = 60


def hourly(n=48, delay=4 * M):
    """An EC-style station: one reading an hour, arriving `delay` after it."""
    return [(i * H, i * H + delay) for i in range(n)]


class TestInferRhythm:
    def test_hourly_ec_station(self):
        # The newest reading is 4 min old when it lands and 64 min old just
        # before the next one replaces it: the peak is cadence plus delay.
        r = infer_rhythm(hourly())
        assert r.cadence_seconds == H
        assert r.peak_age_seconds == H + 4 * M
        assert r.late_after_seconds == H + 4 * M + 2 * H
        assert r.down_after_seconds == DOWN_FLOOR_SECONDS

    def test_batched_delivery_uses_arrival_cadence_not_observation_spacing(self):
        # Ten-minute readings delivered in a batch every 20 minutes (NOAA,
        # Surrey). Observation spacing would say "late after 20 min"; the page
        # only changes every 20, so it must be judged on deliveries.
        rows = []
        for batch in range(60):
            arrive = batch * 20 * M + 70 * M
            for k in range(2):
                obs = batch * 20 * M + k * 10 * M
                rows.append((obs, arrive + k))  # same delivery, a second apart
        r = infer_rhythm(rows)
        assert r.cadence_seconds == 20 * M
        # Newest reading is batch*20+10 min; the next delivery lands at
        # (batch+1)*20 + 70 min: 80 min old.
        assert r.peak_age_seconds == 80 * M
        assert r.late_after_seconds == 80 * M + 40 * M

    def test_fast_station_is_floored_at_an_hour(self):
        rows = [(i * 5 * M, i * 5 * M + 30) for i in range(200)]
        r = infer_rhythm(rows)
        assert r.late_after_seconds == LATE_FLOOR_SECONDS

    def test_backfilled_older_rows_are_not_deliveries(self):
        # A late-arriving old reading never becomes the one on the page, so it
        # must neither count as a delivery nor reset the newest reading.
        rows = hourly()
        rows.append((5 * H, 40 * H))  # hour-5 reading re-sent at hour 40
        assert infer_rhythm(rows) == infer_rhythm(hourly())

    def test_rows_without_arrival_time_are_ignored(self):
        rows = hourly() + [(100 * H, None)]
        assert infer_rhythm(rows) == infer_rhythm(hourly())

    def test_too_little_history_falls_back_to_legacy(self):
        assert infer_rhythm(hourly(n=4)) is LEGACY_RHYTHM
        assert infer_rhythm([]) is LEGACY_RHYTHM

    def test_one_slow_delivery_does_not_set_the_bar(self):
        rows = hourly(n=100)
        rows[50] = (rows[50][0], rows[50][0] + 5 * H)  # one delivery 5 h late
        assert infer_rhythm(rows).peak_age_seconds == H + 4 * M


class TestSlowStations:
    def test_down_is_twice_late_when_late_is_long(self):
        # Daytime-only lightstations: stale (late) at 18 h. Down must stay
        # after late, not collapse into it at the 12 h floor.
        r = rhythm_from_thresholds(18 * H)
        assert r.late_after_seconds == 18 * H
        assert r.down_after_seconds == 36 * H

    def test_three_hourly_lightstation(self):
        r = rhythm_from_thresholds(9 * H)
        assert (r.late_after_seconds, r.down_after_seconds) == (9 * H, 18 * H)


class TestReportStatus:
    def test_bands(self):
        r = infer_rhythm(hourly())  # late 3h04, down 12h
        assert report_status(2 * H, r) == "ok"
        assert report_status(3 * H + 5 * M, r) == "late"
        assert report_status(12 * H + 1, r) == "down"

    def test_thresholds_are_exclusive(self):
        assert report_status(LEGACY_RHYTHM.late_after_seconds, LEGACY_RHYTHM) == "ok"
        assert report_status(LEGACY_RHYTHM.down_after_seconds, LEGACY_RHYTHM) == "late"

    def test_status_fields(self):
        assert status_fields(4 * H, LEGACY_RHYTHM) == {
            "status": "late",
            "late_after_minutes": 180,
            "down_after_minutes": 720,
        }


class TestLoadRhythms:
    def test_reads_recorded_at_text_and_groups_by_station(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE obs (station_id TEXT, observation_time INTEGER, recorded_at TEXT)")
        now = 1_790_000_000 - (1_790_000_000 % H)
        for i in range(24):
            t = now - (24 - i) * H
            arrived = sqlite3.connect(":memory:").execute("SELECT datetime(?, 'unixepoch')", (t + 4 * M,)).fetchone()[0]
            conn.execute("INSERT INTO obs VALUES ('A', ?, ?)", (t, arrived))
        conn.execute("INSERT INTO obs VALUES ('B', ?, NULL)", (now - H,))
        conn.execute("INSERT INTO obs VALUES ('A', ?, NULL)", (now + H,))  # future: ignored

        rhythms = load_rhythms(conn, "obs", "station_id", now)
        assert rhythms["A"].cadence_seconds == H
        assert rhythms["A"].late_after_seconds == H + 4 * M + 2 * H
        assert rhythms["B"] is LEGACY_RHYTHM
