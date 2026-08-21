"""Tests for lightstation report parsing, especially stale retransmission filtering."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.parse.parse_lightstation import (
    extract_observation_day,
    is_stale_retransmission,
    parse_report_file,
    parse_station_entry,
)

# ── Sample report content ──────────────────────────────────────

# March 30, 2026 is a Monday
CURRENT_REPORT = """\
FPCN61 CWVR 301510
CURRENT OBSERVATIONS FROM B.C. LIGHTHOUSES AND BUOYS.

STRAIT OF GEORGIA.
8 AM Monday
CAPE MUDGE. ESTIMATED WIND NORTHWEST 12 KNOTS. SEAS 2 FOOT CHOP.
MERRY ISLAND. ESTIMATED WIND NORTH 3 KNOTS. SEAS RIPPLED.

JUAN DE FUCA STRAIT.
8 AM Monday
TRIAL ISLAND. ESTIMATED WIND NORTH 7 KNOTS. SEAS RIPPLED.
"""

# Same header timestamp but observation is from Friday (stale retransmission)
STALE_REPORT = """\
FPCN61 CWVR 301510
CURRENT OBSERVATIONS FROM B.C. LIGHTHOUSES AND BUOYS.

STRAIT OF GEORGIA.
7 AM Friday
CAPE MUDGE. ESTIMATED WIND SOUTHEAST 30 KNOTS AND GUSTING. SEAS 5 FEET MODERATE.
MERRY ISLAND. ESTIMATED WIND SOUTHEAST 30 KNOTS AND GUSTING. SEAS 6 FEET MODERATE.

JUAN DE FUCA STRAIT.
7 AM Friday
"""

# Report with no observation time line (edge case — should still parse)
NO_TIME_REPORT = """\
FPCN61 CWVR 301510
CURRENT OBSERVATIONS FROM B.C. LIGHTHOUSES AND BUOYS.

STRAIT OF GEORGIA.
CAPE MUDGE. ESTIMATED WIND NORTHWEST 12 KNOTS. SEAS 2 FOOT CHOP.
"""


# ── extract_observation_day ────────────────────────────────────


class TestExtractObservationDay:
    @pytest.mark.parametrize(
        "line, expected",
        [
            ("8 AM Monday", "Monday"),
            ("4 PM Sunday", "Sunday"),
            ("11 AM Wednesday", "Wednesday"),
            ("5 PM Saturday", "Saturday"),
            ("10 AM Tuesday", "Tuesday"),
            ("7 AM Friday", "Friday"),
            ("1 PM Thursday", "Thursday"),
        ],
    )
    def test_standard_formats(self, line, expected):
        assert extract_observation_day(line) == expected

    @pytest.mark.parametrize(
        "line",
        [
            "STRAIT OF GEORGIA.",
            "",
            "FPCN61 CWVR 301510",
            "CURRENT OBSERVATIONS FROM B.C. LIGHTHOUSES AND BUOYS.",
            "CAPE MUDGE. ESTIMATED WIND NORTHWEST 12 KNOTS.",
        ],
    )
    def test_returns_none_for_non_matching(self, line):
        assert extract_observation_day(line) is None


# ── is_stale_retransmission ────────────────────────────────────


class TestIsStaleRetransmission:
    # March 30, 2026 is a Monday
    ref_time = datetime(2026, 3, 30, 16, 0, tzinfo=timezone.utc)

    def test_matching_day_is_current(self):
        result = is_stale_retransmission(
            "FPCN61 CWVR 301510", "8 AM Monday", self.ref_time
        )
        assert result is False

    def test_mismatched_day_is_stale(self):
        result = is_stale_retransmission(
            "FPCN61 CWVR 301510", "7 AM Friday", self.ref_time
        )
        assert result is True

    def test_mismatched_day_saturday(self):
        result = is_stale_retransmission(
            "FPCN61 CWVR 301510", "7 AM Saturday", self.ref_time
        )
        assert result is True

    def test_mismatched_day_sunday(self):
        result = is_stale_retransmission(
            "FPCN61 CWVR 301510", "7 AM Sunday", self.ref_time
        )
        assert result is True

    def test_unparseable_header_returns_none(self):
        result = is_stale_retransmission(
            "GARBAGE HEADER", "8 AM Monday", self.ref_time
        )
        assert result is None

    def test_unparseable_observation_returns_none(self):
        result = is_stale_retransmission(
            "FPCN61 CWVR 301510", "STRAIT OF GEORGIA.", self.ref_time
        )
        assert result is None

    def test_month_boundary(self):
        """Day 31 when reference is early in a month that doesn't have 31 days."""
        # Reference: April 1, 2026 (Wednesday). Day 31 is March 31, and 00:10
        # UTC on it is 5 PM PDT on March 30 — a Monday.
        ref = datetime(2026, 4, 1, 2, 0, tzinfo=timezone.utc)
        result = is_stale_retransmission(
            "FPCN61 CWVR 310010", "5 PM Monday", ref
        )
        assert result is False

    # The header stamp is UTC; the observation line names a local day. The
    # 00/03/06 UTC bulletins are 5/8/11 PM the *previous* day in Pacific time,
    # and comparing the raw UTC day name rejected all three of them, every day
    # — 3 of the 8 daily bulletins, silently. Real pairings, taken from
    # bulletins on disk 2026-08-20/21.
    @pytest.mark.parametrize(
        "header,report_time",
        [
            ("FPCN61 CWVR 200010", "5 PM Wednesday"),
            ("FPCN61 CWVR 200310", "8 PM Wednesday"),
            ("FPCN61 CWVR 200610", "11 PM Wednesday"),
            ("FPCN61 CWVR 201210", "5 AM Thursday"),
            ("FPCN61 CWVR 201510", "8 AM Thursday"),
            ("FPCN61 CWVR 201810", "11 AM Thursday"),
            ("FPCN61 CWVR 202110", "2 PM Thursday"),
            ("FPCN61 CWVR 210010", "5 PM Thursday"),
        ],
    )
    def test_every_daily_slot_is_accepted(self, header, report_time):
        ref = datetime(2026, 8, 21, 1, 0, tzinfo=timezone.utc)
        assert is_stale_retransmission(header, report_time, ref) is False

    def test_evening_slot_still_catches_a_real_retransmission(self):
        """The 03Z fix must not blind the guard: wrong local day is still stale."""
        ref = datetime(2026, 8, 21, 1, 0, tzinfo=timezone.utc)
        # 200310 is 8 PM Wednesday locally, so a Thursday line is a mismatch.
        assert is_stale_retransmission("FPCN61 CWVR 200310", "8 PM Thursday", ref) is True


# ── parse_station_entry ────────────────────────────────────────


class TestParseStationEntry:
    def test_wind_and_seas(self):
        line = "MERRY ISLAND. ESTIMATED WIND NORTH 3 KNOTS. SEAS RIPPLED."
        result = parse_station_entry(line, "STRAIT OF GEORGIA")
        assert result["station_name"] == "MERRY ISLAND"
        assert result["wind_direction"] == "NORTH"
        assert result["wind_speed_kt"] == 3.0
        assert result["wind_estimated"] == 1
        assert result["wind_gusting"] == 0
        assert result["sea_condition"] == "RIPPLED"
        assert result["sea_height_ft"] == 0

    def test_gusting_with_moderate_seas(self):
        line = "CAPE MUDGE. ESTIMATED WIND SOUTHEAST 30 KNOTS AND GUSTING. SEAS 5 FEET MODERATE."
        result = parse_station_entry(line, "STRAIT OF GEORGIA")
        assert result["wind_direction"] == "SOUTHEAST"
        assert result["wind_speed_kt"] == 30.0
        assert result["wind_gusting"] == 1
        assert result["sea_height_ft"] == 5.0
        assert result["sea_condition"] == "MODERATE"

    def test_wind_calm(self):
        line = "BOAT BLUFF. WIND CALM. SEAS RIPPLED."
        result = parse_station_entry(line, "CENTRAL COAST")
        assert result["wind_calm"] == 1
        assert result["wind_speed_kt"] is None
        assert result["wind_direction"] is None

    def test_swell(self):
        line = "LANGARA ISLAND. WIND SOUTH 7 KNOTS. SEAS RIPPLED. LOW WESTERLY SWELL."
        result = parse_station_entry(line, "HECATE STRAIT")
        assert result["swell_intensity"] == "LOW"
        assert result["swell_direction"] == "WESTERLY"

    def test_non_station_line_returns_none(self):
        # Lowercase lines and time lines are not station entries
        assert parse_station_entry("8 AM Monday", "STRAIT OF GEORGIA") is None
        assert parse_station_entry("some lowercase text.", "STRAIT OF GEORGIA") is None


# ── parse_report_file (integration) ───────────────────────────


class TestParseReportFile:
    def test_current_report_parsed(self, tmp_path, monkeypatch):
        """A report whose day-of-week matches the header date is parsed."""
        report_file = tmp_path / "FPCN61_CWVR_301510___00001"
        report_file.write_text(CURRENT_REPORT)

        # Freeze time to March 30, 2026 (Monday)
        monkeypatch.setattr(
            "scripts.parse.parse_lightstation.datetime",
            type("FakeDatetime", (datetime,), {
                "now": classmethod(lambda cls, tz=None: datetime(2026, 3, 30, 16, 0, tzinfo=tz)),
            }),
        )

        observations = parse_report_file(report_file)
        assert len(observations) == 3  # CAPE MUDGE, MERRY ISLAND, TRIAL ISLAND
        station_names = {obs["station_name"] for obs in observations}
        assert "MERRY ISLAND" in station_names
        assert "CAPE MUDGE" in station_names

    def test_stale_retransmission_skipped(self, tmp_path, monkeypatch):
        """A report whose day-of-week doesn't match the header date is skipped."""
        report_file = tmp_path / "FPCN61_CWVR_301510___00002"
        report_file.write_text(STALE_REPORT)

        monkeypatch.setattr(
            "scripts.parse.parse_lightstation.datetime",
            type("FakeDatetime", (datetime,), {
                "now": classmethod(lambda cls, tz=None: datetime(2026, 3, 30, 16, 0, tzinfo=tz)),
            }),
        )

        observations = parse_report_file(report_file)
        assert len(observations) == 0

    def test_no_time_line_still_parsed(self, tmp_path, monkeypatch):
        """A report lacking an observation time line is still parsed (fail-open)."""
        report_file = tmp_path / "FPCN61_CWVR_301510___00003"
        report_file.write_text(NO_TIME_REPORT)

        monkeypatch.setattr(
            "scripts.parse.parse_lightstation.datetime",
            type("FakeDatetime", (datetime,), {
                "now": classmethod(lambda cls, tz=None: datetime(2026, 3, 30, 16, 0, tzinfo=tz)),
            }),
        )

        observations = parse_report_file(report_file)
        assert len(observations) >= 1
