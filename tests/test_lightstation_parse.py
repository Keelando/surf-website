"""Tests for lightstation report parsing, especially stale retransmission filtering."""

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.parse.parse_lightstation import (
    PAIR_OFFSET_MAX_SEC,
    extract_observation_day,
    insert_observations,
    is_stale_retransmission,
    parse_report_file,
    parse_station_entry,
    parse_sxcn_station_line,
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

    def test_seas_in_feet_singular(self):
        # EC writes FOOT for 1-3 ft; these are the majority of FPCN61 readings.
        line = "CHROME ISLAND. ESTIMATED WIND SOUTHEAST 16 KNOTS. SEAS 2 FOOT CHOP."
        result = parse_station_entry(line, "STRAIT OF GEORGIA")
        assert result["sea_height_ft"] == 2.0
        assert result["sea_condition"] == "CHOP"

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


# ── Cross-bulletin merge ───────────────────────────────────────


class TestCrossBulletinMerge:
    """One observation published in two bulletins must land in one row.

    Nine stations are carried by both FPCN61 and an SXCN bulletin. Each copy
    is timestamped from its own WMO header, so the pair sits 30 or 40 minutes
    apart -- far enough for the unique index to treat it as two readings.
    """

    OFFSETS = {"SXCN23": 40 * 60, "SXCN26": 30 * 60}

    @pytest.fixture
    def db(self, tmp_path, monkeypatch):
        """An empty lightstation database, built from the real schema."""
        from scripts.utils.create_lightstation_db import CREATE_INDEXES_SQL, CREATE_TABLE_SQL

        path = tmp_path / "lightstation_data.sqlite"
        conn = sqlite3.connect(path)
        conn.execute(CREATE_TABLE_SQL)
        for statement in CREATE_INDEXES_SQL:
            conn.execute(statement)
        conn.commit()
        conn.close()
        monkeypatch.setattr("scripts.parse.parse_lightstation.DB_PATH", path)
        return path

    def rows(self, db, station="MERRY ISLAND"):
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        found = conn.execute(
            "SELECT * FROM lightstation_observation WHERE station_name = ? ORDER BY observation_time",
            (station,),
        ).fetchall()
        conn.close()
        return found

    def observation(self, source, when, station="MERRY ISLAND", **overrides):
        obs = {
            "station_name": station,
            "region": "STRAIT OF GEORGIA",
            "observation_time": when,
            "report_time_str": source,
            "wind_speed_kt": 11.0,
            "wind_direction": "SOUTHEAST",
            "wind_gusting": 0,
            "wind_calm": 0,
            "wind_estimated": 1,
            "sea_height_ft": 2.0,
            "sea_condition": "CHOP",
            "swell_intensity": None,
            "swell_direction": None,
            "source_file": source,
        }
        obs.update(overrides)
        return obs

    # SXCN26 03/2040Z and FPCN61 03/2110Z, the pair from the bug report.
    SXCN_TIME = 1757_020_800  # arbitrary, only the 30-minute offset matters
    FPCN_TIME = SXCN_TIME + 30 * 60

    def test_sxcn_then_fpcn61_makes_one_row(self, db):
        """The usual arrival order: the coded bulletin lands first."""
        insert_observations([self.observation("SXCN26_CWVR_032040___1", self.SXCN_TIME)])
        insert_observations(
            [self.observation("FPCN61_CWVR_032110___2", self.FPCN_TIME, wind_gusting=1, sea_height_ft=None)]
        )

        rows = self.rows(db)
        assert len(rows) == 1
        assert rows[0]["observation_time"] == self.SXCN_TIME, "SXCN is authoritative for time"
        assert rows[0]["wind_gusting"] == 1, "only FPCN61 can say gusting"
        assert rows[0]["sea_height_ft"] == 2.0, "the SXCN value survives a null"
        assert "SXCN26" in rows[0]["source_file"] and "FPCN61" in rows[0]["source_file"]

    def test_fpcn61_then_sxcn_makes_one_row(self, db):
        """The reverse order -- a re-parse, or a delayed SXCN -- collapses too."""
        insert_observations([self.observation("FPCN61_CWVR_032110___2", self.FPCN_TIME, wind_gusting=1)])
        insert_observations([self.observation("SXCN26_CWVR_032040___1", self.SXCN_TIME)])

        rows = self.rows(db)
        assert len(rows) == 1
        assert rows[0]["observation_time"] == self.SXCN_TIME
        assert rows[0]["wind_gusting"] == 1

    def test_forty_minute_offset_merges(self, db):
        """SXCN23 sits at HH:30 against FPCN61's HH:10 -- 40 minutes, not 30."""
        insert_observations([self.observation("SXCN23_CWVR_061130___1", self.SXCN_TIME, station="BOAT BLUFF")])
        insert_observations(
            [self.observation("FPCN61_CWVR_061210___2", self.SXCN_TIME + 40 * 60, station="BOAT BLUFF")]
        )
        assert len(self.rows(db, "BOAT BLUFF")) == 1

    def test_special_report_stays_separate(self, db):
        """An off-cycle SPECIAL is a real second observation, not a re-publication.

        Addenbroke, 2026-09-06: SXCN23 at 11:30, FPCN61 at 12:10, then a Coast
        Guard SPECIAL at 12:30. A plain 60-minute tolerance would swallow the
        special into the FPCN61 row; the offset band must not.
        """
        base = self.SXCN_TIME
        insert_observations([self.observation("SXCN23_CWVR_061130___1", base, station="ADDENBROKE ISLAND")])
        insert_observations(
            [self.observation("FPCN61_CWVR_061210___2", base + 40 * 60, station="ADDENBROKE ISLAND")]
        )
        insert_observations(
            [self.observation("SXCN23_CWVR_061230___3", base + 60 * 60, station="ADDENBROKE ISLAND")]
        )

        rows = self.rows(db, "ADDENBROKE ISLAND")
        assert len(rows) == 2
        assert [r["observation_time"] for r in rows] == [base, base + 60 * 60]

    def test_other_stations_are_not_paired(self, db):
        """The window only ever collapses one station's own two copies."""
        insert_observations([self.observation("SXCN26_CWVR_032040___1", self.SXCN_TIME, station="MERRY ISLAND")])
        insert_observations([self.observation("FPCN61_CWVR_032110___2", self.FPCN_TIME, station="CHROME ISLAND")])
        assert len(self.rows(db, "MERRY ISLAND")) == 1
        assert len(self.rows(db, "CHROME ISLAND")) == 1

    def test_region_is_left_alone(self, db):
        """Region belongs to the station, so a merge must not reassign it.

        The two products disagree -- SXCN26 files Trial Island under the
        Strait of Georgia, FPCN61 under Juan de Fuca -- and letting the merge
        pick would shuffle the station between groups on the page whenever a
        second copy landed.
        """
        insert_observations(
            [
                self.observation(
                    "SXCN26_CWVR_032040___1",
                    self.SXCN_TIME,
                    station="TRIAL ISLAND",
                    region="STRAIT OF GEORGIA",
                )
            ]
        )
        insert_observations(
            [
                self.observation(
                    "FPCN61_CWVR_032110___2",
                    self.FPCN_TIME,
                    station="TRIAL ISLAND",
                    region="JUAN DE FUCA STRAIT",
                )
            ]
        )
        rows = self.rows(db, "TRIAL ISLAND")
        assert len(rows) == 1
        assert rows[0]["region"] == "STRAIT OF GEORGIA"

    def test_reparsing_the_same_files_is_idempotent(self, db):
        """Re-running the parser over retained bulletins must not re-split the pair."""
        sxcn = self.observation("SXCN26_CWVR_032040___1", self.SXCN_TIME)
        fpcn = self.observation("FPCN61_CWVR_032110___2", self.FPCN_TIME, wind_gusting=1)
        for _ in range(3):
            insert_observations([sxcn])
            insert_observations([fpcn])

        rows = self.rows(db)
        assert len(rows) == 1
        assert rows[0]["source_file"].count("FPCN61") == 1

    def test_no_station_holds_two_copies_of_one_reading(self, db):
        """The invariant, asserted over a batch rather than a single pair."""
        base = self.SXCN_TIME
        batch = []
        for step in range(4):
            when = base + step * 3 * 3600
            batch.append(self.observation("SXCN26_CWVR_0000___%d" % step, when))
            batch.append(self.observation("FPCN61_CWVR_0000___%d" % step, when + 30 * 60))
        insert_observations(batch)

        times = [row["observation_time"] for row in self.rows(db)]
        assert len(times) == 4
        gaps = [b - a for a, b in zip(times, times[1:])]
        assert all(gap >= PAIR_OFFSET_MAX_SEC for gap in gaps), gaps


# ── SXCN24 station names, as the bulletin actually writes them ────────


class TestSxcn24Names:
    """Lines copied from SXCN24 CWVR bulletins of 2026-09-22/23."""

    @pytest.mark.parametrize(
        "line, station",
        [
            ("CAPE MUDGE    CLDY 15 NW08E 1FT CHP", "CAPE MUDGE"),
            ("PINE ISLAND   OVC 05F SE18E 5FT MOD LO-MDT W", "PINE ISLAND"),
            ("CHATHAM       PC 15 NW10E 1FT CHP", "CHATHAM POINT"),
            ("PULTENEY      OVC 15 E05E RPLD", "PULTENEY POINT"),
            ("SCARLETT      OVC 10 SE13E 3FT MOD LO NW VSBY SOUTH 02F", "SCARLETT POINT"),
            ("CAPE SCOTT    CLDY 15 SE15EG 4FT MOD LO SW", "CAPE SCOTT"),
            ("QUATSINO      PC 15 NE12E 2FT CHP MDT SW", "QUATSINO"),
        ],
    )
    def test_every_sxcn24_station_maps(self, line, station):
        data = parse_sxcn_station_line(line, "CENTRAL COAST")
        assert data is not None
        assert data["station_name"] == station

    def test_pine_island_reading(self):
        data = parse_sxcn_station_line("PINE ISLAND   OVC 05F SE18E 5FT MOD LO-MDT W", "CENTRAL COAST")
        assert data["wind_direction"] == "SOUTHEAST"
        assert data["wind_speed_kt"] == 18.0
        assert data["sea_height_ft"] == 5

    @pytest.mark.parametrize("text", ["N/A", "NA", "UNAVAILABLE"])
    def test_egg_island_unavailable_spellings_store_nothing(self, text):
        assert parse_sxcn_station_line(f"EGG ISLAND    {text}", "CENTRAL COAST") is None
