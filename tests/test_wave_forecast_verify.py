"""Tests for scripts/monitoring/verify_wave_forecast.py — the verification writer.

This script turns the forecast archive into a skill record, and every way it
can go wrong is silent: the numbers still come out, they are just not measuring
what they claim. So the tests target the claims rather than the plumbing:

- **Direction errors are circular.** A 010° forecast against a 350° observation
  is a 20° error, not -340°. Northerlies straddle 0° here constantly, and one
  mishandled pair dominates an RMSE.
- **Nearest means nearest reading of that variable**, not nearest row. CRPILE
  reports wind every 10 minutes and (say) waves less often; matching a wave
  forecast to a row whose wave column is NULL would read as a data gap.
- **A missing observation writes nothing** rather than a row with a hole in it,
  so the pair is retried while it is still inside the lookback window.
- **A written pair is never revised.** A later buoy backfill must not rewrite
  a past score.
- **The event gate catches false alarms and misses**, not just rows where the
  observation was large — gating on one side alone flatters the model exactly
  where it failed.
- **Unverifiable variables are skipped, not scored.** `wind_wave_height` has no
  instrument behind it at either station.

Both databases are temporary; no network, no real data.
"""

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.directions import circular_difference
from scripts.fetch import fetch_wave_forecast as wf
from scripts.monitoring import verify_wave_forecast as vf

UTC = timezone.utc

# 2026-08-16 18:00Z run, same anchor as test_wave_forecast.py.
RUN_ISO = "2026-08-16T18:00:00Z"
RUN_EPOCH = 1786903200
HOUR = 3600

# A valid time 24 h past the run, and a "now" well after it so the pair is
# outside the settle window and inside the lookback window.
VALID_EPOCH = RUN_EPOCH + 24 * HOUR
NOW = VALID_EPOCH + 6 * HOUR

STATION = "4600146"


def dt(iso):
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


@pytest.fixture
def forecast_db(tmp_path, monkeypatch):
    """An empty forecast archive with the real schema."""
    path = tmp_path / "wave_forecast.sqlite"
    monkeypatch.setattr(vf, "WAVE_FORECAST_DATABASE", path)
    conn = sqlite3.connect(path)
    wf.ensure_db_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def buoy_db(tmp_path, monkeypatch):
    """A minimal buoy_observation table with the columns we verify against."""
    path = tmp_path / "buoy_data.sqlite"
    monkeypatch.setattr(vf, "BUOY_DATABASE", path)
    columns = sorted({c for c in vf.OBSERVATION_COLUMNS.values() if c})
    conn = sqlite3.connect(path)
    conn.execute(f"""
        CREATE TABLE buoy_observation (
            buoy_id TEXT NOT NULL,
            observation_time INTEGER NOT NULL,
            {", ".join(f"{c} REAL" for c in columns)}
        )
    """)
    conn.commit()
    conn.close()
    return path


def add_forecast(conn, variable="wave_height", value=0.9, run=RUN_EPOCH, valid=VALID_EPOCH,
                 status="ok", station=STATION):
    conn.execute(
        """
        INSERT OR REPLACE INTO wave_forecast
        (station_id, variable, forecast_run_time, valid_time, value, status, model)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (station, variable, run, valid, value, status, wf.MODEL_NAME),
    )
    conn.commit()


def add_observation(path, when, station=STATION, **values):
    columns = ["buoy_id", "observation_time", *values]
    placeholders = ", ".join("?" * len(columns))
    conn = sqlite3.connect(path)
    conn.execute(
        f"INSERT INTO buoy_observation ({', '.join(columns)}) VALUES ({placeholders})",
        (station, when, *values.values()),
    )
    conn.commit()
    conn.close()


def pairs(conn):
    return conn.execute(
        "SELECT station_id, variable, lead_hours, forecast_value, observed_value,"
        " reference_value, obs_offset_seconds, model FROM wave_forecast_verification"
    ).fetchall()


# ── circular difference ─────────────────────────────────────────────


class TestCircularDifference:
    @pytest.mark.parametrize(
        "forecast,observed,expected",
        [
            (10, 350, 20),  # the wrap case plain subtraction gets 340° wrong
            (350, 10, -20),
            (270, 265, 5),
            (0, 0, 0),
            (180, 0, 180),  # exact reversal pinned positive, not -180
            (0, 180, 180),
            (365, 5, 0),  # out-of-range input still normalises
        ],
    )
    def test_smallest_arc(self, forecast, observed, expected):
        assert circular_difference(forecast, observed) == pytest.approx(expected)

    @pytest.mark.parametrize("bad", [None, "north", ""])
    def test_missing_or_nonnumeric_is_none(self, bad):
        assert circular_difference(bad, 180) is None
        assert circular_difference(180, bad) is None

    def test_error_of_uses_it_for_bearings_only(self):
        """A 20° arc, versus a plain subtraction for a scalar."""
        assert vf.error_of("wind_direction", 10, 350) == pytest.approx(20)
        assert vf.error_of("wave_direction", 10, 350) == pytest.approx(20)
        assert vf.error_of("peak_period", 10, 350) == pytest.approx(-340)


# ── observation matching ────────────────────────────────────────────


class TestObservationSeries:
    def test_nearest_prefers_the_closest_reading_either_side(self):
        series = vf.ObservationSeries(
            [(100, 1.0), (200, 2.0), (300, 3.0)], ["wave_height_sig"]
        )
        assert series.nearest("wave_height_sig", 190, 60) == (2.0, 10)
        assert series.nearest("wave_height_sig", 210, 60) == (2.0, -10)

    def test_offset_is_signed(self):
        """A systematic reporting lag should show as a consistent sign."""
        series = vf.ObservationSeries([(1000, 5.0)], ["wave_height_sig"])
        assert series.nearest("wave_height_sig", 700, 600)[1] == 300

    def test_nothing_inside_the_tolerance_is_no_match(self):
        series = vf.ObservationSeries([(100, 1.0)], ["wave_height_sig"])
        assert series.nearest("wave_height_sig", 5000, 1800) == (None, None)

    def test_nulls_are_skipped_per_column_not_per_row(self):
        """The nearest *row* has no wave value; the nearest *reading* is older."""
        series = vf.ObservationSeries(
            [(100, 1.0, 20.0), (195, None, 25.0)],
            ["wave_height_sig", "wind_speed"],
        )
        assert series.nearest("wave_height_sig", 200, 1800) == (1.0, -100)
        assert series.nearest("wind_speed", 200, 1800) == (25.0, -5)

    def test_duplicate_timestamps_keep_the_later_row(self):
        """buoy_observation has no uniqueness constraint; a re-parse can double up."""
        series = vf.ObservationSeries(
            [(100, 1.0), (100, 1.4)], ["wave_height_sig"]
        )
        assert series.nearest("wave_height_sig", 100, 60) == (1.4, 0)

    def test_empty_series_is_no_match(self):
        series = vf.ObservationSeries([], ["wave_height_sig"])
        assert series.nearest("wave_height_sig", 100, 1800) == (None, None)


# ── pairing ─────────────────────────────────────────────────────────


class TestWriteVerificationPairs:
    def test_pairs_forecast_observation_and_persistence_baseline(self, forecast_db, buoy_db):
        add_forecast(forecast_db, value=0.9)
        add_observation(buoy_db, VALID_EPOCH + 300, wave_height_sig=0.7)
        add_observation(buoy_db, RUN_EPOCH + 300, wave_height_sig=0.4)

        vf.write_verification_pairs(forecast_db, now=NOW)

        assert pairs(forecast_db) == [
            (STATION, "wave_height", 24, 0.9, 0.7, 0.4, 300, wf.MODEL_NAME)
        ]

    def test_lead_hours_is_derived_not_assumed(self, forecast_db, buoy_db):
        add_forecast(forecast_db, valid=RUN_EPOCH + 33 * HOUR)
        add_observation(buoy_db, RUN_EPOCH + 33 * HOUR, wave_height_sig=0.7)
        vf.write_verification_pairs(forecast_db, now=RUN_EPOCH + 40 * HOUR)
        assert pairs(forecast_db)[0][2] == 33

    def test_missing_observation_writes_nothing_and_stays_retryable(self, forecast_db, buoy_db):
        add_forecast(forecast_db)
        counts = vf.write_verification_pairs(forecast_db, now=NOW)

        assert pairs(forecast_db) == []
        assert counts["no_observation"] == 1

        # The observation lands later; the pair is still inside the window.
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=0.7)
        vf.write_verification_pairs(forecast_db, now=NOW)
        assert len(pairs(forecast_db)) == 1

    def test_a_written_pair_is_never_revised(self, forecast_db, buoy_db):
        """A late buoy backfill must not rewrite a past score."""
        add_forecast(forecast_db)
        add_observation(buoy_db, VALID_EPOCH + 600, wave_height_sig=0.7)
        vf.write_verification_pairs(forecast_db, now=NOW)

        # A closer, different reading arrives afterwards.
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=1.9)
        vf.write_verification_pairs(forecast_db, now=NOW)

        assert [row[4] for row in pairs(forecast_db)] == [0.7]

    def test_missing_baseline_still_writes_the_pair(self, forecast_db, buoy_db):
        """An outage at t0 costs the skill score, not the pair."""
        add_forecast(forecast_db)
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=0.7)
        counts = vf.write_verification_pairs(forecast_db, now=NOW)

        assert pairs(forecast_db)[0][5] is None
        assert counts["no_reference"] == 1

    def test_unverifiable_variable_is_skipped(self, forecast_db, buoy_db):
        """No instrument reports the wind-sea partition at either station."""
        add_forecast(forecast_db, variable="wind_wave_height", value=0.3)
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=0.7)
        counts = vf.write_verification_pairs(forecast_db, now=NOW)

        assert pairs(forecast_db) == []
        assert counts["unverifiable"] == 1

    def test_masked_steps_are_not_scored(self, forecast_db, buoy_db):
        """A masked step is the model declining to predict, not a prediction."""
        add_forecast(forecast_db, value=None, status="masked")
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=0.7)
        vf.write_verification_pairs(forecast_db, now=NOW)
        assert pairs(forecast_db) == []

    def test_valid_times_inside_the_settle_window_wait(self, forecast_db, buoy_db):
        add_forecast(forecast_db)
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=0.7)

        just_after = VALID_EPOCH + (vf.SETTLE_HOURS - 1) * HOUR
        vf.write_verification_pairs(forecast_db, now=just_after)
        assert pairs(forecast_db) == []

        vf.write_verification_pairs(forecast_db, now=VALID_EPOCH + (vf.SETTLE_HOURS + 1) * HOUR)
        assert len(pairs(forecast_db)) == 1

    def test_rows_older_than_the_lookback_are_abandoned(self, forecast_db, buoy_db):
        add_forecast(forecast_db)
        add_observation(buoy_db, VALID_EPOCH, wave_height_sig=0.7)
        stale_now = VALID_EPOCH + (vf.LOOKBACK_DAYS + 1) * 86400
        vf.write_verification_pairs(forecast_db, now=stale_now)
        assert pairs(forecast_db) == []

    def test_observation_outside_the_tolerance_is_not_a_match(self, forecast_db, buoy_db):
        add_forecast(forecast_db)
        add_observation(buoy_db, VALID_EPOCH + vf.MATCH_TOLERANCE_SECONDS + 60,
                        wave_height_sig=0.7)
        vf.write_verification_pairs(forecast_db, now=NOW)
        assert pairs(forecast_db) == []

    def test_stations_do_not_borrow_each_others_observations(self, forecast_db, buoy_db):
        add_forecast(forecast_db, station="CRPILE")
        add_observation(buoy_db, VALID_EPOCH, station=STATION, wave_height_sig=0.7)
        vf.write_verification_pairs(forecast_db, now=NOW)
        assert pairs(forecast_db) == []


# ── scoring ─────────────────────────────────────────────────────────


class TestScore:
    def test_bias_and_rmse(self):
        # errors: +0.2, -0.2, +0.2 → bias +0.0667, rmse 0.2
        stats = vf.score([(0.9, 0.7, None), (0.5, 0.7, None), (0.9, 0.7, None)], "wave_height")
        assert stats["n"] == 3
        assert stats["bias"] == pytest.approx(0.0667, abs=1e-3)
        assert stats["rmse"] == pytest.approx(0.2, abs=1e-9)
        assert stats["skill_vs_persistence"] is None

    def test_skill_is_positive_when_the_model_beats_persistence(self):
        # model off by 0.1, persistence off by 0.5 → 1 - 0.01/0.25 = 0.96
        stats = vf.score([(0.9, 1.0, 0.5)], "wave_height")
        assert stats["skill_vs_persistence"] == pytest.approx(0.96)

    def test_skill_is_negative_when_persistence_wins(self):
        stats = vf.score([(0.5, 1.0, 0.9)], "wave_height")
        assert stats["skill_vs_persistence"] < 0

    def test_skill_uses_only_pairs_that_have_a_baseline(self):
        """Otherwise it compares two different samples and calls it a ratio."""
        stats = vf.score([(0.9, 1.0, 0.5), (5.0, 1.0, None)], "wave_height")
        assert stats["n"] == 2
        assert stats["n_skill"] == 1
        assert stats["skill_vs_persistence"] == pytest.approx(0.96)

    def test_direction_scores_use_the_circular_error(self):
        """Plain subtraction would score this pair as a 340° miss."""
        stats = vf.score([(10, 350, None)], "wind_direction")
        assert stats["rmse"] == pytest.approx(20)

    def test_no_usable_pairs_is_none(self):
        assert vf.score([], "wave_height") is None
        assert vf.score([(None, 0.7, None)], "wave_height") is None


class TestEventGate:
    def test_either_side_crossing_counts(self):
        assert vf.is_event("wave_height", 0.7, 0.2) is True  # false alarm
        assert vf.is_event("wave_height", 0.2, 0.7) is True  # miss
        assert vf.is_event("wave_height", 0.7, 0.8) is True

    def test_both_sides_below_is_not_an_event(self):
        assert vf.is_event("wave_height", 0.2, 0.3) is False

    def test_ungated_variables_always_count(self):
        assert vf.is_event("wind_speed", 1.0, 2.0) is True
        assert vf.is_event("peak_period", 1.0, 2.0) is True


class TestSummarise:
    def _pair(self, conn, lead, forecast, observed, reference=None, variable="wave_height"):
        conn.execute(
            """
            INSERT INTO wave_forecast_verification
            (station_id, variable, forecast_run_time, valid_time, lead_hours,
             forecast_value, observed_value, reference_value, model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (STATION, variable, RUN_EPOCH, RUN_EPOCH + lead * HOUR, lead,
             forecast, observed, reference, wf.MODEL_NAME),
        )
        conn.commit()

    def test_groups_by_lead_bucket(self, forecast_db):
        self._pair(forecast_db, 3, 0.9, 0.7)
        self._pair(forecast_db, 24, 0.9, 0.7)
        results = vf.summarise(forecast_db)
        assert [r["lead_bucket"] for r in results] == ["0-6h", "13-24h"]

    def test_event_gate_changes_the_sample(self, forecast_db):
        self._pair(forecast_db, 3, 0.9, 0.7)  # event
        self._pair(forecast_db, 4, 0.2, 0.3)  # calm

        gated = vf.summarise(forecast_db, events_only=True)
        ungated = vf.summarise(forecast_db, events_only=False)
        assert gated[0]["n"] == 1
        assert ungated[0]["n"] == 2

    def test_leads_past_the_last_bucket_are_dropped(self, forecast_db):
        """The models publish 48 h; anything beyond is not ours to score."""
        self._pair(forecast_db, 72, 0.9, 0.7)
        assert vf.summarise(forecast_db) == []

    def test_empty_archive_summarises_to_nothing(self, forecast_db):
        assert vf.summarise(forecast_db) == []


class TestObservationColumnMap:
    def test_every_fetched_variable_has_a_verdict(self):
        """A new forecast variable must be mapped or explicitly unverifiable."""
        assert set(vf.OBSERVATION_COLUMNS) == set(wf.VARIABLES)

    def test_circular_variables_are_all_directions(self):
        for variable in vf.CIRCULAR_VARIABLES:
            assert wf.UNITS[variable] == "degrees_true_from"

    def test_direction_units_all_declared_circular(self):
        """The inverse: a new bearing must not default to scalar arithmetic."""
        bearings = {v for v, unit in wf.UNITS.items() if unit == "degrees_true_from"}
        assert bearings == vf.CIRCULAR_VARIABLES
