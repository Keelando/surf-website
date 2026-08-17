"""Tests for reporting-lag tracking (lib/reporting_lag.py)."""

import logging
import sqlite3
import time

import pytest

from lib import reporting_lag


@pytest.fixture(autouse=True)
def quiet_logger(monkeypatch):
    """
    Keep tests out of logs/reporting_lag.log. Two tests below deliberately
    provoke warnings, and those must not land in the production log where
    they would read as a real pipeline failure.
    """
    monkeypatch.setattr(reporting_lag, "logger", logging.getLogger("test_reporting_lag"))


@pytest.fixture
def t0():
    """Yesterday. Times are offsets from here so rows stay inside retention."""
    return int(time.time()) - 86400


@pytest.fixture
def lag_db(tmp_path, monkeypatch):
    """Point the module at a throwaway lag database."""
    path = tmp_path / "reporting_lag.sqlite"
    monkeypatch.setattr(reporting_lag, "REPORTING_LAG_DATABASE", path)
    return path


@pytest.fixture
def source_db():
    """An in-memory stand-in for buoy_data.sqlite."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE buoy_observation (
            buoy_id TEXT,
            observation_time INTEGER,
            recorded_at TEXT
        )
    """)
    return conn


@pytest.fixture
def obs(source_db, t0):
    """Insert an observation, storing ingest time the way the parsers do."""

    def _add(buoy_id, obs_offset, ingest_offset):
        source_db.execute(
            "INSERT INTO buoy_observation VALUES (?, ?, datetime(?, 'unixepoch'))",
            (buoy_id, t0 + obs_offset, t0 + ingest_offset),
        )

    return _add


@pytest.fixture
def record(source_db, t0):
    """Run an export's worth of lag recording, in offsets from t0."""

    def _record(published, at):
        reporting_lag.record_publication(
            "buoy",
            source_db,
            "buoy_observation",
            "buoy_id",
            {k: t0 + v for k, v in published.items()},
            published_at=t0 + at,
        )

    return _record


@pytest.fixture
def rows(lag_db, t0):
    """Lag rows, with absolute timestamps normalized back to t0 offsets."""

    def _rows():
        conn = sqlite3.connect(lag_db)
        conn.row_factory = sqlite3.Row
        out = []
        for r in conn.execute("SELECT * FROM reporting_lag ORDER BY observation_time"):
            row = dict(r)
            for col in ("observation_time", "ingested_at", "published_at", "last_seen_at"):
                if row[col] is not None:
                    row[col] -= t0
            out.append(row)
        return out

    return _rows


def test_splits_source_and_publish_lag(obs, record, rows):
    # Sampled at 1000, landed in our DB at 1300, exported at 1420.
    obs("46131", 1000, 1300)
    record({"46131": 1000}, 1420)

    (row,) = rows()
    assert row["source_lag_seconds"] == 300
    assert row["publish_lag_seconds"] == 120
    assert row["total_lag_seconds"] == 420


def test_stall_grows_stale_seconds_without_new_rows(obs, record, rows):
    obs("46131", 1000, 1300)
    record({"46131": 1000}, 1420)
    # Three later exports, same observation still the newest we hold.
    for t in (2000, 3000, 4000):
        record({"46131": 1000}, t)

    (row,) = rows()
    assert row["stale_seconds"] == 3000  # last_seen_at 4000 - observation 1000
    assert row["published_at"] == 1420  # first publish wins, not overwritten
    assert row["publish_lag_seconds"] == 120


def test_superseded_observation_is_recorded_unpublished(obs, record, rows):
    obs("46131", 1000, 1300)
    record({"46131": 1000}, 1420)

    # Two arrive before the next export; only the newer one reaches the page.
    obs("46131", 2000, 2100)
    obs("46131", 3000, 3100)
    record({"46131": 3000}, 3200)

    published = {r["observation_time"]: r["published_at"] for r in rows()}
    assert published[2000] is None  # collected, never displayed
    assert published[3000] == 3200


def test_first_sighting_does_not_backfill_history(obs, record, rows):
    # A long history exists, but only the published observation is seeded:
    # nothing older has a publish record worth recovering.
    for t in (1000, 2000, 3000):
        obs("46131", t, t + 100)
    record({"46131": 3000}, 3200)

    assert [r["observation_time"] for r in rows()] == [3000]


def test_stations_are_tracked_independently(obs, record, rows):
    obs("46131", 1000, 1100)
    obs("46146", 1000, 1900)
    record({"46131": 1000, "46146": 1000}, 2000)

    by_station = {r["station_id"]: r for r in rows()}
    assert by_station["46131"]["source_lag_seconds"] == 100
    assert by_station["46146"]["source_lag_seconds"] == 900


def test_observation_without_ingest_time_is_skipped(source_db, t0, record, rows):
    # No recorded_at means no way to split the two components; a guessed
    # value would read as zero source lag, which is worse than no row.
    source_db.execute("INSERT INTO buoy_observation VALUES ('46131', ?, NULL)", (t0 + 1000,))
    record({"46131": 1000}, 1420)

    assert rows() == []


def test_purges_beyond_retention(obs, record, rows, monkeypatch):
    obs("46131", 1000, 1300)
    record({"46131": 1000}, 1420)
    assert len(rows()) == 1

    # Retention drops to nothing: the row is older than "now" by a day.
    monkeypatch.setattr(reporting_lag, "REPORTING_LAG_RETENTION_DAYS", 0)
    obs("46131", 2000, 2100)
    record({"46131": 2000}, 2200)

    assert rows() == []


def test_never_raises_when_lag_db_is_unwritable(tmp_path, obs, record, monkeypatch):
    # Telemetry must not take down an export that is otherwise ready to serve.
    monkeypatch.setattr(reporting_lag, "REPORTING_LAG_DATABASE", tmp_path / "nope" / "lag.sqlite")
    obs("46131", 1000, 1300)
    record({"46131": 1000}, 1420)  # must not raise


def test_no_published_stations_is_a_noop(lag_db, record):
    record({}, 1420)
    assert not lag_db.exists()


def test_first_row_per_station_is_flagged_as_seeded(obs, record, rows):
    # Its published_at is when tracking began, not when the value first
    # reached the site, so publish-lag statistics must be able to skip it.
    obs("46131", 1000, 1300)
    record({"46131": 1000}, 1420)
    obs("46131", 2000, 2100)
    record({"46131": 2000}, 2200)

    seeded = {r["observation_time"]: r["seeded"] for r in rows()}
    assert seeded == {1000: 1, 2000: 0}
