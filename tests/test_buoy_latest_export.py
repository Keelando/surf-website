"""Tests for scripts/export/sqlite_to_json.py — the latest-buoy snapshot.

latest_buoy_v2.json is what the home page reads and what /api/v1/buoys/latest
serves, so its rules are a public contract. Their failures are quiet ones:

- **Each field has its own 2-hour window**, measured back from the station's
  newest row. A three-hour-old wave height must not ride along on a fresh wind
  reading as if current; a value from an earlier row inside the window comes
  with its own time in `field_times`.
- **Wind is stored in km/h and published in knots**, under the bare name and
  an explicit `_kt` copy.
- **A quiet wave sensor says since when** (`wave_last_reading_time`), only
  when the waves dropped out of the window and only if they ever arrived.
- **The backup-sensor label comes from the wave reading's own row.** "Most
  recent non-null" would pin an old "radar" label on a fresh first-choice
  reading, which is stored as NULL.
- **A station with no rows still appears**, as a `no_data` stub.

The database and output file are temporary; nothing reaches site/data or the
real logs.
"""

import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.export import sqlite_to_json as sx

MIN = 60
HOUR = 3600


@pytest.fixture
def export(tmp_path, monkeypatch):
    """Run the export over a temporary database; returns (insert, run)."""
    db = tmp_path / "buoy_data.sqlite"
    columns = [*sx.ALL_FIELDS, "wave_sensor"]
    conn = sqlite3.connect(db)
    conn.execute(f"""
        CREATE TABLE buoy_observation (
            buoy_id TEXT NOT NULL,
            observation_time INTEGER NOT NULL,
            recorded_at TEXT,
            {", ".join(f"{c} {'TEXT' if c == 'wave_sensor' else 'REAL'}" for c in columns)}
        )
    """)
    conn.commit()

    out = tmp_path / "latest_buoy_v2.json"
    monkeypatch.setattr(sx, "BUOY_DATABASE", db)
    monkeypatch.setattr(sx, "OUT_PATH", out)
    monkeypatch.setattr(sx, "BUOYS", {"A": {"name": "Station A"}, "EMPTY": {"name": "Empty"}})
    monkeypatch.setattr(sx, "record_publication", lambda *args, **kwargs: None)
    # The module bound its real log file on import; keep test runs out of it.
    quiet = logging.getLogger("test_buoy_latest_export")
    quiet.addHandler(logging.NullHandler())
    quiet.propagate = False
    monkeypatch.setattr(sx, "logger", quiet)

    now = int(time.time())

    def insert(ago_seconds, **fields):
        names = ["buoy_id", "observation_time", *fields]
        conn.execute(
            f"INSERT INTO buoy_observation ({', '.join(names)}) VALUES ({', '.join('?' * len(names))})",
            ["A", now - ago_seconds, *fields.values()],
        )
        conn.commit()

    def run():
        sx.query_and_export()
        return json.loads(out.read_text())

    yield insert, run
    conn.close()


def test_a_field_older_than_the_window_is_dropped(export):
    insert, run = export
    insert(3 * HOUR, wave_height_sig=1.2)
    insert(10 * MIN, wind_speed=20.0)
    a = run()["A"]
    assert "wave_height_sig" not in a
    assert a["wind_speed"] is not None


def test_an_earlier_value_inside_the_window_carries_its_own_time(export):
    insert, run = export
    insert(60 * MIN, wave_height_sig=0.8)
    insert(10 * MIN, wind_speed=20.0)
    a = run()["A"]
    assert a["wave_height_sig"] == 0.8
    assert "wave_height_sig" in a["field_times"]
    assert "wind_speed" not in a.get("field_times", {})


def test_wind_is_published_in_knots_under_both_names(export):
    insert, run = export
    insert(10 * MIN, wind_speed=18.52, wind_gust=37.04)
    a = run()["A"]
    assert a["wind_speed"] == pytest.approx(10.0, abs=0.05)
    assert a["wind_speed_kt"] == a["wind_speed"]
    assert a["wind_gust_kt"] == pytest.approx(20.0, abs=0.05)


def test_a_quiet_wave_sensor_says_since_when(export):
    insert, run = export
    insert(2 * 24 * HOUR, wave_height_sig=0.4)
    insert(10 * MIN, wind_speed=20.0)
    a = run()["A"]
    assert "wave_height_sig" not in a
    expected = sx.datetime.fromtimestamp(int(time.time()) - 2 * 24 * HOUR, tz=sx.timezone.utc)
    published = sx.datetime.fromisoformat(a["wave_last_reading_time"])
    assert abs((published - expected).total_seconds()) < 5


def test_no_wave_note_when_waves_are_fresh_or_never_came(export):
    insert, run = export
    insert(10 * MIN, wind_speed=20.0)
    assert "wave_last_reading_time" not in run()["A"]  # never sent waves

    insert(5 * MIN, wave_height_sig=0.5)
    assert "wave_last_reading_time" not in run()["A"]  # fresh waves


def test_the_backup_label_comes_from_the_wave_readings_own_row(export):
    insert, run = export
    insert(60 * MIN, wave_height_sig=0.6, wave_sensor="radar")
    insert(10 * MIN, wave_height_sig=0.7)  # first choice: stored as NULL
    assert "wave_sensor" not in run()["A"]

    insert(5 * MIN, wave_height_sig=0.9, wave_sensor="radar")
    assert run()["A"]["wave_sensor"] == "radar"


def test_a_station_with_no_rows_is_a_stub(export):
    insert, run = export
    insert(10 * MIN, wind_speed=20.0)
    data = run()
    assert data["EMPTY"] == {"name": "Empty", "no_data": True}
    assert "_meta" in data
