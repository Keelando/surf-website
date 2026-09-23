"""An empty lightstation row (a bulletin's "NA") must never count as a report."""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.lightstation_readings import HAS_READING_SQL  # noqa: E402
from scripts.utils.create_lightstation_db import CREATE_TABLE_SQL  # noqa: E402

EMPTY = dict(
    wind_speed_kt=None,
    wind_direction=None,
    wind_calm=0,
    sea_height_ft=None,
    sea_condition=None,
    swell_intensity=None,
    swell_direction=None,
)


@pytest.mark.parametrize(
    "fields, counts",
    [
        ({}, False),
        ({"wind_calm": 1}, True),
        ({"wind_speed_kt": 0.0}, True),
        ({"sea_condition": "RIPPLED", "sea_height_ft": 0}, True),
        ({"swell_intensity": "LOW"}, True),
    ],
)
def test_has_reading(fields, counts):
    conn = sqlite3.connect(":memory:")
    conn.execute(CREATE_TABLE_SQL)
    row = {**EMPTY, **fields}
    cols = ", ".join(row)
    conn.execute(
        f"INSERT INTO lightstation_observation (station_name, observation_time, {cols}) "
        f"VALUES ('X', 1, {', '.join('?' * len(row))})",
        tuple(row.values()),
    )
    (n,) = conn.execute(f"SELECT COUNT(*) FROM lightstation_observation WHERE {HAS_READING_SQL}").fetchone()
    assert bool(n) is counts
