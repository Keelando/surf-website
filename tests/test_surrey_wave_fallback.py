"""Surrey wave fetcher: the fallback channel fills where the primary has no reading — including a zero."""

import sqlite3

import pytest

from scripts.fetch import fetch_surrey_wave_v2 as wave

PRIMARY, FALLBACK = 2002, 2158  # Hs_Anderra, Hm0_RADAR at Crescent Beach Ocean
META = {
    "name": "Crescent Beach Ocean",
    "flowworks_site_id": 20182,
    "channels": {"wave_height_sig": PRIMARY},
    "fallback_channels": {"wave_height_sig": FALLBACK},
}


class FakeAPI:
    def __init__(self, series):
        self.series = series  # channel id -> [(Pacific "YYYY-MM-DDTHH:MM:SS", value)]

    def get_channel_data(self, site_id, channel_id, hours=2):
        return [{"DataTime": t, "DataValue": v} for t, v in self.series.get(channel_id, [])]


@pytest.fixture
def conn(monkeypatch):
    monkeypatch.setattr(wave.time, "sleep", lambda _s: None)
    c = sqlite3.connect(":memory:")
    c.execute(
        "CREATE TABLE buoy_observation (buoy_id TEXT, observation_time INTEGER, wave_height_sig REAL, source_file TEXT)"
    )
    return c


def stored(conn):
    return [r[0] for r in conn.execute("SELECT wave_height_sig FROM buoy_observation ORDER BY observation_time")]


def test_a_live_primary_wins(conn):
    api = FakeAPI({PRIMARY: [("2026-10-23T13:00:00", 0.4)], FALLBACK: [("2026-10-23T13:00:00", 0.3)]})
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [0.4]


def test_a_zero_primary_gives_way_to_the_fallback(conn):
    api = FakeAPI({PRIMARY: [("2026-10-23T13:00:00", 0.0)], FALLBACK: [("2026-10-23T13:00:00", 0.3)]})
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [0.3]


def test_a_stored_zero_is_replaced_on_the_next_run(conn):
    zeros = FakeAPI({PRIMARY: [("2026-10-23T13:00:00", 0.0)]})
    wave.fetch_and_store(zeros, "CRPILE", META, conn)
    assert stored(conn) == [None]
    both = FakeAPI({PRIMARY: [("2026-10-23T13:00:00", 0.0)], FALLBACK: [("2026-10-23T13:00:00", 0.3)]})
    wave.fetch_and_store(both, "CRPILE", META, conn)
    assert stored(conn) == [0.3]


def test_a_zero_fallback_is_not_stored_either(conn):
    api = FakeAPI({PRIMARY: [("2026-10-23T13:00:00", 0.0)], FALLBACK: [("2026-10-23T13:00:00", 0.0)]})
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [None]
