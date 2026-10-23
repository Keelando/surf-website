"""Surrey wave sensors: each reading comes whole from the first sensor with a real Hs."""

import sqlite3

import pytest

from lib.stations import get_all_buoys
from scripts.fetch import fetch_surrey_wave_v2 as wave

# Channel ids as at Crescent Beach Ocean
ANDERAA_HS, ANDERAA_TP, RADAR_HS, RADAR_TP, KELLER_HS = 2002, 2012, 2158, 2157, 2003
META = {
    "name": "Crescent Beach Ocean",
    "flowworks_site_id": 20182,
    "channels": {},
    "wave_sensors": [
        {"name": "Anderaa", "channels": {"wave_height_sig": ANDERAA_HS, "wave_period_peak": ANDERAA_TP}},
        {"name": "radar", "channels": {"wave_height_sig": RADAR_HS, "wave_period_peak": RADAR_TP}},
        {"name": "Keller pressure sensor", "channels": {"wave_height_sig": KELLER_HS}},
    ],
}
T1, T2 = "2026-10-23T13:00:00", "2026-10-23T13:10:00"  # Pacific, as FlowWorks sends them


class FakeAPI:
    def __init__(self, series):
        self.series = series  # channel id -> [(time, value)]
        self.asked = []

    def get_channel_data(self, site_id, channel_id, hours=2):
        self.asked.append(channel_id)
        return [{"DataTime": t, "DataValue": v} for t, v in self.series.get(channel_id, [])]


@pytest.fixture
def conn(monkeypatch):
    monkeypatch.setattr(wave.time, "sleep", lambda _s: None)
    c = sqlite3.connect(":memory:")
    c.execute(
        "CREATE TABLE buoy_observation (buoy_id TEXT, observation_time INTEGER, wave_height_sig REAL,"
        " wave_period_peak REAL, wave_sensor TEXT, source_file TEXT)"
    )
    return c


def stored(conn):
    return conn.execute(
        "SELECT wave_height_sig, wave_period_peak, wave_sensor FROM buoy_observation ORDER BY observation_time"
    ).fetchall()


def test_a_healthy_first_choice_wins_and_spares_the_backups(conn):
    api = FakeAPI({ANDERAA_HS: [(T1, 0.4)], ANDERAA_TP: [(T1, 5.0)], RADAR_HS: [(T1, 0.3)]})
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [(0.4, 5.0, None)]
    assert RADAR_HS not in api.asked


def test_a_zero_first_choice_hands_the_whole_reading_to_the_radar(conn):
    api = FakeAPI(
        {
            ANDERAA_HS: [(T1, 0.0)],
            ANDERAA_TP: [(T1, 6.0)],  # a period with no height is not kept
            RADAR_HS: [(T1, 0.3)],
            RADAR_TP: [(T1, 4.5)],
        }
    )
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [(0.3, 4.5, "radar")]
    assert KELLER_HS not in api.asked


def test_sensors_are_chosen_per_timestamp(conn):
    api = FakeAPI(
        {
            ANDERAA_HS: [(T1, 0.4), (T2, 0.0)],
            RADAR_HS: [(T1, 0.3), (T2, 0.0)],
            KELLER_HS: [(T2, 0.2)],
        }
    )
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [(0.4, None, None), (0.2, None, "Keller pressure sensor")]


def test_no_sensor_with_a_reading_stores_nothing(conn):
    api = FakeAPI({ANDERAA_HS: [(T1, 0.0)], RADAR_HS: [(T1, 0.0)], KELLER_HS: [(T1, 0.0)]})
    wave.fetch_and_store(api, "CRPILE", META, conn)
    assert stored(conn) == [(None, None, None)]


def test_a_stored_zero_is_replaced_on_the_next_run(conn):
    wave.fetch_and_store(FakeAPI({ANDERAA_HS: [(T1, 0.0)]}), "CRPILE", META, conn)
    assert stored(conn) == [(None, None, None)]
    wave.fetch_and_store(FakeAPI({ANDERAA_HS: [(T1, 0.0)], RADAR_HS: [(T1, 0.3)]}), "CRPILE", META, conn)
    assert stored(conn) == [(0.3, None, "radar")]


def test_registry_lists_each_wave_channel_once():
    """A wave field lives in wave_sensors or channels, never both."""
    for buoy_id, meta in get_all_buoys().items():
        sensors = meta.get("wave_sensors", [])
        sensor_fields = {f for s in sensors for f in s["channels"]}
        assert not sensor_fields & set(meta.get("channels", {})), buoy_id
        assert "fallback_channels" not in meta, buoy_id
        if sensors:
            assert all("wave_height_sig" in s["channels"] for s in sensors), buoy_id
