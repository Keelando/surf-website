"""Tests for scripts/export/export_forecast_verification.py — the 48 h verification plot data.

The export draws two series over one time axis: what the model said about a day
ahead, and what the instruments measured. The failure modes are all quiet ones —
the chart still renders, it just isn't showing what the caption claims:

- **The lead cap must hold.** Every stitched point has to come from a run that
  was at most LEAD_TARGET_HOURS ahead. A point sneaking in at a shorter lead
  would flatter the model with hindsight it didn't have.
- **The sawtooth is real and must survive.** Runs are 6-hourly and the fetch
  taper omits leads 25-26, so the band cycles 19→24. Publishing per-point
  `lead_hours` is what lets the page label it honestly.
- **Masked steps are not zeros.** A masked wind-wave partition means "no wind
  sea this hour"; charting it as 0 would draw a trough that never existed.
- **A variable nothing measures still gets a forecast series**, with an empty
  observed one, rather than being dropped.
- **site/data is public**, so the payload carries an explicit allowlist and no
  stray columns from either database.

Both databases are temporary; no network, no real data.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.export import export_forecast_verification as hx
from scripts.fetch import fetch_wave_forecast as wf

UTC = timezone.utc
HOUR = 3600

# A round "now" so window arithmetic in the tests is readable.
NOW = 1787097600  # 2026-08-19 00:00:00Z
STATION = "4600146"


def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def forecast_db(tmp_path, monkeypatch):
    path = tmp_path / "wave_forecast.sqlite"
    conn = sqlite3.connect(path)
    wf.ensure_db_schema(conn)
    conn.close()
    monkeypatch.setattr(hx, "WAVE_FORECAST_DATABASE", path)
    return path


@pytest.fixture
def buoy_db(tmp_path, monkeypatch):
    path = tmp_path / "buoy_data.sqlite"
    columns = sorted({c for c in hx.OBSERVATION_COLUMNS.values() if c})
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
    monkeypatch.setattr(hx, "BUOY_DATABASE", path)
    return path


@pytest.fixture
def output_dir(tmp_path, monkeypatch):
    path = tmp_path / "verification"
    monkeypatch.setattr(hx, "OUTPUT_DIR", path)
    return path


def add_forecast(path, valid, run, value, variable="wave_height", status="ok",
                 station=STATION):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        INSERT OR REPLACE INTO wave_forecast
        (station_id, variable, forecast_run_time, valid_time, value, status, model)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (station, variable, run, valid, value, status, wf.MODEL_NAME),
    )
    conn.commit()
    conn.close()


def add_observation(path, when, station=STATION, **values):
    columns = ["buoy_id", "observation_time", *values]
    conn = sqlite3.connect(path)
    conn.execute(
        f"INSERT INTO buoy_observation ({', '.join(columns)})"
        f" VALUES ({', '.join('?' * len(columns))})",
        (station, when, *values.values()),
    )
    conn.commit()
    conn.close()


def stitch(path, station=STATION, now=NOW):
    with sqlite3.connect(path) as conn:
        return hx.stitched_forecast(conn, station, now - hx.WINDOW_HOURS * HOUR, now)


# ── lead selection ──────────────────────────────────────────────────


class TestStitchedForecast:
    def test_takes_the_longest_lead_within_the_cap(self, forecast_db):
        """Several runs cover one hour; the oldest one still inside the cap wins."""
        valid = NOW - 5 * HOUR
        for lead, value in ((6, 0.1), (18, 0.2), (24, 0.3)):
            add_forecast(forecast_db, valid, valid - lead * HOUR, value)

        points = stitch(forecast_db)["wave_height"]
        assert points == [{"time": iso(valid), "value": 0.3, "lead_hours": 24}]

    def test_never_exceeds_the_cap(self, forecast_db):
        """A 27 h lead is nearer to 24 than 19 is, and must still be rejected:
        the curve may not claim more notice than it had."""
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 27 * HOUR, 0.9)
        add_forecast(forecast_db, valid, valid - 19 * HOUR, 0.2)

        points = stitch(forecast_db)["wave_height"]
        assert [p["lead_hours"] for p in points] == [19]

    def test_only_longer_leads_than_the_cap_yields_nothing(self, forecast_db):
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 30 * HOUR, 0.9)
        assert stitch(forecast_db) == {}

    def test_the_sawtooth_survives(self, forecast_db):
        """6-hourly runs with no leads 25-26 give a 19→24 cycle, not a flat 24."""
        # Runs must start a full lead-span before the window, as the real
        # archive does — otherwise the oldest hours in the window have only
        # short-lead runs to draw on and the band is not what is under test.
        runs = [NOW - 72 * HOUR + n * 6 * HOUR for n in range(13)]
        for run in runs:
            # The real taper: hourly to +24, then 3-hourly. Leads 25 and 26
            # are the ones that do not exist, and are why the band drops to 19.
            for lead in list(range(0, 25)) + list(range(27, 49, 3)):
                add_forecast(forecast_db, run + lead * HOUR, run, 0.5)

        leads = [p["lead_hours"] for p in stitch(forecast_db)["wave_height"]]
        assert set(leads) <= set(range(hx.LEAD_MIN_HOURS, hx.LEAD_TARGET_HOURS + 1))
        assert leads[:8] == [24, 19, 20, 21, 22, 23, 24, 19]

    def test_masked_steps_are_gaps_not_zeros(self, forecast_db):
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, None,
                     variable="wind_wave_height", status="masked")
        assert "wind_wave_height" not in stitch(forecast_db)

    def test_points_outside_the_window_are_excluded(self, forecast_db):
        old = NOW - (hx.WINDOW_HOURS + 2) * HOUR
        add_forecast(forecast_db, old, old - 24 * HOUR, 0.4)
        assert stitch(forecast_db) == {}

    def test_variables_are_stitched_independently(self, forecast_db):
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3, variable="wave_height")
        add_forecast(forecast_db, valid, valid - 19 * HOUR, 3.1, variable="peak_period")

        series = stitch(forecast_db)
        assert series["wave_height"][0]["lead_hours"] == 24
        assert series["peak_period"][0]["lead_hours"] == 19

    def test_stations_do_not_bleed_into_each_other(self, forecast_db):
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3, station="CRPILE")
        assert stitch(forecast_db, station=STATION) == {}


# ── observations ────────────────────────────────────────────────────


class TestObservations:
    def test_reads_each_variable_it_has_a_column_for(self, buoy_db):
        add_observation(buoy_db, NOW - HOUR, wave_height_sig=0.4, wind_speed=18.0)
        series = hx.observations(STATION, NOW - hx.WINDOW_HOURS * HOUR, NOW)
        assert series["wave_height"] == [{"time": iso(NOW - HOUR), "value": 0.4}]
        assert series["wind_speed"] == [{"time": iso(NOW - HOUR), "value": 18.0}]

    def test_nulls_are_dropped_per_variable_not_per_row(self, buoy_db):
        """CRPILE reports wind but never wave direction; that is not a wind gap."""
        add_observation(buoy_db, NOW - HOUR, wind_speed=18.0)
        series = hx.observations(STATION, NOW - hx.WINDOW_HOURS * HOUR, NOW)
        assert "wind_speed" in series
        assert "wave_direction" not in series

    def test_unverifiable_variables_are_never_observed(self, buoy_db):
        """Nothing measures the wind-sea partition, so it has no observed series."""
        add_observation(buoy_db, NOW - HOUR, wave_height_sig=0.4)
        assert "wind_wave_height" not in hx.observations(
            STATION, NOW - hx.WINDOW_HOURS * HOUR, NOW
        )

    def test_window_bounds_are_respected(self, buoy_db):
        add_observation(buoy_db, NOW - (hx.WINDOW_HOURS + 1) * HOUR, wave_height_sig=9.9)
        add_observation(buoy_db, NOW - HOUR, wave_height_sig=0.4)
        series = hx.observations(STATION, NOW - hx.WINDOW_HOURS * HOUR, NOW)
        assert [p["value"] for p in series["wave_height"]] == [0.4]

    def test_missing_database_is_not_fatal(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hx, "BUOY_DATABASE", tmp_path / "absent.sqlite")
        assert hx.observations(STATION, NOW - HOUR, NOW) == {}


# ── payload ─────────────────────────────────────────────────────────


class TestBuildStationPayload:
    def _payload(self, forecast_db, buoy_db):
        with sqlite3.connect(forecast_db) as conn:
            return hx.build_station_payload(
                conn, STATION, {"name": "Halibut Bank", "lat": 49.337, "lon": -123.731}, NOW
            )

    def test_pairs_the_two_series_under_one_variable(self, forecast_db, buoy_db):
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3)
        add_observation(buoy_db, valid, wave_height_sig=0.4)

        series = self._payload(forecast_db, buoy_db)["series"]["wave_height"]
        assert series["forecast"][0]["value"] == 0.3
        assert series["observed"][0]["value"] == 0.4

    def test_forecast_only_variable_gets_an_empty_observed_series(self, forecast_db, buoy_db):
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.1,
                     variable="wind_wave_height")
        series = self._payload(forecast_db, buoy_db)["series"]["wind_wave_height"]
        assert len(series["forecast"]) == 1
        assert series["observed"] == []

    def test_observed_only_variable_gets_an_empty_forecast_series(self, forecast_db, buoy_db):
        """The archive starts later than the observations; the plot still draws."""
        add_observation(buoy_db, NOW - 5 * HOUR, wave_height_sig=0.4)
        series = self._payload(forecast_db, buoy_db)["series"]["wave_height"]
        assert series["forecast"] == []
        assert len(series["observed"]) == 1

    def test_lead_band_is_published(self, forecast_db, buoy_db):
        """The page must be able to label the band rather than say '24 hours'."""
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3)
        band = self._payload(forecast_db, buoy_db)["lead_band"]
        assert band == {"target_hours": 24, "min_hours": 19, "max_hours": 24}

    def test_units_come_from_the_fetcher(self, forecast_db, buoy_db):
        """One source of truth for units; km/h here, knots only at render time."""
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 20.0, variable="wind_speed")
        assert self._payload(forecast_db, buoy_db)["units"]["wind_speed"] == "km/h"

    def test_nothing_to_draw_returns_none(self, forecast_db, buoy_db):
        assert self._payload(forecast_db, buoy_db) is None

    def test_payload_is_an_explicit_allowlist(self, forecast_db, buoy_db):
        """site/data is public — no stray columns from either database."""
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3)
        add_observation(buoy_db, valid, wave_height_sig=0.4)

        payload = self._payload(forecast_db, buoy_db)
        assert set(payload) == {
            "station_id", "station_name", "location", "generated_utc",
            "window_hours", "lead_band", "units", "series",
        }
        assert set(payload["location"]) == {"lat", "lon"}
        assert set(payload["series"]["wave_height"]["forecast"][0]) == {
            "time", "value", "lead_hours"
        }
        assert set(payload["series"]["wave_height"]["observed"][0]) == {"time", "value"}


class TestExportHindcast:
    def test_writes_one_file_per_station(self, forecast_db, buoy_db, output_dir, monkeypatch):
        monkeypatch.setattr(hx, "STATIONS", {STATION: {"wind": True}})
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3)

        assert hx.export_verification(now=NOW) == 1
        payload = json.loads((output_dir / f"{STATION}.json").read_text())
        assert payload["station_id"] == STATION
        assert payload["series"]["wave_height"]["forecast"][0]["value"] == 0.3

    def test_station_with_no_data_writes_no_file(self, forecast_db, buoy_db, output_dir,
                                                 monkeypatch):
        monkeypatch.setattr(hx, "STATIONS", {STATION: {"wind": True}})
        assert hx.export_verification(now=NOW) == 0
        assert not output_dir.exists() or list(output_dir.iterdir()) == []

    def test_missing_archive_is_not_fatal(self, tmp_path, output_dir, monkeypatch):
        monkeypatch.setattr(hx, "WAVE_FORECAST_DATABASE", tmp_path / "absent.sqlite")
        assert hx.export_verification(now=NOW) == 0

    def test_timestamps_are_iso_utc(self, forecast_db, buoy_db, output_dir, monkeypatch):
        """A naive datetime here would put every point 7-8 hours out."""
        monkeypatch.setattr(hx, "STATIONS", {STATION: {"wind": True}})
        valid = NOW - 5 * HOUR
        add_forecast(forecast_db, valid, valid - 24 * HOUR, 0.3)
        hx.export_verification(now=NOW)

        payload = json.loads((output_dir / f"{STATION}.json").read_text())
        assert payload["series"]["wave_height"]["forecast"][0]["time"] == iso(valid)
        assert payload["generated_utc"].endswith("Z")


class TestStationCoverage:
    def test_every_forecast_station_is_exported(self):
        """The exporter reads STATIONS rather than keeping its own station list.

        Identity, not equality: a copy would drift the moment a station is
        added to the fetcher and not here.
        """
        assert hx.STATIONS is wf.STATIONS

    def test_column_map_is_shared_with_the_verifier(self):
        """One place decides which instrument measures which forecast variable."""
        from scripts.monitoring import verify_wave_forecast as vf

        assert hx.OBSERVATION_COLUMNS is vf.OBSERVATION_COLUMNS
