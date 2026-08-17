"""Tests for scripts/fetch/fetch_wave_forecast.py — the wave + wind fetcher.

The fetcher's job is to turn GeoMet WMS point queries into a forecast archive
that can later be scored against the buoy, so the things worth pinning down are
the ones a silent regression would corrupt without breaking anything visible:

- **Time conversion.** Model valid times are UTC end to end (GetCapabilities →
  `TIME=` → SQLite epoch → JSON key). A naive datetime reaching `.timestamp()`
  would land every row 7–8 hours out and still look plausible on the page.
- **masked vs failed.** A masked step is the model saying "no wind sea here";
  a failed one is a hole in our record. Conflating them poisons the skill
  scores that are the whole point of the archive.
- **Model provenance.** RDWPS waves and HRDPS wind are fetched in one pass and
  publish runs at the same 00/06/12/18Z hours. Without per-row and per-run
  model labelling they blend into one archive, or overwrite each other.
- **Schema migration.** The original table had `value NOT NULL`, so masked
  steps could not be written at all; the run table's key had no model in it.

Network is never touched: every WMS response is a stub.

Verification of the model against the buoy lives elsewhere (the verification
writer is still to be built — docs/project/FORECAST_UPGRADE.md).
"""

import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.forecast_steps import taper_time_steps
from scripts.fetch import fetch_wave_forecast as wf

# 2026-08-16 18:00Z — the run behind the forecast in site/data at the time
# these tests were written. Epochs are hardcoded rather than recomputed with
# the same expression the code uses, so the test actually checks the arithmetic.
RUN_ISO = "2026-08-16T18:00:00Z"
RUN_EPOCH = 1786903200
STEP_ISO = "2026-08-18T06:00:00Z"  # the 0.72 m peak, 23:00 Mon Pacific
STEP_EPOCH = 1787032800

UTC = timezone.utc


def dt(iso):
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


class FakeResponse:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, payload=None, text="", status=200):
        self._payload = payload
        self.text = text
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return self._payload


def feature(value, run=RUN_ISO):
    props = {"value": value}
    if run is not None:
        props["dim_reference_time"] = run
    return {"features": [{"properties": props}]}


def capabilities(start, end, interval="PT1H"):
    return (
        '<Layer><Dimension name="time" units="ISO8601" default="'
        f'{start}">{start}/{end}/{interval}</Dimension></Layer>'
    )


@pytest.fixture
def wave_db(tmp_path, monkeypatch):
    """Point the module at a throwaway database."""
    path = tmp_path / "wave_forecast.sqlite"
    monkeypatch.setattr(wf, "WAVE_FORECAST_DATABASE", path)
    return path


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """The 1.5 s inter-request delay has no place in a test run."""
    monkeypatch.setattr(wf.time, "sleep", lambda _seconds: None)


@pytest.fixture(autouse=True)
def quiet_logger(monkeypatch):
    """Keep test output out of logs/wave_forecast.log.

    `setup_logging` binds to the real log file at import, so without this every
    `npm test` writes invented run lines ("Stored 1 values", schema rebuilds)
    into the log a human reads to check on the actual cron job.
    """
    silent = logging.getLogger("wave_forecast_test")
    silent.addHandler(logging.NullHandler())
    silent.propagate = False
    monkeypatch.setattr(wf, "logger", silent)


# ── to_utc: the single normalisation point for valid times ───────────


class TestToUtc:
    def test_naive_is_read_as_utc_not_local(self):
        """The trap: `.timestamp()` on a naive datetime uses the server zone."""
        assert int(wf.to_utc(datetime(2026, 8, 18, 6, 0)).timestamp()) == STEP_EPOCH

    def test_aware_utc_is_unchanged(self):
        assert wf.to_utc(dt(STEP_ISO)) == dt(STEP_ISO)

    def test_other_zone_is_converted_not_relabelled(self):
        pacific = timezone(timedelta(hours=-7))
        local = datetime(2026, 8, 17, 23, 0, tzinfo=pacific)  # 06:00Z next day
        converted = wf.to_utc(local)
        assert converted.strftime("%Y-%m-%dT%H:%M:%SZ") == STEP_ISO
        assert int(converted.timestamp()) == STEP_EPOCH


# ── get_time_steps: the model's published time axis ─────────────────


class TestGetTimeSteps:
    def _capabilities(self, monkeypatch, text):
        monkeypatch.setattr(
            wf.requests, "get", lambda *args, **kwargs: FakeResponse(text=text)
        )

    def test_rdwps_shape(self, monkeypatch):
        """49 hourly steps spanning the run's 0–48 h."""
        self._capabilities(
            monkeypatch, capabilities(RUN_ISO, "2026-08-18T18:00:00Z")
        )
        steps = wf.get_time_steps("RDWPS_2.5km_SignificantWaveHeight")
        assert len(steps) == 49
        assert steps[0] == dt(RUN_ISO)
        assert steps[-1] == dt("2026-08-18T18:00:00Z")

    def test_steps_are_utc_aware(self, monkeypatch):
        self._capabilities(
            monkeypatch, capabilities(RUN_ISO, "2026-08-18T18:00:00Z")
        )
        steps = wf.get_time_steps("layer")
        assert all(step.tzinfo == UTC for step in steps)
        assert int(steps[0].timestamp()) == RUN_EPOCH

    def test_missing_time_dimension_raises(self, monkeypatch):
        self._capabilities(monkeypatch, "<Layer><Title>no time here</Title></Layer>")
        with pytest.raises(ValueError, match="No time dimension"):
            wf.get_time_steps("layer")

    def test_minute_interval_is_read_as_minutes(self, monkeypatch):
        """Stripping non-digits out of PT30M used to yield a 30-*hour* step."""
        self._capabilities(
            monkeypatch, capabilities(RUN_ISO, "2026-08-16T20:00:00Z", "PT30M")
        )
        steps = wf.get_time_steps("layer")
        assert len(steps) == 5
        assert steps[1] - steps[0] == timedelta(minutes=30)

    def test_unparseable_interval_raises(self, monkeypatch):
        self._capabilities(
            monkeypatch, capabilities(RUN_ISO, "2026-08-18T18:00:00Z", "P1D")
        )
        with pytest.raises(ValueError, match="Unsupported time interval"):
            wf.get_time_steps("layer")


# ── fetch_point: ok / masked / failed ────────────────────────────────


class TestFetchPoint:
    def _session(self, monkeypatch, response, capture=None):
        def fake_get(url, params=None, timeout=None):
            if capture is not None:
                capture.update(params)
            if isinstance(response, Exception):
                raise response
            return response

        monkeypatch.setattr(wf.SESSION, "get", fake_get)

    def test_ok_value(self, monkeypatch):
        self._session(monkeypatch, FakeResponse(feature(0.718)))
        assert wf.fetch_point("layer", 49.337, -123.731, dt(STEP_ISO)) == (
            0.718,
            RUN_ISO,
            "ok",
        )

    def test_sentinel_is_masked_and_keeps_the_run(self, monkeypatch):
        """9999.0 is GRIB's missing value — the response is fine, the cell isn't."""
        self._session(monkeypatch, FakeResponse(feature(9999.0)))
        value, run, status = wf.fetch_point("layer", 49.3, -123.7, dt(STEP_ISO))
        assert (value, status) == (None, "masked")
        assert run == RUN_ISO

    def test_empty_feature_list_is_masked(self, monkeypatch):
        self._session(monkeypatch, FakeResponse({"features": []}))
        assert wf.fetch_point("layer", 49.3, -123.7, dt(STEP_ISO)) == (
            None,
            None,
            "masked",
        )

    def test_http_error_is_failed_not_masked(self, monkeypatch):
        self._session(monkeypatch, FakeResponse(status=500))
        assert wf.fetch_point("layer", 49.3, -123.7, dt(STEP_ISO))[2] == "failed"

    def test_transport_error_is_failed(self, monkeypatch):
        self._session(monkeypatch, TimeoutError("read timed out"))
        assert wf.fetch_point("layer", 49.3, -123.7, dt(STEP_ISO))[2] == "failed"

    def test_zero_is_a_value_not_a_gap(self, monkeypatch):
        """Flat calm is data. A falsy-value check here would drop it."""
        self._session(monkeypatch, FakeResponse(feature(0.0)))
        assert wf.fetch_point("layer", 49.3, -123.7, dt(STEP_ISO)) == (
            0.0,
            RUN_ISO,
            "ok",
        )

    def test_time_parameter_is_the_utc_instant(self, monkeypatch):
        params = {}
        self._session(monkeypatch, FakeResponse(feature(0.5)), capture=params)
        pacific = timezone(timedelta(hours=-7))
        wf.fetch_point(
            "layer", 49.337, -123.731, datetime(2026, 8, 17, 23, 0, tzinfo=pacific)
        )
        assert params["TIME"] == STEP_ISO

    def test_bbox_is_lat_lon_order_centred_on_the_station(self, monkeypatch):
        """WMS 1.3.0 + EPSG:4326 is lat,lon — swapping it queries Kazakhstan."""
        params = {}
        self._session(monkeypatch, FakeResponse(feature(0.5)), capture=params)
        wf.fetch_point("layer", 49.337, -123.731, dt(STEP_ISO))
        south, west, north, east = (float(v) for v in params["BBOX"].split(","))
        assert south < 49.337 < north
        assert west < -123.731 < east
        assert params["I"] == 5 and params["J"] == 5  # centre of a 10x10 grid


# ── fetch_station_forecast: assembling one station's run ─────────────


class TestFetchStationForecast:
    STATION = {"name": "Halibut Bank", "lat": 49.337, "lon": -123.731}
    WAVE_LAYERS = wf.SOURCES[wf.MODEL_NAME]
    WIND_LAYERS = wf.SOURCES[wf.WIND_MODEL_NAME]

    def _stub(self, monkeypatch, results):
        """results: (field, iso) -> (value, run, status)."""
        fields = {layer: field for field, layer in wf.VARIABLES.items()}

        def fake_fetch_point(layer, lat, lon, timestamp):
            key = (fields[layer], timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
            return results.get(key, (0.1, RUN_ISO, "ok"))

        monkeypatch.setattr(wf, "fetch_point", fake_fetch_point)

    def _fetch(self, layers, steps):
        return wf.fetch_station_forecast("4600146", self.STATION, layers, steps)

    def test_masked_step_is_recorded_but_omitted_from_json(self, monkeypatch):
        steps = [dt(RUN_ISO)]
        self._stub(
            monkeypatch,
            {("wind_wave_height", RUN_ISO): (None, RUN_ISO, "masked")},
        )
        forecast, readings, counts, run = self._fetch(self.WAVE_LAYERS, steps)

        assert "wind_wave_height" not in forecast[RUN_ISO]
        assert ("wind_wave_height", steps[0], None, "masked") in readings
        assert counts == {"ok": 3, "masked": 1, "failed": 0}

    def test_masked_gust_is_recorded(self, monkeypatch):
        """HRDPS masks the gust at most hours — that absence is data, not a gap."""
        steps = [dt(RUN_ISO)]
        self._stub(monkeypatch, {("wind_gust", RUN_ISO): (None, RUN_ISO, "masked")})
        forecast, readings, counts, _run = self._fetch(self.WIND_LAYERS, steps)

        assert "wind_gust" not in forecast[RUN_ISO]
        assert ("wind_gust", steps[0], None, "masked") in readings
        assert counts["masked"] == 1

    def test_failed_step_leaves_no_row(self, monkeypatch):
        """An absent row must only ever mean a failed fetch."""
        steps = [dt(RUN_ISO)]
        self._stub(
            monkeypatch, {("peak_period", RUN_ISO): (None, None, "failed")}
        )
        _forecast, readings, counts, _run = self._fetch(self.WAVE_LAYERS, steps)

        assert [r[0] for r in readings].count("peak_period") == 0
        assert counts["failed"] == 1

    def test_json_keys_are_utc_iso(self, monkeypatch):
        steps = [dt(RUN_ISO), dt(STEP_ISO)]
        self._stub(monkeypatch, {})
        forecast, _readings, _counts, _run = self._fetch(self.WAVE_LAYERS, steps)
        assert sorted(forecast) == [RUN_ISO, STEP_ISO]

    def test_mixed_runs_report_the_newest(self, monkeypatch):
        """A fetch straddling a run boundary is labelled with the newer run."""
        newer = "2026-08-17T00:00:00Z"
        steps = [dt(RUN_ISO)]
        self._stub(
            monkeypatch, {("wave_height", RUN_ISO): (0.5, newer, "ok")}
        )
        _f, _r, _c, run = self._fetch(self.WAVE_LAYERS, steps)
        assert run == newer

    def test_values_are_rounded_for_export(self, monkeypatch):
        steps = [dt(RUN_ISO)]
        self._stub(
            monkeypatch, {("wave_height", RUN_ISO): (0.7183333, RUN_ISO, "ok")}
        )
        forecast, _r, _c, _run = self._fetch(self.WAVE_LAYERS, steps)
        assert forecast[RUN_ISO]["wave_height"] == 0.718

    def test_wind_speed_is_converted_to_kmh(self, monkeypatch):
        """GeoMet serves m/s; the site stores km/h. 8.667 m/s = 31.2 km/h = 16.8 kt."""
        steps = [dt(STEP_ISO)]
        self._stub(monkeypatch, {("wind_speed", STEP_ISO): (8.6656656, RUN_ISO, "ok")})
        forecast, readings, _c, _run = self._fetch(self.WIND_LAYERS, steps)

        assert forecast[STEP_ISO]["wind_speed"] == 31.196
        speed = next(r for r in readings if r[0] == "wind_speed")
        assert speed[2] == pytest.approx(31.196, abs=0.001)

    def test_gust_is_converted_to_kmh(self, monkeypatch):
        steps = [dt(STEP_ISO)]
        self._stub(monkeypatch, {("wind_gust", STEP_ISO): (10.71, RUN_ISO, "ok")})
        forecast, _r, _c, _run = self._fetch(self.WIND_LAYERS, steps)
        assert forecast[STEP_ISO]["wind_gust"] == pytest.approx(38.556, abs=0.001)

    def test_wind_direction_is_not_converted(self, monkeypatch):
        """Degrees are degrees — only the speeds carry a unit change."""
        steps = [dt(STEP_ISO)]
        self._stub(monkeypatch, {("wind_direction", STEP_ISO): (276.2, RUN_ISO, "ok")})
        forecast, _r, _c, _run = self._fetch(self.WIND_LAYERS, steps)
        assert forecast[STEP_ISO]["wind_direction"] == 276.2


# ── store_forecast_to_db ─────────────────────────────────────────────


def readings_for(values):
    """[(field, iso, value, status)] -> the readings shape the fetcher emits."""
    return [(field, dt(iso), value, status) for field, iso, value, status in values]


COUNTS = {"ok": 1, "masked": 0, "failed": 0}


def store_wave(readings, counts=None, run_iso=RUN_ISO):
    wf.store_forecast_to_db(
        "4600146", wf.MODEL_NAME, readings, counts or COUNTS, run_iso
    )


def store_wind(readings, counts=None, run_iso=RUN_ISO):
    wf.store_forecast_to_db(
        "4600146", wf.WIND_MODEL_NAME, readings, counts or COUNTS, run_iso
    )


class TestStoreForecastToDb:
    def test_epochs_are_utc(self, wave_db):
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))
        with sqlite3.connect(wave_db) as conn:
            row = conn.execute(
                "SELECT forecast_run_time, valid_time, value, status FROM wave_forecast"
            ).fetchone()
        assert row == (RUN_EPOCH, STEP_EPOCH, 0.718, "ok")

    def test_every_row_carries_its_model(self, wave_db):
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))
        store_wind(readings_for([("wind_speed", STEP_ISO, 31.196, "ok")]))
        with sqlite3.connect(wave_db) as conn:
            rows = dict(conn.execute("SELECT variable, model FROM wave_forecast"))
        assert rows == {
            "wave_height": wf.MODEL_NAME,
            "wind_speed": wf.WIND_MODEL_NAME,
        }

    def test_two_models_sharing_a_run_hour_keep_separate_run_rows(self, wave_db):
        """The reason `model` is in the run table's key: both publish 18Z."""
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))
        store_wind(readings_for([("wind_speed", STEP_ISO, 31.196, "ok")]))
        with sqlite3.connect(wave_db) as conn:
            rows = conn.execute(
                "SELECT model, forecast_run_time FROM wave_forecast_run ORDER BY model"
            ).fetchall()
        assert rows == [
            (wf.WIND_MODEL_NAME, RUN_EPOCH),
            (wf.MODEL_NAME, RUN_EPOCH),
        ]

    def test_models_may_be_stored_at_different_runs(self, wave_db):
        """HRDPS and RDWPS publish at different minutes; a split is legitimate."""
        older = "2026-08-16T12:00:00Z"
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))
        store_wind(
            readings_for([("wind_speed", STEP_ISO, 31.196, "ok")]), run_iso=older
        )
        with sqlite3.connect(wave_db) as conn:
            rows = dict(
                conn.execute("SELECT variable, forecast_run_time FROM wave_forecast")
            )
        assert rows["wave_height"] == RUN_EPOCH
        assert rows["wind_speed"] == RUN_EPOCH - 6 * 3600

    def test_lead_time_is_derivable(self, wave_db):
        """valid_time - forecast_run_time is the axis every skill query groups by."""
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))
        with sqlite3.connect(wave_db) as conn:
            lead = conn.execute(
                "SELECT (valid_time - forecast_run_time) / 3600 FROM wave_forecast"
            ).fetchone()[0]
        assert lead == 36

    def test_masked_row_is_stored_with_null_value(self, wave_db):
        store_wave(
            readings_for([("wind_wave_height", STEP_ISO, None, "masked")]),
            counts={"ok": 0, "masked": 1, "failed": 0},
        )
        with sqlite3.connect(wave_db) as conn:
            row = conn.execute("SELECT value, status FROM wave_forecast").fetchone()
        assert row == (None, "masked")

    def test_run_row_records_provenance_and_counts(self, wave_db):
        counts = {"ok": 110, "masked": 22, "failed": 1}
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]), counts=counts)
        with sqlite3.connect(wave_db) as conn:
            row = conn.execute(
                "SELECT station_id, forecast_run_time, model, n_ok, n_masked, n_failed"
                " FROM wave_forecast_run"
            ).fetchone()
        assert row == ("4600146", RUN_EPOCH, wf.MODEL_NAME, 110, 22, 1)

    def test_refetching_a_run_replaces_rather_than_duplicates(self, wave_db):
        for value in (0.70, 0.718):
            store_wave(readings_for([("wave_height", STEP_ISO, value, "ok")]))
        with sqlite3.connect(wave_db) as conn:
            rows = conn.execute("SELECT value FROM wave_forecast").fetchall()
        assert rows == [(0.718,)]

    def test_no_run_time_writes_nothing(self, wave_db):
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]), run_iso=None)
        assert not wave_db.exists()

    def test_old_runs_are_purged_but_verification_survives(self, wave_db, monkeypatch):
        """Raw runs are disposable; the skill record is the long-term archive."""
        monkeypatch.setattr(wf.time, "time", lambda: float(RUN_EPOCH))
        stale_run = dt(RUN_ISO) - timedelta(days=wf.WAVE_FORECAST_RETENTION_DAYS + 1)
        stale_iso = stale_run.strftime("%Y-%m-%dT%H:%M:%SZ")

        store_wave(readings_for([("wave_height", stale_iso, 0.2, "ok")]), run_iso=stale_iso)
        with sqlite3.connect(wave_db) as conn:
            conn.execute(
                "INSERT INTO wave_forecast_verification (station_id, variable,"
                " forecast_run_time, valid_time, lead_hours, forecast_value,"
                " observed_value) VALUES ('4600146', 'wave_height', ?, ?, 36, 0.2, 0.3)",
                (int(stale_run.timestamp()), int(stale_run.timestamp()) + 36 * 3600),
            )
            conn.commit()

        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))

        with sqlite3.connect(wave_db) as conn:
            runs = conn.execute(
                "SELECT DISTINCT forecast_run_time FROM wave_forecast"
            ).fetchall()
            verifications = conn.execute(
                "SELECT COUNT(*) FROM wave_forecast_verification"
            ).fetchone()[0]
        assert runs == [(RUN_EPOCH,)]
        assert verifications == 1


# ── schema migration from the pre-2026-08-15 layout ─────────────────


class TestMigrateDbSchema:
    def _legacy_db(self, path, with_status=False):
        conn = sqlite3.connect(path)
        status_col = ", status TEXT NOT NULL DEFAULT 'ok'" if with_status else ""
        conn.execute(f"""
            CREATE TABLE wave_forecast (
                station_id TEXT NOT NULL,
                variable TEXT NOT NULL,
                forecast_run_time INTEGER NOT NULL,
                valid_time INTEGER NOT NULL,
                value REAL NOT NULL{status_col},
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
            )
        """)
        conn.execute(
            "INSERT INTO wave_forecast (station_id, variable, forecast_run_time,"
            " valid_time, value) VALUES ('4600146', 'wave_height', ?, ?, 0.5)",
            (RUN_EPOCH, STEP_EPOCH),
        )
        conn.commit()
        return conn

    def test_not_null_is_relaxed_and_rows_survive(self, tmp_path):
        path = tmp_path / "legacy.sqlite"
        conn = self._legacy_db(path)

        wf.migrate_db_schema(conn)

        conn.execute(
            "INSERT INTO wave_forecast (station_id, variable, forecast_run_time,"
            " valid_time, value, status) VALUES ('4600146', 'wind_wave_height',"
            " ?, ?, NULL, 'masked')",
            (RUN_EPOCH, STEP_EPOCH),
        )
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM wave_forecast").fetchone()[0] == 2
        conn.close()

    def test_indexes_are_recreated_after_the_rebuild(self, tmp_path):
        """They lived on the dropped table; losing them silently is the risk."""
        path = tmp_path / "legacy.sqlite"
        conn = self._legacy_db(path)
        wf.migrate_db_schema(conn)
        names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        assert "idx_wave_forecast_station_run" in names
        assert "idx_wave_forecast_lead" in names
        conn.close()

    def test_existing_status_values_are_carried_over(self, tmp_path):
        path = tmp_path / "legacy.sqlite"
        conn = self._legacy_db(path, with_status=True)
        conn.execute("UPDATE wave_forecast SET status = 'ok'")
        conn.commit()
        wf.migrate_db_schema(conn)
        assert conn.execute("SELECT status FROM wave_forecast").fetchone() == ("ok",)
        conn.close()

    def test_reference_value_column_is_added(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "legacy.sqlite")
        conn.execute("""
            CREATE TABLE wave_forecast_verification (
                station_id TEXT NOT NULL,
                variable TEXT NOT NULL,
                forecast_run_time INTEGER NOT NULL,
                valid_time INTEGER NOT NULL,
                lead_hours INTEGER NOT NULL,
                forecast_value REAL,
                observed_value REAL,
                obs_offset_seconds INTEGER,
                PRIMARY KEY (station_id, variable, forecast_run_time, valid_time)
            )
        """)
        conn.commit()

        wf.migrate_db_schema(conn)

        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(wave_forecast_verification)")
        }
        assert "reference_value" in cols
        conn.close()

    def test_model_column_is_added_and_backfilled(self, tmp_path):
        """Every pre-2026-08-17 row is RDWPS waves, matched on its variable."""
        path = tmp_path / "legacy.sqlite"
        conn = self._legacy_db(path)
        wf.migrate_db_schema(conn)
        assert conn.execute("SELECT model FROM wave_forecast").fetchall() == [
            (wf.MODEL_NAME,)
        ]
        conn.close()

    def test_unknown_variables_are_left_unlabelled(self, tmp_path):
        """Better a visible blank than a row asserted into the wrong model."""
        path = tmp_path / "legacy.sqlite"
        conn = self._legacy_db(path)
        conn.execute(
            "INSERT INTO wave_forecast (station_id, variable, forecast_run_time,"
            " valid_time, value) VALUES ('4600146', 'retired_field', ?, ?, 1.0)",
            (RUN_EPOCH, STEP_EPOCH),
        )
        conn.commit()

        wf.migrate_db_schema(conn)

        labels = dict(conn.execute("SELECT variable, model FROM wave_forecast"))
        assert labels["wave_height"] == wf.MODEL_NAME
        assert labels["retired_field"] == ""
        conn.close()

    def test_run_table_gains_model_in_its_primary_key(self, tmp_path):
        """Without this, the second model written each pass overwrites the first."""
        conn = sqlite3.connect(tmp_path / "legacy.sqlite")
        conn.execute("""
            CREATE TABLE wave_forecast_run (
                station_id TEXT NOT NULL,
                forecast_run_time INTEGER NOT NULL,
                fetched_at INTEGER NOT NULL,
                model TEXT NOT NULL,
                n_ok INTEGER NOT NULL DEFAULT 0,
                n_masked INTEGER NOT NULL DEFAULT 0,
                n_failed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (station_id, forecast_run_time)
            )
        """)
        conn.execute(
            "INSERT INTO wave_forecast_run (station_id, forecast_run_time,"
            " fetched_at, model, n_ok) VALUES ('4600146', ?, ?, ?, 132)",
            (RUN_EPOCH, RUN_EPOCH, wf.MODEL_NAME),
        )
        conn.commit()

        wf.migrate_db_schema(conn)

        pk = [row[1] for row in conn.execute("PRAGMA table_info(wave_forecast_run)") if row[5]]
        assert pk == ["station_id", "model", "forecast_run_time"]
        # The existing RDWPS run survives, and HRDPS can now share its hour.
        conn.execute(
            "INSERT INTO wave_forecast_run (station_id, model, forecast_run_time,"
            " fetched_at, n_ok) VALUES ('4600146', ?, ?, ?, 99)",
            (wf.WIND_MODEL_NAME, RUN_EPOCH, RUN_EPOCH),
        )
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM wave_forecast_run").fetchone()[0] == 2
        conn.close()

    def test_migration_is_idempotent(self, wave_db):
        store_wave(readings_for([("wave_height", STEP_ISO, 0.718, "ok")]))
        conn = sqlite3.connect(wave_db)
        wf.migrate_db_schema(conn)
        wf.migrate_db_schema(conn)
        assert conn.execute("SELECT COUNT(*) FROM wave_forecast").fetchone()[0] == 1
        assert conn.execute("SELECT model FROM wave_forecast").fetchone() == (
            wf.MODEL_NAME,
        )
        conn.close()


# ── module wiring ────────────────────────────────────────────────────


class TestConfiguration:
    def test_every_variable_has_a_unit(self):
        assert set(wf.VARIABLES) == set(wf.UNITS)

    def test_field_names_are_unique_across_models(self):
        """The storage keys rely on `variable` alone identifying the model.

        If two models ever published a field of the same name, one would
        silently overwrite the other in wave_forecast — the primary key does
        not include `model`. Adding a source means checking this holds.
        """
        named = [field for layers in wf.SOURCES.values() for field in layers]
        assert len(named) == len(set(named))

    def test_wind_fields_are_stored_in_kmh(self):
        """Site convention: store km/h, display knots. GeoMet serves m/s."""
        for field in ("wind_speed", "wind_gust"):
            assert wf.UNITS[field] == "km/h"
            assert field in wf.CONVERSIONS
        assert wf.CONVERSIONS["wind_speed"](10) == 36.0

    def test_only_speeds_are_converted(self):
        assert set(wf.CONVERSIONS) == {"wind_speed", "wind_gust"}

    def test_request_budget_matches_the_documented_footprint(self):
        """33 steps x 7 variables x 1 buoy = 231 point queries, plus one
        GetCapabilities per model = 233/run, 932/day at 4 runs (~1.1% of the
        86,400 guidance). Mirrors the table in docs/DATA_FEEDS.md."""
        published = [dt(RUN_ISO) + timedelta(hours=h) for h in range(49)]
        steps = taper_time_steps(published, wf.FINE_HORIZON_HOURS, wf.COARSE_STEP_HOURS)
        per_run = len(steps) * len(wf.VARIABLES) * len(wf.BUOY_IDS) + len(wf.SOURCES)
        assert len(steps) == 33
        assert per_run == 233
        assert per_run * 4 == 932
        assert per_run * 4 < 86_400 * 0.02  # MSC usage guidance

    def test_burst_rate_is_unchanged_by_the_extra_variables(self):
        """The guidance is a rate; only FETCH_DELAY moves it, never step count."""
        assert wf.FETCH_DELAY == 1.5
        assert 1 / (wf.FETCH_DELAY + 0.45) < 0.6  # req/s, measured network time

    def test_json_is_written_outside_the_repo_data_dir_allowlist(self, tmp_path, monkeypatch):
        """site/data is a public surface: only allowlisted fields may be written."""
        monkeypatch.setattr(wf, "OUTPUT_DIR", tmp_path)
        written = {}
        monkeypatch.setattr(
            wf, "safe_json_write", lambda path, data: written.update(data)
        )

        wf.save_forecast(
            "4600146",
            {"name": "Halibut Bank", "lat": 49.337, "lon": -123.731},
            {STEP_ISO: {"wave_height": 0.718, "wind_speed": 31.196}},
            {wf.MODEL_NAME: RUN_ISO, wf.WIND_MODEL_NAME: RUN_ISO},
        )

        assert set(written) == {
            "station_id",
            "station_name",
            "location",
            "generated_utc",
            "model",
            "model_run_time",
            "models",
            "units",
            "forecast",
        }
        assert written["model_run_time"] == RUN_ISO

    def test_json_carries_per_model_provenance(self, tmp_path, monkeypatch):
        """One series, two models — the payload has to say which is which."""
        monkeypatch.setattr(wf, "OUTPUT_DIR", tmp_path)
        written = {}
        monkeypatch.setattr(
            wf, "safe_json_write", lambda path, data: written.update(data)
        )
        older = "2026-08-16T12:00:00Z"

        wf.save_forecast(
            "4600146",
            {"name": "Halibut Bank", "lat": 49.337, "lon": -123.731},
            {STEP_ISO: {"wave_height": 0.718}},
            {wf.MODEL_NAME: RUN_ISO, wf.WIND_MODEL_NAME: older},
        )

        models = {entry["name"]: entry for entry in written["models"]}
        assert models[wf.MODEL_NAME]["run_time"] == RUN_ISO
        assert models[wf.WIND_MODEL_NAME]["run_time"] == older
        assert models[wf.WIND_MODEL_NAME]["variables"] == [
            "wind_direction",
            "wind_gust",
            "wind_speed",
        ]

    def test_a_model_that_failed_entirely_reports_a_null_run(self, tmp_path, monkeypatch):
        """Wind failing must not silently borrow the wave model's run time."""
        monkeypatch.setattr(wf, "OUTPUT_DIR", tmp_path)
        written = {}
        monkeypatch.setattr(
            wf, "safe_json_write", lambda path, data: written.update(data)
        )

        wf.save_forecast(
            "4600146",
            {"name": "Halibut Bank", "lat": 49.337, "lon": -123.731},
            {STEP_ISO: {"wave_height": 0.718}},
            {wf.MODEL_NAME: RUN_ISO},
        )

        models = {entry["name"]: entry for entry in written["models"]}
        assert models[wf.WIND_MODEL_NAME]["run_time"] is None
