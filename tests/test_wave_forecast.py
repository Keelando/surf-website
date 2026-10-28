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
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

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
def fresh_pacer(monkeypatch):
    """PACER is module state; each test starts with a calm one."""
    monkeypatch.setattr(wf, "PACER", wf.Pacer(wf.FETCH_DELAY, wf.BACKOFF_CEILING, wf.GIVE_UP_AFTER))


@pytest.fixture
def fixture_clock(monkeypatch):
    """Hold "now" a day after the fixture forecast, so the retention purge
    keeps it — on the real clock it aged past 60 days on 2026-10-17."""
    monkeypatch.setattr(wf.time, "time", lambda: STEP_EPOCH + 86400)


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
    WAVE_LAYERS = wf.layers_for(wf.MODEL_NAME, wf.STATIONS["4600146"])
    WIND_LAYERS = wf.layers_for(wf.WIND_MODEL_NAME, wf.STATIONS["4600146"])

    def _stub(self, monkeypatch, results):
        """results: (field, iso) -> (value, run, status)."""
        fields = {layer: field for field, layer in wf.VARIABLES.items()}

        def fake_fetch_point(layer, lat, lon, timestamp, on_error=None):
            key = (fields[layer], timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
            value, run, status = results.get(key, (0.1, RUN_ISO, "ok"))
            if status == "failed" and on_error:
                on_error("http_429")
            return value, run, status

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

    def test_failed_requests_become_fetch_error_rows(self, monkeypatch):
        steps = [dt(RUN_ISO)]
        self._stub(monkeypatch, {("peak_period", RUN_ISO): (None, None, "failed")})
        errors = []
        wf.fetch_station_forecast("4600146", self.STATION, self.WAVE_LAYERS, steps, errors)
        assert len(errors) == 1
        _at, station, layer, valid, kind, fetch_pass = errors[0]
        assert (station, layer, kind, fetch_pass) == (
            "4600146",
            wf.VARIABLES["peak_period"],
            "http_429",
            "main",
        )
        assert valid == int(dt(RUN_ISO).timestamp())


# ── Pacer: slow down while GeoMet pushes back ────────────────────────


class TestPacer:
    def _pacer(self, monkeypatch):
        slept = []
        monkeypatch.setattr(wf.time, "sleep", slept.append)
        return wf.Pacer(1.1, 30, 20), slept

    def test_normal_pace_is_the_fetch_delay(self, monkeypatch):
        pacer, slept = self._pacer(monkeypatch)
        pacer.wait("ok")
        pacer.wait("masked")
        assert slept == [1.1, 1.1]

    def test_failures_double_the_wait_up_to_the_ceiling(self, monkeypatch):
        pacer, slept = self._pacer(monkeypatch)
        for _ in range(7):
            pacer.wait("failed")
        assert slept == pytest.approx([2.2, 4.4, 8.8, 17.6, 30, 30, 30])

    def test_one_success_restores_the_normal_pace(self, monkeypatch):
        pacer, slept = self._pacer(monkeypatch)
        pacer.wait("failed")
        pacer.wait("failed")
        pacer.wait("ok")
        assert slept[-1] == 1.1
        assert pacer.consecutive_failures == 0

    def test_gives_up_after_too_many_failures_in_a_row(self, monkeypatch):
        pacer, _slept = self._pacer(monkeypatch)
        for _ in range(19):
            pacer.wait("failed")
        with pytest.raises(wf.GeoMetUnavailable):
            pacer.wait("failed")

    def test_backoff_never_raises_the_rate(self):
        """Backing off only lengthens waits, so the rate ceiling still holds."""
        assert wf.BACKOFF_CEILING > wf.FETCH_DELAY
        assert wf.GIVE_UP_AFTER >= 10  # a busy spell is not an outage


# ── error_kind: why a request failed ─────────────────────────────────


class TestErrorKind:
    def _http_error(self, code):
        response = requests.Response()
        response.status_code = code
        return requests.HTTPError(response=response)

    def test_429_is_its_own_kind(self):
        assert wf.error_kind(self._http_error(429)) == "http_429"

    def test_other_statuses_are_grouped_by_class(self):
        assert wf.error_kind(self._http_error(503)) == "http_5xx"
        assert wf.error_kind(self._http_error(404)) == "http_4xx"

    def test_refused_and_dropped_connections(self):
        refused = requests.ConnectionError("Failed to establish a new connection: [Errno 111] Connection refused")
        dropped = requests.ConnectionError(
            "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"
        )
        assert wf.error_kind(refused) == "refused"
        assert wf.error_kind(dropped) == "disconnected"

    def test_timeouts_including_connect_timeouts(self):
        assert wf.error_kind(requests.ReadTimeout()) == "timeout"
        assert wf.error_kind(requests.ConnectTimeout()) == "timeout"

    def test_unparseable_body(self):
        assert wf.error_kind(ValueError("Expecting value")) == "bad_response"


# ── Retry pass: one more try at the failed points ───────────────────


class TestRetryPass:
    STATION = {"name": "Halibut Bank", "lat": 49.337, "lon": -123.731}
    LAYERS = {"wave_height": wf.VARIABLES["wave_height"]}

    def _main_pass(self):
        steps = [dt(RUN_ISO), dt(STEP_ISO)]
        fetch = {
            "layers": self.LAYERS,
            "steps": steps,
            "forecast": {RUN_ISO: {"wave_height": 0.5}},
            "readings": [("wave_height", steps[0], 0.5, "ok")],
            "counts": {"ok": 1, "masked": 0, "failed": 1},
            "run_time": RUN_ISO,
        }
        return fetch, steps

    def _stub(self, monkeypatch, result):
        def fake_fetch_point(layer, lat, lon, timestamp, on_error=None):
            if result[2] == "failed" and on_error:
                on_error("refused")
            return result

        monkeypatch.setattr(wf, "fetch_point", fake_fetch_point)

    def test_failed_points_are_the_ones_without_a_reading(self):
        fetch, steps = self._main_pass()
        assert wf.failed_points(fetch["layers"], steps, fetch["readings"]) == [("wave_height", steps[1])]

    def test_a_recovered_point_fills_the_gap_and_the_counts(self, monkeypatch):
        fetch, steps = self._main_pass()
        self._stub(monkeypatch, (0.6, RUN_ISO, "ok"))
        failed = wf.failed_points(fetch["layers"], steps, fetch["readings"])
        assert wf.retry_failed_points("4600146", self.STATION, self.LAYERS, failed, fetch) == 1
        assert fetch["forecast"][STEP_ISO]["wave_height"] == 0.6
        assert fetch["counts"] == {"ok": 2, "masked": 0, "failed": 0}
        assert wf.failed_points(fetch["layers"], steps, fetch["readings"]) == []

    def test_a_point_from_a_newer_run_is_left_out(self, monkeypatch):
        """Two runs in one table would read as one forecast."""
        fetch, steps = self._main_pass()
        self._stub(monkeypatch, (0.6, "2026-08-17T00:00:00Z", "ok"))
        errors = []
        failed = wf.failed_points(fetch["layers"], steps, fetch["readings"])
        assert wf.retry_failed_points("4600146", self.STATION, self.LAYERS, failed, fetch, errors) == 0
        assert STEP_ISO not in fetch["forecast"]
        assert fetch["counts"]["failed"] == 1
        assert [row[4:] for row in errors] == [("run_changed", "retry")]

    def test_a_second_failure_is_logged_as_a_retry(self, monkeypatch):
        fetch, steps = self._main_pass()
        self._stub(monkeypatch, (None, None, "failed"))
        errors = []
        failed = wf.failed_points(fetch["layers"], steps, fetch["readings"])
        wf.retry_failed_points("4600146", self.STATION, self.LAYERS, failed, fetch, errors)
        assert [row[4:] for row in errors] == [("refused", "retry")]
        assert fetch["counts"]["failed"] == 1

    def test_errors_are_stored_and_pruned(self, wave_db, fixture_clock):
        now = int(wf.time.time())
        old = now - (wf.FETCH_ERROR_RETENTION_DAYS + 1) * 86400
        wf.store_fetch_errors(
            [
                (now, "4600146", "layer", now, "http_429", "main"),
                (old, "4600146", "layer", old, "refused", "main"),
            ]
        )
        conn = sqlite3.connect(wave_db)
        rows = conn.execute("SELECT kind, pass FROM fetch_error").fetchall()
        conn.close()
        assert rows == [("http_429", "main")]

    def test_summary_counts_kinds_most_frequent_first(self):
        rows = [(0, "s", "l", 0, k, "main") for k in ("refused", "http_429", "http_429")]
        assert wf.summarize_errors(rows) == "http_429×2, refused×1"


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


@pytest.mark.usefixtures("fixture_clock")
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


@pytest.mark.usefixtures("fixture_clock")
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

    def _requests_per_run(self):
        """Point queries in one run, counted per station.

        Not a multiplication any more: the in-Strait stations cost 7 variables
        a step, the open-Pacific ones 10 (waves and swell, no wind).
        """
        published = [dt(RUN_ISO) + timedelta(hours=h) for h in range(49)]
        steps = taper_time_steps(published, wf.FINE_HORIZON_HOURS, wf.COARSE_STEP_HOURS)
        per_run = sum(
            len(steps) * len(wf.layers_for(model, config))
            for config in wf.STATIONS.values()
            for model in wf.models_for(config)
        )
        return len(steps), per_run

    def test_request_budget_matches_the_documented_footprint(self):
        """33 steps x 7 variables = 231 point queries per station with wind;
        the two open-Pacific points skip wind (4 variables) and take swell
        (6 more), 330 each. Four of the one and two of the other, plus one
        GetCapabilities per model, is 1,586/run and 6,344/day at 4 runs —
        9.1% of the 86,400 guidance once storm surge is counted too. Mirrors
        the table in docs/DATA_FEEDS.md, which is maintained by hand — this
        test is what keeps it honest, so update both together when STATIONS
        or the step count changes."""
        steps, per_run_queries = self._requests_per_run()
        per_run = per_run_queries + len(wf.SOURCES)
        with_wind = [c for c in wf.STATIONS.values() if c.get("wind", True)]
        assert steps == 33
        assert len(wf.STATIONS) == 6
        assert len(with_wind) == 4
        assert per_run == 1_586
        assert per_run * 4 == 6_344
        assert per_run * 4 + 1_550 < 86_400 * 0.10  # MSC guidance, with storm surge

    def test_dropping_wind_actually_saves_the_requests_it_claims(self):
        """The open-Pacific points take waves only. If models_for() ever stops
        being honoured in the fetch loop the cost comes back silently — the
        budget above would still pass, because it asks the same function."""
        waves_only = [c for c in wf.STATIONS.values() if not c.get("wind", True)]
        assert len(waves_only) == 2
        assert all(wf.models_for(c) == (wf.MODEL_NAME,) for c in waves_only)
        source = Path(wf.__file__).read_text()
        assert "if model not in models_for(config):" in source, (
            "main() must skip models a station opts out of"
        )

    def test_swell_is_fetched_only_where_a_station_opts_in(self):
        """Swell partitions are masked almost every hour inside the Strait, so
        only the open-Pacific points take them; and they must actually reach
        the fetch loop and the budget, not just the config."""
        swell = {sid for sid, c in wf.STATIONS.items() if c.get("swell")}
        assert swell == {"46087", "4600206"}
        for sid, config in wf.STATIONS.items():
            fields = set(wf.layers_for(wf.MODEL_NAME, config))
            assert (wf.SWELL_FIELDS <= fields) == (sid in swell)
            assert {"wave_height", "peak_period", "wave_direction"} <= fields
            assert not wf.SWELL_FIELDS & set(wf.layers_for(wf.WIND_MODEL_NAME, config))
        source = Path(wf.__file__).read_text()
        assert "layers = layers_for(model, config)" in source, (
            "main() must fetch the per-station layer set, not all of SOURCES"
        )

    def test_swell_layers_are_the_rdwps_partitions(self):
        layers = wf.SOURCES[wf.MODEL_NAME]
        assert {layers[f] for f in wf.SWELL_FIELDS} == {
            f"RDWPS_2.5km_{n}Swell{v}"
            for n in ("First", "Second")
            for v in ("SignificantWaveHeight", "PeakWavePeriod", "MeanWaveDir")
        }
        assert wf.UNITS["swell_direction"] == "degrees_true_from"

    def test_stale_lock_threshold_exceeds_a_full_run(self):
        """acquire_lock() removes a lock it judges stale. Runtime scales with
        STATIONS (~7.6 min each with wind, ~4.3 without), so a threshold that
        does not scale with it will eventually declare a live fetch dead and
        let two runs overlap."""
        _, per_run = self._requests_per_run()
        runtime_s = per_run * (wf.FETCH_DELAY + 0.45)
        source = Path(wf.__file__).read_text()
        threshold = int(re.search(r"if age > (\d+):", source).group(1))
        assert threshold > runtime_s * 1.5, (
            f"stale-lock threshold {threshold}s is too close to the "
            f"~{runtime_s / 60:.0f} min runtime of {len(wf.STATIONS)} stations"
        )
        assert threshold < 6 * 3600  # cleared before the next scheduled run

    def test_burst_rate_is_unchanged_by_the_extra_variables(self):
        """The guidance is a rate; only FETCH_DELAY moves it, never step count."""
        assert wf.FETCH_DELAY == 1.1
        assert 1 / (wf.FETCH_DELAY + 0.45) < 0.7  # req/s, measured network time

    def test_the_rate_stays_under_the_guidance_even_with_no_network_time(self):
        """The ceiling is what matters, not the typical case.

        Our rate depends on server response time we do not control. A slower
        server only lowers it, so the worst case is instant responses — the
        delay alone. That has to stay under MSC's ~1 req/s WITH HEADROOM, not
        sit exactly on it: sizing to a best case is how you end up over a limit
        on the day the server gets faster. At 0.5 s this ceiling would be
        2.0 req/s; at 1.0 s it would be exactly 1.0.
        """
        assert 1 / wf.FETCH_DELAY <= 0.95

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
            wf.STATIONS["4600146"],
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
            wf.STATIONS["4600146"],
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
            wf.STATIONS["4600146"],
            {STEP_ISO: {"wave_height": 0.718}},
            {wf.MODEL_NAME: RUN_ISO},
        )

        models = {entry["name"]: entry for entry in written["models"]}
        assert models[wf.WIND_MODEL_NAME]["run_time"] is None
