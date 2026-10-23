"""The DFO tide fetcher's limits: windows fit the API, and catch-up and horizon are bounded."""

from datetime import datetime, timedelta, timezone

from scripts.parse import tide_to_sqlite as tide

NOW = datetime(2026, 10, 23, 10, 6, tzinfo=timezone.utc)


def test_windows_never_exceed_the_api_span():
    windows = tide.query_windows(NOW, NOW + timedelta(days=8))
    assert len(windows) == 2
    assert all(end - start <= tide.MAX_QUERY_SPAN for start, end in windows)
    assert windows[0][0] == NOW and windows[-1][1] == NOW + timedelta(days=8)
    assert windows[0][1] == windows[1][0]  # contiguous


def test_short_range_is_one_window():
    assert tide.query_windows(NOW, NOW + timedelta(days=2)) == [(NOW, NOW + timedelta(days=2))]


def test_prediction_horizon_is_capped():
    assert tide.PREDICTION_HORIZON <= tide.MAX_PREDICTION_HORIZON
    # And the capped horizon is at most three requests per series.
    assert len(tide.query_windows(NOW, NOW + tide.MAX_PREDICTION_HORIZON)) <= 3


def test_request_budget_covers_a_normal_run():
    stations = len(tide.load_stations())
    # metadata + 1 observation window + prediction and high/low windows
    windows = len(tide.query_windows(NOW, NOW + tide.PREDICTION_HORIZON))
    per_station = 1 + 1 + windows + (windows + 1)
    assert stations * per_station <= tide.MAX_REQUESTS_PER_RUN


def test_safe_get_stops_at_the_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(tide, "_requests_made", tide.MAX_REQUESTS_PER_RUN)
    monkeypatch.setattr(tide.requests, "get", lambda *a, **k: calls.append(a))
    assert tide.safe_get("https://example.invalid/") is None
    assert calls == []
