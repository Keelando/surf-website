"""Tests for lib/windy.py.

The important one is `test_read_station_status_never_returns_password`. The
Windy observation endpoint echoes each station's own password back inside its
`header` block, and callers of this module feed monitoring output. If the
allowlist in `read_station_status` ever widens to a raw copy, a credential can
reach a log or a file under `site/data/`, which Caddy serves publicly.
"""

import lib.windy as windy


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


# Shape of a real response, including the fields we must never copy out.
_HEADER = {
    "name": "Test Station",
    "is_online": True,
    "last_observation_time": "2026-08-14T00:00:00.000Z",
    "elev_m": 5,
    "share_option": "public",
    "password": "SUPERSECRETSTATIONPASSWORD",
    "userId": 11279207,
    "lat": 49.0,
    "lon": -122.9,
}


def test_read_station_status_never_returns_password(monkeypatch):
    monkeypatch.setattr(windy.requests, "get", lambda *a, **k: _FakeResponse({"header": _HEADER}))

    status = windy.read_station_status({"id": "abc", "password": "SUPERSECRETSTATIONPASSWORD"})

    # Neither the key nor the value may survive, under any nesting.
    assert "password" not in status
    assert "SUPERSECRETSTATIONPASSWORD" not in repr(status)
    assert "userId" not in status

    # The fields we do rely on still come through.
    assert status["name"] == "Test Station"
    assert status["is_online"] is True
    assert status["http_status"] == 200


def test_safe_fields_exclude_credentials():
    """Guard the allowlist itself against a careless widening."""
    for forbidden in ("password", "userId", "token", "key"):
        assert forbidden not in windy.SAFE_STATION_FIELDS


def test_read_station_status_handles_http_error(monkeypatch):
    monkeypatch.setattr(windy.requests, "get", lambda *a, **k: _FakeResponse({}, status_code=401))

    status = windy.read_station_status({"id": "abc", "password": "x"})

    assert status["http_status"] == 401
    assert "error" in status


def test_read_station_status_handles_network_error(monkeypatch):
    def _boom(*a, **k):
        raise windy.requests.RequestException("connection reset")

    monkeypatch.setattr(windy.requests, "get", _boom)

    status = windy.read_station_status({"id": "abc", "password": "x"})

    assert "error" in status
    assert "RequestException" in status["error"]


def test_credentials_require_both_halves(monkeypatch):
    """A station with only half a pair is skipped, not half-configured."""
    values = {
        "WINDY_CRPILE_ID": "id-1",
        "WINDY_CRPILE_PASSWORD": "pw-1",
        "WINDY_CRCHAN_ID": "id-2",  # password missing
        "WINDY_COLEB_PASSWORD": "pw-3",  # id missing
    }
    monkeypatch.setattr(windy, "get_env", lambda name: values.get(name))

    credentials = windy.load_windy_credentials()

    assert set(credentials) == {"CRPILE"}
    assert credentials["CRPILE"] == {"id": "id-1", "password": "pw-1"}


def test_auth_uses_bearer_header_not_query_param():
    """The password must never travel in a URL — requests logs URLs on error."""
    headers = windy.auth_headers({"id": "abc", "password": "pw"})

    assert headers == {"Authorization": "Bearer pw"}
    assert "PASSWORD" not in windy.WINDY_UPDATE_URL
    assert "PASSWORD" not in windy.WINDY_READ_URL
