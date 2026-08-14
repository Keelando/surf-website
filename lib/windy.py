"""
Windy Stations API v2 — shared configuration, credentials and read-back.

Windy replaced the Stations API in January 2026. The account-wide upload key
is gone; every station now has its own auto-generated password that both
uploads and reads its data. The legacy endpoint answers HTTP 410 and shuts
down entirely at the end of 2026.

This module exists so the pusher (`scripts/fetch/fetch_surrey_wave_v2.py`) and
the monitor (`scripts/monitoring/health_check.py`) share one definition of
which stations we publish and how their credentials are named. The monitor
cannot simply import the pusher: that module calls `require_env` for the
Surrey FlowWorks credentials at import time, so a missing Surrey key would
take the whole health check down with it.

Two traps are baked into the code below, both learned the hard way:

1. `read_station_status` returns an explicit allowlist of fields, never the
   raw response. The observation endpoint echoes the station's own `password`
   back inside its `header` block. The health check keeps Windy results out of
   `site/data/system_health.json` (Caddy serves that publicly) and logs them
   instead — the allowlist here is the second line of defence, so that a
   future caller which *does* publish its output cannot leak a credential.
2. The update endpoint answers HTTP 200 with a zero-length body whether or not
   the observation lands. A green push proves nothing; `read_station_status`
   is the only honest confirmation.

Credentials are per station, named `WINDY_<STATION>_ID` and
`WINDY_<STATION>_PASSWORD` in `config/.env`. See `docs/SECRETS.md`.
"""

from __future__ import annotations

import requests

from lib.env import get_env

# Both endpoints authenticate with the station password as a bearer token.
# Never pass it as the `PASSWORD` query parameter Windy also accepts:
# `requests` embeds the full URL in HTTPError strings, which would write the
# password verbatim into the caller's log file on every rejected request.
WINDY_UPDATE_URL = "https://stations.windy.com/api/v2/observation/update"
WINDY_READ_URL = "https://stations.windy.com/api/v2/observation"

# Stations published to Windy. Deliberately a fixed tuple rather than
# "whatever config/.env happens to hold", so adding a station stays an
# explicit, reviewable edit.
WINDY_PUSH_STATIONS = ("CRPILE", "CRCHAN", "COLEB")

# Set False to stop publishing without removing credentials. Resumed
# 2026-08-14 on the v2 API after 2.5 months paused.
WINDY_PUSH_ENABLED = True

# The only fields ever copied out of a Windy response. Everything else,
# including the echoed `password`, is dropped. Do not widen this without
# re-reading the module docstring.
SAFE_STATION_FIELDS = ("name", "is_online", "last_observation_time", "elev_m", "share_option")


def auth_headers(credentials: dict) -> dict:
    """Bearer-token header for one station's password."""
    return {"Authorization": f"Bearer {credentials['password']}"}


def load_windy_credentials(logger=None) -> dict[str, dict]:
    """Per-station identifier/password pairs from `config/.env`.

    A station is skipped unless it has both halves of its pair, so a partly
    configured account publishes the stations it can rather than failing
    outright.
    """
    credentials: dict[str, dict] = {}
    for station_key in WINDY_PUSH_STATIONS:
        station_id = get_env(f"WINDY_{station_key}_ID")
        password = get_env(f"WINDY_{station_key}_PASSWORD")
        if station_id and password:
            credentials[station_key] = {"id": station_id, "password": password}
        elif logger:
            logger.warning(f"{station_key}: no Windy credentials in config/.env - skipping")
    return credentials


def read_station_status(credentials: dict, timeout: int = 15) -> dict:
    """What Windy actually holds for one station.

    Returns only `SAFE_STATION_FIELDS` plus an `http_status`, or an `error`
    key on failure. This is the check that catches a station sitting Offline
    behind a stream of successful-looking uploads.
    """
    try:
        response = requests.get(
            WINDY_READ_URL,
            params={"id": credentials["id"], "latestLimit": 1},
            headers=auth_headers(credentials),
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"error": f"{type(e).__name__}: {e}"}

    if response.status_code != 200:
        return {"http_status": response.status_code, "error": f"HTTP {response.status_code}"}

    try:
        header = response.json().get("header", {})
    except ValueError:
        return {"http_status": 200, "error": "response was not JSON"}

    # Allowlist copy — see trap 1 in the module docstring.
    status = {field: header.get(field) for field in SAFE_STATION_FIELDS}
    status["http_status"] = 200
    return status
