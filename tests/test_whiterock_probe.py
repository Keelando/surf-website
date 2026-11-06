"""The White Rock out-of-service probe's verdict (scripts/fetch/fetch_whiterock_weather.py).

In 2026-11 the sensors moved off their October freeze and froze again at new
values; a probe that only compared one look with the stored freeze said
"BACK ONLINE?" every day. The verdict now needs movement between two looks.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "fetch"))

from fetch_whiterock_weather import probe_verdict  # noqa: E402

OCTOBER = {"wind_speed": 1.9, "wind_direction": 166.0, "wind_gust": 3.3, "humidity": 89.0, "pressure": 1011.6}
NOVEMBER = {"wind_speed": 11.7, "wind_direction": 192.0, "wind_gust": 22.4, "humidity": 89.0, "pressure": 1014.8}


def test_frozen_at_the_stored_values_is_still_down():
    assert probe_verdict(OCTOBER, OCTOBER, OCTOBER) == "STILL DOWN (feed publishing, sensors still frozen)"


def test_frozen_again_at_new_values_is_still_down():
    verdict = probe_verdict(NOVEMBER, NOVEMBER, OCTOBER)
    assert verdict.startswith("STILL DOWN (sensors frozen again, at new values:")
    assert "wind_speed 11.7" in verdict


def test_moving_between_looks_is_back_online():
    later = {**NOVEMBER, "wind_speed": 13.2, "wind_direction": 188.0}
    verdict = probe_verdict(NOVEMBER, later, OCTOBER)
    assert verdict.startswith("BACK ONLINE? wind_speed, wind_direction moving")
    assert "wind_speed 11.7 -> 13.2" in verdict


def test_a_missing_field_is_not_movement():
    assert probe_verdict(NOVEMBER, {**NOVEMBER, "humidity": None}, OCTOBER).startswith("STILL DOWN")
    assert probe_verdict({**NOVEMBER, "humidity": None}, NOVEMBER, OCTOBER).startswith("STILL DOWN")
