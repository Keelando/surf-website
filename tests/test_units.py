"""Tests for lib/units.py — unit conversion utilities."""

import pytest
from lib.units import kmh_to_knots, knots_to_kmh, ms_to_kmh, meters_to_feet


# ── km/h ↔ knots ──────────────────────────────────────────────

class TestKmhToKnots:
    def test_known_values(self):
        assert kmh_to_knots(0) == 0.0
        assert kmh_to_knots(10) == 5.4
        assert kmh_to_knots(100) == 54.0

    def test_string_numeric(self):
        assert kmh_to_knots("25") == 13.5

    def test_none(self):
        assert kmh_to_knots(None) is None

    def test_non_numeric_string(self):
        assert kmh_to_knots("MM") is None
        assert kmh_to_knots("") is None

    def test_negative(self):
        """Negative speeds are physically meaningless but shouldn't crash."""
        result = kmh_to_knots(-10)
        assert result is not None


class TestKnotsToKmh:
    def test_known_values(self):
        assert knots_to_kmh(0) == 0.0
        assert knots_to_kmh(10) == 18.5
        assert knots_to_kmh(20) == 37.0

    def test_none(self):
        assert knots_to_kmh(None) is None

    def test_non_numeric(self):
        assert knots_to_kmh("NA") is None


class TestRoundtrip:
    """kmh→knots→kmh should be approximately identity."""

    @pytest.mark.parametrize("kmh", [0, 10, 25, 50, 100, 150])
    def test_roundtrip_within_tolerance(self, kmh):
        result = knots_to_kmh(kmh_to_knots(kmh))
        assert abs(result - kmh) < 0.2, f"{kmh} km/h roundtripped to {result}"


# ── m/s → km/h ────────────────────────────────────────────────

class TestMsToKmh:
    def test_known_values(self):
        assert ms_to_kmh(0) == 0.0
        assert ms_to_kmh(1) == 3.6
        assert ms_to_kmh(10) == 36.0

    def test_none(self):
        assert ms_to_kmh(None) is None

    def test_string_numeric(self):
        assert ms_to_kmh("5.5") == 19.8

    def test_non_numeric(self):
        assert ms_to_kmh("M") is None


# ── meters → feet ─────────────────────────────────────────────

class TestMetersToFeet:
    def test_known_values(self):
        assert meters_to_feet(0) == 0.0
        assert meters_to_feet(1) == 3.3
        assert meters_to_feet(2) == 6.6

    def test_none(self):
        assert meters_to_feet(None) is None

    def test_string_numeric(self):
        assert meters_to_feet("3.0") == 9.8

    def test_non_numeric(self):
        assert meters_to_feet("NA") is None
