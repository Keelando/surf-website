"""Tests for lib/directions.py — compass direction utilities."""

import pytest
from lib.directions import (
    degrees_to_cardinal,
    cardinal_to_degrees,
    is_offshore_wind,
    DIRS_16,
    DIRS_8,
)


# ── degrees → cardinal ────────────────────────────────────────

class TestDegreesToCardinal16:
    """16-point compass tests."""

    @pytest.mark.parametrize("degrees, expected", [
        (0, "N"),
        (45, "NE"),
        (90, "E"),
        (135, "SE"),
        (180, "S"),
        (225, "SW"),
        (270, "W"),
        (315, "NW"),
    ])
    def test_cardinal_points(self, degrees, expected):
        assert degrees_to_cardinal(degrees) == expected

    @pytest.mark.parametrize("degrees, expected", [
        (22.5, "NNE"),
        (67.5, "ENE"),
        (112.5, "ESE"),
        (157.5, "SSE"),
        (202.5, "SSW"),
        (247.5, "WSW"),
        (292.5, "WNW"),
        (337.5, "NNW"),
    ])
    def test_intercardinal_points(self, degrees, expected):
        assert degrees_to_cardinal(degrees) == expected

    def test_wraps_at_360(self):
        assert degrees_to_cardinal(360) == "N"
        assert degrees_to_cardinal(720) == "N"

    def test_negative_degrees(self):
        assert degrees_to_cardinal(-90) == "W"

    def test_boundary_north(self):
        """359° and 1° should both be N."""
        assert degrees_to_cardinal(359) == "N"
        assert degrees_to_cardinal(1) == "N"


class TestDegreesToCardinal8:
    """8-point compass tests."""

    @pytest.mark.parametrize("degrees, expected", [
        (0, "N"),
        (45, "NE"),
        (90, "E"),
        (135, "SE"),
        (180, "S"),
        (225, "SW"),
        (270, "W"),
        (315, "NW"),
    ])
    def test_cardinal_points(self, degrees, expected):
        assert degrees_to_cardinal(degrees, points=8) == expected

    def test_rounds_to_nearest(self):
        """20° is closer to N than NE in 8-point."""
        assert degrees_to_cardinal(20, points=8) == "N"


class TestDegreesToCardinalEdge:
    def test_none(self):
        assert degrees_to_cardinal(None) is None

    def test_non_numeric(self):
        assert degrees_to_cardinal("NW") is None

    def test_string_numeric(self):
        assert degrees_to_cardinal("270") == "W"

    def test_invalid_points(self):
        with pytest.raises(ValueError):
            degrees_to_cardinal(0, points=32)


# ── cardinal → degrees ────────────────────────────────────────

class TestCardinalToDegrees:
    @pytest.mark.parametrize("cardinal, expected", [
        ("N", 0),
        ("E", 90),
        ("S", 180),
        ("W", 270),
        ("NE", 45),
        ("NW", 315),
    ])
    def test_known_values(self, cardinal, expected):
        assert cardinal_to_degrees(cardinal) == expected

    def test_case_insensitive(self):
        assert cardinal_to_degrees("nw") == 315
        assert cardinal_to_degrees("Sw") == 225

    def test_16_point(self):
        assert cardinal_to_degrees("NNE") == 22
        assert cardinal_to_degrees("WSW") == 247

    def test_none(self):
        assert cardinal_to_degrees(None) is None

    def test_invalid(self):
        assert cardinal_to_degrees("NORTH") is None
        assert cardinal_to_degrees("X") is None


class TestCardinalRoundtrip:
    """degrees → cardinal → degrees should be consistent for exact compass points."""

    @pytest.mark.parametrize("direction", DIRS_8)
    def test_8point_roundtrip(self, direction):
        degrees = cardinal_to_degrees(direction)
        assert degrees is not None
        result = degrees_to_cardinal(degrees, points=8)
        assert result == direction


# ── offshore wind detection ───────────────────────────────────

class TestIsOffshoreWind:
    def test_direct_offshore(self):
        """Wind from same direction as coast bearing = offshore."""
        assert is_offshore_wind(270, 270) is True

    def test_direct_onshore(self):
        """Wind from opposite direction = onshore."""
        assert is_offshore_wind(90, 270) is False

    def test_crossshore(self):
        """Wind at exactly 90° to coast = boundary (not offshore)."""
        assert is_offshore_wind(0, 270) is False
        assert is_offshore_wind(180, 270) is False

    def test_oblique_offshore(self):
        """Wind within 90° of coast bearing = offshore."""
        assert is_offshore_wind(300, 270) is True
        assert is_offshore_wind(240, 270) is True

    def test_none_handling(self):
        assert is_offshore_wind(None, 270) is None
        assert is_offshore_wind(270, None) is None

    def test_non_numeric(self):
        assert is_offshore_wind("NW", 270) is None
