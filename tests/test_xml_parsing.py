"""Tests for SWOB-ML XML parsing (wind_to_sqlite.py parse_and_collect_fields)."""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.parse.wind_to_sqlite import FIELD_MAP, parse_and_collect_fields

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "wind"


# ── Parse real XML fixtures ──────────────────────────────────


class TestParseRealXML:
    def test_parse_cwsb(self):
        """Parse Point Atkinson (CWSB) wind observation."""
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        result = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        assert result is not None
        station_id, station_name, timestamp, fields = result
        assert station_id == "CWSB"
        assert "observation_time" in fields

    def test_cwsb_extracts_wind_speed(self):
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        _, _, _, fields = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        assert "wind_speed_kmh" in fields
        assert isinstance(fields["wind_speed_kmh"], float)

    def test_cwsb_extracts_temperature(self):
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        _, _, _, fields = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        assert "air_temp_c" in fields
        assert fields["air_temp_c"] == 9.8

    def test_cwsb_extracts_pressure(self):
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        _, _, _, fields = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        assert "pressure_hpa" in fields
        assert fields["pressure_hpa"] == 1011.0

    def test_cwsb_wind_direction_is_int(self):
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        _, _, _, fields = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        if "wind_direction_deg" in fields:
            assert isinstance(fields["wind_direction_deg"], int)

    def test_parse_cwgt(self):
        """Parse Sisters Island (CWGT) wind observation."""
        tree = ET.parse(FIXTURES_DIR / "sample_CWGT.xml")
        result = parse_and_collect_fields(tree.getroot(), "CWGT-AUTO-minute-swob.xml")
        assert result is not None
        station_id, _, _, _ = result
        assert station_id == "CWGT"

    def test_parse_cvtf(self):
        """Parse Tsawwassen (CVTF) wind observation."""
        tree = ET.parse(FIXTURES_DIR / "sample_CVTF.xml")
        result = parse_and_collect_fields(tree.getroot(), "CVTF-AUTO-minute-swob.xml")
        assert result is not None
        station_id, _, _, _ = result
        assert station_id == "CVTF"


# ── Station ID extraction from filenames ─────────────────────


class TestStationIdFromFilename:
    """Test the two filename formats for extracting station IDs."""

    def _parse_with_filename(self, filename):
        """Parse the CWSB fixture with a custom filename."""
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        result = parse_and_collect_fields(tree.getroot(), filename)
        if result is None:
            return None
        return result[0]  # station_id

    def test_format_date_prefix(self):
        sid = self._parse_with_filename("2025-11-19-2307-CWSB-AUTO-minute-swob.xml")
        assert sid == "CWSB"

    def test_format_station_prefix(self):
        sid = self._parse_with_filename("CWGT-AUTO-swob.xml")
        assert sid == "CWGT"

    def test_format_man_station(self):
        sid = self._parse_with_filename("CYVR-MAN-swob.xml")
        assert sid == "CYVR"

    def test_no_filename_falls_back_to_xml(self):
        """Without a filename, should fall back to XML identification elements."""
        tree = ET.parse(FIXTURES_DIR / "sample_CWSB.xml")
        result = parse_and_collect_fields(tree.getroot(), filename=None)
        # Should still get a station ID from XML fields
        assert result is not None


# ── Missing / invalid data handling ──────────────────────────


class TestMissingDataHandling:
    def test_msng_values_skipped(self):
        """MSNG (missing) values in SWOB-ML should not appear in parsed fields."""
        xml_str = """<?xml version="1.0"?>
        <om:ObservationCollection xmlns:om="http://www.opengis.net/om/1.0"
            xmlns="http://dms.ec.gc.ca/schema/point-observation/2.0"
            xmlns:gml="http://www.opengis.net/gml">
        <om:member><om:Observation>
        <om:metadata><set><identification-elements>
            <element name="tc_id" value="WSB"/>
            <element name="stn_nam" value="TEST"/>
        </identification-elements></set></om:metadata>
        <om:samplingTime><gml:TimeInstant>
            <gml:timePosition>2024-11-20T14:00:00.000Z</gml:timePosition>
        </gml:TimeInstant></om:samplingTime>
        <om:result><elements>
            <element name="avg_wnd_spd_pst10mts" value="MSNG"/>
            <element name="air_temp" value="10.5"/>
        </elements></om:result>
        </om:Observation></om:member>
        </om:ObservationCollection>"""
        root = ET.fromstring(xml_str)
        result = parse_and_collect_fields(root, filename=None)
        assert result is not None
        _, _, _, fields = result
        assert "wind_speed_kmh" not in fields
        assert "air_temp_c" in fields
        assert fields["air_temp_c"] == 10.5

    def test_empty_xml_returns_none(self):
        """XML with no timestamp should return None."""
        xml_str = """<?xml version="1.0"?>
        <om:ObservationCollection xmlns:om="http://www.opengis.net/om/1.0"
            xmlns="http://dms.ec.gc.ca/schema/point-observation/2.0"
            xmlns:gml="http://www.opengis.net/gml">
        <om:member><om:Observation>
        <om:metadata><set><identification-elements>
        </identification-elements></set></om:metadata>
        <om:result><elements></elements></om:result>
        </om:Observation></om:member>
        </om:ObservationCollection>"""
        root = ET.fromstring(xml_str)
        result = parse_and_collect_fields(root, filename=None)
        assert result is None

    def test_no_mapped_fields_returns_none(self):
        """XML with timestamp but no known fields should return None."""
        xml_str = """<?xml version="1.0"?>
        <om:ObservationCollection xmlns:om="http://www.opengis.net/om/1.0"
            xmlns="http://dms.ec.gc.ca/schema/point-observation/2.0"
            xmlns:gml="http://www.opengis.net/gml">
        <om:member><om:Observation>
        <om:metadata><set><identification-elements>
            <element name="tc_id" value="WSB"/>
        </identification-elements></set></om:metadata>
        <om:samplingTime><gml:TimeInstant>
            <gml:timePosition>2024-11-20T14:00:00.000Z</gml:timePosition>
        </gml:TimeInstant></om:samplingTime>
        <om:result><elements>
            <element name="unknown_field" value="42"/>
        </elements></om:result>
        </om:Observation></om:member>
        </om:ObservationCollection>"""
        root = ET.fromstring(xml_str)
        result = parse_and_collect_fields(root, filename=None)
        assert result is None


# ── Field map coverage ───────────────────────────────────────


class TestFieldMap:
    def test_field_map_not_empty(self):
        assert len(FIELD_MAP) > 0

    def test_all_mapped_values_are_known_columns(self):
        known_columns = {
            "wind_speed_kmh", "wind_gust_kmh", "wind_direction_deg",
            "air_temp_c", "pressure_hpa", "rainfall_1hr_mm", "rainfall_6hr_mm",
            "humidity_percent", "dewpoint_c", "pressure_mslp_hpa", "visibility_km",
        }
        for src, dest in FIELD_MAP.items():
            assert dest in known_columns, f"FIELD_MAP['{src}'] = '{dest}' not in known columns"

    def test_wind_speed_has_multiple_aliases(self):
        """Wind speed should map from multiple SWOB-ML field names."""
        speed_keys = [k for k, v in FIELD_MAP.items() if v == "wind_speed_kmh"]
        assert len(speed_keys) >= 2
