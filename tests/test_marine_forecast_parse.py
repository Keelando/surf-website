"""Tests for marine text-forecast parsing across multiple EC zone files.

The parser slugifies zone and area keys straight out of the XML, so these
tests are mostly about that contract: a new zone should flow through without
any parser edit, and areas must not collide or overwrite each other.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.parse import parse_marine_forecast as pmf

# ── Sample zone XML ────────────────────────────────────────────

GEORGIA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<marineData>
  <dateTime name="xmlCreation" zone="UTC" UTCOffset="0">
    <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
    <hour>17</hour><minute>27</minute>
  </dateTime>
  <area countryCode="CA" region="Pacific Coast" subRegion="Georgia Basin">Strait of Georgia</area>
  <warnings>
    <location name="Strait of Georgia - south of Nanaimo">
      <event name="Strong wind warning" status="IN EFFECT" category="wind">
        <dateTime name="Issued" zone="UTC" UTCOffset="0">
          <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
          <hour>16</hour><minute>00</minute>
        </dateTime>
      </event>
    </location>
  </warnings>
  <regularForecast>
    <dateTime name="Issued" zone="UTC" UTCOffset="0">
      <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
      <hour>17</hour><minute>30</minute>
    </dateTime>
    <location name="Strait of Georgia - north of Nanaimo">
      <weatherCondition>
        <periodOfCoverage>Today Tonight and Friday.</periodOfCoverage>
        <wind>Wind northwest 5 to 15 knots.</wind>
        <weatherVisibility>Showers Friday evening.</weatherVisibility>
      </weatherCondition>
    </location>
    <location name="Strait of Georgia - south of Nanaimo">
      <weatherCondition>
        <periodOfCoverage>Today Tonight and Friday.</periodOfCoverage>
        <wind>Wind northwest 15 to 20 knots.</wind>
      </weatherCondition>
    </location>
  </regularForecast>
  <extendedForecast>
    <location>
      <weatherCondition>
        <forecastPeriod name="Saturday">Wind variable 5 to 15 knots.</forecastPeriod>
      </weatherCondition>
    </location>
  </extendedForecast>
</marineData>
"""

# A zone the parser has never seen, with a warning but no regular forecast entry.
JUAN_DE_FUCA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<marineData>
  <dateTime name="xmlCreation" zone="UTC" UTCOffset="0">
    <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
    <hour>17</hour><minute>05</minute>
  </dateTime>
  <area countryCode="CA" region="Pacific Coast" subRegion="Georgia Basin">Juan de Fuca Strait</area>
  <warnings>
    <location name="Juan de Fuca Strait - east">
      <event name="Gale warning" status="IN EFFECT" category="wind">
        <dateTime name="Issued" zone="UTC" UTCOffset="0">
          <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
          <hour>16</hour><minute>30</minute>
        </dateTime>
      </event>
    </location>
  </warnings>
  <regularForecast>
    <dateTime name="Issued" zone="UTC" UTCOffset="0">
      <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
      <hour>17</hour><minute>10</minute>
    </dateTime>
    <location name="Juan de Fuca Strait - west">
      <weatherCondition>
        <periodOfCoverage>Today and Friday.</periodOfCoverage>
        <wind>Wind west 20 to 30 knots.</wind>
      </weatherCondition>
    </location>
  </regularForecast>
</marineData>
"""


@pytest.fixture
def zone_dir(tmp_path, monkeypatch):
    """A stand-in for the sr3 delivery directory."""

    def write(zone_code, stamp, content):
        path = tmp_path / f"{stamp}_MSC_MarineWeather_{zone_code}_en.xml"
        path.write_text(content)
        return path

    monkeypatch.setattr(pmf, "DATA_DIR", tmp_path)
    return write


# ── slugify ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Strait of Georgia - north of Nanaimo", "strait_of_georgia_north_of_nanaimo"),
        ("Juan de Fuca Strait", "juan_de_fuca_strait"),
        ("Queen Charlotte Sound", "queen_charlotte_sound"),
        ("  Howe Sound  ", "howe_sound"),
        ("", ""),
        (None, ""),
    ],
)
def test_slugify(name, expected):
    assert pmf.slugify(name) == expected


# ── single zone file ───────────────────────────────────────────


def test_parses_both_georgia_zones(zone_dir):
    zone_dir("m0000028", "20260820T172713.893Z", GEORGIA_XML)

    doc = pmf.build_document()
    area = doc["areas"]["strait_of_georgia"]

    assert area["area"] == "Strait of Georgia"
    assert area["sub_region"] == "Georgia Basin"
    assert area["zone_code"] == "m0000028"
    assert set(area["locations"]) == {
        "strait_of_georgia_north_of_nanaimo",
        "strait_of_georgia_south_of_nanaimo",
    }

    north = area["locations"]["strait_of_georgia_north_of_nanaimo"]
    assert north["forecast"]["wind"] == "Wind northwest 5 to 15 knots."
    assert north["forecast"]["weather"] == "Showers Friday evening."
    assert north["issued_utc"] == "2026-08-20T17:30:00+00:00"
    assert north["warnings"] == []

    south = area["locations"]["strait_of_georgia_south_of_nanaimo"]
    assert [w["type"] for w in south["warnings"]] == ["Strong wind warning"]
    # A zone with no weatherVisibility must not invent an empty key
    assert "weather" not in south["forecast"]

    assert area["extended_forecast"] == [{"period": "Saturday", "forecast": "Wind variable 5 to 15 knots."}]
    assert doc["generated_utc"] == "2026-08-20T17:27:00+00:00"


# ── multiple zone files ────────────────────────────────────────


def test_new_zone_needs_no_parser_change(zone_dir):
    """The whole point of slugified keys: a zone the parser never knew about."""
    zone_dir("m0000028", "20260820T172713.893Z", GEORGIA_XML)
    zone_dir("m0000104", "20260820T170500.000Z", JUAN_DE_FUCA_XML)

    doc = pmf.build_document()

    assert set(doc["areas"]) == {"juan_de_fuca_strait", "strait_of_georgia"}
    jdf = doc["areas"]["juan_de_fuca_strait"]
    assert jdf["locations"]["juan_de_fuca_strait_west"]["forecast"]["wind"] == "Wind west 20 to 30 knots."

    # Top-level timestamp is the newest area, not the last one parsed
    assert doc["generated_utc"] == "2026-08-20T17:27:00+00:00"


def test_warning_only_zone_still_appears(zone_dir):
    """A zone can carry a warning without a regularForecast entry."""
    zone_dir("m0000104", "20260820T170500.000Z", JUAN_DE_FUCA_XML)

    locations = pmf.build_document()["areas"]["juan_de_fuca_strait"]["locations"]

    assert "juan_de_fuca_strait_east" in locations
    east = locations["juan_de_fuca_strait_east"]
    assert east["zone_name"] == "Juan de Fuca Strait - east"
    assert [w["type"] for w in east["warnings"]] == ["Gale warning"]
    assert "forecast" not in east


def test_only_newest_file_per_zone_is_used(zone_dir):
    """sr3 keeps up to a day of files; stale ones must not win."""
    stale = GEORGIA_XML.replace("Wind northwest 5 to 15 knots.", "STALE WIND")
    old = zone_dir("m0000028", "20260820T060000.000Z", stale)
    new = zone_dir("m0000028", "20260820T172713.893Z", GEORGIA_XML)
    import os

    os.utime(old, (1_000_000, 1_000_000))
    os.utime(new, (2_000_000, 2_000_000))

    area = pmf.build_document()["areas"]["strait_of_georgia"]
    north = area["locations"]["strait_of_georgia_north_of_nanaimo"]
    assert north["forecast"]["wind"] == "Wind northwest 5 to 15 knots."


def test_two_zone_codes_one_area_merge(zone_dir):
    """Distinct zone codes describing the same area must merge, not clobber."""
    other_half = GEORGIA_XML.replace(
        '<location name="Strait of Georgia - north of Nanaimo">',
        '<location name="Strait of Georgia - central">',
    ).replace("<hour>17</hour><minute>27</minute>", "<hour>16</hour><minute>00</minute>")
    zone_dir("m0000028", "20260820T172713.893Z", GEORGIA_XML)
    zone_dir("m0000029", "20260820T160000.000Z", other_half)

    area = pmf.build_document()["areas"]["strait_of_georgia"]

    assert "strait_of_georgia_central" in area["locations"]
    assert "strait_of_georgia_north_of_nanaimo" in area["locations"]
    # Metadata comes from the newer file
    assert area["zone_code"] == "m0000028"


# ── degenerate input ───────────────────────────────────────────


def test_unparsable_file_is_skipped_not_fatal(zone_dir):
    zone_dir("m0000028", "20260820T172713.893Z", GEORGIA_XML)
    zone_dir("m0000999", "20260820T170000.000Z", "<marineData><area>")

    doc = pmf.build_document()

    assert set(doc["areas"]) == {"strait_of_georgia"}


def test_empty_directory_yields_no_areas(zone_dir):
    doc = pmf.build_document()

    assert doc["areas"] == {}
    assert doc["generated_utc"] is None


# ── extended forecast, repeated per location ───────────────────

# A zone bulletin repeats the extended forecast once per location it covers,
# and a single-location zone omits the location's name attribute entirely.
MULTI_LOCATION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<marineData>
  <dateTime name="xmlCreation" zone="UTC" UTCOffset="0">
    <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
    <hour>22</hour><minute>57</minute>
  </dateTime>
  <area countryCode="CA" region="Pacific Coast" subRegion="Georgia Basin">Juan de Fuca Strait</area>
  <regularForecast>
    <dateTime name="Issued" zone="UTC" UTCOffset="0">
      <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
      <hour>23</hour><minute>00</minute>
    </dateTime>
    <location name="Juan de Fuca Strait - east entrance">
      <weatherCondition><wind>Wind light.</wind></weatherCondition>
    </location>
    <location name="Juan de Fuca Strait - west entrance">
      <weatherCondition><wind>Wind west 5 to 15 knots.</wind></weatherCondition>
    </location>
  </regularForecast>
  <extendedForecast>
    <location name="Juan de Fuca Strait - east entrance">
      <weatherCondition>
        <forecastPeriod name="Saturday">Wind west 15 to 25 knots.</forecastPeriod>
        <forecastPeriod name="Sunday">Wind west 5 to 15 knots.</forecastPeriod>
      </weatherCondition>
    </location>
    <location name="Juan de Fuca Strait - west entrance">
      <weatherCondition>
        <forecastPeriod name="Saturday">Wind west 15 to 25 knots.</forecastPeriod>
        <forecastPeriod name="Sunday">Wind west 5 to 15 knots.</forecastPeriod>
      </weatherCondition>
    </location>
  </extendedForecast>
</marineData>
"""

# A single-location zone: no name attribute anywhere.
UNNAMED_LOCATION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<marineData>
  <dateTime name="xmlCreation" zone="UTC" UTCOffset="0">
    <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
    <hour>22</hour><minute>57</minute>
  </dateTime>
  <area countryCode="CA" region="Pacific Coast" subRegion="Georgia Basin">Howe Sound</area>
  <regularForecast>
    <dateTime name="Issued" zone="UTC" UTCOffset="0">
      <year>2026</year><month name="August">08</month><day name="Thursday">20</day>
      <hour>23</hour><minute>00</minute>
    </dateTime>
    <location>
      <weatherCondition><wind>Wind northerly outflow 5 to 10 knots.</wind></weatherCondition>
    </location>
  </regularForecast>
  <extendedForecast>
    <location>
      <weatherCondition>
        <forecastPeriod name="Saturday">Wind southerly inflow 10 to 20 knots.</forecastPeriod>
      </weatherCondition>
    </location>
  </extendedForecast>
</marineData>
"""


def test_extended_forecast_is_not_repeated_per_location(zone_dir):
    """The bug: flattening every location's block gave 2 locations x 2 days."""
    zone_dir("m0000009", "20260820T225315.849Z", MULTI_LOCATION_XML)

    area = pmf.build_document()["areas"]["juan_de_fuca_strait"]

    assert [e["period"] for e in area["extended_forecast"]] == ["Saturday", "Sunday"]
    for zone_key in ("juan_de_fuca_strait_east_entrance", "juan_de_fuca_strait_west_entrance"):
        periods = area["locations"][zone_key]["extended_forecast"]
        assert [e["period"] for e in periods] == ["Saturday", "Sunday"]


def test_single_location_zone_falls_back_to_the_area_name(zone_dir):
    """Howe Sound and WCVI South omit the location name, so they had no zone."""
    zone_dir("m0000102", "20260820T225751.252Z", UNNAMED_LOCATION_XML)

    area = pmf.build_document()["areas"]["howe_sound"]

    assert list(area["locations"]) == ["howe_sound"]
    assert area["locations"]["howe_sound"]["zone_name"] == "Howe Sound"
    assert area["locations"]["howe_sound"]["forecast"]["wind"].startswith("Wind northerly")
    # An unnamed extended block covers the area; it must not mint a second zone.
    assert [e["period"] for e in area["extended_forecast"]] == ["Saturday"]
