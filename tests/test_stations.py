"""Tests for lib/stations.py — station registry lookups and filtering."""

import json

import pytest

from lib.stations import STATIONS, StationRegistry

# ── Registry loading ─────────────────────────────────────────


class TestRegistryLoading:
    def test_loads_from_default_path(self):
        assert STATIONS._data is not None

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            StationRegistry(tmp_path / "nonexistent.json")

    def test_loads_custom_file(self, tmp_path):
        minimal = {"buoys": {}, "tides": {}, "wind": {}, "_metadata": {}}
        f = tmp_path / "stations.json"
        f.write_text(json.dumps(minimal))
        reg = StationRegistry(f)
        assert reg.buoys == {}
        assert reg.tides == {}
        assert reg.wind == {}


# ── Buoy lookups ─────────────────────────────────────────────


class TestBuoyLookups:
    def test_get_all_buoys_not_empty(self):
        assert len(STATIONS.buoys) > 0

    def test_get_known_buoy(self):
        halibut = STATIONS.get_buoy("4600146")
        assert halibut is not None
        assert halibut["name"] == "Halibut Bank"
        assert "lat" in halibut
        assert "lon" in halibut

    def test_get_nonexistent_buoy(self):
        assert STATIONS.get_buoy("FAKE_ID") is None

    def test_buoys_by_source(self):
        ec_buoys = STATIONS.get_buoys_by_source("Environment Canada")
        assert len(ec_buoys) > 0
        for _, data in ec_buoys.items():
            assert data["source"] == "Environment Canada"

    def test_buoys_by_nonexistent_source(self):
        assert STATIONS.get_buoys_by_source("FakeSource") == {}

    def test_validate_buoy_id(self):
        assert STATIONS.validate_buoy_id("4600146") is True
        assert STATIONS.validate_buoy_id("FAKE") is False


# ── Tide station lookups ─────────────────────────────────────


class TestTideLookups:
    def test_get_all_tides_not_empty(self):
        assert len(STATIONS.tides) > 0

    def test_get_known_tide_station(self):
        pa = STATIONS.get_tide_station("point_atkinson")
        assert pa is not None
        assert "name" in pa
        assert "id" in pa
        assert "lat" in pa

    def test_get_nonexistent_tide_station(self):
        assert STATIONS.get_tide_station("fake_station") is None

    def test_validate_tide_station(self):
        assert STATIONS.validate_tide_station("point_atkinson") is True
        assert STATIONS.validate_tide_station("fake") is False

    def test_filter_has_observations(self):
        obs = STATIONS.get_tide_stations(has_observations=True)
        for key, data in obs.items():
            assert "wlo" in data.get("series", []), f"{key} missing 'wlo' series"

    def test_filter_has_predictions(self):
        pred = STATIONS.get_tide_stations(has_predictions=True)
        for key, data in pred.items():
            assert "wlp" in data.get("series", []), f"{key} missing 'wlp' series"

    def test_filter_no_observations(self):
        no_obs = STATIONS.get_tide_stations(has_observations=False)
        for key, data in no_obs.items():
            assert "wlo" not in data.get("series", []), f"{key} has 'wlo' series"

    def test_get_tide_station_by_id(self):
        # Point Atkinson uses a MongoDB-style ID, DFO code is in "code" field
        pa = STATIONS.get_tide_station("point_atkinson")
        station_id = pa["id"]
        result = STATIONS.get_tide_station_by_id(station_id)
        assert result is not None
        assert result["name"] == "Point Atkinson"

    def test_get_tide_station_by_bad_id(self):
        assert STATIONS.get_tide_station_by_id("99999") is None


# ── Wind station lookups ─────────────────────────────────────


class TestWindLookups:
    def test_get_all_wind_not_empty(self):
        assert len(STATIONS.wind) > 0

    def test_get_known_wind_station(self):
        sisters = STATIONS.get_wind_station("CWGT")
        assert sisters is not None
        assert "name" in sisters

    def test_get_nonexistent_wind_station(self):
        assert STATIONS.get_wind_station("FAKE") is None


# ── Coordinates ──────────────────────────────────────────────


class TestCoordinates:
    def test_get_all_coordinates(self):
        coords = STATIONS.get_all_coordinates()
        assert len(coords) > 0
        for c in coords:
            assert "type" in c
            assert "lat" in c
            assert "lon" in c
            assert c["type"] in ("buoy", "tide")

    def test_coordinates_are_numeric(self):
        coords = STATIONS.get_all_coordinates()
        for c in coords:
            assert isinstance(c["lat"], (int, float))
            assert isinstance(c["lon"], (int, float))

    def test_coordinates_in_pacific_northwest(self):
        """All stations should be roughly in the Pacific Northwest region."""
        coords = STATIONS.get_all_coordinates()
        for c in coords:
            assert 45 < c["lat"] < 55, f"{c['name']} lat {c['lat']} out of range"
            assert -130 < c["lon"] < -120, f"{c['name']} lon {c['lon']} out of range"


# ── Metadata ─────────────────────────────────────────────────


class TestMetadata:
    def test_metadata_exists(self):
        meta = STATIONS.metadata
        assert isinstance(meta, dict)


# ── Station data integrity ───────────────────────────────────


class TestStationDataIntegrity:
    def test_all_buoys_have_required_fields(self):
        for bid, data in STATIONS.buoys.items():
            assert "name" in data, f"Buoy {bid} missing 'name'"
            assert "lat" in data, f"Buoy {bid} missing 'lat'"
            assert "lon" in data, f"Buoy {bid} missing 'lon'"
            assert "source" in data, f"Buoy {bid} missing 'source'"

    def test_all_tide_stations_have_required_fields(self):
        for key, data in STATIONS.tides.items():
            assert "name" in data, f"Tide {key} missing 'name'"
            assert "lat" in data, f"Tide {key} missing 'lat'"
            assert "lon" in data, f"Tide {key} missing 'lon'"
            assert "id" in data, f"Tide {key} missing 'id'"

    def test_all_wind_stations_have_required_fields(self):
        for sid, data in STATIONS.wind.items():
            assert "name" in data, f"Wind {sid} missing 'name'"
            assert "lat" in data, f"Wind {sid} missing 'lat'"
            assert "lon" in data, f"Wind {sid} missing 'lon'"


# ── Lightstation coordinates ─────────────────────────────────


class TestLightstationCoordinates:
    """Guards on the lightstation coordinates specifically.

    Every entry is transcribed from the Canadian Coast Guard's *List of
    Lights, Buoys and Fog Signals — Pacific Coast*, which gives each light to
    a tenth of an arcsecond. Two entries were not: McInnes Island and Egg
    Island had been pinned off a map to two decimal places, putting McInnes
    24 km from the light (a user reported it) and Egg Island 3 km out.

    A bounding box would not have caught either — both were plausible BC
    coastal points. The tell was the rounding, so that is what is asserted.
    """

    # Wide enough for Langara Island (54.26 N, 133.06 W) at the northwest
    # corner of Haida Gwaii and Trial Islands (48.40 N) off Victoria.
    LAT_RANGE = (48.0, 55.0)
    LON_RANGE = (-134.0, -122.0)

    def test_within_bc_coast(self):
        for sid, data in STATIONS.lightstations.items():
            lat, lon = data["lat"], data["lon"]
            assert self.LAT_RANGE[0] < lat < self.LAT_RANGE[1], f"{sid} lat {lat} out of range"
            assert self.LON_RANGE[0] < lon < self.LON_RANGE[1], f"{sid} lon {lon} out of range"

    def test_not_rounded_to_two_decimals(self):
        """Two decimal places is ~1 km — an eyeballed pin, not a transcription."""
        for sid, data in STATIONS.lightstations.items():
            for field in ("lat", "lon"):
                value = data[field]
                decimals = len(str(value).partition(".")[2])
                assert decimals >= 3, (
                    f"{sid} {field}={value} has {decimals} decimal places; "
                    "take the value from the CCG List of Lights, Pacific Coast"
                )
