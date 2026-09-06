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


# ── Display labels ───────────────────────────────────────────


class TestDisplayLabels:
    """The label a station is named by must identify exactly one station.

    `name` alone does not: four places carry both a tide gauge and a wind
    station of the same name (Point Atkinson, Tsawwassen, Tofino, White Rock),
    and Entrance Island is both a wind station and a lightstation. The
    disambiguation lives in `short_name`, which is also what
    `health_check.display_name()` reports and what the footer's status badge
    prints when a station goes down. Before 2026-09-06 the footer kept its own
    map instead and fell back to the first word of the name, so Cape Mudge,
    Cape Beale and Cape Scott all rendered as "Cape".
    """

    GROUPS = ("buoys", "tides", "wind", "lightstations")

    def _labels(self):
        """(label, group, key) for every station, as the footer would show it."""
        out = []
        for group in self.GROUPS:
            for key, data in getattr(STATIONS, group).items():
                out.append((data.get("short_name") or data["name"], group, key))
        return out

    @staticmethod
    def _key(label):
        """Fold a label to what a reader actually distinguishes by."""
        return "".join(c for c in label.lower() if c.isalnum())

    def test_labels_are_unique_across_every_station_type(self):
        seen = {}
        clashes = []
        for label, group, key in self._labels():
            if label in seen:
                clashes.append(f"{label!r}: {seen[label]} and {group}/{key}")
            seen[label] = f"{group}/{key}"
        assert not clashes, "labels shared by more than one station:\n  " + "\n  ".join(clashes)

    def test_no_label_is_a_truncation_of_another(self):
        """Exact equality is too weak a bar.

        "Entrance Is." and "Entrance Island" are different strings and name
        different stations — a wind station and a lightstation — but nobody
        reading a status badge would tell them apart. So the rule is prefix
        distinctness after folding away case and punctuation: no label may be
        the start of another. That is what catches an abbreviation being
        introduced on one side of a pair and not the other, which is exactly
        how the old footer map went wrong.
        """
        entries = [(self._key(label), label, group, key) for label, group, key in self._labels()]
        entries.sort()
        clashes = []
        for i, (folded, label, group, key) in enumerate(entries):
            for other_folded, other_label, other_group, other_key in entries[i + 1 :]:
                if not other_folded.startswith(folded):
                    break  # sorted, so nothing further can share this prefix
                clashes.append(
                    f"{label!r} ({group}/{key}) is a truncation of "
                    f"{other_label!r} ({other_group}/{other_key})"
                )
        assert not clashes, "confusable labels:\n  " + "\n  ".join(clashes)

    def test_a_station_sharing_a_name_carries_a_short_name(self):
        """The twins specifically: whichever way a future edit goes, at least
        one of a same-named pair must carry a short_name, or they collapse."""
        by_name = {}
        for group in self.GROUPS:
            for key, data in getattr(STATIONS, group).items():
                by_name.setdefault(data["name"], []).append((group, key, data))
        for name, entries in by_name.items():
            if len(entries) < 2:
                continue
            labelled = [e for e in entries if e[2].get("short_name")]
            assert len(labelled) >= len(entries) - 1, (
                f"{name!r} is used by {len(entries)} stations "
                f"({', '.join(f'{g}/{k}' for g, k, _ in entries)}) "
                f"but only {len(labelled)} carry a short_name"
            )

    def test_short_names_stay_short(self):
        """They exist to keep the status badge readable; a long one defeats
        the point and the badge is a single line in the footer."""
        for label, group, key in self._labels():
            data = getattr(STATIONS, group)[key]
            if data.get("short_name"):
                assert len(data["short_name"]) <= 18, f"{group}/{key}: {data['short_name']!r}"


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
