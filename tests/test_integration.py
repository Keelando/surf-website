"""Integration tests: station registry consistency, JSON export schema, XML→SQLite round-trip, system health."""

import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.config import EXPORT_DIR, STATIONS_FILE
from lib.stations import STATIONS
from scripts.parse.wind_to_sqlite import insert_sqlite, parse_and_collect_fields

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ── Station registry consistency ─────────────────────────────


class TestStationRegistryConsistency:
    """Verify stations.json is internally consistent."""

    def test_no_duplicate_buoy_ids(self):
        raw = json.loads(STATIONS_FILE.read_text())
        buoy_ids = list(raw.get("buoys", {}).keys())
        assert len(buoy_ids) == len(set(buoy_ids))

    def test_no_duplicate_tide_keys(self):
        raw = json.loads(STATIONS_FILE.read_text())
        tide_keys = list(raw.get("tides", {}).keys())
        assert len(tide_keys) == len(set(tide_keys))

    def test_no_duplicate_wind_ids(self):
        raw = json.loads(STATIONS_FILE.read_text())
        wind_ids = list(raw.get("wind", {}).keys())
        assert len(wind_ids) == len(set(wind_ids))

    def test_tide_station_ids_unique(self):
        """DFO station IDs (like 07795) should be unique across all tide stations."""
        ids = [data["id"] for data in STATIONS.tides.values()]
        assert len(ids) == len(set(ids)), f"Duplicate tide IDs: {[i for i in ids if ids.count(i) > 1]}"

    def test_all_tide_stations_have_series(self):
        """Every tide station should declare which data series it has."""
        for key, data in STATIONS.tides.items():
            assert "series" in data, f"Tide station {key} missing 'series' field"
            assert isinstance(data["series"], list)

    def test_buoy_coordinates_are_plausible(self):
        for bid, data in STATIONS.buoys.items():
            lat, lon = data["lat"], data["lon"]
            assert -90 <= lat <= 90, f"{bid} lat {lat} invalid"
            assert -180 <= lon <= 180, f"{bid} lon {lon} invalid"

    def test_wind_coordinates_are_plausible(self):
        for sid, data in STATIONS.wind.items():
            lat, lon = data["lat"], data["lon"]
            assert -90 <= lat <= 90, f"{sid} lat {lat} invalid"
            assert -180 <= lon <= 180, f"{sid} lon {lon} invalid"


# ── JSON export schema validation ────────────────────────────


class TestJsonExportSchema:
    """Spot-check key fields in the exported JSON files."""

    @pytest.fixture
    def buoy_json(self):
        path = EXPORT_DIR / "latest_buoy_v2.json"
        if not path.exists():
            pytest.skip("latest_buoy_v2.json not found (exports may not have run)")
        return json.loads(path.read_text())

    @pytest.fixture
    def wind_json(self):
        path = EXPORT_DIR / "latest_wind.json"
        if not path.exists():
            pytest.skip("latest_wind.json not found")
        return json.loads(path.read_text())

    @pytest.fixture
    def tide_json(self):
        path = EXPORT_DIR / "tide-latest.json"
        if not path.exists():
            pytest.skip("tide-latest.json not found")
        return json.loads(path.read_text())

    def test_buoy_json_has_known_station(self, buoy_json):
        assert "4600146" in buoy_json, "Halibut Bank missing from buoy JSON"

    def test_buoy_station_has_expected_fields(self, buoy_json):
        halibut = buoy_json.get("4600146", {})
        for field in ["name", "observation_time", "stale"]:
            assert field in halibut, f"Halibut Bank missing '{field}'"

    def test_buoy_stale_is_boolean(self, buoy_json):
        for sid, data in buoy_json.items():
            if isinstance(data, dict) and "stale" in data:
                assert isinstance(data["stale"], bool), f"{sid} stale is not bool"

    def test_wind_json_has_known_station(self, wind_json):
        # At least one known EC wind station should be present
        known = {"CWGT", "CWSB", "CWEL", "CVTF"}
        present = set(wind_json.keys()) & known
        assert len(present) > 0, f"No known wind stations in JSON (keys: {list(wind_json.keys())[:5]})"

    def test_wind_station_has_expected_fields(self, wind_json):
        for sid, data in wind_json.items():
            if isinstance(data, dict):
                assert "name" in data or "station_name" in data, f"{sid} missing name"
                break

    def test_tide_json_has_known_station(self, tide_json):
        stations = tide_json.get("stations", tide_json)
        known = {"point_atkinson", "campbell_river", "crescent_pile"}
        present = set(stations.keys()) & known
        assert len(present) > 0, f"No known tide stations in JSON (keys: {list(stations.keys())[:5]})"

    def test_tide_station_has_observation(self, tide_json):
        stations = tide_json.get("stations", tide_json)
        # Check any station for expected structure
        for key, data in stations.items():
            if isinstance(data, dict) and ("observation" in data or "prediction_now" in data):
                return  # found at least one
        pytest.fail("No tide station has observation or prediction_now")


# ── System health ────────────────────────────────────────────


class TestSystemHealth:
    """Verify system health report meets minimum station availability."""

    HEALTH_FILE = EXPORT_DIR / "system_health.json"
    # Hard floor: if we're below this, fail regardless — pipeline is probably broken
    MIN_HEALTHY_STATIONS = 30

    @pytest.fixture
    def health_json(self):
        if not self.HEALTH_FILE.exists():
            pytest.skip("system_health.json not found")
        return json.loads(self.HEALTH_FILE.read_text())

    def test_storage_drive_mounted(self, health_json):
        storage = health_json.get("checks", {}).get("storage_mount", {})
        assert storage.get("mounted") is True, (
            "External storage drive /mnt/storage is not mounted — webcam archiving is disabled"
        )
        usage = storage.get("usage_percent", 0)
        free_gb = storage.get("free_gb", 0)
        print(f"\n  Storage: {usage}% used ({free_gb}GB free)")
        assert usage < 90, f"Storage critically full: {usage}%"

    @staticmethod
    def _pipeline_is_healthy(health_json):
        """Check whether our ingest pipeline is running (DBs written, exports fresh).

        If the pipeline is healthy but stations are stale, the outage is
        upstream at Environment Canada, not on our end.
        """
        # Databases should have recent writes
        dbs = health_json.get("checks", {}).get("database_integrity", {}).get("databases", {})
        total_writes = sum(db.get("recent_writes_1h", 0) for db in dbs.values())
        if total_writes == 0:
            return False, "no database writes in the last hour"

        # Exports should be reasonably fresh (at least some under 2 hours)
        exports = health_json.get("checks", {}).get("export_freshness", {}).get("exports", {})
        fresh_exports = sum(1 for e in exports.values() if e.get("age_minutes", 9999) < 120)
        if fresh_exports < 3:
            return False, f"only {fresh_exports} exports are fresh"

        return True, "databases active, exports fresh"

    def test_minimum_station_availability(self, health_json):
        freshness = health_json.get("checks", {}).get("data_freshness", {})
        total = freshness.get("total_stations", 0)
        stale = freshness.get("stale_count", 0)
        healthy = total - stale
        pct = (healthy / total * 100) if total else 0
        stale_stations = freshness.get("stale_stations", [])
        stale_names = [s.get("name", s.get("id")) for s in stale_stations]

        print(f"\n  Station health: {healthy}/{total} up ({pct:.0f}%)")
        if stale_names:
            print(f"  Down: {', '.join(stale_names)}")

        # Hard floor — something is very wrong
        assert healthy >= self.MIN_HEALTHY_STATIONS, (
            f"Only {healthy}/{total} stations healthy ({pct:.0f}%). "
            f"Down: {', '.join(stale_names)}"
        )

        # Soft check: if some stations are stale, distinguish our fault vs EC's
        expected_healthy = total - 3  # allow up to 3 stale before probing
        if healthy < expected_healthy:
            pipeline_ok, reason = self._pipeline_is_healthy(health_json)
            if pipeline_ok:
                # All stale stations are external — classify by type for the report
                by_type = {}
                for s in stale_stations:
                    by_type.setdefault(s.get("type", "unknown"), []).append(s.get("name", s.get("id")))
                detail = "; ".join(f"{t}: {', '.join(names)}" for t, names in by_type.items())
                print(f"  Pipeline healthy ({reason}) — stale stations are upstream (EC)")
                print(f"  Upstream outages: {detail}")
            else:
                pytest.fail(
                    f"Pipeline issue detected ({reason}). "
                    f"{stale} stations stale: {', '.join(stale_names)}"
                )


# ── XML → SQLite round-trip ──────────────────────────────────


class TestXmlToSqliteRoundtrip:
    """Parse a fixture XML and insert into an in-memory SQLite database."""

    @pytest.fixture
    def wind_db(self):
        """Create an in-memory wind database with the expected schema."""
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE wind_observation (
                station_id TEXT NOT NULL,
                station_name TEXT,
                observation_time INTEGER NOT NULL,
                wind_speed_kmh REAL,
                wind_gust_kmh REAL,
                wind_direction_deg REAL,
                air_temp_c REAL,
                pressure_hpa REAL,
                rainfall_1hr_mm REAL,
                rainfall_6hr_mm REAL,
                humidity_percent REAL,
                dewpoint_c REAL,
                pressure_mslp_hpa REAL,
                visibility_km REAL,
                source_file TEXT,
                UNIQUE(station_id, observation_time)
            )
        """)
        conn.commit()
        return conn

    def test_parse_and_insert_cwsb(self, wind_db):
        tree = ET.parse(FIXTURES_DIR / "wind" / "sample_CWSB.xml")
        result = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        assert result is not None

        station_id, station_name, timestamp, fields = result
        cur = wind_db.cursor()
        insert_sqlite(cur, station_id, station_name, fields["observation_time"], fields, "sample_CWSB.xml")
        wind_db.commit()

        # Verify data was inserted
        cur.execute("SELECT COUNT(*) FROM wind_observation WHERE station_id = 'CWSB'")
        assert cur.fetchone()[0] == 1

        # Verify field values
        cur.execute("SELECT wind_speed_kmh, air_temp_c, pressure_hpa FROM wind_observation WHERE station_id = 'CWSB'")
        row = cur.fetchone()
        assert row[0] is not None  # wind_speed
        assert row[1] == 9.8  # air_temp
        assert row[2] == 1011.0  # pressure

    def test_deduplication(self, wind_db):
        """INSERT OR IGNORE should prevent duplicate rows."""
        tree = ET.parse(FIXTURES_DIR / "wind" / "sample_CWSB.xml")
        result = parse_and_collect_fields(tree.getroot(), "2024-11-20-1400-CWSB-AUTO-minute-swob.xml")
        station_id, station_name, timestamp, fields = result

        cur = wind_db.cursor()
        insert_sqlite(cur, station_id, station_name, fields["observation_time"], fields, "file1.xml")
        insert_sqlite(cur, station_id, station_name, fields["observation_time"], fields, "file2.xml")
        wind_db.commit()

        cur.execute("SELECT COUNT(*) FROM wind_observation WHERE station_id = 'CWSB'")
        assert cur.fetchone()[0] == 1  # should still be 1

    def test_multiple_stations(self, wind_db):
        """Insert observations from different stations."""
        cur = wind_db.cursor()
        for xml_file, filename in [
            ("sample_CWSB.xml", "2024-11-20-1400-CWSB-AUTO-minute-swob.xml"),
            ("sample_CWGT.xml", "CWGT-AUTO-minute-swob.xml"),
            ("sample_CVTF.xml", "CVTF-AUTO-minute-swob.xml"),
        ]:
            tree = ET.parse(FIXTURES_DIR / "wind" / xml_file)
            result = parse_and_collect_fields(tree.getroot(), filename)
            if result:
                sid, sname, ts, fields = result
                insert_sqlite(cur, sid, sname, fields["observation_time"], fields, xml_file)
        wind_db.commit()

        cur.execute("SELECT COUNT(DISTINCT station_id) FROM wind_observation")
        count = cur.fetchone()[0]
        assert count >= 2  # at least 2 different stations
