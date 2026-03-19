"""Tests for lib/config.py — path resolution, constants, and safe_json_write."""

import json

from lib.config import (
    BUOY_DATABASE,
    BUOY_FRESHNESS_WINDOW,
    BUOY_RETENTION_DAYS,
    DATA_DIR,
    DECIMALS,
    EXPORT_DIR,
    MAX_WAVE_HEIGHT,
    MAX_WIND_SPEED,
    MIN_VALID_PRESSURE,
    PROJECT_ROOT,
    STATIONS_FILE,
    STORM_SURGE_DATABASE,
    TIDE_DATABASE,
    TIDE_FRESHNESS_WINDOW,
    UPDATE_FREQ,
    WIND_DATABASE,
    ensure_directories,
    get_database_info,
    safe_json_write,
)

# ── Path resolution ──────────────────────────────────────────


class TestPathResolution:
    def test_project_root_exists(self):
        assert PROJECT_ROOT.exists()

    def test_project_root_contains_lib(self):
        assert (PROJECT_ROOT / "lib").is_dir()

    def test_data_dir_is_absolute(self):
        assert DATA_DIR.is_absolute()

    def test_export_dir_under_project(self):
        assert str(EXPORT_DIR).startswith(str(PROJECT_ROOT))

    def test_stations_file_exists(self):
        assert STATIONS_FILE.exists()

    def test_database_paths_are_absolute(self):
        for db in [BUOY_DATABASE, TIDE_DATABASE, WIND_DATABASE, STORM_SURGE_DATABASE]:
            assert db.is_absolute(), f"{db} is not absolute"

    def test_database_paths_in_data_dir(self):
        for db in [BUOY_DATABASE, TIDE_DATABASE, WIND_DATABASE, STORM_SURGE_DATABASE]:
            assert str(db).startswith(str(DATA_DIR)), f"{db} not under {DATA_DIR}"


# ── Constants ────────────────────────────────────────────────


class TestConstants:
    def test_freshness_windows_are_positive(self):
        assert BUOY_FRESHNESS_WINDOW > 0
        assert TIDE_FRESHNESS_WINDOW > 0

    def test_retention_days_positive(self):
        assert BUOY_RETENTION_DAYS > 0

    def test_thresholds_reasonable(self):
        assert MAX_WAVE_HEIGHT > 0
        assert MAX_WIND_SPEED > 0
        assert MIN_VALID_PRESSURE < 1013  # below standard atmosphere

    def test_decimals_dict(self):
        assert isinstance(DECIMALS, dict)
        for key, val in DECIMALS.items():
            assert isinstance(val, int)
            assert val >= 0

    def test_update_freq_dict(self):
        assert isinstance(UPDATE_FREQ, dict)
        for key, val in UPDATE_FREQ.items():
            assert isinstance(val, (int, float))
            assert val > 0


# ── safe_json_write ──────────────────────────────────────────


class TestSafeJsonWrite:
    def test_writes_valid_json(self, tmp_path):
        out = tmp_path / "test.json"
        data = {"key": "value", "num": 42}
        safe_json_write(out, data)
        result = json.loads(out.read_text())
        assert result == data

    def test_overwrites_existing(self, tmp_path):
        out = tmp_path / "test.json"
        safe_json_write(out, {"old": True})
        safe_json_write(out, {"new": True})
        result = json.loads(out.read_text())
        assert result == {"new": True}

    def test_creates_parent_dirs(self, tmp_path):
        out = tmp_path / "sub" / "dir" / "test.json"
        safe_json_write(out, {"nested": True})
        assert out.exists()

    def test_no_tmp_file_left(self, tmp_path):
        out = tmp_path / "test.json"
        safe_json_write(out, {"clean": True})
        tmp_file = out.with_suffix(".json.tmp")
        assert not tmp_file.exists()

    def test_sort_keys(self, tmp_path):
        out = tmp_path / "test.json"
        safe_json_write(out, {"b": 2, "a": 1}, sort_keys=True)
        text = out.read_text()
        assert text.index('"a"') < text.index('"b"')

    def test_indent(self, tmp_path):
        out = tmp_path / "test.json"
        safe_json_write(out, {"k": "v"}, indent=4)
        text = out.read_text()
        assert "    " in text  # 4-space indent


# ── ensure_directories ───────────────────────────────────────


class TestEnsureDirectories:
    def test_does_not_raise(self):
        """Should be safe to call multiple times."""
        ensure_directories()
        ensure_directories()


# ── get_database_info ────────────────────────────────────────


class TestGetDatabaseInfo:
    def test_returns_all_databases(self):
        info = get_database_info()
        assert "buoy_database" in info
        assert "tide_database" in info
        assert "storm_surge_database" in info
        assert "wind_database" in info

    def test_each_entry_has_expected_keys(self):
        info = get_database_info()
        for name, entry in info.items():
            assert "path" in entry
            assert "exists" in entry
            assert "size_mb" in entry
