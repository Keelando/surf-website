"""Tests for the webcam capture + storage-metrics pipeline.

Covers the archive-resilience hardening added 2026-06-15 (commit history around
`902475e`/`c906ea7`): the website must keep updating even when the /mnt/storage
USB-SATA archive drive is wedged, read-only, or unmounted, and the storage
metrics exporter must read its camera roster from config/webcams.json.

Targets:
- lib.webcam.time_limit          — shared SIGALRM watchdog, raises StorageTimeout
- fetch_webcam.archive_is_writable — writable / read-only / timeout / unmounted
- fetch_webcam.archive_frame      — best-effort, never fatal
- fetch_webcam.main              — website updates even when archive is dead
- storage_metrics.load_webcam_archives — roster from webcams.json
- storage_metrics.get_webcam_metrics   — trimmed dict + timeout-bounded reads
- health_check._load_webcam_config — skips `_`-prefixed meta keys
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.export import storage_metrics_to_mqtt as storage_metrics
from scripts.fetch import fetch_webcam
from scripts.monitoring import health_check

# ── time_limit (shared SIGALRM watchdog, re-exported via fetch_webcam) ──


class TestTimeLimit:
    def test_fast_block_does_not_raise(self):
        with fetch_webcam.time_limit(5, "fast"):
            pass  # completes well within budget

    def test_slow_block_raises_storage_timeout(self):
        with pytest.raises(fetch_webcam.StorageTimeout):
            with fetch_webcam.time_limit(0.05, "slow"):
                time.sleep(0.5)

    def test_message_names_the_operation(self):
        with pytest.raises(fetch_webcam.StorageTimeout, match="my-probe"):
            with fetch_webcam.time_limit(0.05, "my-probe"):
                time.sleep(0.5)

    def test_handler_restored_after_use(self):
        import signal

        before = signal.getsignal(signal.SIGALRM)
        with fetch_webcam.time_limit(5, "x"):
            pass
        assert signal.getsignal(signal.SIGALRM) is before
        # A second use still works (itimer was cleared, handler restored).
        with pytest.raises(fetch_webcam.StorageTimeout):
            with fetch_webcam.time_limit(0.05, "again"):
                time.sleep(0.5)
        assert signal.getsignal(signal.SIGALRM) is before


# ── archive_is_writable ──────────────────────────────────────


class TestArchiveIsWritable:
    def test_writable_dir_returns_true(self, tmp_path, monkeypatch):
        monkeypatch.setattr("os.path.ismount", lambda p: True)
        logger = MagicMock()
        assert fetch_webcam.archive_is_writable(tmp_path, logger) is True
        logger.error.assert_not_called()
        # The probe file must be cleaned up, not left behind.
        assert not list(tmp_path.glob(".write_probe.*"))

    def test_read_only_returns_false_and_logs_degraded(self, tmp_path, monkeypatch):
        monkeypatch.setattr("os.path.ismount", lambda p: True)

        def boom(*a, **k):
            raise OSError(30, "Read-only file system")

        monkeypatch.setattr(fetch_webcam.os, "open", boom)
        logger = MagicMock()
        assert fetch_webcam.archive_is_writable(tmp_path, logger) is False
        assert "ARCHIVE DEGRADED" in logger.error.call_args[0][0]

    def test_wedged_drive_times_out_returns_false(self, tmp_path, monkeypatch):
        monkeypatch.setattr("os.path.ismount", lambda p: True)
        monkeypatch.setattr(fetch_webcam, "ARCHIVE_PROBE_TIMEOUT", 0.05)
        # Simulate a hung bridge: the fsync inside the probe never returns in time.
        monkeypatch.setattr(fetch_webcam.os, "fsync", lambda fd: time.sleep(0.5))
        logger = MagicMock()
        assert fetch_webcam.archive_is_writable(tmp_path, logger) is False
        assert "ARCHIVE DEGRADED" in logger.error.call_args[0][0]
        assert "wedged" in logger.error.call_args[0][0]

    def test_unmounted_and_remount_fails_returns_false(self, tmp_path, monkeypatch):
        monkeypatch.setattr("os.path.ismount", lambda p: False)

        def fake_mount(*a, **k):
            return MagicMock(returncode=1, stderr="no such device")

        monkeypatch.setattr(fetch_webcam.subprocess, "run", fake_mount)
        logger = MagicMock()
        assert fetch_webcam.archive_is_writable(tmp_path, logger) is False
        assert "not mounted" in logger.error.call_args[0][0]


# ── archive_frame (best-effort, never fatal) ─────────────────


class TestArchiveFrame:
    def _make_jpg(self, path):
        path.write_bytes(b"\xff\xd8\xff\xe0fake-jpeg-bytes")
        return path

    def test_success_returns_dest_and_copies(self, tmp_path):
        src = self._make_jpg(tmp_path / "CB_20260615_120000_1.jpg")
        archive = tmp_path / "archive"
        archive.mkdir()
        logger = MagicMock()
        dest = fetch_webcam.archive_frame(src, archive, logger)
        assert dest == archive / src.name
        assert dest.read_bytes() == src.read_bytes()

    def test_copy_failure_returns_none_never_raises(self, tmp_path, monkeypatch):
        src = self._make_jpg(tmp_path / "CB_x.jpg")

        def boom(*a, **k):
            raise OSError(5, "Input/output error")

        monkeypatch.setattr(fetch_webcam.shutil, "copy2", boom)
        logger = MagicMock()
        # Must return None rather than propagating — the website run continues.
        assert fetch_webcam.archive_frame(src, tmp_path, logger) is None
        assert "ARCHIVE DEGRADED" in logger.error.call_args[0][0]

    def test_copy_timeout_returns_none(self, tmp_path, monkeypatch):
        src = self._make_jpg(tmp_path / "CB_y.jpg")
        monkeypatch.setattr(fetch_webcam, "ARCHIVE_COPY_TIMEOUT", 0.05)
        monkeypatch.setattr(fetch_webcam.shutil, "copy2", lambda *a, **k: time.sleep(0.5))
        logger = MagicMock()
        assert fetch_webcam.archive_frame(src, tmp_path, logger) is None


# ── main(): website updates even when the archive is unavailable ──


class TestWebsiteDecoupledFromArchive:
    """The core guarantee: latest.jpg/latest.json are written from a local temp
    capture and never gated on archive health."""

    def _config(self, tmp_path):
        archive = tmp_path / "archive"
        website = tmp_path / "website"
        archive.mkdir()
        website.mkdir()
        return {
            "name": "Test Cam",
            "image_url": "http://example.invalid/frame.jpg",
            "archive_dir": archive,
            "website_dir": website,
            "prefix": "TC",
            "source_text": "Test Source",
            "check_daylight": False,
        }

    def _patch_common(self, monkeypatch, config):
        monkeypatch.setattr(fetch_webcam, "WEBCAM_CONFIGS", {"test": config})
        monkeypatch.setattr(fetch_webcam, "setup_logger", lambda name: MagicMock())
        monkeypatch.setattr(sys, "argv", ["fetch_webcam.py", "test"])

        def fake_download(url, dest, logger, **kwargs):
            Path(dest).write_bytes(b"\xff\xd8\xff\xe0frame")
            return True

        monkeypatch.setattr(fetch_webcam, "download_image", fake_download)
        monkeypatch.setattr(fetch_webcam, "manage_slideshow_images", lambda *a, **k: None)

    def test_dead_archive_still_updates_website_and_exits_zero(self, tmp_path, monkeypatch):
        config = self._config(tmp_path)
        self._patch_common(monkeypatch, config)
        # Archive is wedged/unwritable for this whole run.
        monkeypatch.setattr(fetch_webcam, "archive_is_writable", lambda *a, **k: False)

        with pytest.raises(SystemExit) as exc:
            fetch_webcam.main()
        assert exc.value.code == 0

        # Website artifacts written despite the dead archive...
        assert (config["website_dir"] / "latest.jpg").exists()
        assert (config["website_dir"] / "latest.json").exists()
        # ...and nothing leaked into the archive dir.
        assert not list(config["archive_dir"].glob("*.jpg"))

    def test_healthy_archive_writes_both_website_and_archive(self, tmp_path, monkeypatch):
        config = self._config(tmp_path)
        self._patch_common(monkeypatch, config)
        monkeypatch.setattr(fetch_webcam, "archive_is_writable", lambda *a, **k: True)
        monkeypatch.setattr(fetch_webcam, "cleanup_old_archives", lambda *a, **k: None)

        with pytest.raises(SystemExit) as exc:
            fetch_webcam.main()
        assert exc.value.code == 0

        assert (config["website_dir"] / "latest.jpg").exists()
        assert len(list(config["archive_dir"].glob("TC_*.jpg"))) == 1


# ── storage_metrics_to_mqtt ──────────────────────────────────


class TestLoadWebcamArchives:
    def test_reads_roster_from_webcams_json(self):
        archives = storage_metrics.load_webcam_archives()
        assert archives, "expected at least one camera"
        # Underscore-prefixed meta keys must be dropped.
        assert all(not cam_id.startswith("_") for cam_id in archives)

    def test_each_entry_has_path_prefix_name(self):
        for cam in storage_metrics.load_webcam_archives().values():
            assert set(cam) == {"path", "prefix", "name"}
            assert isinstance(cam["path"], Path)

    def test_includes_drift_fixed_cameras(self):
        # mudbay_sw and ambleside were restored/renamed when the exporter moved
        # to reading webcams.json (commit c906ea7); guard against re-drift.
        ids = set(storage_metrics.load_webcam_archives())
        assert {"mudbay_sw", "ambleside"} <= ids


class TestGetWebcamMetrics:
    def _cam(self, path, prefix="TC"):
        return {"path": path, "prefix": prefix, "name": "Test Cam"}

    def test_counts_only_prefixed_jpgs(self, tmp_path):
        for i in range(3):
            (tmp_path / f"TC_2026061{i}_120000.jpg").write_bytes(b"x" * 500 * 1024)
        # Decoys that must be ignored.
        (tmp_path / "OTHER_20260615.jpg").write_bytes(b"x")
        (tmp_path / "TC_notes.txt").write_bytes(b"x")

        m = storage_metrics.get_webcam_metrics(self._cam(tmp_path))
        assert m["image_count"] == 3
        assert m["total_size_mb"] > 0
        assert datetime.fromisoformat(m["newest_image_date"])  # parseable ISO

    def test_returns_only_trimmed_keys(self, tmp_path):
        (tmp_path / "TC_20260615_120000.jpg").write_bytes(b"x" * 10)
        m = storage_metrics.get_webcam_metrics(self._cam(tmp_path))
        # No *_age or oldest_* sensors — those were trimmed (40→22 sensors).
        assert set(m) == {"image_count", "total_size_mb", "newest_image_date"}

    def test_empty_dir_returns_zeroed_metrics(self, tmp_path):
        m = storage_metrics.get_webcam_metrics(self._cam(tmp_path))
        assert m == {"image_count": 0, "total_size_mb": 0, "newest_image_date": None}

    def test_missing_dir_returns_none(self, tmp_path):
        m = storage_metrics.get_webcam_metrics(self._cam(tmp_path / "nope"))
        assert m is None


class TestStorageMetricsTimeout:
    """A wedged USB-SATA bridge must not hang the hourly exporter: reads are
    bounded by SIGALRM and degrade to None (sensor expires to 'unavailable')."""

    def test_disk_metrics_times_out_to_none(self, monkeypatch):
        monkeypatch.setattr(storage_metrics, "STORAGE_READ_TIMEOUT", 0.05)
        monkeypatch.setattr(storage_metrics.shutil, "disk_usage", lambda p: time.sleep(0.5))
        assert storage_metrics.get_disk_metrics() is None

    def test_webcam_scan_times_out_to_none(self, tmp_path, monkeypatch):
        (tmp_path / "TC_20260615_120000.jpg").write_bytes(b"x" * 10)
        monkeypatch.setattr(storage_metrics, "STORAGE_READ_TIMEOUT", 0.05)
        # Simulate a hung bridge during the directory scan.
        monkeypatch.setattr(
            storage_metrics.Path, "glob", lambda self, pat: time.sleep(0.5)
        )
        cam = {"path": tmp_path, "prefix": "TC", "name": "Test Cam"}
        assert storage_metrics.get_webcam_metrics(cam) is None


# ── health_check webcam-config loader ────────────────────────


class TestHealthCheckLoader:
    def test_skips_underscore_meta_keys(self):
        # The real webcams.json carries a `_schedule_note`; the loader must not
        # surface it as a camera (it would crash on cam["name"]).
        cams = health_check._load_webcam_config()
        assert cams, "expected at least one camera"
        assert all(not cam_id.startswith("_") for cam_id in cams)
        assert all("name" in meta for meta in cams.values())
