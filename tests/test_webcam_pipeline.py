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
- health_check._webcam_in_scope        — daylight-only cams leave the count after dark
- health_check._reporting_lightstations — `reporting: false` entries leave the count
"""

import re
import sys
import time
from datetime import datetime, timezone
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


class TestWebcamRegistryConsistency:
    """A camera is described in four places and they had drifted apart.

    - `config/stations.json` ["webcams"] — tracked and public: identity and
      position. Drives the map pin and the exported /data/stations.json.
    - `config/webcams.json` — gitignored: the fetch mechanics (URLs, referers,
      crop, archive paths), which is why it cannot live in the public repo.
    - `site/assets/js/webcams-v4.js` — the page's own card list.
    - `config/crontab.txt` — the only authority on how often a camera runs.

    What they disagreed about on 2026-09-06, before these tests existed:
    coordinates differing by up to 24 km (boundarybay still held the old
    Boundary Bay position after the camera became the White Rock East Beach
    one), three of six names, and the Mud Bay cams' interval — cron runs them
    every 15 minutes while two sources said 10, so the page told readers a
    cadence the pipeline never had.

    The fix for the duplication itself is to have the fetch side read position
    and identity from the registry; until then these tests are what stops the
    copies drifting again.
    """

    JS_PATH = Path(__file__).parent.parent / "site" / "assets" / "js" / "webcams-v4.js"
    CRONTAB = Path(__file__).parent.parent / "config" / "crontab.txt"

    @staticmethod
    def _registry_cams():
        from lib.stations import get_all_webcams

        return get_all_webcams()

    @staticmethod
    def _private_cams():
        return health_check._load_webcam_config()

    def _js_intervals(self):
        """{cam_id: updateInterval} as the webcams page states it."""
        text = self.JS_PATH.read_text()
        return {
            m.group(1): int(m.group(2))
            for m in re.finditer(r'id: "(\w+)".*?updateInterval: (\d+)', text, re.S)
        }

    def _cron_intervals(self):
        """{cam_id: minutes} implied by the schedule, or None if irregular."""
        out = {}
        for line in self.CRONTAB.read_text().splitlines():
            m = re.match(r"^([\d,]+) .*fetch_webcam\.py (\w+)", line)
            if not m:
                continue
            mins = sorted(int(x) for x in m.group(1).split(","))
            gaps = {(b - a) % 60 for a, b in zip(mins, mins[1:] + [mins[0] + 60])}
            out[m.group(2)] = gaps.pop() if len(gaps) == 1 else None
        return out

    def test_the_same_cameras_exist_everywhere(self):
        registry, private = set(self._registry_cams()), set(self._private_cams())
        assert registry == private, (
            f"only in stations.json: {sorted(registry - private)}; "
            f"only in webcams.json: {sorted(private - registry)}"
        )
        assert registry <= set(self._js_intervals()), sorted(registry - set(self._js_intervals()))

    def test_names_agree_between_the_public_and_private_registries(self):
        private = self._private_cams()
        mismatches = [
            f"{cam_id}: stations.json {cam['name']!r} vs webcams.json {private[cam_id]['name']!r}"
            for cam_id, cam in self._registry_cams().items()
            if cam_id in private and cam["name"] != private[cam_id]["name"]
        ]
        assert not mismatches, "\n  ".join(mismatches)

    def test_positions_agree_between_the_public_and_private_registries(self):
        private = self._private_cams()
        mismatches = []
        for cam_id, cam in self._registry_cams().items():
            other = private.get(cam_id)
            if not other or other.get("lat") is None:
                continue
            drift_m = max(abs(cam["lat"] - other["lat"]), abs(cam["lon"] - other["lon"])) * 111_320
            if drift_m > 1:
                mismatches.append(
                    f"{cam_id}: stations.json ({cam['lat']}, {cam['lon']}) vs "
                    f"webcams.json ({other['lat']}, {other['lon']}) — {drift_m:.0f} m apart"
                )
        assert not mismatches, "\n  ".join(mismatches)

    def test_stated_interval_matches_what_cron_actually_runs(self):
        """The crontab is the authority; everything else is a claim about it."""
        cron = self._cron_intervals()
        registry, private, js = self._registry_cams(), self._private_cams(), self._js_intervals()
        wrong = []
        for cam_id, minutes in cron.items():
            if minutes is None:
                continue  # deliberately irregular schedule; nothing to compare
            for where, stated in (
                ("stations.json", registry.get(cam_id, {}).get("update_frequency_minutes")),
                ("webcams.json", private.get(cam_id, {}).get("interval")),
                ("webcams-v4.js", js.get(cam_id)),
            ):
                if stated is not None and stated != minutes:
                    wrong.append(f"{cam_id}: cron runs every {minutes} min, {where} says {stated}")
        assert not wrong, "\n  ".join(wrong)


class TestWebcamDisplayLabels:
    """Cams share the footer's down-list with buoys, tides, wind stations and
    lightstations, so their labels have to be distinct from those too — and
    from each other. Two of them were the same place under different
    instruments ("White Rock Pier Cam" and the White Rock tide gauge), and the
    two Mud Bay cams differ only by "(SE)"/"(SW)" at the end of a long name,
    which is exactly where a compact badge truncates. See TestDisplayLabels in
    tests/test_stations.py for the station-side rule this mirrors."""

    @staticmethod
    def _fold(label):
        return "".join(c for c in label.lower() if c.isalnum())

    def _all_labels(self):
        """Every label the footer can print: cams plus every station type."""
        from lib.stations import STATIONS

        labels = []
        for cam_id, cam in health_check._load_webcam_config().items():
            labels.append((cam.get("short_name") or cam["name"], f"webcam/{cam_id}"))
        for group in ("buoys", "tides", "wind", "lightstations"):
            for key, data in getattr(STATIONS, group).items():
                labels.append((data.get("short_name") or data["name"], f"{group}/{key}"))
        return labels

    def test_every_cam_has_a_short_name(self):
        for cam_id, cam in health_check._load_webcam_config().items():
            assert cam.get("short_name"), f"{cam_id}: {cam['name']!r} has no short_name"

    def test_cam_labels_do_not_collide_with_anything_else(self):
        seen, clashes = {}, []
        for label, where in self._all_labels():
            folded = self._fold(label)
            if folded in seen:
                clashes.append(f"{label!r}: {seen[folded]} and {where}")
            seen[folded] = where
        assert not clashes, "labels shared by more than one station:\n  " + "\n  ".join(clashes)

    def test_no_cam_label_is_a_truncation_of_another(self):
        entries = sorted((self._fold(label), label, where) for label, where in self._all_labels())
        clashes = []
        for i, (folded, label, where) in enumerate(entries):
            for other_folded, other_label, other_where in entries[i + 1 :]:
                if not other_folded.startswith(folded):
                    break
                clashes.append(f"{label!r} ({where}) is a truncation of {other_label!r} ({other_where})")
        assert not clashes, "confusable labels:\n  " + "\n  ".join(clashes)


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


# ── health_check daylight scoping ────────────────────────────


class TestWebcamScope:
    """A daylight-only cam should leave the health fraction after dark.

    Mud Bay and its siblings are switched off overnight on purpose, so counting
    them as stations that failed to report publishes a nightly dip that means
    nothing. Out of scope has to mean out of the denominator too.
    """

    # Mud Bay HD, 15-minute cadence, 30-minute capture margin.
    CAM = {
        "name": "Mud Bay HD (SE)",
        "interval": 15,
        "daylight_only": True,
        "disabled": False,
        "lat": 49.0714,
        "lon": -122.9554,
        "daylight_margin_minutes": 30,
    }

    def _at(self, iso):
        return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)

    def test_counted_in_broad_daylight(self):
        # 13:00 PDT on a late-August day.
        assert health_check._webcam_in_scope(self.CAM, self._at("2026-08-29T20:00:00"))

    def test_not_counted_at_night(self):
        # 22:00 PDT, hours past sunset + margin.
        assert not health_check._webcam_in_scope(self.CAM, self._at("2026-08-30T05:00:00"))

    def test_not_counted_immediately_after_dawn(self):
        # 06:30 PDT: light, but the newest frame is still last night's. The cam
        # rejoins the count only once it has had its warning window to catch up.
        assert not health_check._webcam_in_scope(self.CAM, self._at("2026-08-29T13:30:00"))

    def test_247_cam_always_counted(self):
        cam = dict(self.CAM, daylight_only=False)
        assert health_check._webcam_in_scope(cam, self._at("2026-08-30T05:00:00"))

    def test_disabled_cam_never_counted(self):
        cam = dict(self.CAM, disabled=True)
        assert not health_check._webcam_in_scope(cam, self._at("2026-08-29T20:00:00"))

    def test_daylight_cam_without_position_still_judged(self):
        # No lat/lon to reason about: keep checking it rather than silently
        # dropping a camera out of the count.
        cam = dict(self.CAM, lat=None, lon=None)
        assert health_check._webcam_in_scope(cam, self._at("2026-08-30T05:00:00"))


# ── health_check lightstation scoping ────────────────────────


class TestReportingLightstations:
    """Registry entries flagged `"reporting": false` are out of the fraction.

    Chatham Point, Egg Island and Green Island are real lights that have never
    appeared in the bulletins we ingest. They stay on the map; they must not
    read as three stations permanently down.
    """

    def test_flagged_stations_excluded(self):
        everything = health_check.get_all_lightstations()
        reporting = health_check._reporting_lightstations()

        flagged = [k for k, v in everything.items() if v.get("reporting") is False]
        assert flagged, "expected at least one non-reporting lightstation in the registry"
        assert not set(flagged) & set(reporting)
        assert len(reporting) == len(everything) - len(flagged)

    def test_unflagged_stations_kept(self):
        reporting = health_check._reporting_lightstations()
        assert all(meta.get("reporting", True) for meta in reporting.values())


# ── dedupe: a frozen camera must not read as fresh ───────────


class TestDedupeFrozenCamera:
    """A dead camera usually keeps serving its last good frame with a 200. The
    fetch succeeds, `latest.json` is rewritten, and the site calls a week-old
    picture current — which is what the Ambleside cam did from 2026-08-27 until
    dedupe was turned on for it (2026-09-06). The only visible tell was the
    timestamp burned into the image.

    The contract these tests pin: when the bytes have not changed, nothing
    downstream of the download runs — no slideshow entry, and above all no new
    `latest.json` timestamp, because that timestamp is the sole input to the
    staleness badge on the webcams page."""

    def _config(self, tmp_path, **overrides):
        archive = tmp_path / "archive"
        website = tmp_path / "website"
        archive.mkdir()
        website.mkdir()
        config = {
            "name": "Frozen Cam",
            "image_url": "http://example.invalid/frame.jpg",
            "archive_dir": archive,
            "website_dir": website,
            "prefix": "FC",
            "source_text": "Test Source",
            "check_daylight": False,
            "dedupe": True,
        }
        config.update(overrides)
        return config

    def _run(self, monkeypatch, config, payload, head_size=None):
        """One full main() pass serving `payload`. Returns (slideshow_calls,
        download_calls) so a skip can be told from a publish."""
        calls = {"slideshow": 0, "download": 0}
        monkeypatch.setattr(fetch_webcam, "WEBCAM_CONFIGS", {"test": config})
        monkeypatch.setattr(fetch_webcam, "setup_logger", lambda name: MagicMock())
        monkeypatch.setattr(sys, "argv", ["fetch_webcam.py", "test"])
        monkeypatch.setattr(fetch_webcam, "archive_is_writable", lambda *a, **k: False)

        def fake_download(url, dest, logger, **kwargs):
            calls["download"] += 1
            Path(dest).write_bytes(payload)
            return True

        def fake_slideshow(*a, **k):
            calls["slideshow"] += 1

        monkeypatch.setattr(fetch_webcam, "download_image", fake_download)
        monkeypatch.setattr(fetch_webcam, "manage_slideshow_images", fake_slideshow)
        monkeypatch.setattr(fetch_webcam, "head_image", lambda *a, **k: head_size)

        with pytest.raises(SystemExit) as exc:
            fetch_webcam.main()
        assert exc.value.code == 0
        return calls

    def test_identical_frame_does_not_advance_latest_json(self, tmp_path, monkeypatch):
        config = self._config(tmp_path)
        frame = b"\xff\xd8\xff\xe0frozen-frame"

        first = self._run(monkeypatch, config, frame)
        assert first["slideshow"] == 1
        published = (config["website_dir"] / "latest.json").read_bytes()

        # Same bytes again: the run must stop before anything is published.
        second = self._run(monkeypatch, config, frame)
        assert second["slideshow"] == 0
        assert (config["website_dir"] / "latest.json").read_bytes() == published

    def test_changed_frame_still_publishes(self, tmp_path, monkeypatch):
        config = self._config(tmp_path)

        self._run(monkeypatch, config, b"\xff\xd8\xff\xe0frame-one")
        first = (config["website_dir"] / "latest.json").read_bytes()

        moved = self._run(monkeypatch, config, b"\xff\xd8\xff\xe0frame-two-longer")
        assert moved["slideshow"] == 1
        assert (config["website_dir"] / "latest.json").read_bytes() != first
        assert (config["website_dir"] / "latest.jpg").read_bytes().endswith(b"frame-two-longer")

    def test_head_size_match_skips_the_download_entirely(self, tmp_path, monkeypatch):
        """Stage 1 exists to save the transfer, not just the write."""
        config = self._config(tmp_path)
        frame = b"\xff\xd8\xff\xe0frozen-frame"

        self._run(monkeypatch, config, frame)
        second = self._run(monkeypatch, config, frame, head_size=len(frame))
        assert second["download"] == 0
        assert second["slideshow"] == 0

    def test_head_size_change_falls_through_to_the_fetch(self, tmp_path, monkeypatch):
        """A camera whose server reports a stale Content-Length must not be
        able to pin us to an old frame — a mismatch always re-fetches."""
        config = self._config(tmp_path)

        self._run(monkeypatch, config, b"\xff\xd8\xff\xe0frame-one")
        moved = self._run(monkeypatch, config, b"\xff\xd8\xff\xe0frame-two", head_size=99999)
        assert moved["download"] == 1
        assert moved["slideshow"] == 1

    def test_annotation_does_not_defeat_the_hash(self, tmp_path, monkeypatch):
        """The Mud Bay cams burn a timestamp into every frame, which would make
        every image unique if the hash were taken after annotation. It is taken
        on the downloaded bytes instead — this test is what keeps it there."""
        config = self._config(tmp_path, annotate_timestamp=True)
        frame = b"\xff\xd8\xff\xe0frozen-frame"

        def fake_annotate(path, timestamp, logger):
            Path(path).write_bytes(Path(path).read_bytes() + str(timestamp).encode())
            return True

        monkeypatch.setattr(fetch_webcam, "annotate_image", fake_annotate)
        assert self._run(monkeypatch, config, frame)["slideshow"] == 1

        monkeypatch.setattr(fetch_webcam, "annotate_image", fake_annotate)
        assert self._run(monkeypatch, config, frame)["slideshow"] == 0

    def test_dedupe_off_republishes_the_same_frame(self, tmp_path, monkeypatch):
        """The opt-out has to keep working: YouTube cams re-encode every frame,
        so byte equality there would be an accident, not a signal."""
        config = self._config(tmp_path, dedupe=False)
        frame = b"\xff\xd8\xff\xe0frozen-frame"

        self._run(monkeypatch, config, frame)
        assert self._run(monkeypatch, config, frame)["slideshow"] == 1
        assert not (config["website_dir"] / fetch_webcam.DEDUPE_SIDECAR).exists()
