#!/usr/bin/env python3
"""
Generic Webcam Capture Script

Fetches frames from YouTube livestreams, direct URLs, and Yawcam servers,
saving them to both archive and website directories with slideshow support.

Usage:
    python3 fetch_webcam.py <config_name>

Example:
    python3 fetch_webcam.py whiterock
    python3 fetch_webcam.py boundarybay

Requirements:
- yt-dlp
- ffmpeg
"""

import contextlib
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from lib.daylight import is_daylight

# Setup path for imports
from lib.logging_config import setup_logging
from lib.webcam import (
    annotate_image,
    capture_yawcam_image,
    capture_youtube_frame,
    cleanup_old_archives,
    download_image,
    head_image,
    manage_slideshow_images,
)

DEDUPE_SIDECAR = ".last_fetch.json"


def _read_dedupe_sidecar(website_dir):
    """Load the {size, sha256, fetched_at} sidecar, or return {} if missing/corrupt."""
    path = website_dir / DEDUPE_SIDECAR
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_dedupe_sidecar(website_dir, size, sha256, fetched_at):
    path = website_dir / DEDUPE_SIDECAR
    try:
        with open(path, "w") as f:
            json.dump(
                {"size": size, "sha256": sha256, "fetched_at": fetched_at},
                f,
                indent=2,
            )
    except OSError:
        pass


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

# Webcam configurations live in config/webcams.json (gitignored).
# See config/webcams.example.json for the schema.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
WEBCAM_CONFIG_PATH = REPO_ROOT / "config" / "webcams.json"


def load_webcam_configs():
    """Load webcam configs from JSON, resolving paths relative to repo root.

    archive_dir is taken as-is (typically an absolute /mnt/storage path).
    website_dir is resolved relative to the repo root if not already absolute.
    Keys beginning with `_` (e.g. `_comment`, `_permission_note`) are dropped.
    """
    if not WEBCAM_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Webcam config not found at {WEBCAM_CONFIG_PATH}. "
            f"Copy config/webcams.example.json to config/webcams.json and edit."
        )
    with open(WEBCAM_CONFIG_PATH) as f:
        raw = json.load(f)

    configs = {}
    for name, cfg in raw.items():
        if name.startswith("_"):
            continue
        cfg = {k: v for k, v in cfg.items() if not k.startswith("_")}
        cfg["archive_dir"] = Path(cfg["archive_dir"])
        website_dir = Path(cfg["website_dir"])
        cfg["website_dir"] = website_dir if website_dir.is_absolute() else REPO_ROOT / website_dir
        configs[name] = cfg
    return configs


WEBCAM_CONFIGS = load_webcam_configs()


STORAGE_MOUNT = Path("/mnt/storage")

# How long an actual write to the archive may take before we declare the drive
# wedged. Bounds the worst case so a hung USB-SATA bridge can't stall (and stack)
# cron jobs. The probe is tiny; the copy is one JPEG.
ARCHIVE_PROBE_TIMEOUT = 10  # seconds
ARCHIVE_COPY_TIMEOUT = 30  # seconds


class StorageTimeout(Exception):
    """Raised when a storage operation exceeds its time budget (wedged bridge)."""


@contextlib.contextmanager
def _time_limit(seconds, what):
    """Abort the wrapped block with StorageTimeout if it runs past `seconds`.

    Uses SIGALRM, so it only interrupts at points the kernel can deliver a
    signal — a truly uninterruptible (D-state) syscall on a dead bridge may
    still outlast this, but it bounds the common "slow/flaky" case. Main thread only.
    """
    def _handler(signum, frame):
        raise StorageTimeout(f"{what} exceeded {seconds}s")

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def archive_is_writable(archive_dir, logger):
    """Return True only if the archive is mounted AND actually accepts a write.

    os.path.ismount() returns True for a drive that has silently remounted
    read-only or whose USB bridge has wedged — both of which then fail every
    real write. So we don't trust it: we create, fsync, and delete a probe file
    inside the cam's own archive dir (which the cron user owns; the /mnt/storage
    root is root-owned and not a valid probe target), bounded by a timeout.

    On failure we log loudly with an "ARCHIVE DEGRADED" marker and return False
    so the caller skips archiving but still updates the website.
    """
    if not os.path.ismount(STORAGE_MOUNT):
        logger.warning(f"{STORAGE_MOUNT} is not mounted — attempting to mount")
        try:
            result = subprocess.run(
                ["mount", str(STORAGE_MOUNT)],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode != 0:
                logger.warning(f"Mount failed: {result.stderr.strip()}")
        except (subprocess.TimeoutExpired, OSError) as e:
            logger.warning(f"Mount attempt failed: {e}")
        if not os.path.ismount(STORAGE_MOUNT):
            logger.error("ARCHIVE DEGRADED: storage not mounted — skipping archive, website still updates")
            return False

    probe = archive_dir / f".write_probe.{os.getpid()}"
    try:
        with _time_limit(ARCHIVE_PROBE_TIMEOUT, "storage write probe"):
            archive_dir.mkdir(parents=True, exist_ok=True)
            fd = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                os.write(fd, b"probe")
                os.fsync(fd)
            finally:
                os.close(fd)
            os.unlink(probe)
        return True
    except StorageTimeout as e:
        logger.error(
            f"ARCHIVE DEGRADED: {e} — USB bridge likely wedged; skipping archive, website still updates"
        )
    except OSError as e:
        logger.error(
            f"ARCHIVE DEGRADED: storage mounted but not writable ({e}) — "
            "likely remounted read-only or I/O error; skipping archive, website still updates"
        )
    # Best-effort probe cleanup, itself time-bounded in case the bridge is hung.
    with contextlib.suppress(Exception), _time_limit(3, "probe cleanup"):
        os.unlink(probe)
    return False


def archive_frame(capture_path, archive_dir, logger):
    """Best-effort copy of the captured frame into the archive. Never fatal.

    Returns the destination Path on success, or None on any failure/timeout.
    """
    dest = archive_dir / capture_path.name
    try:
        with _time_limit(ARCHIVE_COPY_TIMEOUT, "archive copy"):
            shutil.copy2(capture_path, dest)
        logger.info(f"Archived frame: {dest}")
        return dest
    except StorageTimeout as e:
        logger.error(f"ARCHIVE DEGRADED: {e} — frame not archived this run, website still updated")
    except OSError as e:
        logger.error(f"ARCHIVE DEGRADED: failed to archive frame ({e}) — website still updated")
    return None


def setup_logger(config_name):
    """Setup logging for this webcam using centralized logging config."""
    return setup_logging(f"webcam_{config_name}", console=False)


def main():
    """Main execution."""
    if len(sys.argv) < 2:
        print("Usage: python3 fetch_webcam.py <config_name>")
        print(f"Available configs: {', '.join(WEBCAM_CONFIGS.keys())}")
        sys.exit(1)

    config_name = sys.argv[1]
    if config_name not in WEBCAM_CONFIGS:
        print(f"Error: Unknown config '{config_name}'")
        print(f"Available configs: {', '.join(WEBCAM_CONFIGS.keys())}")
        sys.exit(1)

    config = WEBCAM_CONFIGS[config_name]
    logger = setup_logger(config_name)

    logger.info(f"=== {config['name']} Webcam Capture Started ===")

    # Check if daylight check is enabled for this webcam
    if config.get("check_daylight", False):
        margin = config.get("daylight_margin_minutes", 30)
        if not is_daylight(config["lat"], config["lon"], margin_minutes=margin):
            logger.info(f"Skipping capture - it's nighttime (outside daylight hours + {margin}min margin)")
            logger.info("Webcam captures are only taken during daylight hours for this location")
            sys.exit(0)
        logger.info(f"Daylight check passed (within {margin}min of sunrise/sunset) - proceeding with capture")
    else:
        logger.info("Daylight check disabled for this webcam - capturing 24/7")

    # Generate timestamp-based filename
    timestamp = datetime.now(timezone.utc)
    timestamp_readable = timestamp.strftime("%Y%m%d_%H%M%S")
    timestamp_unix = int(timestamp.timestamp())
    filename = f"{config['prefix']}_{timestamp_readable}_{timestamp_unix}.jpg"

    # The website (latest.jpg / slideshow / latest.json) MUST update on every
    # successful fetch, fully independent of archive-drive health. So we ALWAYS
    # capture to a local temp file, drive the website from it, and only then copy
    # to the archive as a best-effort secondary step. A dead or read-only archive
    # can no longer abort this run and take the live site down with it.
    config["website_dir"].mkdir(parents=True, exist_ok=True)
    tmpdir = Path(tempfile.mkdtemp(prefix="webcam_"))
    capture_path = tmpdir / filename
    try:
        # Dedupe is opt-in per cam (currently only meaningful for direct image_url sources).
        dedupe_enabled = config.get("dedupe", False) and "image_url" in config
        dedupe_state = _read_dedupe_sidecar(config["website_dir"]) if dedupe_enabled else {}

        # Check if this is a direct image URL, YouTube stream, or Yawcam
        if "image_url" in config:
            # Stage 1 (dedupe): HEAD the URL and skip if Content-Length matches the last stored.
            if dedupe_enabled and dedupe_state.get("size"):
                remote_size = head_image(
                    config["image_url"],
                    logger,
                    referer=config.get("image_referer"),
                    user_agent=config.get("image_user_agent"),
                    from_email=config.get("image_from"),
                )
                if remote_size is not None and remote_size == dedupe_state["size"]:
                    logger.info(
                        f"Dedupe skip (HEAD): Content-Length {remote_size} matches last fetch — no new frame"
                    )
                    sys.exit(0)
                logger.info(f"HEAD size {remote_size} differs from last {dedupe_state['size']} — fetching")

            # Direct image download
            if not download_image(
                config["image_url"],
                capture_path,
                logger,
                referer=config.get("image_referer"),
                user_agent=config.get("image_user_agent"),
                from_email=config.get("image_from"),
            ):
                logger.error("Failed to download image - aborting")
                sys.exit(1)

            # Stage 2 (dedupe): hash the downloaded bytes; if identical to last, discard.
            if dedupe_enabled:
                new_size = capture_path.stat().st_size
                new_sha = _sha256_file(capture_path)
                if dedupe_state.get("sha256") == new_sha:
                    logger.info(
                        f"Dedupe skip (hash): sha256 {new_sha[:12]}… matches last fetch — discarding"
                    )
                    sys.exit(0)
                # Stash for the post-success sidecar write below.
                config["_dedupe_new_size"] = new_size
                config["_dedupe_new_sha"] = new_sha
        elif "youtube_url" in config:
            # YouTube livestream capture
            video_id = config.get("video_id")
            max_height = config.get("max_height", 480)  # Default 480p
            if not capture_youtube_frame(
                config["youtube_url"], capture_path, config["crop"], logger, video_id, max_height
            ):
                logger.error("Failed to capture YouTube frame - aborting")
                sys.exit(1)
        elif "yawcam_url" in config:
            # Yawcam server capture
            quality = config.get("yawcam_quality", 50)
            if not capture_yawcam_image(config["yawcam_url"], capture_path, quality, logger):
                logger.error("Failed to capture Yawcam image - aborting")
                sys.exit(1)
        else:
            logger.error("Config must have 'image_url', 'youtube_url', or 'yawcam_url'")
            sys.exit(1)

        # Annotate image with timestamp if enabled
        if config.get("annotate_timestamp", False):
            if not annotate_image(capture_path, timestamp, logger):
                logger.warning("Failed to annotate image, continuing anyway")

        # ---- Website update (ALWAYS, sourced from the local temp file) ----

        # Atomically update latest.jpg
        website_latest = config["website_dir"] / "latest.jpg"
        website_temp = config["website_dir"] / f"latest.tmp.{timestamp_unix}.jpg"
        try:
            shutil.copy2(capture_path, website_temp)
            website_temp.rename(website_latest)
            # Ensure web-readable permissions (0644)
            os.chmod(website_latest, 0o644)
            logger.info(f"Atomically updated: {website_latest}")
        except Exception as e:
            logger.error(f"Failed to copy to website directory: {e}")

        # Manage slideshow images
        manage_slideshow_images(config["website_dir"], capture_path, timestamp_unix, logger)

        # Create metadata JSON for website
        metadata = {
            "filename": filename,
            "timestamp": timestamp.isoformat(),
            "timestamp_unix": timestamp_unix,
            "source": config["source_text"],
            "url": config.get("youtube_url") or config.get("image_url") or config.get("source_url"),
        }

        metadata_path = config["website_dir"] / "latest.json"
        try:
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Updated metadata: {metadata_path}")
        except Exception as e:
            logger.error(f"Failed to write metadata: {e}")

        # Persist dedupe state for next run (pre-annotation size/hash captured above).
        if dedupe_enabled and "_dedupe_new_sha" in config:
            _write_dedupe_sidecar(
                config["website_dir"],
                config["_dedupe_new_size"],
                config["_dedupe_new_sha"],
                timestamp.isoformat(),
            )

        # ---- Archive (BEST-EFFORT secondary step; can never be fatal) ----
        if archive_is_writable(config["archive_dir"], logger):
            if archive_frame(capture_path, config["archive_dir"], logger) is not None:
                cleanup_old_archives(config["archive_dir"], config["prefix"], logger)
        else:
            logger.warning("Completed without archiving — storage unavailable; website updated normally")

        logger.info(f"=== {config['name']} Webcam Capture Completed Successfully ===")
        sys.exit(0)
    finally:
        # Always remove the local temp capture dir, on every exit path.
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
