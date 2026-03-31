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

import json
import os
import shutil
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
    manage_slideshow_images,
)

# Webcam configurations
# YouTube streams support: max_height (resolution), interval_minutes, check_daylight, crop
# Direct image URLs (like mudbay) just fetch the image as-is
WEBCAM_CONFIGS = {
    "whiterock": {
        "name": "White Rock Pier Cam",
        "youtube_url": "https://www.youtube.com/watch?v=4MK3E9EWDSY",
        "video_id": "4MK3E9EWDSY",
        "archive_dir": Path("/mnt/storage/whiterock_cam"),
        "website_dir": Path(__file__).parent.parent.parent / "site" / "data" / "wrcam",
        "prefix": "WR",
        "crop": "in_w*0.75:in_h:in_w*0.25:0",  # Crop left 25% (street), keep right 75% (pier/sea)
        "source_text": "White Rock Pier - YouTube Livestream",
        "lat": 49.0253,
        "lon": -122.8031,
        "max_height": 720,  # 720p for detail
        "check_daylight": False,  # Capture 24/7
        "interval_minutes": 10,  # Snapshot every 10 minutes
        "cron_offset": 0,  # Runs at :00, :10, :20, etc.
    },
    "boundarybay": {
        "name": "White Rock East Beach",
        "youtube_url": "https://www.youtube.com/watch?v=O8RsAq9RUlA",
        "video_id": "O8RsAq9RUlA",
        "archive_dir": Path("/mnt/storage/boundarybay_cam"),
        "website_dir": Path(__file__).parent.parent.parent / "site" / "data" / "bbcam",
        "prefix": "BB",
        "crop": "in_w:in_h:0:0",  # Full frame
        "source_text": "White Rock East Beach - YouTube Livestream",
        "lat": 49.0042,
        "lon": -123.0128,
        "max_height": 480,  # 480p default
        "check_daylight": False,  # Capture 24/7
        "interval_minutes": 10,  # Snapshot every 10 minutes
        "cron_offset": 2,  # Runs at :02, :12, :22, etc.
    },
    "coxbay": {
        "name": "Cox Bay",
        "youtube_url": "https://www.youtube.com/watch?v=LqaP8m2OIqM",
        "video_id": "LqaP8m2OIqM",
        "archive_dir": Path("/mnt/storage/coxbay_cam"),
        "website_dir": Path(__file__).parent.parent.parent / "site" / "data" / "coxbay",
        "prefix": "CB",
        "crop": "in_w:in_h:0:0",  # Full frame
        "source_text": "Cox Bay (Tofino) - Pacific Sands Beach Resort Livestream",
        "lat": 49.1167,
        "lon": -125.9000,
        "max_height": 720,  # 720p for surf detail
        "check_daylight": True,  # Only capture during daylight
        "daylight_margin_minutes": 75,
        "interval_minutes": 15,  # Snapshot every 15 minutes
        "cron_offset": 4,  # Runs at :04, :19, :34, :49
    },
    "mudbay": {
        "name": "Mud Bay HD",
        "image_url": "https://oxblue.com/archive/c6713f391eef15e5c1dbfc6a003b83a0/1024x768.jpg",
        "archive_dir": Path("/mnt/storage/mudbay_cam"),
        "website_dir": Path(__file__).parent.parent.parent / "site" / "data" / "mudbay",
        "prefix": "MB",
        "crop": "in_w:in_h:0:0",  # Full frame
        "source_text": "Mud Bay HD - OxBlue Construction Cam",
        "lat": 49.07138649092664,
        "lon": -122.95538135838513,
        # No max_height - direct image URL, fetched as-is (1024x768)
        "check_daylight": True,  # Only capture during daylight
        "daylight_margin_minutes": 75,
        "interval_minutes": 30,  # Snapshot every 30 minutes
        "cron_offset": 6,  # Runs at :06, :36
        "annotate_timestamp": True,  # Add timestamp overlay
    },
    # -------------------------------------------------------------------------
    # HOLLYBURN SAILING CLUB WEBCAM - PERMISSION REQUIRED
    # Approval granted to halibutbank.ca by Hollyburn Sailing Club (Jan 2026).
    # If you fork this project, you MUST obtain your own permission from
    # Hollyburn Sailing Club before fetching from their webcam feed.
    # Contact: https://www.hollyburnsailingclub.ca/
    # -------------------------------------------------------------------------
    "ambleside": {
        "name": "Ambleside (Hollyburn Sailing Club)",
        "yawcam_url": "http://onsite.hollyburnsailingclub.ca:8081/",
        "archive_dir": Path("/mnt/storage/ambleside_cam"),
        "website_dir": Path(__file__).parent.parent.parent / "site" / "data" / "ambleside",
        "prefix": "AB",
        "crop": "in_w:in_h:0:0",  # Full frame
        "source_text": "Hollyburn Sailing Club Webcam",
        "source_url": "https://www.hollyburnsailingclub.ca/webcam",
        "lat": 49.326635134999776,
        "lon": -123.1529396759124,
        # Yawcam quality setting (1-100)
        "yawcam_quality": 50,
        "check_daylight": True,  # Only capture during daylight
        "daylight_margin_minutes": 60,
        "interval_minutes": 20,  # Snapshot every 20 minutes (conservative rate)
        "cron_offset": 8,  # Runs at :08, :28, :48
    },
}


STORAGE_MOUNT = Path("/mnt/storage")


def ensure_storage_mounted(logger):
    """Check if /mnt/storage is mounted; attempt mount if not.

    Returns True if mounted (or successfully remounted), False otherwise.
    """
    if os.path.ismount(STORAGE_MOUNT):
        return True

    logger.warning(f"{STORAGE_MOUNT} is not mounted — attempting to mount")
    try:
        result = subprocess.run(
            ["mount", str(STORAGE_MOUNT)],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and os.path.ismount(STORAGE_MOUNT):
            logger.info(f"{STORAGE_MOUNT} remounted successfully")
            return True
        logger.warning(f"Mount failed: {result.stderr.strip()}")
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.warning(f"Mount attempt failed: {e}")

    return False


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

    # Check if external storage is mounted
    storage_mounted = ensure_storage_mounted(logger)
    if not storage_mounted:
        logger.warning("External storage unavailable — will update website only, skipping archive")

    # Ensure directories exist
    if storage_mounted:
        config["archive_dir"].mkdir(parents=True, exist_ok=True)
    config["website_dir"].mkdir(parents=True, exist_ok=True)

    # Generate timestamp-based filename
    timestamp = datetime.now(timezone.utc)
    timestamp_readable = timestamp.strftime("%Y%m%d_%H%M%S")
    timestamp_unix = int(timestamp.timestamp())
    filename = f"{config['prefix']}_{timestamp_readable}_{timestamp_unix}.jpg"

    # Capture to archive directory, or a temp file if storage is unmounted
    if storage_mounted:
        archive_path = config["archive_dir"] / filename
    else:
        _tmpdir = tempfile.mkdtemp(prefix="webcam_")
        archive_path = Path(_tmpdir) / filename

    # Check if this is a direct image URL, YouTube stream, or Yawcam
    if "image_url" in config:
        # Direct image download
        if not download_image(config["image_url"], archive_path, logger):
            logger.error("Failed to download image - aborting")
            sys.exit(1)
    elif "youtube_url" in config:
        # YouTube livestream capture
        video_id = config.get("video_id")
        max_height = config.get("max_height", 480)  # Default 480p
        if not capture_youtube_frame(config["youtube_url"], archive_path, config["crop"], logger, video_id, max_height):
            logger.error("Failed to capture YouTube frame - aborting")
            sys.exit(1)
    elif "yawcam_url" in config:
        # Yawcam server capture
        quality = config.get("yawcam_quality", 50)
        if not capture_yawcam_image(config["yawcam_url"], archive_path, quality, logger):
            logger.error("Failed to capture Yawcam image - aborting")
            sys.exit(1)
    else:
        logger.error("Config must have 'image_url', 'youtube_url', or 'yawcam_url'")
        sys.exit(1)

    # Annotate image with timestamp if enabled
    if config.get("annotate_timestamp", False):
        if not annotate_image(archive_path, timestamp, logger):
            logger.warning("Failed to annotate image, continuing anyway")

    # Atomically update latest.jpg
    website_latest = config["website_dir"] / "latest.jpg"
    website_temp = config["website_dir"] / f"latest.tmp.{timestamp_unix}.jpg"
    try:
        shutil.copy2(archive_path, website_temp)
        website_temp.rename(website_latest)
        # Ensure web-readable permissions (0644)
        os.chmod(website_latest, 0o644)
        logger.info(f"Atomically updated: {website_latest}")
    except Exception as e:
        logger.error(f"Failed to copy to website directory: {e}")

    # Manage slideshow images
    manage_slideshow_images(config["website_dir"], archive_path, timestamp_unix, logger)

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

    # Cleanup old archives (only when storage is mounted)
    if storage_mounted:
        cleanup_old_archives(config["archive_dir"], config["prefix"], logger)
    else:
        # Clean up temp capture file
        try:
            archive_path.unlink(missing_ok=True)
            archive_path.parent.rmdir()
        except OSError:
            pass
        logger.warning("Completed without archiving — storage was unavailable")

    logger.info(f"=== {config['name']} Webcam Capture Completed Successfully ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
