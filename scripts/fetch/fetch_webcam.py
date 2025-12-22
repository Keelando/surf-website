#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
"""
Generic Webcam Capture Script

Fetches frames from YouTube livestreams and saves them to both archive
and website directories with slideshow support (keeps last 7 images).

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
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import logging

# Import daylight detection
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.daylight import is_daylight

# Webcam configurations
WEBCAM_CONFIGS = {
    "whiterock": {
        "name": "White Rock Pier Cam",
        "youtube_url": "https://www.youtube.com/watch?v=4MK3E9EWDSY",
        "archive_dir": Path("/mnt/storage/whiterock_cam"),
        "website_dir": Path.home() / "site" / "data" / "wrcam",
        "prefix": "WR",
        "crop": "in_w*0.75:in_h:in_w*0.25:0",  # Crop left 25% (street), keep right 75% (pier/sea)
        "source_text": "White Rock Pier - YouTube Livestream",
        "lat": 49.0253,
        "lon": -122.8031,
        "check_daylight": False  # Capture 24/7
    },
    "boundarybay": {
        "name": "White Rock East Beach",
        "youtube_url": "https://www.youtube.com/watch?v=O8RsAq9RUlA",
        "archive_dir": Path("/mnt/storage/boundarybay_cam"),
        "website_dir": Path.home() / "site" / "data" / "bbcam",
        "prefix": "BB",
        "crop": "in_w:in_h:0:0",  # Full frame, no cropping
        "source_text": "White Rock East Beach - YouTube Livestream",
        "lat": 49.0042,
        "lon": -123.0128,
        "check_daylight": False  # Capture 24/7
    },
    "coxbay": {
        "name": "Cox Bay",
        "youtube_url": "https://www.youtube.com/watch?v=LqaP8m2OIqM",
        "archive_dir": Path("/mnt/storage/coxbay_cam"),
        "website_dir": Path.home() / "site" / "data" / "coxbay",
        "prefix": "CB",
        "crop": "in_w:in_h:0:0",  # Full frame initially - adjust after testing
        "source_text": "Cox Bay (Tofino) - Pacific Sands Beach Resort Livestream",
        "lat": 49.1167,
        "lon": -125.9000,
        "check_daylight": True,  # Only capture during daylight
        "daylight_margin_minutes": 60  # Stop 1 hour after sunset, start 1 hour before sunrise
    }
}

# Disk space management - cleanup when disk usage exceeds this percentage
DISK_USAGE_THRESHOLD_PERCENT = 80

# Keep last N images for slideshow
SLIDESHOW_IMAGES_COUNT = 7


def setup_logger(config_name):
    """Setup logging for this webcam"""
    log_path = Path(__file__).parent / "logs" / f"webcam_{config_name}.log"
    log_path.parent.mkdir(exist_ok=True)  # Ensure logs/ directory exists
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def get_stream_url(youtube_url, logger):
    """Get the actual stream URL from YouTube using yt-dlp"""
    try:
        logger.info(f"Fetching stream URL from YouTube: {youtube_url}")

        result = subprocess.run(
            ["yt-dlp", "-f", "best", "-g", youtube_url],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            logger.error(f"yt-dlp failed: {result.stderr}")
            return None

        stream_url = result.stdout.strip()
        logger.info(f"Got stream URL (length: {len(stream_url)} chars)")
        return stream_url

    except subprocess.TimeoutExpired:
        logger.error("yt-dlp timed out after 30 seconds")
        return None
    except Exception as e:
        logger.error(f"Failed to get stream URL: {e}")
        return None


def capture_frame(stream_url, output_path, timestamp, crop_filter, logger):
    """Capture a single frame from the stream using ffmpeg with cropping"""
    try:
        logger.info(f"Capturing frame to: {output_path}")

        # Build filter: crop only (stream already has its own timestamp)
        filter_complex = f"crop={crop_filter}"

        result = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-i", stream_url,
                "-vf", filter_complex,
                "-frames:v", "1",
                "-q:v", "3",
                "-y",
                str(output_path)
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            logger.error(f"ffmpeg failed: {result.stderr}")
            return False

        if output_path.exists():
            size_kb = output_path.stat().st_size / 1024
            logger.info(f"Frame captured successfully ({size_kb:.1f} KB)")
            return True
        else:
            logger.error("ffmpeg completed but output file not found")
            return False

    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timed out after 60 seconds")
        return False
    except Exception as e:
        logger.error(f"Failed to capture frame: {e}")
        return False


def cleanup_old_archives(archive_dir, prefix, logger):
    """Remove oldest archive images when disk usage exceeds threshold"""
    try:
        import shutil

        # Get disk usage for the archive directory's filesystem
        disk_usage = shutil.disk_usage(archive_dir)
        usage_percent = (disk_usage.used / disk_usage.total) * 100

        logger.info(f"Disk usage: {usage_percent:.1f}% ({disk_usage.used / (1024**3):.1f}GB / {disk_usage.total / (1024**3):.1f}GB)")

        # Only cleanup if above threshold
        if usage_percent < DISK_USAGE_THRESHOLD_PERCENT:
            logger.info(f"Disk usage below {DISK_USAGE_THRESHOLD_PERCENT}% threshold - no cleanup needed")
            return

        logger.info(f"Disk usage above {DISK_USAGE_THRESHOLD_PERCENT}% threshold - cleaning up oldest images")

        # Get all archive images sorted by modification time (oldest first)
        all_images = sorted(
            archive_dir.glob(f"{prefix}_*.jpg"),
            key=lambda p: p.stat().st_mtime
        )

        if not all_images:
            logger.warning("No images found to cleanup")
            return

        deleted_count = 0
        deleted_size_mb = 0

        # Delete oldest images until we're below threshold (with 5% buffer)
        target_percent = DISK_USAGE_THRESHOLD_PERCENT - 5

        for img_path in all_images:
            # Re-check disk usage after each deletion
            disk_usage = shutil.disk_usage(archive_dir)
            current_percent = (disk_usage.used / disk_usage.total) * 100

            if current_percent <= target_percent:
                break

            # Keep at least the most recent 24 hours of images
            age_hours = (datetime.now().timestamp() - img_path.stat().st_mtime) / 3600
            if age_hours < 24:
                logger.info(f"Stopped cleanup - remaining images are less than 24 hours old")
                break

            size_mb = img_path.stat().st_size / (1024 * 1024)
            img_path.unlink()
            deleted_count += 1
            deleted_size_mb += size_mb

        if deleted_count > 0:
            disk_usage = shutil.disk_usage(archive_dir)
            final_percent = (disk_usage.used / disk_usage.total) * 100
            logger.info(f"Cleaned up {deleted_count} old images ({deleted_size_mb:.1f} MB)")
            logger.info(f"New disk usage: {final_percent:.1f}%")
        else:
            logger.warning(f"Disk usage still above threshold but no images old enough to delete")

    except Exception as e:
        logger.warning(f"Failed to cleanup old archives: {e}")


def manage_slideshow_images(website_dir, new_image_path, logger):
    """Manage slideshow images - keep only the last N images"""
    try:
        slideshow_dir = website_dir / "slideshow"
        slideshow_dir.mkdir(exist_ok=True)

        # Get all existing slideshow images sorted by modification time
        existing_images = sorted(
            slideshow_dir.glob("*.jpg"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )

        # Copy new image to slideshow directory with numbered name
        import shutil
        timestamp = datetime.now(timezone.utc)
        slideshow_filename = f"img_{int(timestamp.timestamp())}.jpg"
        slideshow_path = slideshow_dir / slideshow_filename
        shutil.copy2(new_image_path, slideshow_path)
        logger.info(f"Added to slideshow: {slideshow_filename}")

        # Re-get list with new image
        existing_images = sorted(
            slideshow_dir.glob("*.jpg"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )

        # Keep only the last N images, delete older ones
        if len(existing_images) > SLIDESHOW_IMAGES_COUNT:
            for old_image in existing_images[SLIDESHOW_IMAGES_COUNT:]:
                old_image.unlink()
                logger.info(f"Removed old slideshow image: {old_image.name}")

        # Create manifest of current slideshow images (newest to oldest)
        manifest = []
        for img in existing_images[:SLIDESHOW_IMAGES_COUNT]:
            manifest.append({
                "filename": img.name,
                "timestamp": datetime.fromtimestamp(img.stat().st_mtime, tz=timezone.utc).isoformat(),
                "path": f"slideshow/{img.name}"
            })

        manifest_path = website_dir / "slideshow_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Updated slideshow manifest: {len(manifest)} images")

    except Exception as e:
        logger.error(f"Failed to manage slideshow images: {e}")


def main():
    """Main execution"""
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

    # Ensure directories exist
    config["archive_dir"].mkdir(parents=True, exist_ok=True)
    config["website_dir"].mkdir(parents=True, exist_ok=True)

    # Get stream URL
    stream_url = get_stream_url(config["youtube_url"], logger)
    if not stream_url:
        logger.error("Failed to get stream URL - aborting")
        sys.exit(1)

    # Generate timestamp-based filename
    timestamp = datetime.now(timezone.utc)
    timestamp_readable = timestamp.strftime("%Y%m%d_%H%M%S")
    timestamp_unix = int(timestamp.timestamp())
    filename = f"{config['prefix']}_{timestamp_readable}_{timestamp_unix}.jpg"

    # Capture to archive directory first
    archive_path = config["archive_dir"] / filename
    if not capture_frame(stream_url, archive_path, timestamp, config["crop"], logger):
        logger.error("Failed to capture frame - aborting")
        sys.exit(1)

    # Atomically update latest.jpg
    website_latest = config["website_dir"] / "latest.jpg"
    website_temp = config["website_dir"] / f"latest.tmp.{timestamp_unix}.jpg"
    try:
        import shutil
        shutil.copy2(archive_path, website_temp)
        website_temp.rename(website_latest)
        logger.info(f"Atomically updated: {website_latest}")
    except Exception as e:
        logger.error(f"Failed to copy to website directory: {e}")

    # Manage slideshow images
    manage_slideshow_images(config["website_dir"], archive_path, logger)

    # Create metadata JSON for website
    metadata = {
        "filename": filename,
        "timestamp": timestamp.isoformat(),
        "timestamp_unix": timestamp_unix,
        "source": config["source_text"],
        "url": config["youtube_url"]
    }

    metadata_path = config["website_dir"] / "latest.json"
    try:
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Updated metadata: {metadata_path}")
    except Exception as e:
        logger.error(f"Failed to write metadata: {e}")

    # Cleanup old archives
    cleanup_old_archives(config["archive_dir"], config["prefix"], logger)

    logger.info(f"=== {config['name']} Webcam Capture Completed Successfully ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
