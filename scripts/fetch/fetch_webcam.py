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
import requests

# Determine yt-dlp path (use venv's yt-dlp if running in venv)
_venv_bin = Path(sys.executable).parent
YT_DLP_PATH = str(_venv_bin / "yt-dlp") if (_venv_bin / "yt-dlp").exists() else "yt-dlp"

# Import shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.daylight import is_daylight
from lib.logging_config import setup_logging

# Webcam configurations
# YouTube streams support: max_height (resolution), interval_minutes, check_daylight, crop
# Direct image URLs (like mudbay) just fetch the image as-is
WEBCAM_CONFIGS = {
    "whiterock": {
        "name": "White Rock Pier Cam",
        "youtube_url": "https://www.youtube.com/watch?v=4MK3E9EWDSY",
        "video_id": "4MK3E9EWDSY",
        "archive_dir": Path("/mnt/storage/whiterock_cam"),
        "website_dir": Path.home() / "site" / "data" / "wrcam",
        "prefix": "WR",
        "crop": "in_w*0.75:in_h:in_w*0.25:0",  # Crop left 25% (street), keep right 75% (pier/sea)
        "source_text": "White Rock Pier - YouTube Livestream",
        "lat": 49.0253,
        "lon": -122.8031,
        "max_height": 720,          # 720p for detail
        "check_daylight": False,    # Capture 24/7
        "interval_minutes": 10,     # Snapshot every 10 minutes
        "cron_offset": 0            # Runs at :00, :10, :20, etc.
    },
    "boundarybay": {
        "name": "White Rock East Beach",
        "youtube_url": "https://www.youtube.com/watch?v=O8RsAq9RUlA",
        "video_id": "O8RsAq9RUlA",
        "archive_dir": Path("/mnt/storage/boundarybay_cam"),
        "website_dir": Path.home() / "site" / "data" / "bbcam",
        "prefix": "BB",
        "crop": "in_w:in_h:0:0",    # Full frame
        "source_text": "White Rock East Beach - YouTube Livestream",
        "lat": 49.0042,
        "lon": -123.0128,
        "max_height": 480,          # 480p default
        "check_daylight": False,    # Capture 24/7
        "interval_minutes": 10,     # Snapshot every 10 minutes
        "cron_offset": 2            # Runs at :02, :12, :22, etc.
    },
    "coxbay": {
        "name": "Cox Bay",
        "youtube_url": "https://www.youtube.com/watch?v=LqaP8m2OIqM",
        "video_id": "LqaP8m2OIqM",
        "archive_dir": Path("/mnt/storage/coxbay_cam"),
        "website_dir": Path.home() / "site" / "data" / "coxbay",
        "prefix": "CB",
        "crop": "in_w:in_h:0:0",    # Full frame
        "source_text": "Cox Bay (Tofino) - Pacific Sands Beach Resort Livestream",
        "lat": 49.1167,
        "lon": -125.9000,
        "max_height": 720,          # 720p for surf detail
        "check_daylight": True,     # Only capture during daylight
        "daylight_margin_minutes": 75,
        "interval_minutes": 15,     # Snapshot every 15 minutes
        "cron_offset": 4            # Runs at :04, :19, :34, :49
    },
    "mudbay": {
        "name": "Mud Bay HD",
        "image_url": "https://oxblue.com/archive/c6713f391eef15e5c1dbfc6a003b83a0/1024x768.jpg",
        "archive_dir": Path("/mnt/storage/mudbay_cam"),
        "website_dir": Path.home() / "site" / "data" / "mudbay",
        "prefix": "MB",
        "crop": "in_w:in_h:0:0",    # Full frame
        "source_text": "Mud Bay HD - OxBlue Construction Cam",
        "lat": 49.07138649092664,
        "lon": -122.95538135838513,
        # No max_height - direct image URL, fetched as-is (1024x768)
        "check_daylight": True,     # Only capture during daylight
        "daylight_margin_minutes": 75,
        "interval_minutes": 30,     # Snapshot every 30 minutes
        "cron_offset": 6,           # Runs at :06, :36
        "annotate_timestamp": True  # Add timestamp overlay
    },
    "englishbay": {
        "name": "English Bay (Hollyburn Sailing Club)",
        "yawcam_url": "http://onsite.hollyburnsailingclub.ca:8081/",
        "archive_dir": Path("/mnt/storage/englishbay_cam"),
        "website_dir": Path.home() / "site" / "data" / "englishbay",
        "prefix": "EB",
        "crop": "in_w:in_h:0:0",    # Full frame
        "source_text": "Hollyburn Sailing Club Webcam",
        "source_url": "https://www.hollyburnsailingclub.ca/webcam",
        "lat": 49.2997,
        "lon": -123.1594,
        # Yawcam quality setting (1-100)
        "yawcam_quality": 50,
        "check_daylight": True,     # Only capture during daylight
        "daylight_margin_minutes": 60,
        "interval_minutes": 20,     # Snapshot every 20 minutes (conservative rate)
        "cron_offset": 8            # Runs at :08, :28, :48
    }
}

# Disk space management - cleanup when disk usage exceeds this percentage
DISK_USAGE_THRESHOLD_PERCENT = 80

# Keep last N images for slideshow
SLIDESHOW_IMAGES_COUNT = 7


def setup_logger(config_name):
    """Setup logging for this webcam using centralized logging config"""
    return setup_logging(f'webcam_{config_name}')


def capture_youtube_thumbnail(video_id, output_path, crop_filter, logger):
    """Capture frame using YouTube's live thumbnail (fastest, lowest bandwidth).

    YouTube generates live thumbnails for active streams that update every 10-30 seconds.
    This is much faster and uses less bandwidth than downloading video segments.

    Returns True if successful, False if thumbnail unavailable.
    """
    import tempfile
    import os

    # YouTube live thumbnail URLs (try higher quality first)
    thumbnail_urls = [
        f"https://i.ytimg.com/vi/{video_id}/sddefault_live.jpg",   # 640x480
        f"https://i.ytimg.com/vi/{video_id}/hqdefault_live.jpg",   # 480x360
        f"https://i.ytimg.com/vi/{video_id}/mqdefault_live.jpg",   # 320x180
    ]

    for thumb_url in thumbnail_urls:
        try:
            logger.info(f"Trying YouTube thumbnail: {thumb_url.split('/')[-1]}")
            response = requests.get(thumb_url, timeout=10)

            # Check for valid image (not a placeholder)
            # Real thumbnails are usually >5 KB, placeholders are tiny
            if response.status_code == 200 and len(response.content) > 5000:
                # Save to temp file first for cropping
                temp_fd, temp_thumb = tempfile.mkstemp(suffix='.jpg')
                os.close(temp_fd)

                try:
                    with open(temp_thumb, 'wb') as f:
                        f.write(response.content)

                    logger.info(f"Thumbnail downloaded ({len(response.content)/1024:.1f} KB)")

                    # Always crop thumbnail to 16:9 to match Deno output (avoid black bars)
                    # Thumbnails are 4:3 (640x480), crop to 16:9 (640x360)
                    # Then apply any additional config crop filter
                    crop_16_9 = "in_w:in_w*9/16:0:(in_h-in_w*9/16)/2"
                    if crop_filter and crop_filter != "in_w:in_h:0:0":
                        # Chain 16:9 crop with config crop
                        vf_filter = f"crop={crop_16_9},crop={crop_filter}"
                    else:
                        vf_filter = f"crop={crop_16_9}"

                    result = subprocess.run(
                        [
                            "ffmpeg",
                            "-hide_banner",
                            "-loglevel", "error",
                            "-i", temp_thumb,
                            "-vf", vf_filter,
                            "-q:v", "3",
                            "-y",
                            str(output_path)
                        ],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result.returncode != 0:
                        logger.warning(f"Crop to 16:9 failed: {result.stderr}")
                        # Fall back to uncropped (better than nothing)
                        import shutil
                        shutil.copy2(temp_thumb, output_path)

                    if output_path.exists():
                        # Ensure web-readable permissions (0644)
                        os.chmod(output_path, 0o644)
                        size_kb = output_path.stat().st_size / 1024
                        logger.info(f"Thumbnail capture successful ({size_kb:.1f} KB)")
                        return True

                finally:
                    if os.path.exists(temp_thumb):
                        os.unlink(temp_thumb)

        except requests.Timeout:
            logger.warning(f"Thumbnail request timed out")
            continue
        except Exception as e:
            logger.warning(f"Thumbnail fetch failed: {e}")
            continue

    return False


def capture_youtube_frame_deno(youtube_url, output_path, crop_filter, logger, max_height=480):
    """Primary: Capture frame using yt-dlp with Deno.

    Uses Deno JavaScript runtime to handle YouTube's PO token authentication.
    Returns True if successful, False if Deno unavailable or capture fails.
    """
    import tempfile
    import os
    import shutil

    # Check if Deno is available
    deno_path = os.path.expanduser("~/.deno/bin/deno")
    if not os.path.exists(deno_path) and not shutil.which("deno"):
        logger.info("Deno not installed, skipping Deno method")
        return False

    temp_video = None
    try:
        temp_fd, temp_video = tempfile.mkstemp(suffix='.mp4')
        os.close(temp_fd)

        logger.info(f"Downloading video via yt-dlp + Deno ({max_height}p)...")

        # Use Deno for JS challenges, explicitly specify path
        result = subprocess.run(
            [
                YT_DLP_PATH,
                "--js-runtimes", f"deno:{deno_path}",
                "-f", f"best[height<={max_height}]",
                "--downloader-args", "ffmpeg:-t 2",  # 2 seconds of video
                "--no-continue",
                "-o", temp_video,
                youtube_url
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            logger.warning(f"yt-dlp Deno failed: {result.stderr[:300] if result.stderr else 'no error output'}")
            return False

        if not os.path.exists(temp_video) or os.path.getsize(temp_video) == 0:
            logger.warning("yt-dlp Deno completed but video file is missing or empty")
            return False

        logger.info(f"Video segment downloaded ({os.path.getsize(temp_video) / 1024:.1f} KB)")

        # Extract frame with optional crop
        filter_complex = f"crop={crop_filter}" if crop_filter and crop_filter != "in_w:in_h:0:0" else None
        ffmpeg_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-i", temp_video,
        ]
        if filter_complex:
            ffmpeg_cmd.extend(["-vf", filter_complex])
        ffmpeg_cmd.extend([
            "-frames:v", "1",
            "-q:v", "3",
            "-y",
            str(output_path)
        ])

        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            logger.warning(f"ffmpeg frame extraction failed: {result.stderr}")
            return False

        if output_path.exists():
            os.chmod(output_path, 0o644)
            size_kb = output_path.stat().st_size / 1024
            logger.info(f"Deno capture successful ({size_kb:.1f} KB)")
            return True

        return False

    except subprocess.TimeoutExpired:
        logger.warning("yt-dlp Deno capture timed out")
        return False
    except Exception as e:
        logger.warning(f"Deno capture failed: {e}")
        return False
    finally:
        if temp_video and os.path.exists(temp_video):
            try:
                os.unlink(temp_video)
            except:
                pass


def capture_youtube_frame_web_embedded(youtube_url, output_path, crop_filter, logger):
    """Last resort: Capture frame using yt-dlp web_embedded client (144p).

    Downloads a short video segment and extracts a frame.
    Uses web_embedded player client which doesn't require Deno/JS runtime.
    """
    import tempfile
    import os

    temp_video = None
    try:
        # Create temp file for video segment
        temp_fd, temp_video = tempfile.mkstemp(suffix='.mp4')
        os.close(temp_fd)

        # Download 0.5-second video segment using yt-dlp
        # web_embedded client works without Deno and avoids most 403 errors
        logger.info("Downloading video segment via yt-dlp (web_embedded)...")
        result = subprocess.run(
            [
                YT_DLP_PATH,
                "--extractor-args", "youtube:player_client=web_embedded",
                "-f", "worst",
                "--downloader-args", "ffmpeg:-t 0.5",
                "--no-continue",
                "-o", temp_video,
                youtube_url
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            logger.warning(f"yt-dlp web_embedded failed: {result.stderr[:200]}")
            # Try fallback with request-no-ads
            logger.info("Trying fallback: request-no-ads=false...")
            result = subprocess.run(
                [
                    YT_DLP_PATH,
                    "--extractor-args", "youtube:request-no-ads=false",
                    "-f", "worst",
                    "--downloader-args", "ffmpeg:-t 0.5",
                    "--no-continue",
                    "-o", temp_video,
                    youtube_url
                ],
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                logger.error(f"yt-dlp fallback also failed: {result.stderr[:200]}")
                return False

        if not os.path.exists(temp_video) or os.path.getsize(temp_video) == 0:
            logger.error("yt-dlp completed but video file is missing or empty")
            return False

        logger.info(f"Video segment downloaded ({os.path.getsize(temp_video) / 1024:.1f} KB)")

        # Extract frame from downloaded video
        filter_complex = f"crop={crop_filter}"

        result = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-i", temp_video,
                "-vf", filter_complex,
                "-frames:v", "1",
                "-q:v", "3",
                "-y",
                str(output_path)
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            logger.error(f"ffmpeg frame extraction failed: {result.stderr}")
            return False

        if output_path.exists():
            # Ensure web-readable permissions (0644)
            os.chmod(output_path, 0o644)
            size_kb = output_path.stat().st_size / 1024
            logger.info(f"Frame extracted successfully ({size_kb:.1f} KB)")
            return True
        else:
            logger.error("ffmpeg completed but output file not found")
            return False

    except subprocess.TimeoutExpired:
        logger.error("yt-dlp capture timed out")
        return False
    except Exception as e:
        logger.error(f"Failed to capture frame: {e}")
        return False
    finally:
        # Clean up temp video file
        if temp_video and os.path.exists(temp_video):
            try:
                os.unlink(temp_video)
            except:
                pass


def capture_youtube_frame(youtube_url, output_path, crop_filter, logger, video_id=None, max_height=480):
    """Capture a frame from YouTube livestream.

    Strategy (3-tier, quality-optimized):
    1. yt-dlp + Deno (configurable resolution) - requires Deno runtime
    2. YouTube live thumbnail (640x360 cropped to 16:9, 10-30s delay)
    3. yt-dlp web_embedded (144p) - last resort
    """
    logger.info(f"Capturing frame from YouTube: {youtube_url}")

    # Extract video_id from URL if not provided
    if not video_id:
        if "watch?v=" in youtube_url:
            video_id = youtube_url.split("watch?v=")[-1].split("&")[0]
        elif "youtu.be/" in youtube_url:
            video_id = youtube_url.split("youtu.be/")[-1].split("?")[0]

    # Strategy 1: Try yt-dlp + Deno (best quality)
    logger.info(f"Attempting yt-dlp + Deno capture ({max_height}p)...")
    if capture_youtube_frame_deno(youtube_url, output_path, crop_filter, logger, max_height):
        return True

    # Strategy 2: Fall back to thumbnail (good quality, reliable)
    if video_id:
        logger.info("Deno failed, trying thumbnail capture (640x360)...")
        if capture_youtube_thumbnail(video_id, output_path, crop_filter, logger):
            return True

    # Strategy 3: Last resort - web_embedded (low quality but works)
    logger.info("Thumbnail failed, trying web_embedded (144p last resort)...")
    if capture_youtube_frame_web_embedded(youtube_url, output_path, crop_filter, logger):
        return True

    logger.error("All capture methods failed")
    return False


def download_image(image_url, output_path, logger):
    """Download an image directly from a URL using curl"""
    try:
        logger.info(f"Downloading image from: {image_url}")
        logger.info(f"Saving to: {output_path}")

        result = subprocess.run(
            [
                "curl",
                "-f",  # Fail silently on HTTP errors
                "-L",  # Follow redirects
                "-o", str(output_path),
                "--max-time", "30",
                "--connect-timeout", "10",
                image_url
            ],
            capture_output=True,
            text=True,
            timeout=40
        )

        if result.returncode != 0:
            logger.error(f"curl failed with code {result.returncode}: {result.stderr}")
            return False

        if output_path.exists():
            size_kb = output_path.stat().st_size / 1024
            logger.info(f"Image downloaded successfully ({size_kb:.1f} KB)")
            return True
        else:
            logger.error("curl completed but output file not found")
            return False

    except subprocess.TimeoutExpired:
        logger.error("curl timed out after 40 seconds")
        return False
    except Exception as e:
        logger.error(f"Failed to download image: {e}")
        return False


def capture_yawcam_image(base_url, output_path, quality, logger):
    """Capture an image from a Yawcam server.

    Yawcam requires a session handshake before serving images.
    1. GET /get?id=<session_id>&r=<random> - establish session
    2. GET /out.jpg?q=<quality>&id=<session_id>&r=<timestamp> - grab image
    """
    import random
    import time
    import os

    session = requests.Session()

    try:
        # Generate random session ID (like the JavaScript does)
        session_id = random.random()

        # Step 1: Handshake
        logger.info(f"Yawcam handshake with {base_url}")
        handshake_url = f"{base_url}get"
        handshake_params = {
            'id': session_id,
            'r': random.random()
        }

        response = session.get(handshake_url, params=handshake_params, timeout=10)
        response.raise_for_status()

        status = response.text.strip()
        if status == "2many":
            logger.warning("Yawcam: Too many viewers - try again later")
            return False
        elif status == "pass":
            logger.error("Yawcam: Webcam requires password")
            return False
        elif status != "ok":
            logger.warning(f"Yawcam handshake response: '{status}'")
            # Continue anyway in case it's still a valid session

        # Small delay between handshake and image grab
        time.sleep(0.5)

        # Step 2: Grab image
        logger.info(f"Yawcam grabbing image (quality={quality})")
        image_url = f"{base_url}out.jpg"
        image_params = {
            'q': quality,
            'id': session_id,
            'r': int(time.time() * 1000)
        }

        response = session.get(image_url, params=image_params, timeout=15)
        response.raise_for_status()

        # Validate image
        if len(response.content) < 1000:
            logger.warning(f"Yawcam: Response too small ({len(response.content)} bytes)")
            return False

        if not response.content.startswith(b'\xff\xd8'):
            logger.warning("Yawcam: Not a valid JPEG")
            return False

        # Write image atomically
        tmp_path = output_path.with_suffix(".jpg.tmp")
        tmp_path.write_bytes(response.content)
        tmp_path.replace(output_path)

        # Ensure web-readable permissions (0644)
        os.chmod(output_path, 0o644)

        size_kb = len(response.content) / 1024
        logger.info(f"Yawcam image captured successfully ({size_kb:.1f} KB)")
        return True

    except requests.Timeout:
        logger.error("Yawcam request timed out")
        return False
    except requests.RequestException as e:
        logger.error(f"Yawcam request failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Yawcam capture failed: {e}")
        return False


def annotate_image(image_path, timestamp, logger):
    """Add timestamp annotation to image using Pillow"""
    try:
        import pytz
        from PIL import Image, ImageDraw, ImageFont

        # Convert UTC timestamp to PST
        pst = pytz.timezone('America/Vancouver')
        timestamp_pst = timestamp.astimezone(pst)

        # Format as "Retrieval time: 2026-01-13T11:40PST"
        timestamp_str = f"Retrieval time: {timestamp_pst.strftime('%Y-%m-%dT%H:%M')}PST"

        logger.info(f"Annotating image with: {timestamp_str}")

        # Open the image
        img = Image.open(image_path)
        draw = ImageDraw.Draw(img)

        # Try to use a nice font, fall back to default if not available
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        except:
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", 24)
            except:
                # Use default font if no system fonts available
                font = ImageFont.load_default()
                logger.warning("Using default font (system fonts not found)")

        # Position for text (top left with some padding)
        position = (10, 10)

        # Draw text with black outline for readability
        outline_width = 2
        x, y = position

        # Draw outline (black)
        for offset_x in range(-outline_width, outline_width + 1):
            for offset_y in range(-outline_width, outline_width + 1):
                if offset_x != 0 or offset_y != 0:
                    draw.text((x + offset_x, y + offset_y), timestamp_str, font=font, fill='black')

        # Draw main text (white)
        draw.text(position, timestamp_str, font=font, fill='white')

        # Save the annotated image (overwrite original)
        img.save(image_path, 'JPEG', quality=95)

        logger.info("Image annotation successful")
        return True

    except Exception as e:
        logger.error(f"Failed to annotate image: {e}")
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
        import os
        timestamp = datetime.now(timezone.utc)
        slideshow_filename = f"img_{int(timestamp.timestamp())}.jpg"
        slideshow_path = slideshow_dir / slideshow_filename
        shutil.copy2(new_image_path, slideshow_path)
        # Ensure web-readable permissions (0644)
        os.chmod(slideshow_path, 0o644)
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

    # Generate timestamp-based filename
    timestamp = datetime.now(timezone.utc)
    timestamp_readable = timestamp.strftime("%Y%m%d_%H%M%S")
    timestamp_unix = int(timestamp.timestamp())
    filename = f"{config['prefix']}_{timestamp_readable}_{timestamp_unix}.jpg"

    # Capture to archive directory first
    archive_path = config["archive_dir"] / filename

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
        import shutil
        import os
        shutil.copy2(archive_path, website_temp)
        website_temp.rename(website_latest)
        # Ensure web-readable permissions (0644)
        os.chmod(website_latest, 0o644)
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
        "url": config.get("youtube_url") or config.get("image_url") or config.get("source_url")
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
