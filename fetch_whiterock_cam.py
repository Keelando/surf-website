#!/usr/bin/env python3
"""
White Rock Pier Webcam Capture

Fetches the latest frame from the White Rock Pier YouTube livestream
and saves it to both the Kingston drive (archive) and website data directory.

Requirements:
- yt-dlp
- ffmpeg

Storage:
- Archive: /mnt/storage/whiterock_cam/ (timestamped images)
- Website: ~/site/data/wrcam/ (latest image + metadata)
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import logging

# Configuration
YOUTUBE_URL = "https://www.youtube.com/watch?v=4MK3E9EWDSY"
ARCHIVE_DIR = Path("/mnt/storage/whiterock_cam")
WEBSITE_DIR = Path.home() / "site" / "data" / "wrcam"
LOG_PATH = Path(__file__).parent / "whiterock_cam.log"

# Keep last N days of archive images (to manage disk space)
ARCHIVE_RETENTION_DAYS = 30

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_stream_url():
    """Get the actual stream URL from YouTube using yt-dlp"""
    try:
        logger.info(f"Fetching stream URL from YouTube: {YOUTUBE_URL}")

        # Use yt-dlp to get the best video stream URL
        result = subprocess.run(
            ["yt-dlp", "-f", "best", "-g", YOUTUBE_URL],
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


def capture_frame(stream_url, output_path, timestamp):
    """Capture a single frame from the stream using ffmpeg with cropping and timestamp overlay"""
    try:
        logger.info(f"Capturing frame to: {output_path}")

        # Format timestamp for overlay (YYYY-MM-DD HH:MM:SS PST/PDT)
        # Determine if we're in PST or PDT based on the timestamp
        import time
        is_dst = time.localtime(timestamp.timestamp()).tm_isdst
        tz_abbr = "PDT" if is_dst else "PST"
        timestamp_str = timestamp.strftime("%Y-%m-%d %H\\:%M\\:%S") + f" {tz_abbr}"

        # Use ffmpeg to grab one frame with cropping and timestamp overlay
        # Crop filter removes black bars (adjust values as needed)
        # Format: crop=width:height:x:y
        # Current setting: crop to 16:9 aspect ratio, removing top/bottom black bars
        # Drawtext adds timestamp in bottom-right corner
        filter_complex = (
            "crop=in_w:in_h-200:0:100,"  # Crop black bars
            f"drawtext=text='{timestamp_str}':"
            "fontsize=24:fontcolor=white:borderw=2:bordercolor=black:"
            "x=w-tw-10:y=h-th-10"  # Bottom-right corner with 10px padding
        )

        result = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-i", stream_url,
                "-vf", filter_complex,
                "-frames:v", "1",
                "-q:v", "5",  # JPEG quality (5 is excellent, web-optimized)
                "-y",  # Overwrite output file
                str(output_path)
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            logger.error(f"ffmpeg failed: {result.stderr}")
            return False

        # Verify the file was created
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


def cleanup_old_archives():
    """Remove archive images older than ARCHIVE_RETENTION_DAYS"""
    try:
        from datetime import timedelta

        cutoff_time = datetime.now().timestamp() - (ARCHIVE_RETENTION_DAYS * 24 * 3600)
        deleted_count = 0

        for img_path in ARCHIVE_DIR.glob("WR_*.jpg"):
            if img_path.stat().st_mtime < cutoff_time:
                img_path.unlink()
                deleted_count += 1

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old archive images")

    except Exception as e:
        logger.warning(f"Failed to cleanup old archives: {e}")


def main():
    """Main execution"""
    logger.info("=== White Rock Pier Webcam Capture Started ===")

    # Ensure directories exist
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    WEBSITE_DIR.mkdir(parents=True, exist_ok=True)

    # Get stream URL
    stream_url = get_stream_url()
    if not stream_url:
        logger.error("Failed to get stream URL - aborting")
        sys.exit(1)

    # Generate timestamp-based filename with readable format
    # Format: WR_YYYYMMDD_HHMMSS_UnixTimestamp.jpg
    # Example: WR_20251203_143025_1764801605.jpg
    timestamp = datetime.now(timezone.utc)
    timestamp_readable = timestamp.strftime("%Y%m%d_%H%M%S")
    timestamp_unix = int(timestamp.timestamp())
    filename = f"WR_{timestamp_readable}_{timestamp_unix}.jpg"

    # Capture to archive directory first
    archive_path = ARCHIVE_DIR / filename
    if not capture_frame(stream_url, archive_path, timestamp):
        logger.error("Failed to capture frame - aborting")
        sys.exit(1)

    # Atomically move to website directory as "latest.jpg"
    # Write to temp file first, then atomic rename to avoid serving partial images
    website_latest = WEBSITE_DIR / "latest.jpg"
    website_temp = WEBSITE_DIR / f"latest.tmp.{timestamp_unix}.jpg"
    try:
        import shutil
        shutil.copy2(archive_path, website_temp)
        # Atomic rename - this ensures users never see partial images
        website_temp.rename(website_latest)
        logger.info(f"Atomically updated: {website_latest}")
    except Exception as e:
        logger.error(f"Failed to copy to website directory: {e}")

    # Create metadata JSON for website
    metadata = {
        "filename": filename,
        "timestamp": timestamp.isoformat(),
        "timestamp_unix": int(timestamp.timestamp()),
        "source": "White Rock Pier YouTube Livestream",
        "url": YOUTUBE_URL
    }

    metadata_path = WEBSITE_DIR / "latest.json"
    try:
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Updated metadata: {metadata_path}")
    except Exception as e:
        logger.error(f"Failed to write metadata: {e}")

    # Cleanup old archives
    cleanup_old_archives()

    logger.info("=== White Rock Pier Webcam Capture Completed Successfully ===")
    sys.exit(0)


if __name__ == "__main__":
    main()
