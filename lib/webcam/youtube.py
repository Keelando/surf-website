"""
YouTube livestream frame capture with 3-tier fallback strategy.

Strategy (quality-optimized):
1. yt-dlp + Deno (configurable resolution) - requires Deno runtime
2. YouTube live thumbnail (640x360 cropped to 16:9, 10-30s delay)
3. yt-dlp web_embedded (144p) - last resort
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import requests

# Determine yt-dlp path (use venv's yt-dlp if running in venv)
_venv_bin = Path(sys.executable).parent
YT_DLP_PATH = str(_venv_bin / "yt-dlp") if (_venv_bin / "yt-dlp").exists() else "yt-dlp"


def capture_youtube_thumbnail(video_id, output_path, crop_filter, logger):
    """Capture frame using YouTube's live thumbnail (fastest, lowest bandwidth).

    YouTube generates live thumbnails for active streams that update every 10-30 seconds.
    This is much faster and uses less bandwidth than downloading video segments.

    Returns True if successful, False if thumbnail unavailable.
    """
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

    Args:
        youtube_url: Full YouTube URL (e.g., https://www.youtube.com/watch?v=...)
        output_path: Path object where the captured frame will be saved
        crop_filter: ffmpeg crop filter (e.g., "in_w*0.75:in_h:in_w*0.25:0")
        logger: Logger instance for output
        video_id: Optional video ID to skip URL parsing
        max_height: Maximum video height for Deno capture (default 480)

    Returns:
        True if capture successful, False otherwise
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
