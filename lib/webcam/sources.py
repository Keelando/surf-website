"""
Image acquisition from direct URLs and Yawcam servers.
"""

import os
import random
import subprocess
import time

import requests


def download_image(image_url, output_path, logger):
    """Download an image directly from a URL using curl.

    Args:
        image_url: URL of the image to download
        output_path: Path object where the image will be saved
        logger: Logger instance for output

    Returns:
        True if download successful, False otherwise
    """
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

    Args:
        base_url: Base URL of the Yawcam server (e.g., "http://example.com:8081/")
        output_path: Path object where the image will be saved
        quality: JPEG quality setting (1-100)
        logger: Logger instance for output

    Returns:
        True if capture successful, False otherwise
    """
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
