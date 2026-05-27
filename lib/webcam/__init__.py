"""
Webcam capture and processing utilities.

This module provides functions for capturing images from various sources:
- YouTube livestreams (with 3-tier fallback strategy)
- Direct image URLs
- Yawcam servers

It also provides image processing (annotation) and storage management
(archive cleanup, slideshow rotation).
"""

from lib.webcam.processing import annotate_image
from lib.webcam.sources import capture_yawcam_image, download_image, head_image
from lib.webcam.storage import cleanup_old_archives, manage_slideshow_images
from lib.webcam.youtube import capture_youtube_frame

__all__ = [
    "capture_youtube_frame",
    "download_image",
    "capture_yawcam_image",
    "head_image",
    "annotate_image",
    "cleanup_old_archives",
    "manage_slideshow_images",
]
