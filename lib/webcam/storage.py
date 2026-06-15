"""
Archive cleanup and slideshow management utilities.
"""

import contextlib
import json
import os
import shutil
import signal
from datetime import datetime, timezone

# Default constants
DEFAULT_DISK_USAGE_THRESHOLD_PERCENT = 80
DEFAULT_SLIDESHOW_IMAGES_COUNT = 7


class StorageTimeout(Exception):
    """Raised when a storage operation exceeds its time budget (wedged USB-SATA bridge)."""


@contextlib.contextmanager
def time_limit(seconds, what):
    """Abort the wrapped block with StorageTimeout if it runs past `seconds`.

    Uses SIGALRM, so it only interrupts at points the kernel can deliver a
    signal — a truly uninterruptible (D-state) syscall on a dead bridge may
    still outlast this, but it bounds the common slow/flaky case. Main thread only.

    Shared by the webcam fetch pipeline (probe/copy budgets) and the storage
    metrics exporter (disk-read budget) so the SIGALRM logic lives in one place.
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


def cleanup_old_archives(archive_dir, prefix, logger, threshold_percent=DEFAULT_DISK_USAGE_THRESHOLD_PERCENT):
    """Remove oldest archive images when disk usage exceeds threshold.

    Deletes oldest images until disk usage is below threshold, but always
    keeps at least 24 hours of images.

    Args:
        archive_dir: Path to the archive directory
        prefix: Image filename prefix (e.g., "WR" for White Rock)
        logger: Logger instance for output
        threshold_percent: Disk usage percentage threshold (default 80%)
    """
    try:
        # Get disk usage for the archive directory's filesystem
        disk_usage = shutil.disk_usage(archive_dir)
        usage_percent = (disk_usage.used / disk_usage.total) * 100

        logger.info(
            "Disk usage: "
            f"{usage_percent:.1f}% "
            f"({disk_usage.used / (1024**3):.1f}GB / {disk_usage.total / (1024**3):.1f}GB)"
        )

        # Only cleanup if above threshold
        if usage_percent < threshold_percent:
            logger.info(f"Disk usage below {threshold_percent}% threshold - no cleanup needed")
            return

        logger.info(f"Disk usage above {threshold_percent}% threshold - cleaning up oldest images")

        # Get all archive images sorted by modification time (oldest first)
        all_images = sorted(archive_dir.glob(f"{prefix}_*.jpg"), key=lambda p: p.stat().st_mtime)

        if not all_images:
            logger.warning("No images found to cleanup")
            return

        deleted_count = 0
        deleted_size_mb = 0

        # Delete oldest images until we're below threshold (with 5% buffer)
        target_percent = threshold_percent - 5

        for img_path in all_images:
            # Re-check disk usage after each deletion
            disk_usage = shutil.disk_usage(archive_dir)
            current_percent = (disk_usage.used / disk_usage.total) * 100

            if current_percent <= target_percent:
                break

            # Keep at least the most recent 24 hours of images
            age_hours = (datetime.now().timestamp() - img_path.stat().st_mtime) / 3600
            if age_hours < 24:
                logger.info("Stopped cleanup - remaining images are less than 24 hours old")
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
            logger.warning("Disk usage still above threshold but no images old enough to delete")

    except Exception as e:
        logger.warning(f"Failed to cleanup old archives: {e}")


def get_timestamp_from_filename(filepath):
    """Extract unix timestamp from slideshow image filename (img_TIMESTAMP.jpg)."""
    try:
        name = filepath.stem  # img_1769281562
        return int(name.split("_")[1])
    except (IndexError, ValueError):
        return 0


def manage_slideshow_images(
    website_dir,
    new_image_path,
    timestamp_unix,
    logger,
    max_images=DEFAULT_SLIDESHOW_IMAGES_COUNT,
):
    """Manage slideshow images - keep only the last N images.

    Copies the new image to the slideshow directory, removes old images
    beyond the limit, and updates the manifest JSON.

    Args:
        website_dir: Path to the website directory
        new_image_path: Path to the newly captured image
        timestamp_unix: Unix timestamp of when image was captured
        logger: Logger instance for output
        max_images: Maximum number of slideshow images to keep (default 7)
    """
    try:
        slideshow_dir = website_dir / "slideshow"
        slideshow_dir.mkdir(exist_ok=True)

        # Get all existing slideshow images sorted by timestamp in filename (newest first)
        existing_images = sorted(slideshow_dir.glob("img_*.jpg"), key=get_timestamp_from_filename, reverse=True)

        # Copy new image to slideshow directory with capture timestamp in filename
        slideshow_filename = f"img_{timestamp_unix}.jpg"
        slideshow_path = slideshow_dir / slideshow_filename
        shutil.copy(new_image_path, slideshow_path)
        # Ensure web-readable permissions (0644)
        os.chmod(slideshow_path, 0o644)
        logger.info(f"Added to slideshow: {slideshow_filename}")

        # Re-get list with new image, sorted by timestamp in filename (newest first)
        existing_images = sorted(slideshow_dir.glob("img_*.jpg"), key=get_timestamp_from_filename, reverse=True)

        # Keep only the last N images, delete older ones
        if len(existing_images) > max_images:
            for old_image in existing_images[max_images:]:
                old_image.unlink()
                logger.info(f"Removed old slideshow image: {old_image.name}")

        # Create manifest of current slideshow images (newest to oldest)
        manifest = []
        for img in existing_images[:max_images]:
            img_timestamp = get_timestamp_from_filename(img)
            manifest.append(
                {
                    "filename": img.name,
                    "timestamp": datetime.fromtimestamp(img_timestamp, tz=timezone.utc).isoformat(),
                    "path": f"slideshow/{img.name}",
                }
            )

        manifest_path = website_dir / "slideshow_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Updated slideshow manifest: {len(manifest)} images")

    except Exception as e:
        logger.error(f"Failed to manage slideshow images: {e}")
