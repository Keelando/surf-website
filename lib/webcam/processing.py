"""
Image post-processing utilities (annotation, etc.).
"""

from datetime import timedelta
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont

# zoneinfo reads the system's tz data, which knows BC keeps UTC-7 all year
# from 2026-03-08. pytz did not: it ships its own copy of the database
# (2025.2 here), which put Vancouver back on PST after 2026-11-01, so every
# stamp read an hour early ("14:21PST" on a 15:21 frame) and the cams looked
# an hour more delayed than they were.
PACIFIC = ZoneInfo("America/Vancouver")


def pacific_label(local):
    """The zone name for a Pacific time: "PDT" at UTC-7, "PST" at UTC-8.

    Not %Z: up-to-date tz data names BC's permanent UTC-7 "MST", which reads
    as Mountain time. "PDT" matches the website (shared/format-time.js).
    """
    return "PDT" if local.utcoffset() == timedelta(hours=-7) else "PST"


def retrieval_label(timestamp):
    """ "Retrieval time: 2026-11-06T15:21PDT" for a UTC datetime."""
    local = timestamp.astimezone(PACIFIC)
    return f"Retrieval time: {local.strftime('%Y-%m-%dT%H:%M')}{pacific_label(local)}"


def annotate_image(image_path, timestamp, logger):
    """Add timestamp annotation to image using Pillow.

    Adds a timestamp overlay in the top-left corner with white text
    and black outline for readability.

    Args:
        image_path: Path to the image file (will be overwritten)
        timestamp: datetime object (UTC) to display
        logger: Logger instance for output

    Returns:
        True if annotation successful, False otherwise
    """
    try:
        timestamp_str = retrieval_label(timestamp)

        logger.info(f"Annotating image with: {timestamp_str}")

        # Open the image
        img = Image.open(image_path)
        draw = ImageDraw.Draw(img)

        # Try to use a nice font, fall back to default if not available
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        except OSError:
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", 24)
            except OSError:
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
                    draw.text((x + offset_x, y + offset_y), timestamp_str, font=font, fill="black")

        # Draw main text (white)
        draw.text(position, timestamp_str, font=font, fill="white")

        # Save the annotated image (overwrite original)
        img.save(image_path, "JPEG", quality=95)

        logger.info("Image annotation successful")
        return True

    except Exception as e:
        logger.error(f"Failed to annotate image: {e}")
        return False
