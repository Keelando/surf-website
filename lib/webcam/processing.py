"""
Image post-processing utilities (annotation, etc.).
"""

import pytz
from PIL import Image, ImageDraw, ImageFont


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
