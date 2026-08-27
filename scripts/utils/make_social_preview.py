"""Render the 1200x630 social preview card.

Kept in the repo so the card can be regenerated when the wording changes,
rather than being an opaque binary nobody can reproduce. Colours are the
site's own brand variables from site/assets/css/style-v4.css.
"""

import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630
PRIMARY_DARK = (0, 75, 124)     # --color-primary-dark  #004b7c
PRIMARY = (0, 109, 175)         # --color-primary       #006daf
SURFACE = (255, 255, 255)
MUTED = (168, 205, 230)

FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

img = Image.new("RGB", (W, H), PRIMARY_DARK)
d = ImageDraw.Draw(img)

# Vertical gradient, dark at the top to deeper blue at the bottom.
for y in range(H):
    t = y / H
    d.line(
        [(0, y), (W, y)],
        fill=tuple(
            round(a + (b - a) * t)
            for a, b in zip(PRIMARY_DARK, (0, 40, 70))
        ),
    )

# Wave bands across the lower third - three offset sine curves, the site's
# subject matter rendered as the only ornament.
for i, (amp, period, yoff, alpha) in enumerate(
    [(26, 520, 470, 38), (34, 660, 520, 30), (22, 420, 566, 22)]
):
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ld = ImageDraw.Draw(layer)
    pts = [
        (x, yoff + amp * math.sin(2 * math.pi * (x + i * 140) / period))
        for x in range(0, W + 1, 4)
    ]
    ld.polygon(pts + [(W, H), (0, H)], fill=(255, 255, 255, alpha))
    img = Image.alpha_composite(img.convert("RGBA"), layer).convert("RGB")
    d = ImageDraw.Draw(img)

title = ImageFont.truetype(FONT_BOLD, 82)
sub = ImageFont.truetype(FONT, 38)
foot = ImageFont.truetype(FONT, 30)

d.text((80, 150), "Salish Sea", font=title, fill=SURFACE)
d.text((80, 244), "Wave Conditions", font=title, fill=SURFACE)
d.text(
    (80, 360),
    "Live buoys, wind, tides and marine forecasts",
    font=sub,
    fill=MUTED,
)

# Accent rule under the wordmark.
d.rectangle([80, 118, 214, 126], fill=PRIMARY)
d.text((80, 540), "halibutbank.ca", font=foot, fill=(255, 255, 255))

out = Path("site/assets/images/social-preview.png")
img.save(out, optimize=True)
print(f"{out}  {out.stat().st_size / 1024:.0f} KB  {img.size}")
