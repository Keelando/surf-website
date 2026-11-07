#!/usr/bin/env python3
"""Generate the page-background CSS block in site/assets/css/style-v4.css.

See docs/project/PAGE_BACKGROUND.md. Usage:
    python3 scripts/utils/make_page_background.py           # print the block
    python3 scripts/utils/make_page_background.py --write   # splice it into style-v4.css

Emits the chosen layers (sea + fabric over periodic depth bands); the trial's
other layers are parked below for reuse. SVG tiles carry one colour per theme
(a data URI cannot read a CSS variable) and are url-encoded."""

import math
from pathlib import Path
from urllib.parse import quote


def uri(svg):
    return 'url("data:image/svg+xml,' + quote(svg, safe=" =:/,.'-") + '")'


# graticule() and soundings() made the trial's "chart" layer and contours()
# its "contours" layer. PARKED 2026-11-07, not emitted: kept for reuse.


def graticule(color, opacity):
    ticks = "".join(f"M{x} 0v4M0 {x}h4" for x in range(32, 160, 32))
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='160' height='160'>"
        f"<path d='M0 0.5H160M0.5 0V160{ticks}' stroke='{color}' stroke-opacity='{opacity}' fill='none'/></svg>"
    )


SOUNDINGS = [
    (46, 70, "14"),
    (231, 38, "23"),
    (392, 112, "9"),
    (118, 214, "31"),
    (318, 262, "18"),
    (482, 228, "42"),
    (64, 404, "27"),
    (252, 452, "6"),
    (430, 418, "35"),
    (170, 330, "12"),
]


def soundings(color, opacity):
    t = "".join(f"<text x='{x}' y='{y}'>{n}</text>" for x, y, n in SOUNDINGS)
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='560' height='560'>"
        f"<g fill='{color}' fill-opacity='{opacity}' font-family='Georgia,serif'"
        f" font-style='italic' font-size='11'>{t}</g></svg>"
    )


def blob(cx, cy, r, phase, k):
    pts = []
    for i in range(48):
        a = 2 * math.pi * i / 48
        rr = r * (
            1 + 0.16 * math.sin(3 * a + phase) + 0.09 * math.sin(5 * a + 2 * phase + k) + 0.05 * math.sin(8 * a + k)
        )
        pts.append(f"{cx + rr * math.cos(a):.0f} {cy + rr * 0.78 * math.sin(a):.0f}")
    return "M" + "L".join(pts) + "Z"


def contours(color, opacity):
    """Generic depth contours. PARKED 2026-11-07: tried as a page layer, user
    liked them but chose sea + fabric; kept here for reuse elsewhere (e.g. a
    section divider, an empty-state, the 404 page). Not emitted below."""
    paths = [blob(190, 200, r, 0.7, i) for i, r in enumerate((34, 70, 108, 146))]
    paths += [blob(500, 470, r, 2.1, i) for i, r in enumerate((26, 58, 92))]
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='680' height='640'>"
        f"<path d='{''.join(paths)}' stroke='{color}' stroke-opacity='{opacity}'"
        " stroke-linejoin='round' fill='none'/></svg>"
    )


def rgb01(hexcolor):
    return [int(hexcolor[i : i + 2], 16) / 255 for i in (1, 3, 5)]


def alpha_from_noise(color, strength, invert=False):
    """feColorMatrix: paint `color`, alpha = strength * (red channel), or its inverse."""
    r, g, b = rgb01(color)
    a = f"{-strength if invert else strength} 0 0 0 {strength if invert else 0}"
    return f"0 0 0 0 {r:.3f} 0 0 0 0 {g:.3f} 0 0 0 0 {b:.3f} {a}"


def fabric(color, strength):
    # Linen: horizontal and vertical "threads" from stretched fractal noise,
    # averaged, then turned into a faint wash of the theme colour.
    return (
        "<svg xmlns='http://www.w3.org/2000/svg' width='240' height='240'>"
        "<filter id='f' x='0' y='0' width='1' height='1'>"
        "<feTurbulence type='fractalNoise' baseFrequency='0.9 0.035' numOctaves='2' seed='3'"
        " stitchTiles='stitch' result='h'/>"
        "<feTurbulence type='fractalNoise' baseFrequency='0.035 0.9' numOctaves='2' seed='8'"
        " stitchTiles='stitch' result='v'/>"
        "<feComposite in='h' in2='v' operator='arithmetic' k2='0.5' k3='0.5'/>"
        f"<feColorMatrix type='matrix' values='{alpha_from_noise(color, strength)}'/></filter>"
        "<rect width='240' height='240' filter='url(#f)'/></svg>"
    )


def crest_alpha(color, gain, floor):
    """feColorMatrix: paint `color` only where the lit surface is brighter than
    `floor`: alpha = gain * (red - floor), clipped at 0 by the filter."""
    r, g, b = rgb01(color)
    return f"0 0 0 0 {r:.3f} 0 0 0 0 {g:.3f} 0 0 0 0 {b:.3f} {gain} 0 0 0 {-gain * floor:.3f}"


def sea(color, strength, crests=False):
    """Wavy surface: long, wind-stretched turbulence lit from the low west.

    Light theme tints the shaded troughs; dark (crests=True) lights the crests,
    since shading barely shows on navy."""
    alpha = crest_alpha(color, strength, 0.55) if crests else alpha_from_noise(color, strength, invert=True)
    return (
        "<svg xmlns='http://www.w3.org/2000/svg' width='720' height='480'>"
        "<filter id='s' x='0' y='0' width='1' height='1'>"
        "<feTurbulence type='fractalNoise' baseFrequency='0.0035 0.0125' numOctaves='4' seed='11'"
        " stitchTiles='stitch'/>"
        "<feDiffuseLighting surfaceScale='11' diffuseConstant='1' lighting-color='white'>"
        "<feDistantLight azimuth='235' elevation='38'/></feDiffuseLighting>"
        f"<feColorMatrix type='matrix' values='{alpha}'/></filter>"
        "<rect width='720' height='480' filter='url(#s)'/></svg>"
    )


LIGHT, DARK_INK = "#0b2a45", "#9fc3e6"
# periodic: the same fade repeated down the page as soft bands, a short rise
# (--bg-depth-rise) into each band and the long fade out of it.
PERIODIC = (
    # The top fade keeps the deeper tint (only dark headings sit under the
    # nav); the repeating bands use --bg-band-top, paler in light mode, because
    # muted body text lands on them further down (AA measured 2026-11-07:
    # #b8d0e3 bands put #666-class text at 3.6-4.2:1).
    "linear-gradient(to bottom, var(--bg-depth-top) 0, transparent var(--bg-depth-span)),"
    " repeating-linear-gradient(to bottom, var(--bg-band-top) 0, var(--bg-band-top) var(--bg-band-hold),"
    " var(--color-background) var(--bg-band-span),"
    " var(--color-background) calc(var(--bg-depth-period) - var(--bg-depth-rise)),"
    " var(--bg-band-top) var(--bg-depth-period))"
)
CSS = f"""/* ── Page background (docs/project/PAGE_BACKGROUND.md) ─────────────────────
   Picked 2026-11-07 from a trial on the dev preview: a wavy sea surface and a
   faint linen weave over a depth gradient that repeats down the page in broad
   bands. Generated by scripts/utils/make_page_background.py --write: edit the
   script and regenerate, never the data URIs here (SVG tiles carry one colour
   per theme, since a data URI cannot read a CSS variable).
   - Light mode: the band tint (--bg-band-top) must stay pale. #b8d0e3 bands
     put muted body text at 3.6-4.2:1; re-check AA after any change (axe
     cannot score text over a background image).
   - background-attachment stays scroll: a fixed layer is the tablet repaint
     cost FRONTEND_CHANGELOG 2026-10-29 warns about.
   - Sea and fabric only from 900px, where there are margins to show them;
     phones get the bands alone. */
:root {{
  /* The fade under the nav starts deeper than the repeating bands: only dark
     headings ("Stations map") sit there. */
  --bg-depth-top: #b8d0e3;
  --bg-depth-span: 680px;
  --bg-band-top: #dae7f1;
  --bg-band-hold: 240px;
  --bg-band-span: 900px;
  --bg-depth-period: 1800px;
  --bg-depth-rise: 360px;
  --bg-fabric: {uri(fabric(LIGHT, 0.10))};
  --bg-sea: {uri(sea(LIGHT, 0.19))};
  --bg-texture: none, none;
}}

[data-theme="dark"] {{
  --bg-depth-top: var(--brand-navy);
  --bg-band-top: var(--brand-navy);
  --bg-fabric: {uri(fabric(DARK_INK, 0.08))};
  --bg-sea: {uri(sea(DARK_INK, 0.42, crests=True))};
}}

@media (min-width: 900px) {{
  :root {{
    --bg-texture: var(--bg-fabric), var(--bg-sea);
  }}
}}

body {{
  background-image:
    var(--bg-texture),
    {PERIODIC};
}}
/* ── end page background ── */
"""

START, END = "/* ── Page background", "/* ── end page background ── */\n"
# From this file, not lib.config.PROJECT_ROOT: run in the dev worktree, the
# shared venv's editable install resolves lib/ to the MAIN checkout
# (docs/DEV_PREVIEW.md, trap 4), so PROJECT_ROOT would write production's
# style-v4.css, live the moment it is saved.
TARGET = Path(__file__).resolve().parents[2] / "site" / "assets" / "css" / "style-v4.css"


if __name__ == "__main__":
    import sys

    if "--write" not in sys.argv:
        print(CSS)
    else:
        text = TARGET.read_text()
        a = text.index(START)
        b = text.index(END, a) + len(END)
        TARGET.write_text(text[:a] + CSS + text[b:])
        print(f"wrote {TARGET}")
