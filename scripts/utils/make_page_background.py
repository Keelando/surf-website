#!/usr/bin/env python3
"""Generate the page-background CSS block in site/assets/css/style-v4.css.

See docs/project/PAGE_BACKGROUND.md. Usage: python3 scripts/utils/make_page_background.py
--write (or no flag to print). Layers, each switched on independently by a token in
?bg= during the trial (theme-manager.js writes them space-separated into data-bg):
chart (graticule + soundings), fabric (linen weave), sea (wavy surface relief). Any token turns on
the depth gradient underneath. SVG tiles, one colour per theme (a data URI
cannot read a CSS variable), url-encoded. Prints the CSS block."""

import math
from urllib.parse import quote


def uri(svg):
    return 'url("data:image/svg+xml,' + quote(svg, safe=" =:/,.'-") + '")'


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
GRADIENT = "linear-gradient(to bottom, var(--bg-depth-top) 0, var(--color-background) var(--bg-depth-span))"
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
CSS = f"""/* ── Page background: TRIAL (docs/project/PAGE_BACKGROUND.md) ──────────────
   Layers picked on dev.halibutbank.ca with ?bg=, combined with "+":
   ?bg=gradient | chart | fabric | sea | periodic, e.g. ?bg=sea+fabric.
   theme-manager.js writes the tokens into data-bg; any token turns on the
   depth gradient. Must not ship as a switch: keep the chosen layers, drop the
   rest and the ?bg= reader. Tiles are generated (one
   colour per theme, since an SVG data URI cannot read a CSS variable) by
   scripts/utils/make_page_background.py — regenerate rather than hand-edit. background-attachment stays scroll: a
   fixed layer is the tablet repaint cost FRONTEND_CHANGELOG 2026-10-29 warns
   about. Pattern layers only where there are margins to show them. */
:root {{
  /* Light starts from a pale sea tint, not navy: page-level headings
     ("Stations map") sit on this right under the nav and must stay AA. */
  --bg-depth-top: #b8d0e3;
  --bg-depth-span: 680px;
  --bg-depth-period: 1800px;
  --bg-band-hold: 240px;
  --bg-band-span: 900px;
  --bg-band-top: #dae7f1;
  --bg-depth-rise: 360px;
  --bg-l-depth: {GRADIENT};
  --bg-graticule: {uri(graticule(LIGHT, 0.07))};
  --bg-soundings: {uri(soundings(LIGHT, 0.16))};
  --bg-fabric: {uri(fabric(LIGHT, 0.10))};
  --bg-sea: {uri(sea(LIGHT, 0.19))};
  --bg-l-chart: none, none;
  --bg-l-fabric: none;
  --bg-l-sea: none;
}}

[data-theme="dark"] {{
  --bg-depth-top: var(--brand-navy);
  --bg-band-top: var(--brand-navy);
  --bg-graticule: {uri(graticule(DARK_INK, 0.06))};
  --bg-soundings: {uri(soundings(DARK_INK, 0.13))};
  --bg-fabric: {uri(fabric(DARK_INK, 0.08))};
  --bg-sea: {uri(sea(DARK_INK, 0.42, crests=True))};
}}

@media (min-width: 900px) {{
  [data-bg~="chart"] {{ --bg-l-chart: var(--bg-soundings), var(--bg-graticule); }}
  [data-bg~="fabric"] {{ --bg-l-fabric: var(--bg-fabric); }}
  [data-bg~="sea"] {{ --bg-l-sea: var(--bg-sea); }}
}}

[data-bg~="periodic"] {{
  --bg-l-depth: {PERIODIC};
}}

[data-bg] body {{
  background-image:
    var(--bg-l-fabric),
    var(--bg-l-chart),
    var(--bg-l-sea),
    var(--bg-l-depth);
}}
/* ── end page background trial ── */
"""

START, END = "/* ── Page background: TRIAL", "/* ── end page background trial ── */\n"
TARGET = __import__("pathlib").Path(__file__).resolve().parents[2] / "site/assets/css/style-v4.css"

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
