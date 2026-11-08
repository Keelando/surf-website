# Page background: a chart grid over a depth gradient

**Status:** BUILT on `dev` 2026-11-07, awaiting "ship it". Trial on the preview
(`?bg=` layers: gradient, chart, contours, fabric, sea, periodic); the user picked
**sea + fabric over periodic depth bands**, now always on, switch removed. Generated
by `scripts/utils/make_page_background.py --write` (needs node + Playwright and
Pillow): the textures are drawn as SVG there and rasterized by
`render_page_background.mjs` into one 720x480 tile per theme, 2x AVIF q60
(28/160 KB), content-hashed names. As SVG filter tiles they doubled raster work
on a touch tablet; WebP was 431/643 KB (lossless alpha); 1x or q40 blurred the
weave. Light-mode band tint must stay pale (#dae7f1) for AA; re-run the
gradient-contrast probe after any tweak (axe cannot score text over images).
Chart grid and contours are parked in the generator for reuse.
**Why:** with the photo shrunk to a banner on desktop (UI_OVERHAUL.md,
"Step 3: winds", round 2) the page around the panels is flat colour. User:
"on desktop the page does look very plain now", "background still looks
plain. ideas?" The photo can't come back as the backdrop: it is 1600px, and
a desktop needs ~3,000px or more, so it was upscaled 1.2-2.4x and grainy.

## The pick (user 2026-11-06: "I like your idea… let's try it")

**3 + 1**: the navy header fades down into the page over the first few
hundred pixels (depth, like looking down into water), and a faint
nautical-chart graticule sits in the margins: thin latitude / longitude
lines with small degree labels at the edges, and a few scattered depth
soundings. Vector only (CSS gradients + an inline SVG pattern), so it is
crisp at any pixel density and costs next to nothing to load. Calm: it
gives the empty space structure without competing with the numbers.

Considered and set aside:
- **Depth contours (2):** more character, but generic contour wallpaper
  unless drawn from real Salish Sea bathymetry (a simplified one-off export
  from GEBCO or CHS would do it). Keep as the alternative if the grid reads
  too sparse.
- **The header's teal wave repeated (4):** reads as wallpaper and echoes the
  home rows' swell lines. Skip.

## Plan

1. **Behind a switch first.** `?bg=chart`, `?bg=contours`, `?bg=gradient`
   (and none) on dev.halibutbank.ca, so the user can flip between them in
   their own browser before one is picked. A tiny inline reader of the query
   string setting `data-bg` on `<html>`; nothing remembered. Remove the switch
   once a pick is made (it must not ship).
2. **Where it lives:** `style-v4.css`, on `body`, so every page gets it at
   once (one shared change, the sitewide surface the overhaul plan allows).
   Light and dark both: a variable on `:root`, overridden under
   `[data-theme="dark"]`, only variables in the rules.
3. **The gradient:** from the header's navy (`--brand-navy`) at the top of
   the page to the page background by ~360-480px, so the banner and the
   first rows sit on it. Check the panel's edge still reads in both themes
   (the panel is solid `--color-surface` on desktop).
4. **The graticule:** an SVG pattern tile (say a 160px cell) of hairlines at
   ~4-6% opacity in light, a little more in dark; degree labels only along
   the viewport's outer edges, or none if they look like noise. Soundings: a
   handful of small numerals at very low contrast. Fixed to the page, not
   the viewport (`background-attachment: scroll`): a fixed layer is the
   tablet repaint cost that FRONTEND_CHANGELOG 2026-10-29 warns about.
5. **Desktop first.** On phones the panel covers almost the whole width, so
   the margins are 8px; decide after seeing it whether phones get the
   gradient only.
6. **Checks:** both engines, both themes, 390/768/1280/1920; the axe audit
   (contrast of text on the panel is unchanged, but the summary-foot links
   and section heads sit on the page background); a scroll on the emulated
   tablet (4x CPU) to confirm no new paint cost; Lighthouse CLS stays 0.

## Done when

The user has flipped through the options on the preview and picked one; it
is built without the switch, checked as above, and on `dev` for the next
"ship it".
