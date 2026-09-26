/**
 * Leaflet popup sizing, shared by all three maps (stations, winds,
 * lightstations).
 *
 * Every popup binds with the same options, so a popup is the same width
 * whichever marker opened it. Left to Leaflet's defaults (minWidth 50,
 * maxWidth 300) the width came out of whatever the longest line happened to
 * be — measured across twelve popups it produced eight different widths
 * between 286px and 349px, and the ones that hit the cap looked as though
 * they were reserving space for the optional "Wave Forecast" button. Fixing
 * both bounds to the same value makes that impossible.
 *
 * The width is a *budget*, not a constant, because the popup does not stop at
 * the content box. Leaflet nests the content inside
 * `.leaflet-popup-content-wrapper`, and `stations-map-v4.css` caps that
 * wrapper at `88vw` on phones. Hard-coding 280 while the wrapper is capped at
 * 88vw made the two rules contradict each other below about 372px of
 * viewport: at 360px the wrapper allowed 316.8px and the content plus its
 * chrome needed 327px, so the last ~10px of every popup on every map was
 * clipped against the wrapper's `overflow-x: hidden`. Ask for a width the
 * wrapper can actually hold and the contradiction cannot arise.
 */

/** The width every popup wants, whenever the viewport can afford it. */
const TARGET_WIDTH_PX = 280;

/**
 * Non-content width inside the wrapper on a phone: `.leaflet-popup-content`'s
 * horizontal margin (2 × 0.7rem, set in the mobile block of
 * stations-map-v4.css) plus the wrapper's 1px padding and 1px border a side.
 * Rounded up, so the budget errs narrow.
 *
 * This constant is only true because that mobile margin rule carries
 * `!important` — leaflet.css loads after our stylesheet and its own
 * `margin: 13px 24px 13px 20px` otherwise wins, which is how 44px of margin
 * ended up inside a budget that had allowed for 22px.
 */
const MOBILE_CHROME_PX = 28;

/** Matches `max-width: 88vw` on `.leaflet-popup-content-wrapper`. */
const MOBILE_VIEWPORT_FRACTION = 0.88;

/** Matches the `@media (max-width: 768px)` block that sets that max-width. */
const MOBILE_BREAKPOINT_PX = 768;

/**
 * Never go below `.station-popup`'s own `min-width` (200px in
 * stations-map-v4.css) — asking for less just moves the overflow inward.
 */
const MIN_WIDTH_PX = 200;

/**
 * How far from the map's left edge a popup must stay to clear the control
 * column. Every map stacks zoom + fullscreen top-left: a 34px bar (30px
 * buttons + 2px border a side) behind Leaflet's 10px margin on desktop, and
 * a 4px margin on phones (stations-map-v4.css tucks the controls into the
 * corner there). The rest is a small gap.
 *
 * This has to be done by panning, not z-index. Popups live inside
 * `.leaflet-map-pane`, a transformed element and so its own stacking context
 * at z-index 400; the controls are its siblings at 1000. No z-index on the
 * popup can climb out of that pane, and dropping the controls below 400 puts
 * them under the tiles.
 */
const CONTROL_CLEARANCE_PX = 55;
const MOBILE_CONTROL_CLEARANCE_PX = 44;

/** Leaflet's own `autoPanPadding`, kept for the edges that have no controls. */
const DEFAULT_PAN_PADDING_PX = 5;

/** Fallback when no map is passed: a phone map spans the viewport minus the
 *  page's 8px side padding. */
const MOBILE_MAP_GUTTER_PX = 16;

/**
 * Popup options for `bindPopup`, sized to the current viewport and map.
 *
 * Read at bind time, which is marker-creation time. A device that changes
 * width mid-session (a rotation) keeps the width it was built with; the
 * budget is a cap rather than a layout, so the result is a popup narrower
 * than it could be, never one that overflows.
 *
 * `autoPanPaddingTopLeft` makes Leaflet pan any popup that would open under
 * the controls out to their right. That only works if the popup fits in the
 * map *beside* the control column, so on a phone the width budget also
 * subtracts the clearance: at 360px the popup gives up ~25px rather than
 * sit under the zoom buttons. (Leaflet resolves a popup too wide for both
 * paddings by honouring the left one and clipping the right edge; only
 * below MIN_WIDTH_PX does the clearance shrink instead.)
 *
 * @param {L.Map} [map] - the map the popup belongs to; its real width beats
 *   the viewport-minus-gutter guess.
 * @returns {{minWidth: number, maxWidth: number,
 *   autoPanPaddingTopLeft: [number, number]}} Equal width bounds, so the
 *   width is fixed rather than content-driven.
 */
export function getPopupOptions(map) {
  if (window.innerWidth > MOBILE_BREAKPOINT_PX) {
    return {
      minWidth: TARGET_WIDTH_PX,
      maxWidth: TARGET_WIDTH_PX,
      autoPanPaddingTopLeft: [CONTROL_CLEARANCE_PX, DEFAULT_PAN_PADDING_PX],
    };
  }

  const mapWidth = map?.getSize().x || window.innerWidth - MOBILE_MAP_GUTTER_PX;
  const besideControls =
    mapWidth - MOBILE_CONTROL_CLEARANCE_PX - DEFAULT_PAN_PADDING_PX - MOBILE_CHROME_PX;
  const budget = Math.floor(window.innerWidth * MOBILE_VIEWPORT_FRACTION) - MOBILE_CHROME_PX;
  const width = Math.max(MIN_WIDTH_PX, Math.min(TARGET_WIDTH_PX, budget, besideControls));
  const spare = mapWidth - (width + MOBILE_CHROME_PX) - DEFAULT_PAN_PADDING_PX;
  const clearance = Math.max(DEFAULT_PAN_PADDING_PX, Math.min(MOBILE_CONTROL_CLEARANCE_PX, spare));
  return {
    minWidth: width,
    maxWidth: width,
    autoPanPaddingTopLeft: [clearance, DEFAULT_PAN_PADDING_PX],
  };
}
