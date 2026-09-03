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
 * Popup options for `bindPopup`, sized to the current viewport.
 *
 * Read at bind time, which is marker-creation time. A device that changes
 * width mid-session (a rotation) keeps the width it was built with; the
 * budget is a cap rather than a layout, so the result is a popup narrower
 * than it could be, never one that overflows.
 *
 * @returns {{minWidth: number, maxWidth: number}} Equal bounds, so the width
 *   is fixed rather than content-driven.
 */
export function getPopupOptions() {
  if (window.innerWidth > MOBILE_BREAKPOINT_PX) {
    return { minWidth: TARGET_WIDTH_PX, maxWidth: TARGET_WIDTH_PX };
  }

  const budget = Math.floor(window.innerWidth * MOBILE_VIEWPORT_FRACTION) - MOBILE_CHROME_PX;
  const width = Math.max(MIN_WIDTH_PX, Math.min(TARGET_WIDTH_PX, budget));
  return { minWidth: width, maxWidth: width };
}
