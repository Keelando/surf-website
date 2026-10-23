/**
 * Line icons for buttons and links (ES module).
 *
 * One consistent set in place of the emoji the site used to put on its
 * controls (📍 📊 🔗 …), which rendered differently on every OS. Each is a
 * 24×24 stroke drawing in currentColor, so it takes the colour of the text
 * beside it. Decorative: aria-hidden, the button's text names the action.
 *
 * Add a shape here rather than inlining SVG in a page module, so the set
 * stays one style.
 */

const PATHS = {
  // A clock face: the 12-hour history.
  history: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  // A line chart rising over an axis.
  chart: '<path d="M4 4v16h16"/><path d="m7 15 4-5 3 3 5-6"/>',
  // A map pin.
  map: '<path d="M12 21s-6.5-5.6-6.5-11a6.5 6.5 0 0 1 13 0c0 5.4-6.5 11-6.5 11z"/><circle cx="12" cy="10" r="2.3"/>',
  // A warning triangle: marine warnings (banner, forecasts page).
  warning:
    '<path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
  // An arrow leaving a box: opens another site.
  external:
    '<path d="M14 4h6v6"/><path d="M20 4 11 13"/><path d="M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/>',
};

/**
 * Inline SVG for a named icon, or "" for an unknown name.
 *
 * @param {keyof typeof PATHS} name
 * @param {string} [className] - Extra class on the <svg>
 * @returns {string}
 */
export function icon(name, className = "") {
  const paths = PATHS[name];
  if (!paths) return "";
  const cls = className ? `icon ${className}` : "icon";
  return (
    `<svg class="${cls}" viewBox="0 0 24 24" fill="none" stroke="currentColor" ` +
    `stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` +
    `${paths}</svg>`
  );
}

/** The names available, for tests. */
export const ICON_NAMES = Object.keys(PATHS);
