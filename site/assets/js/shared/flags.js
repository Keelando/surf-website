/**
 * Small drawn flags for a station's country (ES module).
 *
 * In place of the 🇨🇦 / 🇺🇸 emoji the winds table used to carry (user
 * 2026-11-06: liked the flags, but emoji render differently on every
 * system, as letters on some, and clash with the line icons). Simplified
 * for ~16px: Canada's bands and leaf, the US's stripes and canton without
 * stars. A hairline edge keeps the white parts visible on a white panel.
 *
 * The country comes from the registry's `country` ("CA" / "US",
 * config/stations.json), not from its `flag` emoji, which for some stations
 * names a source instead. Decorative: aria-hidden; pass `label` where the
 * country should be spoken.
 */

const EDGE =
  '<rect x=".5" y=".5" width="31" height="15" rx="1" fill="none" stroke="rgba(0,0,0,.25)"/>';

const SHAPES = {
  CA:
    '<rect width="32" height="16" fill="#fff"/>' +
    '<rect width="8" height="16" fill="#d52b1e"/>' +
    '<rect x="24" width="8" height="16" fill="#d52b1e"/>' +
    '<path d="M16 2.4l1 1.9 1.3-.6-.5 3.1 1.8-1.9.4 1.1 1.9-.4-.6 2.1.9.5-2.9 2.3.4 1-2.7-.4.1 2.6h-.6l.1-2.6-2.7.4.4-1-2.9-2.3.9-.5-.6-2.1 1.9.4.4-1.1 1.8 1.9-.5-3.1 1.3.6z" fill="#d52b1e"/>',
  // Thirteen stripes (seven red) and a canton over the top seven.
  US:
    '<rect width="32" height="16" fill="#fff"/>' +
    '<rect y="0.0" width="32" height="1.23" fill="#b22234"/><rect y="2.46" width="32" height="1.23" fill="#b22234"/><rect y="4.92" width="32" height="1.23" fill="#b22234"/><rect y="7.38" width="32" height="1.23" fill="#b22234"/><rect y="9.85" width="32" height="1.23" fill="#b22234"/><rect y="12.31" width="32" height="1.23" fill="#b22234"/><rect y="14.77" width="32" height="1.23" fill="#b22234"/>' +
    '<rect width="13" height="8.62" fill="#3c3b6e"/>',
};

const NAMES = { CA: "Canada", US: "United States" };

/**
 * Inline SVG flag for "CA" or "US", or "" for anything else.
 *
 * @param {string} country - The registry's `country`
 * @param {Object} [options]
 * @param {string} [options.className] - Extra class on the <svg>
 * @param {boolean} [options.label] - Speak the country ("Canada") instead of hiding the flag
 * @returns {string}
 */
export function flagIcon(country, { className = "", label = false } = {}) {
  const shape = SHAPES[country];
  if (!shape) return "";
  const cls = className ? `flag ${className}` : "flag";
  const a11y = label ? `role="img" aria-label="${NAMES[country]}"` : 'aria-hidden="true"';
  return `<svg class="${cls}" viewBox="0 0 32 16" width="16" height="8" ${a11y}>${shape}${EDGE}</svg>`;
}

/** "Canada" / "United States", or "" for an unknown code. */
export function countryName(country) {
  return NAMES[country] ?? "";
}
