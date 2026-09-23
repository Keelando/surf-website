/**
 * Shared map-marker and direction-vector SVG builders (ES module).
 *
 * Meteorological convention: direction values = where wind/waves are COMING
 * FROM; arrows point where they are TRAVELING TO. The arrow SVG points DOWN
 * at rotation 0, so rotating by the raw direction gives the travel heading.
 *
 * Canonical replacements for the three drifted createDirectionalMarker
 * copies (stations-map.js, winds-map.js, lightstation-map.js) and the two
 * createAngularSpreadVector copies (main.js, webcams-v4.js) — see
 * shared/README.md for migration status.
 */

import { STALE_MARKER_OPACITY, statusMarkerOpacity } from "./staleness.js";

/**
 * ECharts symbol path for chart direction arrows.
 * Canonical home once chart pages migrate; chart-utils-v4.js keeps its own
 * copy until then.
 */
export const DIRECTION_ARROW_PATH = "path://M0,15 L-3,-5 L0,0 L3,-5 Z";

/**
 * Create a directional map marker: rotated triangular arrow with an
 * optional value label above it. Returns HTML for a Leaflet divIcon.
 *
 * Reconciles the three legacy copies: superset behaviour from
 * stations-map.js (wave/wind/wind-on-wave types, stale opacity),
 * aria-hidden from winds-map.js, themed label/arrow colours (the
 * lightstation-map.js red-badge label converges here when that page
 * migrates).
 *
 * @param {number|null} direction - Degrees, meteorological (coming FROM).
 *   null draws a round dot instead of an arrow: the station has a value to
 *   show but no direction to point it (a NOAA spectral file running behind
 *   the heights, a calm wind). Better than the old type-emoji fallback, which
 *   hid the value and read as "no data" when there was data.
 * @param {number|null} value - Wave height (m) or wind speed (kt)
 * @param {Object} [opts]
 * @param {string} [opts.type] - 'wave', 'wind', or 'wave-inferred'
 * @param {"ok"|"late"|"down"} [opts.status] - Reporting status: late dims
 *   the marker part-way, down fully. Wins over `stale`.
 * @param {boolean} [opts.stale] - Legacy: dims the marker fully when true
 * @param {number|null} [opts.windSpeed] - Knots, drawn as a second smaller
 *   line under the wave-height label. Ignored on 'wind' markers, whose
 *   primary label is already the wind speed. Callers gate this on zoom: a
 *   whole map of two-line labels overlaps.
 * @returns {string} HTML for the marker
 */
export function createDirectionalMarker(
  direction,
  value,
  { type = "wind", status = null, stale = false, windSpeed = null } = {},
) {
  const isWind = type === "wind";
  // 'wave-inferred': a wave station with no directional sensor, pointed by
  // its wind direction. Wave blue like any other wave station - grey read as
  // "switched off", which is what stale markers already look like - but drawn
  // hollow, so the map never claims a direction it did not measure.
  const isInferred = type === "wave-inferred";
  const arrowColor = isWind ? "var(--map-arrow-wind, #dc2626)" : "var(--map-arrow-wave, #0077be)";
  const fillColor = isInferred ? "var(--map-marker-bg, #ffffff)" : "currentColor";
  const opacity = status ? statusMarkerOpacity(status) : stale ? STALE_MARKER_OPACITY : 1.0;

  // Halo that keeps a transparent-background label readable over map tiles.
  const labelHalo =
    "1px 1px 2px rgba(255,255,255,0.9), -1px -1px 2px rgba(255,255,255,0.9), 1px -1px 2px rgba(255,255,255,0.9), -1px 1px 2px rgba(255,255,255,0.9)";

  // Label: wind speed in knots (rounded), otherwise wave height in metres.
  // Coloured by the quantity, not the station — see --map-marker-wind-text in
  // stations-map-v4.css. Waves blue, wind near-black, wherever either appears.
  const labelColor = isWind
    ? "var(--map-marker-wind-text, #1a1a1a)"
    : "var(--map-marker-text, #0077be)";
  let valueLabel = "";
  if (value !== null && value !== undefined) {
    const text = isWind ? `${Math.round(value)}kt` : `${value.toFixed(1)}m`;
    valueLabel = `<div style="
      background: transparent;
      color: ${labelColor};
      padding: 2px 5px;
      border-radius: 3px;
      font-size: 13px;
      font-weight: bold;
      white-space: nowrap;
      text-shadow: ${labelHalo};
      margin-bottom: -3px;
    ">${text}</div>`;
  }

  // Second line: wind speed under a wave marker. Same near-black as a
  // standalone wind marker's label — it is the same quantity — and a size down
  // from the wave height above it, which is what tells the two apart.
  let windLabel = "";
  if (!isWind && windSpeed !== null && windSpeed !== undefined) {
    windLabel = `<div style="
      background: transparent;
      color: var(--map-marker-wind-text, #1a1a1a);
      padding: 0 5px;
      font-size: 11px;
      font-weight: bold;
      white-space: nowrap;
      text-shadow: ${labelHalo};
      margin-bottom: -2px;
    ">${Math.round(windSpeed)}kt</div>`;
  }

  // ECharts-style arrow path, fattened for map visibility; points down at
  // rotation 0. Without a direction, a dot in the same 26x30 box so the icon
  // anchors line up either way.
  const hasDirection = direction !== null && direction !== undefined;
  const glyph = hasDirection
    ? `<div style="transform: rotate(${direction}deg); transform-origin: center center;">
        <svg aria-hidden="true" width="26" height="30" viewBox="-6 -10 12 24" style="filter: drop-shadow(0 2px 3px rgba(0,0,0,0.5)); color: ${arrowColor};">
          <path d="M0,12 L-5,-8 L0,-5 L5,-8 Z" fill="${fillColor}" fill-opacity="0.98" stroke="currentColor" stroke-width="${isInferred ? 2 : 1.5}"/>
        </svg>
      </div>`
    : `<svg aria-hidden="true" width="26" height="30" viewBox="-6 -10 12 24" style="filter: drop-shadow(0 2px 3px rgba(0,0,0,0.5)); color: ${arrowColor};">
        <circle cx="0" cy="2" r="4.5" fill="${fillColor}" fill-opacity="0.98" stroke="currentColor" stroke-width="1.5"/>
      </svg>`;
  return `
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; opacity: ${opacity};">
      ${valueLabel}
      ${windLabel}
      ${glyph}
    </div>
  `;
}

/**
 * Angular spread vector: compass circle with the mean direction arrow and a
 * shaded sector spanning the directional spread. Returns an SVG HTML string
 * ("" when direction/spread unavailable).
 *
 * @param {number|null} avgDirection - Mean direction in degrees (coming FROM)
 * @param {number|null} spread - Angular spread in degrees
 * @param {number} [size] - Rendered width/height in px
 * @returns {string} SVG markup, or "" if inputs are null
 */
export function createAngularSpreadVector(avgDirection, spread, size = 70) {
  if (avgDirection == null || spread == null) return "";

  const halfSpread = spread / 2;
  const minDir = avgDirection - halfSpread;
  const maxDir = avgDirection + halfSpread;

  const cx = size / 2;
  const cy = size / 2;
  const radius = size * 0.42;

  // Sector shows TRAVEL directions (add 180° to convert from source),
  // matching the arrow. SVG arc angles are compass minus 90°.
  const startAngleRad = ((minDir + 180 - 90) * Math.PI) / 180;
  const endAngleRad = ((maxDir + 180 - 90) * Math.PI) / 180;
  const arcRadius = radius + 2;

  const x1 = cx + arcRadius * Math.cos(startAngleRad);
  const y1 = cy + arcRadius * Math.sin(startAngleRad);
  const x2 = cx + arcRadius * Math.cos(endAngleRad);
  const y2 = cy + arcRadius * Math.sin(endAngleRad);

  const largeArc = spread > 180 ? 1 : 0;

  return `
    <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" style="display: inline-block; vertical-align: middle; margin-left: 0.5rem;">
      <!-- Background circle -->
      <circle cx="${cx}" cy="${cy}" r="${radius + 2}" fill="none" stroke="#e0e7ee" stroke-width="1"/>

      <!-- Spread sector -->
      <path d="M ${cx},${cy} L ${x1},${y1} A ${arcRadius},${arcRadius} 0 ${largeArc},1 ${x2},${y2} Z"
            fill="rgba(30, 136, 229, 0.15)"
            stroke="rgba(30, 136, 229, 0.3)"
            stroke-width="1"/>

      <!-- Main direction arrow (single arrow within sector) -->
      <g transform="rotate(${avgDirection} ${cx} ${cy})">
        <line x1="${cx}" y1="${cy - radius + 8}" x2="${cx}" y2="${cy + radius - 3}"
              stroke="#1e88e5" stroke-width="2.5"/>
        <path d="M${cx},${cy + radius + 2} L${cx - 5},${cy + radius - 8} L${cx + 5},${cy + radius - 8} Z"
              fill="#1e88e5"/>
      </g>

      <!-- Cardinal directions -->
      <text x="${cx}" y="8" text-anchor="middle" font-size="8" fill="#999">N</text>
      <text x="${size - 6}" y="${cy + 3}" text-anchor="middle" font-size="8" fill="#999">E</text>
      <text x="${cx}" y="${size - 2}" text-anchor="middle" font-size="8" fill="#999">S</text>
      <text x="6" y="${cy + 3}" text-anchor="middle" font-size="8" fill="#999">W</text>
    </svg>
  `;
}

/**
 * DOM-node variant of createAngularSpreadVector for callers that append
 * elements instead of HTML strings (webcams page). Browser-only.
 *
 * @returns {SVGElement|null} SVG element, or null if inputs are null
 */
export function createAngularSpreadVectorElement(avgDirection, spread, size = 70) {
  const html = createAngularSpreadVector(avgDirection, spread, size);
  if (!html) return null;
  const template = document.createElement("template");
  template.innerHTML = html.trim();
  return template.content.firstElementChild;
}
