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

import { STALE_MARKER_OPACITY } from "./staleness.js";

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
 * @param {number} direction - Degrees, meteorological (coming FROM)
 * @param {number|null} value - Wave height (m) or wind speed (kt)
 * @param {Object} [opts]
 * @param {string} [opts.type] - 'wave', 'wind', or 'wind-on-wave'
 * @param {boolean} [opts.stale] - Dims the marker when true
 * @returns {string} HTML for the marker
 */
export function createDirectionalMarker(direction, value, { type = "wind", stale = false } = {}) {
  const isWave = type === "wave";
  const isWind = type === "wind";
  const arrowColor = isWave
    ? "var(--map-arrow-wave, #0077be)"
    : isWind
      ? "var(--map-arrow-wind, #dc2626)"
      : "var(--map-arrow-nodir, #555555)";
  const opacity = stale ? STALE_MARKER_OPACITY : 1.0;

  // Label: wind speed in knots (rounded), otherwise wave height in metres
  let valueLabel = "";
  if (value !== null && value !== undefined) {
    const text = isWind ? `${Math.round(value)}kt` : `${value.toFixed(1)}m`;
    valueLabel = `<div style="
      background: transparent;
      color: var(--map-marker-text, #004b7c);
      padding: 2px 5px;
      border-radius: 3px;
      font-size: 13px;
      font-weight: bold;
      white-space: nowrap;
      text-shadow: 1px 1px 2px rgba(255,255,255,0.9), -1px -1px 2px rgba(255,255,255,0.9), 1px -1px 2px rgba(255,255,255,0.9), -1px 1px 2px rgba(255,255,255,0.9);
      margin-bottom: -3px;
    ">${text}</div>`;
  }

  // ECharts-style arrow path, fattened for map visibility; points down at
  // rotation 0
  return `
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; opacity: ${opacity};">
      ${valueLabel}
      <div style="transform: rotate(${direction}deg); transform-origin: center center;">
        <svg aria-hidden="true" width="26" height="30" viewBox="-6 -10 12 24" style="filter: drop-shadow(0 2px 3px rgba(0,0,0,0.5)); color: ${arrowColor};">
          <path d="M0,12 L-5,-8 L0,-5 L5,-8 Z" fill="currentColor" fill-opacity="0.98" stroke="currentColor" stroke-width="1.5"/>
        </svg>
      </div>
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
