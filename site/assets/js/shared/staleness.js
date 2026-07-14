/**
 * Shared staleness-presentation helpers (ES module).
 *
 * Staleness itself is computed by the backend (each JSON export carries a
 * `stale` flag); these helpers only standardize how stale data LOOKS.
 * Canonical replacements for blocks inlined in stations-map.js (buoy +
 * lightstation popups), lightstation-map.js, and winds-map.js — see
 * shared/README.md for migration status.
 */

/** Map-marker opacity when the observation is stale. */
export const STALE_MARKER_OPACITY = 0.35;

/**
 * Colour theme + header line for a map-popup "Latest Conditions" block.
 *
 * @param {boolean} isStale - Backend-computed stale flag
 * @param {Object} [opts]
 * @param {string} [opts.label] - Header label, e.g. "Latest Conditions"
 * @param {string} [opts.staleLabel] - Label when stale, if different (winds: "Current Wind" / "Last Wind")
 * @param {string} [opts.threshold] - Human-readable threshold, e.g. ">3h"
 * @returns {{bg: string, border: string, headingColor: string, headerText: string}}
 */
export function stalePopupTheme(
  isStale,
  { label = "Latest Conditions", staleLabel = label, threshold = ">3h" } = {},
) {
  return {
    bg: isStale
      ? "var(--color-callout-danger-bg, #fff5f5)"
      : "var(--color-callout-info-bg, #f0f8ff)",
    border: isStale ? "var(--color-accent-red)" : "var(--color-primary)",
    headingColor: isStale
      ? "var(--color-accent-red)"
      : "var(--map-popup-heading, var(--color-primary-dark))",
    headerText: isStale ? `${staleLabel} (STALE - ${threshold} old):` : `${label}:`,
  };
}

/** Emphasis line appended at the bottom of a popup when data is stale. */
export function staleDataWarningHTML() {
  return `<div style="color: var(--color-accent-red); font-size: 0.85em; margin-top: 4px; font-weight: 600;">⚠️ STALE DATA</div>`;
}
