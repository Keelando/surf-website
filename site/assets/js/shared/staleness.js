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

/**
 * Human-readable observation age: "5 minutes ago" / "3 hours ago" / "2 days ago".
 *
 * @param {number|null} ageMinutes
 * @returns {string|null} null when the age is unknown
 */
export function formatDataAge(ageMinutes) {
  if (ageMinutes == null) return null;

  if (ageMinutes < 60) {
    const mins = Math.round(ageMinutes);
    return `${mins} minute${mins !== 1 ? "s" : ""} ago`;
  }
  if (ageMinutes < 1440) {
    const hours = Math.round(ageMinutes / 60);
    return `${hours} hour${hours !== 1 ? "s" : ""} ago`;
  }
  const days = Math.round(ageMinutes / 1440);
  return `${days} day${days !== 1 ? "s" : ""} ago`;
}

/**
 * Human-readable staleness threshold for one lightstation, e.g. ">9h".
 *
 * The number is per station and comes from the export, which measures it
 * against that station's own inferred publishing cadence — most report every
 * three hours and are flagged at 9 h, while Cape Mudge, Chatham Point and
 * Pulteney Point report four times a day in daylight only and are normally
 * silent for 15 h overnight, so they are flagged at 18 h. Hardcoding ">12h"
 * here, as all three renderers used to, was wrong for both groups at once.
 *
 * @param {Object} station - Entry from latest_lightstation.json
 * @returns {string} Threshold label, falling back to the pre-2026-09-06 flat
 *   value when an older payload has no `stale_after_hours`.
 */
export function staleThresholdLabel(station) {
  const hours = station?.stale_after_hours;
  return `>${Number.isFinite(hours) ? Math.round(hours) : 12}h`;
}

/** Emphasis line appended at the bottom of a popup when data is stale. */
export function staleDataWarningHTML() {
  return `<div style="color: var(--color-accent-red); font-size: 0.85em; margin-top: 4px; font-weight: 600;">⚠️ STALE DATA</div>`;
}
