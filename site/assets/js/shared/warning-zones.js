/**
 * Marine warning selection and classification (ES module).
 *
 * The pure half of the warning banner: which zones may raise a sitewide
 * warning, which warnings in a forecast document are active, and how a warning
 * type maps to severity styling. No DOM, no storage, no network — everything
 * here is data in, data out, so it can be unit-tested directly.
 *
 * The DOM half (rendering, dismissal, htmx wiring) stays in
 * `site/assets/js/warning-banner.js`, which imports from here.
 */

/**
 * Zones whose warnings are allowed to raise the sitewide banner.
 *
 * This is NOT the list of zones we carry — `config/sr3/marine_forecast.conf`
 * is the only source of truth for that, and the forecasts page renders all of
 * them. This is the much narrower "interrupt me on every page" set: home
 * waters only, so a gale off the west coast of Vancouver Island does not pop a
 * banner over the tide tables.
 *
 * Intended to become the *default* for a per-user opt-in, not a permanent
 * hardcoded answer — see TODO.md. When that lands, `getBannerZones()` is the
 * single place that changes: read the user's stored selection, fall back to
 * this. Nothing else needs to know.
 */
export const DEFAULT_BANNER_ZONES = [
  "strait_of_georgia_north_of_nanaimo",
  "strait_of_georgia_south_of_nanaimo",
];

/**
 * Severity ranking used to order warnings. Lower sorts first.
 * Unknown types fall to the end rather than being dropped — an unrecognised
 * warning is still a warning.
 */
const SEVERITY_ORDER = {
  "Storm warning": 1,
  Storm: 1,
  "Gale warning": 2,
  Gale: 2,
  "Strong wind warning": 3,
  "Strong wind": 3,
  "Wind warning": 4,
  Wind: 4,
};

/**
 * Resolve which zones may raise the sitewide banner.
 *
 * Single extension point for the planned per-user opt-in: read the stored
 * selection here and fall back to DEFAULT_BANNER_ZONES.
 *
 * @returns {Array<string>} Zone keys
 */
export function getBannerZones() {
  return DEFAULT_BANNER_ZONES;
}

/**
 * Build the dismissal identity for a warning.
 *
 * Includes the issue time so that a re-issued warning of the same type in the
 * same zone is treated as new rather than staying dismissed.
 *
 * @param {Object} warning - Warning object
 * @returns {string} Stable id
 */
export function getWarningId(warning) {
  const issued = warning.issued_utc || "unknown";
  return `${warning.zone_key}_${warning.type}_${issued}`;
}

/**
 * Collect active warnings from a marine forecast document, restricted to the
 * banner zones and sorted most severe first.
 *
 * @param {Object} data - Parsed marine_forecast.json
 * @param {Array<string>} [bannerZones] - Zone keys allowed to raise a banner
 * @returns {Array<Object>} Active warnings, severity-sorted
 */
export function collectActiveWarnings(data, bannerZones = getBannerZones()) {
  const warnings = [];

  if (!data || !data.areas) return warnings;

  // The banner appears on every page, so it is deliberately narrower than the
  // forecasts page: that page renders every zone we carry, this interrupts you
  // only for home waters. Zones outside the set are still parsed, exported and
  // rendered — they just do not raise a sitewide banner.
  for (const areaData of Object.values(data.areas)) {
    for (const [zoneKey, zoneData] of Object.entries(areaData.locations || {})) {
      if (!bannerZones.includes(zoneKey)) continue;
      if (!Array.isArray(zoneData.warnings)) continue;

      zoneData.warnings.forEach((warning) => {
        if (warning.status !== "IN EFFECT") return;
        warnings.push({
          ...warning,
          zone_key: zoneKey,
          zone_name: zoneData.zone_name || warning.location,
          area_name: areaData.area || "",
        });
      });
    }
  }

  warnings.sort((a, b) => (SEVERITY_ORDER[a.type] || 99) - (SEVERITY_ORDER[b.type] || 99));

  return warnings;
}

/**
 * Map a warning type to its CSS severity class.
 * @param {string} type - Warning type
 * @returns {string} CSS class name
 */
export function getWarningSeverityClass(type) {
  const typeLower = String(type || "").toLowerCase();

  if (typeLower.includes("storm")) return "warning-storm";
  if (typeLower.includes("gale")) return "warning-gale";
  if (typeLower.includes("strong wind") || typeLower.includes("wind")) {
    return "warning-strong-wind";
  }
  if (typeLower.includes("waterspout") || typeLower.includes("water spout")) {
    return "warning-waterspout";
  }

  return "warning-default";
}

/**
 * Map a warning type to its display icon.
 * @param {string} type - Warning type
 * @returns {string} Emoji
 */
export function getWarningIcon(type) {
  const typeLower = String(type || "").toLowerCase();

  if (typeLower.includes("storm")) return "⚠️";
  if (typeLower.includes("gale")) return "💨";
  if (typeLower.includes("strong wind") || typeLower.includes("wind")) return "🌬️";

  return "⚠️";
}
