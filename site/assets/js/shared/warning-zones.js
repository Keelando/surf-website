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
 * This is the *default* only — the set a reader who has never opened the
 * picker gets. `getBannerZones()` resolves a stored selection against it. See
 * `docs/project/WARNING_ZONE_OPT_IN.md`.
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
 * Pure on purpose: the stored selection is read by
 * `shared/warning-preferences.js` and handed in, so this module never touches
 * storage and stays testable without a browser.
 *
 * `null` means the reader has never chosen and gets the default. An empty
 * array is a real choice and is honoured as one — the storm floor in
 * `collectActiveWarnings()` is what makes that safe to allow.
 *
 * Stored keys are filtered against the zones the live document actually
 * carries, because an sr3 `accept` change or an EC rename can retire a zone
 * out from under a stored preference. The filter happens here, on the way
 * out; the stored list is left alone, so a bulletin missing for one cycle does
 * not quietly erase a choice.
 *
 * @param {Array<string>|null} [stored] - Stored selection, null if never chosen
 * @param {Array<string>} [availableZoneKeys] - Zone keys the document carries
 * @returns {Array<string>} Zone keys
 */
export function getBannerZones(stored = null, availableZoneKeys = null) {
  const chosen = Array.isArray(stored) ? stored : DEFAULT_BANNER_ZONES;
  if (!Array.isArray(availableZoneKeys)) return [...chosen];
  return chosen.filter((zoneKey) => availableZoneKeys.includes(zoneKey));
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
 * Whether a warning clears the severity floor that ignores zone selection.
 *
 * A storm warning is 48+ kt. A reader who never found the picker — or who
 * deliberately turned every zone off — should still be interrupted by one,
 * wherever it is. Gale and strong-wind warnings stay zone-scoped, and that is
 * exactly what keeps the banner quiet enough to be believed when it does fire.
 *
 * This is a floor, not an override: it widens the resolved zone set, never
 * narrows it, and it applies whether that set came from the default or the
 * reader's own choice. It is the one place "off" is not honoured, and the
 * reason zero selected zones can be allowed silently at all.
 *
 * Precedent: US Wireless Emergency Alerts let you opt out of every category
 * except national alerts; NWS never lets its top tier be suppressed in-page.
 *
 * Deliberately kept here rather than in the picker — the picker must not be
 * able to switch it off.
 *
 * @param {Object} warning - Warning object
 * @returns {boolean} True when the warning banners regardless of zone
 */
function clearsSeverityFloor(warning) {
  return getWarningSeverityClass(warning.type) === "warning-storm";
}

/**
 * Collect active warnings from a marine forecast document, restricted to the
 * banner zones (plus the storm floor) and sorted most severe first.
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
  // only for the zones you follow. Zones outside the set are still parsed,
  // exported and rendered — they just do not raise a sitewide banner, unless
  // the warning clears the storm floor above.
  for (const areaData of Object.values(data.areas)) {
    for (const [zoneKey, zoneData] of Object.entries(areaData.locations || {})) {
      if (!Array.isArray(zoneData.warnings)) continue;
      const zoneSelected = bannerZones.includes(zoneKey);

      zoneData.warnings.forEach((warning) => {
        if (warning.status !== "IN EFFECT") return;
        if (!zoneSelected && !clearsSeverityFloor(warning)) return;
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
