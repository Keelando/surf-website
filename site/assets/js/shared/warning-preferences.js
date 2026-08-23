/**
 * Warning-banner preferences: reading and writing the reader's zone choice
 * (ES module).
 *
 * Storage is *injected*, never reached for — every function takes a storage
 * object, so these are pure enough to unit-test with a fake and the browser's
 * `localStorage` only appears at the call site. That is what keeps
 * `warning-zones.js` free of storage: it resolves, this remembers.
 *
 * Resolution itself (default, availability filter, the storm floor) lives in
 * `warning-zones.js` beside the default set — one place, not two.
 */

/**
 * The stored key. A JSON array, not a comma-joined string.
 *
 * The distinction is load-bearing: `[]` is a real choice ("alert me about no
 * zones", still storm-floored) and absent means "never chosen, use the
 * default". A joined string cannot tell `""` from unset, so the two states
 * would collapse and a reader who deliberately turned everything off would
 * have the default silently handed back to them.
 */
export const BANNER_ZONES_STORAGE_KEY = "warning_banner_zones";

/**
 * Read the reader's stored zone selection.
 *
 * Anything unusable — absent, unparseable, or the wrong shape — is reported as
 * "never chosen" rather than thrown or treated as an empty choice, so a
 * corrupt value degrades to the default instead of silencing the banner.
 * Private browsing and disabled site data make the getter itself throw; that
 * is the same case.
 *
 * @param {Storage} storage - localStorage or a stand-in
 * @returns {Array<string>|null} Zone keys, or null when never chosen
 */
export function readBannerZones(storage) {
  let raw = null;
  try {
    raw = storage.getItem(BANNER_ZONES_STORAGE_KEY);
  } catch {
    return null;
  }
  if (raw === null || raw === undefined) return null;

  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!Array.isArray(parsed)) return null;
  return parsed.filter((key) => typeof key === "string");
}

/**
 * Store the reader's zone selection.
 *
 * Failure is silent by design: a preference that cannot be saved is a
 * degraded session, not an error worth interrupting anyone over.
 *
 * @param {Storage} storage - localStorage or a stand-in
 * @param {Array<string>} zoneKeys - Zone keys to remember
 * @returns {boolean} Whether the write landed
 */
export function writeBannerZones(storage, zoneKeys) {
  try {
    storage.setItem(BANNER_ZONES_STORAGE_KEY, JSON.stringify(Array.from(zoneKeys)));
    return true;
  } catch {
    return false;
  }
}
