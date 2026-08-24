/**
 * Marine zone vocabulary and ordering (ES module).
 *
 * The shared half of "which zones exist and how do we name them". Pure: a
 * parsed `marine_forecast.json` goes in, a zone list comes out. No DOM, no
 * storage, no network, so it can be unit-tested directly.
 *
 * Two consumers depend on these agreeing exactly: the forecasts page zone
 * `<select>` and the warning-banner zone picker beside it. A reader who picks
 * "Howe Sound" from the dropdown and then looks for "Howe Sound" in the
 * checkbox list must find the same words in the same order — which is the
 * whole reason the labelling and ordering live here rather than in either
 * page's own file.
 */

/**
 * Halibut Bank sits in this zone, so it is what the forecasts page opens on
 * and the area it belongs to is what sorts first everywhere.
 */
export const DEFAULT_ZONE_KEY = "strait_of_georgia_south_of_nanaimo";

/**
 * Flatten the {areas: {locations: {}}} document into a selectable zone list.
 *
 * @param {Object} data - Parsed marine_forecast.json
 * @returns {Array<Object>} [{zoneKey, zoneName, areaKey, areaName, zoneData, areaData}]
 */
export function listZones(data) {
  if (!data || !data.areas) return [];

  const zones = [];
  for (const [areaKey, areaData] of Object.entries(data.areas)) {
    for (const [zoneKey, zoneData] of Object.entries(areaData.locations || {})) {
      zones.push({
        zoneKey,
        zoneName: zoneData.zone_name || zoneKey.replace(/_/g, " "),
        areaKey,
        areaName: areaData.area || areaKey.replace(/_/g, " "),
        zoneData,
        areaData,
      });
    }
  }
  return zones;
}

/**
 * Shorten a zone name for display beneath its area's heading.
 *
 * EC's zone_name repeats the area it belongs to ("Juan de Fuca Strait - west
 * entrance"), which under a group already labelled "Juan de Fuca Strait" is
 * three redundant words on every line — and on a phone it is what pushed the
 * <select> wider than the viewport. The group carries the area, the row
 * carries only what distinguishes it.
 *
 * @param {string} zoneName - Full zone name from the forecast document
 * @param {string} areaName - Area name the zone sits under
 * @returns {string} Display label
 */
export function shortZoneLabel(zoneName, areaName) {
  if (!areaName || zoneName === areaName) return zoneName;

  // EC uses " - " as the separator; anything else is left alone rather than
  // guessed at, so an unfamiliar naming shape degrades to the full name.
  const prefix = `${areaName} - `;
  if (!zoneName.startsWith(prefix)) return zoneName;

  const rest = zoneName.slice(prefix.length);
  return rest.charAt(0).toUpperCase() + rest.slice(1);
}

/**
 * Zones whose EC name is too long for a picker row.
 *
 * Display only, and deliberately narrow: `marine_forecast.json` keeps EC's
 * name and so does everywhere a warning is actually *read* — the banner, the
 * jump strip, the zone card heading. This shortens the two controls where the
 * full name costs the most: a `<select>` that a long option widens past a
 * phone's viewport, and a checkbox list read as a column of names.
 *
 * Keyed by zone key rather than by matching on the name, so an EC rewording
 * degrades to the full name instead of silently mislabelling a zone.
 */
export const PICKER_SHORT_NAMES = {
  west_coast_vancouver_island_south: "West Coast Van Island",
};

/**
 * The label both pickers show for a zone.
 *
 * The one place that decision is made: the `<select>` and the warning-zone
 * checkbox list beside it must show the same words for the same water, or a
 * reader who picks a zone from one cannot find it in the other.
 *
 * @param {Object} zone - Zone record from listZones()
 * @param {boolean} [areaHasSiblings] - Whether the zone shares its area, in
 *   which case the area name is already carried by the group heading
 * @returns {string} Display label
 */
export function pickerZoneLabel(zone, areaHasSiblings = false) {
  const short = PICKER_SHORT_NAMES[zone.zoneKey];
  if (short) return short;

  return areaHasSiblings ? shortZoneLabel(zone.zoneName, zone.areaName) : zone.zoneName;
}

/**
 * Group zones by area, home waters first.
 *
 * The home area is the one DEFAULT_ZONE_KEY belongs to, read from the data
 * rather than named again here, so the pin follows the default zone if that
 * ever moves. Everything else keeps document order.
 *
 * @param {Array<Object>} zones - Zone list from listZones()
 * @returns {Array<Object>} [{areaName, zones}] in display order
 */
export function orderZonesForDisplay(zones) {
  const byArea = new Map();
  zones.forEach((zone) => {
    if (!byArea.has(zone.areaName)) byArea.set(zone.areaName, []);
    byArea.get(zone.areaName).push(zone);
  });

  const homeArea = zones.find((z) => z.zoneKey === DEFAULT_ZONE_KEY)?.areaName;

  const groups = [];
  if (homeArea && byArea.has(homeArea)) {
    groups.push({ areaName: homeArea, zones: byArea.get(homeArea) });
  }
  for (const [areaName, areaZones] of byArea) {
    if (areaName === homeArea) continue;
    groups.push({ areaName, zones: areaZones });
  }
  return groups;
}
