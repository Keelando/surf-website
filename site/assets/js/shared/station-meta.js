/**
 * Station-metadata predicates (ES module).
 *
 * Pure helpers over a station entry from stations.json (`buoys` section).
 * These replace the inline station-ID checks that were scattered through
 * main.js / stations-map.js under drifting names (isBoundaryBay / isSurrey /
 * isCrescentStation were all the same set) — see
 * docs/project/BUOY_CARD_REFACTOR.md.
 *
 * `config/stations.json` is the single source of truth; it ships to the
 * frontend verbatim as /data/stations.json. Every helper tolerates a
 * missing meta object (unknown station → EC-style defaults).
 */

/** NOAA NDBC station (badge, spectral details, dominant-period wording). */
export function isNoaaStation(meta) {
  return meta?.source === "NOAA NDBC";
}

/** Surrey FlowWorks station (badge/border styling). */
export function isSurreyStation(meta) {
  return meta?.source === "Surrey FlowWorks";
}

/**
 * Pile-mounted wave station (Crescent Beach/Channel): near-shore sensors
 * reporting small waves — drives 2-decimal heights and 1-decimal temps.
 */
export function isPileStation(meta) {
  return meta?.type === "pile_mounted_wave_station";
}

/**
 * Station whose cards/history show swell instead of combined sig wave
 * (Neah Bay: open-ocean site where swell is the representative metric).
 * Driven by the `wave_display: "swell"` config field.
 */
export function usesSwellDisplay(meta) {
  return meta?.wave_display === "swell";
}

/**
 * NOAA stations report a "dominant" period (DPD) rather than a significant
 * one — cards tag the period and add a footnote. Swell-display stations
 * show swell period instead, so they're excluded.
 */
export function usesDominantPeriod(meta) {
  return isNoaaStation(meta) && !usesSwellDisplay(meta);
}

/** Decimal places for wave heights (pile stations resolve centimetres). */
export function waveHeightPrecision(meta) {
  return isPileStation(meta) ? 2 : 1;
}

/**
 * Reports more often than hourly — history tables filter these to
 * on-the-hour rows so 12h fits the table.
 */
export function reportsSubHourly(meta) {
  return (meta?.update_frequency_minutes ?? 60) < 60;
}

/** Upstream source page URL, or null when the station has none. */
export function sourceUrl(meta) {
  return meta?.source_url ?? null;
}
