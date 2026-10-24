/**
 * How big a wave is FOR THAT STATION, for the decorative wave lines (the
 * header band in nav.js, the station rows in station-list.js). 1 m in the
 * Strait of Georgia is well above average; at La Perouse Bank it is a quiet
 * day. Display only: nothing here judges safety or feeds a number on screen.
 */

/**
 * Significant wave height (m) at which each station's line is "getting
 * feisty". Strait of Georgia and La Perouse are the user's own calls
 * (2026-10-24); the rest were set against each buoy's 90th–95th percentile
 * over the 30 days the database held then (24 Sep–24 Oct 2026), which also
 * matched those two calls. Early-autumn data: revisit after a winter.
 */
export const ROUGH_HEIGHT_M = {
  4600146: 1.0, // Halibut Bank
  4600304: 1.0, // English Bay
  4600303: 1.0, // Southern Georgia Strait
  4600131: 1.0, // Sentry Shoal
  CRPILE: 0.5, // Crescent Beach Ocean (Boundary Bay, shallow)
  CRCHAN: 0.5, // Crescent Channel Ocean
  46087: 2.5, // Neah Bay (open to the Pacific)
  46088: 1.0, // New Dungeness (inner strait)
  46267: 1.5, // Angeles Point
  4600206: 2.5, // La Perouse Bank
};

/** For a station not in the table. */
const DEFAULT_ROUGH_M = 2;

/**
 * Amplitude factor for a station's wave line: 0.2 for a flat calm, 1.0 at the
 * station's rough height, clamped at 1.2 (reached at 1.25× that height).
 */
export function waveAmplitude(height, stationId) {
  const rough = ROUGH_HEIGHT_M[stationId] ?? DEFAULT_ROUGH_M;
  return Math.min(1.2, Math.max(0.2, 0.2 + (0.8 * height) / rough));
}
