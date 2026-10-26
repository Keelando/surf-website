/**
 * How big a wave is FOR THAT STATION, for the decorative wave lines (the
 * header band in nav.js, the station rows in station-list.js). 1 m in the
 * Strait of Georgia is well above average; at La Perouse Bank it is a quiet
 * day. Display only: nothing here judges safety or feeds a number on screen.
 *
 * Each buoy's scale is `rough_wave_height_m` in config/stations.json
 * (published in /data/stations.json): the significant wave height at which
 * its line is "getting feisty". Strait of Georgia (1 m) and La Perouse (2.5 m)
 * are the user's own calls (2026-10-24); the rest were set against each buoy's
 * 90th-95th percentile over the 30 days the database held then (24 Sep-24 Oct
 * 2026), which also matched those two calls. Early-autumn data: revisit after
 * a winter.
 */

/** For a station without a rough height, or when stations.json is missing. */
export const DEFAULT_ROUGH_M = 2;

/**
 * Amplitude factor for a station's wave line: 0.2 for a flat calm, 1.0 at the
 * station's rough height, clamped at 1.2 (reached at 1.25x that height).
 *
 * @param {number} height - Significant wave height, m
 * @param {number} [roughM] - The station's rough_wave_height_m
 */
export function waveAmplitude(height, roughM) {
  const rough = Number.isFinite(roughM) && roughM > 0 ? roughM : DEFAULT_ROUGH_M;
  return Math.min(1.2, Math.max(0.2, 0.2 + (0.8 * height) / rough));
}
