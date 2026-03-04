/**
 * Geodetic Station Calibration Module
 * Handles geodetic tide stations that use CGVD28 datum
 */

import { GEODETIC_STATIONS, GEODETIC_METHODOLOGIES } from './constants.js';

/**
 * Check if a station is a geodetic tide station (Surrey FlowWorks).
 * Geodetic stations use CGVD28 datum and require calibration.
 *
 * @param {string} stationKey - Station identifier
 * @returns {boolean} True if station is geodetic
 */
export function isGeodeticStation(stationKey) {
  return GEODETIC_STATIONS.includes(stationKey);
}

/**
 * Determine which methodology to use for geodetic calibration:
 * - Crescent Beach Ocean: Apply offset to PREDICTIONS
 * - Crescent Channel Ocean: Apply offset to OBSERVATIONS
 *
 * @param {string} stationKey - Station identifier
 * @returns {string|null} Methodology name or null if not geodetic
 */
export function getGeodeticMethodology(stationKey) {
  return GEODETIC_METHODOLOGIES[stationKey] || null;
}

/**
 * Get the most recent geodetic offset value for a station.
 *
 * @param {string} stationKey - Station identifier
 * @param {Object} tideTimeseriesData - Timeseries data object
 * @returns {number|null} Offset value in meters, or null if unavailable
 */
export function getCurrentGeodeticOffset(stationKey, tideTimeseriesData) {
  if (!tideTimeseriesData?.stations?.[stationKey]?.geodetic_offsets) {
    return null;
  }

  const offsets = tideTimeseriesData.stations[stationKey].geodetic_offsets;
  if (offsets.length === 0) return null;

  // Find the most recent offset
  const now = Date.now();
  let closestOffset = null;
  let closestDiff = Infinity;

  for (const offset of offsets) {
    const offsetTime = new Date(offset.time).getTime();
    const diff = Math.abs(now - offsetTime);
    if (diff < closestDiff) {
      closestDiff = diff;
      closestOffset = offset.value;
    }
  }

  return closestOffset;
}

/**
 * Store geodetic residuals for a station
 * Used to sync chart and card displays
 *
 * @param {Array} residuals - Array of residual objects {time, value}
 * @returns {Array} The stored residuals
 */
export function storeGeodeticResiduals(residuals) {
  return residuals;
}
