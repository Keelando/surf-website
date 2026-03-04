/**
 * Data Loading Module
 * Handles fetching and caching of tide data from JSON endpoints
 */

/**
 * Data store - holds all loaded tide data
 */
export class TideDataStore {
  constructor() {
    this.tideCurrentData = null;
    this.tideTimeseriesData = null;
    this.tideHighLowData = null;
    this.combinedWaterLevelData = null;
    this.stationsMetadata = null;
    this.currentStationKey = null;
    this.currentDayOffset = 0; // 0 = today, 1 = tomorrow, 2 = day after
    this.currentGeodeticResiduals = []; // Stores residuals for geodetic stations
  }

  /**
   * Load all tide data from JSON endpoints
   *
   * @param {Object} logger - Logger instance
   * @param {Function} fetchWithTimeout - Fetch function with timeout
   * @returns {Promise<void>}
   */
  async loadAll(logger, fetchWithTimeout) {
    try {
      // Load all tide JSON files plus stations metadata and combined water level
      const [
        tideCurrentData_temp,
        tideTimeseriesData_temp,
        tideHighLowData_temp,
        combinedWaterLevelData_temp,
        stationsMetadata_temp
      ] = await Promise.all([
        fetchWithTimeout(`/data/tide-latest.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/tide-timeseries.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/tide-hi-low.json?t=${Date.now()}`),
        fetchWithTimeout(`/data/combined-water-level.json?t=${Date.now()}`).catch(() => null),
        fetchWithTimeout(`/data/stations.json?t=${Date.now()}`).catch(() => null)
      ]);

      this.tideCurrentData = tideCurrentData_temp;
      this.tideTimeseriesData = tideTimeseriesData_temp;
      this.tideHighLowData = tideHighLowData_temp;

      if (combinedWaterLevelData_temp) {
        this.combinedWaterLevelData = combinedWaterLevelData_temp;
        logger.info('Tides', 'Loaded combined water level data');
      } else {
        logger.warn('Tides', 'Combined water level data not available');
      }

      if (stationsMetadata_temp) {
        this.stationsMetadata = stationsMetadata_temp.tides || {};
      }

    } catch (error) {
      logger.error('Tides', 'Error loading tide data', error);
      throw error;
    }
  }

  /**
   * Get current observation for a station
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} Current observation data
   */
  getCurrentObservation(stationKey) {
    return this.tideCurrentData?.stations?.[stationKey]?.observation || null;
  }

  /**
   * Get current prediction for a station
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} Current prediction data
   */
  getCurrentPrediction(stationKey) {
    return this.tideCurrentData?.stations?.[stationKey]?.prediction_now || null;
  }

  /**
   * Get tide offset (storm surge) for a station
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} Tide offset data
   */
  getTideOffset(stationKey) {
    return this.tideCurrentData?.stations?.[stationKey]?.tide_offset || null;
  }

  /**
   * Get timeseries data for a station
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} Timeseries data
   */
  getTimeseries(stationKey) {
    return this.tideTimeseriesData?.stations?.[stationKey] || null;
  }

  /**
   * Get high/low events for a station
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} High/low data
   */
  getHighLow(stationKey) {
    return this.tideHighLowData?.stations?.[stationKey] || null;
  }

  /**
   * Get combined water level data for a station
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} Combined water level data
   */
  getCombinedWaterLevel(stationKey) {
    return this.combinedWaterLevelData?.stations?.[stationKey] || null;
  }

  /**
   * Get all combined water level data (for all stations)
   *
   * @returns {Object|null} Full combined water level data object
   */
  getAllCombinedWaterLevel() {
    return this.combinedWaterLevelData;
  }

  /**
   * Get station metadata
   *
   * @param {string} stationKey - Station identifier
   * @returns {Object|null} Station metadata
   */
  getStationMetadata(stationKey) {
    return this.stationsMetadata?.[stationKey] || null;
  }

  /**
   * Get all available station keys
   *
   * @returns {string[]} Array of station keys
   */
  getAvailableStations() {
    if (!this.tideCurrentData?.stations) return [];
    return Object.keys(this.tideCurrentData.stations);
  }

  /**
   * Set current selected station
   *
   * @param {string} stationKey - Station identifier
   */
  setCurrentStation(stationKey) {
    this.currentStationKey = stationKey;
  }

  /**
   * Get current selected station
   *
   * @returns {string|null} Current station key
   */
  getCurrentStation() {
    return this.currentStationKey;
  }

  /**
   * Set current day offset
   *
   * @param {number} offset - Day offset (0 = today, 1 = tomorrow, etc.)
   */
  setDayOffset(offset) {
    this.currentDayOffset = offset;
  }

  /**
   * Get current day offset
   *
   * @returns {number} Current day offset
   */
  getDayOffset() {
    return this.currentDayOffset;
  }

  /**
   * Store geodetic residuals
   *
   * @param {Array} residuals - Array of residual objects
   */
  setGeodeticResiduals(residuals) {
    this.currentGeodeticResiduals = residuals;
  }

  /**
   * Get geodetic residuals
   *
   * @returns {Array} Array of residual objects
   */
  getGeodeticResiduals() {
    return this.currentGeodeticResiduals;
  }
}
