/* =====================================================
   Tide Configuration & State Management
   ===================================================== */

/**
 * Global state management for tide page
 * Provides centralized access to all data and state
 */
(function() {
  // Internal state (private)
  let tideCurrentData = null;
  let tideTimeseriesData = null;
  let tideHighLowData = null;
  let combinedWaterLevelData = null;
  let stationsMetadata = null;
  let sunlightTimesData = null;
  let tideChart = null;
  let currentDayOffset = 0; // 0 = today, 1 = tomorrow, 2 = day after
  let currentStationKey = null;
  let currentGeodeticResiduals = []; // Stores residuals for geodetic stations

  // Station name formatting
  const STATION_DISPLAY_NAMES = {
    'point_atkinson': 'Point Atkinson',
    'kitsilano': 'Kitsilano',
    'tsawwassen': 'Tsawwassen',
    'whiterock': 'White Rock',
    'crescent_pile': 'Crescent Beach',
    'crescent_beach_ocean': 'Crescent Beach Ocean (Geodetic)',
    'crescent_channel_ocean': 'Crescent Channel Ocean (Geodetic)',
    'nanaimo': 'Nanoose Bay (Nanaimo)',
    'new_westminster': 'New Westminster',
    'campbell_river': 'Campbell River',
    'tofino': 'Tofino',
    'ucluelet': 'Ucluelet',
    'port_renfrew': 'Port Renfrew',
    'victoria_harbor': 'Victoria Harbor'
  };

  // Public API
  window.TideConfig = {
    // Data getters
    getTideCurrentData: () => tideCurrentData,
    getTideTimeseriesData: () => tideTimeseriesData,
    getTideHighLowData: () => tideHighLowData,
    getCombinedWaterLevelData: () => combinedWaterLevelData,
    getStationsMetadata: () => stationsMetadata,
    getSunlightTimesData: () => sunlightTimesData,

    // Data setters
    setTideCurrentData: (data) => { tideCurrentData = data; },
    setTideTimeseriesData: (data) => { tideTimeseriesData = data; },
    setTideHighLowData: (data) => { tideHighLowData = data; },
    setCombinedWaterLevelData: (data) => { combinedWaterLevelData = data; },
    setStationsMetadata: (data) => { stationsMetadata = data; },
    setSunlightTimesData: (data) => { sunlightTimesData = data; },

    // Chart instance
    getTideChart: () => tideChart,
    setTideChart: (chart) => { tideChart = chart; },

    // Current state
    getCurrentDayOffset: () => currentDayOffset,
    setCurrentDayOffset: (offset) => { currentDayOffset = offset; },
    getCurrentStationKey: () => currentStationKey,
    setCurrentStationKey: (key) => { currentStationKey = key; },

    // Geodetic residuals
    getGeodeticResiduals: () => currentGeodeticResiduals,
    setGeodeticResiduals: (residuals) => { currentGeodeticResiduals = residuals; },

    // Constants
    STATION_DISPLAY_NAMES: STATION_DISPLAY_NAMES,

    // Convenience method to get display name
    getStationDisplayName: (key) => STATION_DISPLAY_NAMES[key] || key
  };
})();
