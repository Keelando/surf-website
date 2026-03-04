/**
 * Tides Page - Refactored Integration Layer
 *
 * This file integrates all the modular tide display components.
 * It replaces the original monolithic tides.js with a cleaner, more maintainable structure.
 *
 * Module structure:
 * - constants.js: Configuration and lookup tables
 * - data-loader.js: Data fetching and management (TideDataStore)
 * - geodetic.js: Geodetic station handling
 * - utils.js: Time formatting and helper functions
 * - sunlight.js: Sunrise/sunset display (SunlightDataStore)
 * - ui-controls.js: Station dropdowns and day navigation
 * - display.js: Station info, observations, predictions, storm surge, high/low tables
 * - chart-renderer.js: ECharts tide chart visualization
 */

import { TideDataStore } from './tides-modules/data-loader.js';
import { SunlightDataStore } from './tides-modules/sunlight.js';
import {
  populateStationDropdown,
  setupDayNavigation,
  updateDayLabel,
  updateNavigationButtons,
  hideStation
} from './tides-modules/ui-controls.js';
import {
  displayStation,
  displayHighLowTable
} from './tides-modules/display.js';
import {
  displayTideChart,
  disposeChart,
  getCurrentGeodeticResiduals
} from './tides-modules/chart-renderer.js';
import { displaySunlightTimes } from './tides-modules/sunlight.js';
import { updateTimestamp, showError, showSelectedTideOnMap } from './tides-modules/utils.js';

/* =====================================================
   Global State
   ===================================================== */

let tideDataStore = null;
let sunlightDataStore = null;

/* =====================================================
   Data Loading
   ===================================================== */

async function loadTideData() {
  try {
    // Initialize stores if needed
    if (!tideDataStore) {
      tideDataStore = new TideDataStore();
    }
    if (!sunlightDataStore) {
      sunlightDataStore = new SunlightDataStore();
    }

    // Load all data
    await Promise.all([
      tideDataStore.loadAll(window.logger || console, window.fetchWithTimeout || fetch),
      sunlightDataStore.load()
    ]);

    // Populate UI
    populateStationDropdown(
      tideDataStore,
      (stationKey) => displayStationWrapper(stationKey),
      () => hideStation()
    );

    updateTimestamp();

  } catch (error) {
    if (window.logger) {
      window.logger.error('Tides', 'Error loading tide data', error);
    } else {
      console.error('Error loading tide data:', error);
    }
    showError();
  }
}

/* =====================================================
   Display Wrappers (Wire Callbacks)
   ===================================================== */

/**
 * Display a selected station - wires together all display functions
 */
function displayStationWrapper(stationKey) {
  displayStation(
    stationKey,
    tideDataStore,
    () => setupDayNavigationWrapper(),
    (key, offset) => displayTideChartWrapper(key, offset),
    (key, offset) => displaySunlightWrapper(key, offset)
  );
}

/**
 * Setup day navigation with callbacks
 */
function setupDayNavigationWrapper() {
  setupDayNavigation(
    tideDataStore,
    () => updateChartForDay()
  );
}

/**
 * Display tide chart with all dependencies
 */
function displayTideChartWrapper(stationKey, dayOffset) {
  const tideTimeseriesData = {
    stations: {}
  };

  // Build timeseries data structure expected by chart
  const stationData = tideDataStore.getTimeseries(stationKey);
  if (stationData) {
    tideTimeseriesData.stations[stationKey] = stationData;
  }

  displayTideChart(
    stationKey,
    dayOffset,
    tideTimeseriesData,
    tideDataStore.getAllCombinedWaterLevel(),
    (key, dateStr) => sunlightDataStore.getForDate(key, dateStr)
  );

  // After chart display, get geodetic residuals and store them for storm surge card
  const residuals = getCurrentGeodeticResiduals();
  if (residuals && residuals.length > 0) {
    tideDataStore.setGeodeticResiduals(residuals);
  }
}

/**
 * Display sunlight times with callback
 */
function displaySunlightWrapper(stationKey, dayOffset) {
  displaySunlightTimes(
    stationKey,
    dayOffset,
    sunlightDataStore,
    tideDataStore,
    () => updateChartForDay()
  );
}

/**
 * Update chart and related elements when day changes
 */
function updateChartForDay() {
  const stationKey = tideDataStore.getCurrentStation();
  const dayOffset = tideDataStore.getDayOffset();

  if (stationKey) {
    // Update chart
    displayTideChartWrapper(stationKey, dayOffset);

    // Update day label
    updateDayLabel(dayOffset);

    // Update navigation buttons
    updateNavigationButtons(dayOffset);

    // Update high/low table
    const highlowStation = tideDataStore.getHighLow(stationKey);
    if (highlowStation) {
      displayHighLowTable(highlowStation, dayOffset);
    }

    // Update sunlight widget
    displaySunlightWrapper(stationKey, dayOffset);
  }
}

/* =====================================================
   Initialization
   ===================================================== */

/**
 * Initialize tide page after HTMX loads footer
 */
async function initializeTidePage() {
  await loadTideData();
}

/**
 * Auto-refresh tide data every 5 minutes
 */
function startAutoRefresh() {
  setInterval(loadTideData, 5 * 60 * 1000);
}

/**
 * Handle window resize for chart
 */
function handleResize() {
  // Chart resize is handled internally by chart-renderer.js
  // This is a placeholder for any global resize logic
}

/* =====================================================
   Event Listeners
   ===================================================== */

// Wait for HTMX to load footer with timestamp
document.addEventListener('htmx:load', async function() {
  await initializeTidePage();
}, { once: true });

// Auto-refresh
startAutoRefresh();

// Window resize
window.addEventListener('resize', handleResize);

/* =====================================================
   Global Exports (for HTML onclick handlers)
   ===================================================== */

// Export showSelectedTideOnMap to window for onclick handler
window.showSelectedTideOnMap = showSelectedTideOnMap;

// Export data stores for debugging (optional)
window.__tideDataStore = tideDataStore;
window.__sunlightDataStore = sunlightDataStore;
