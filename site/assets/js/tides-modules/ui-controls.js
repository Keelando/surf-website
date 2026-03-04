/**
 * UI Controls Module
 * Handles station dropdowns, navigation buttons, and day selection
 */

import { STATION_DISPLAY_NAMES } from './constants.js';

/**
 * Populate the station dropdown with available stations
 *
 * @param {Object} tideDataStore - Tide data store instance
 * @param {Function} displayStationCallback - Callback when station is selected
 * @param {Function} hideStationCallback - Callback when selection is cleared
 * @returns {void}
 */
export function populateStationDropdown(tideDataStore, displayStationCallback, hideStationCallback) {
  const select = document.getElementById('tide-station-select');
  const selectBottom = document.getElementById('tide-station-select-bottom');

  // Get unique stations from current data (observations only)
  const stations = tideDataStore.getAvailableStations();

  if (stations.length === 0) {
    select.innerHTML = '<option value="">No stations available</option>';
    if (selectBottom) selectBottom.innerHTML = '<option value="">No stations available</option>';
    return;
  }

  // Separate stations into regular and geodetic
  const regularStations = [];
  const geodeticStations = [];

  stations.forEach(stationKey => {
    const metadata = tideDataStore.getStationMetadata(stationKey);
    const isGeodetic = metadata?.type === 'SURREY_FLOWWORKS';

    if (isGeodetic) {
      geodeticStations.push(stationKey);
    } else {
      regularStations.push(stationKey);
    }
  });

  // Sort each group alphabetically
  const sortByName = (a, b) => {
    const nameA = STATION_DISPLAY_NAMES[a] || a;
    const nameB = STATION_DISPLAY_NAMES[b] || b;
    return nameA.localeCompare(nameB);
  };
  regularStations.sort(sortByName);
  geodeticStations.sort(sortByName);

  // Build options HTML with optgroups
  let optionsHTML = '<option value="">-- Select a Station --</option>';

  // Regular chart datum stations
  if (regularStations.length > 0) {
    optionsHTML += '<optgroup label="Chart Datum Stations">';
    regularStations.forEach(stationKey => {
      const metadata = tideDataStore.getStationMetadata(stationKey);
      const hasObservations = metadata?.series && metadata.series.includes('wlo');
      const indicator = hasObservations ? ' 📡' : '';
      const displayName = (STATION_DISPLAY_NAMES[stationKey] || stationKey) + indicator;
      optionsHTML += `<option value="${stationKey}">${displayName}</option>`;
    });
    optionsHTML += '</optgroup>';
  }

  // Geodetic stations
  if (geodeticStations.length > 0) {
    optionsHTML += '<optgroup label="Geodetic Stations (CGVD28)">';
    geodeticStations.forEach(stationKey => {
      const displayName = (STATION_DISPLAY_NAMES[stationKey] || stationKey) + ' 📊';
      // Use unicode em-space (\u2003) for indentation - browsers don't support padding in option elements
      optionsHTML += `<option value="${stationKey}" class="geodetic-station" style="color: #888; font-style: italic;">\u2003\u2003${displayName}</option>`;
    });
    optionsHTML += '</optgroup>';
  }

  // Populate both dropdowns
  select.innerHTML = optionsHTML;
  if (selectBottom) selectBottom.innerHTML = optionsHTML;

  // Add change listener to top dropdown
  select.addEventListener('change', (e) => {
    if (e.target.value) {
      displayStationCallback(e.target.value);
      // Sync bottom dropdown
      if (selectBottom) selectBottom.value = e.target.value;
    } else {
      hideStationCallback();
      if (selectBottom) selectBottom.value = '';
    }
  });

  // Add change listener to bottom dropdown
  if (selectBottom) {
    selectBottom.addEventListener('change', (e) => {
      if (e.target.value) {
        displayStationCallback(e.target.value);
        // Sync top dropdown
        select.value = e.target.value;
      } else {
        hideStationCallback();
        select.value = '';
      }
    });
  }

  // Check for URL parameter first
  const urlParams = new URLSearchParams(window.location.search);
  const stationParam = urlParams.get('station');

  // Priority: URL param > current selection > default to Point Atkinson
  let stationToDisplay = null;

  if (stationParam && stations.includes(stationParam)) {
    // Use URL parameter if present and valid
    stationToDisplay = stationParam;
  } else if (tideDataStore.getCurrentStation() && stations.includes(tideDataStore.getCurrentStation())) {
    // Preserve current selection on auto-refresh
    stationToDisplay = tideDataStore.getCurrentStation();
  } else if (stations.includes('point_atkinson')) {
    // Default to Point Atkinson
    stationToDisplay = 'point_atkinson';
  }

  if (stationToDisplay) {
    select.value = stationToDisplay;
    if (selectBottom) selectBottom.value = stationToDisplay;
    displayStationCallback(stationToDisplay);
  }
}

/**
 * Setup day navigation buttons
 *
 * @param {Object} tideDataStore - Tide data store instance
 * @param {Function} updateChartCallback - Callback to update chart when day changes
 * @returns {void}
 */
export function setupDayNavigation(tideDataStore, updateChartCallback) {
  const prevBtn = document.getElementById('prev-day-btn');
  const nextBtn = document.getElementById('next-day-btn');

  // Remove existing listeners by cloning
  const newPrevBtn = prevBtn.cloneNode(true);
  const newNextBtn = nextBtn.cloneNode(true);
  prevBtn.replaceWith(newPrevBtn);
  nextBtn.replaceWith(newNextBtn);

  // Add new listeners
  newPrevBtn.addEventListener('click', () => {
    const currentOffset = tideDataStore.getDayOffset();
    if (currentOffset > 0) {
      tideDataStore.setDayOffset(currentOffset - 1);
      updateChartCallback();
    }
  });

  newNextBtn.addEventListener('click', () => {
    const currentOffset = tideDataStore.getDayOffset();
    if (currentOffset < 2) {  // We have 3 days of data (0-2)
      tideDataStore.setDayOffset(currentOffset + 1);
      updateChartCallback();
    }
  });

  updateDayLabel(tideDataStore.getDayOffset());
  updateNavigationButtons(tideDataStore.getDayOffset());
}

/**
 * Update the day label display
 *
 * @param {number} dayOffset - Current day offset (0=today, 1=tomorrow, 2=day after)
 * @returns {void}
 */
export function updateDayLabel(dayOffset) {
  const label = document.getElementById('chart-date-label');
  const pacific = 'America/Vancouver';
  const now = new Date();

  // Get current year/month/day in Pacific timezone
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: pacific,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  const parts = formatter.formatToParts(now);
  const year = parseInt(parts.find(p => p.type === 'year').value);
  const month = parseInt(parts.find(p => p.type === 'month').value);
  const day = parseInt(parts.find(p => p.type === 'day').value);

  // Create date and add offset
  const targetDate = new Date(year, month - 1, day);
  targetDate.setDate(targetDate.getDate() + dayOffset);

  const dateStr = targetDate.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric'
  });

  if (dayOffset === 0) {
    label.textContent = `Today (${dateStr})`;
  } else if (dayOffset === 1) {
    label.textContent = `Tomorrow (${dateStr})`;
  } else {
    label.textContent = dateStr;
  }
}

/**
 * Update navigation button states (enabled/disabled)
 *
 * @param {number} dayOffset - Current day offset (0=today, 1=tomorrow, 2=day after)
 * @returns {void}
 */
export function updateNavigationButtons(dayOffset) {
  const prevBtn = document.getElementById('prev-day-btn');
  const nextBtn = document.getElementById('next-day-btn');

  prevBtn.disabled = dayOffset === 0;
  nextBtn.disabled = dayOffset === 2;  // We have 3 days of data (0-2)

  prevBtn.style.opacity = dayOffset === 0 ? '0.3' : '1';
  nextBtn.style.opacity = dayOffset === 2 ? '0.3' : '1';
  prevBtn.style.cursor = dayOffset === 0 ? 'not-allowed' : 'pointer';
  nextBtn.style.cursor = dayOffset === 2 ? 'not-allowed' : 'pointer';
}

/**
 * Hide station display
 *
 * @returns {void}
 */
export function hideStation() {
  document.getElementById('tide-loading').style.display = 'none';
  document.getElementById('tide-current-section').style.display = 'none';
  document.getElementById('tide-error').style.display = 'none';
}
