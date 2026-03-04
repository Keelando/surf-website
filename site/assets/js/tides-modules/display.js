/**
 * Display Module
 * Handles rendering of station information, observations, predictions, storm surge, and high/low tables
 */

import { STATION_DISPLAY_NAMES } from './constants.js';
import { isGeodeticStation, getGeodeticMethodology, getCurrentGeodeticOffset } from './geodetic.js';
import { formatTime, getAgeString } from './utils.js';

/**
 * Main station display coordinator
 * Shows all station information when a station is selected
 *
 * @param {string} stationKey - Station identifier
 * @param {Object} tideDataStore - Tide data store instance
 * @param {Function} setupDayNavigationCallback - Callback to setup day navigation
 * @param {Function} displayTideChartCallback - Callback to display chart
 * @param {Function} displaySunlightCallback - Callback to display sunlight times
 * @returns {void}
 */
export function displayStation(
  stationKey,
  tideDataStore,
  setupDayNavigationCallback,
  displayTideChartCallback,
  displaySunlightCallback
) {
  const section = document.getElementById('tide-current-section');
  const loading = document.getElementById('tide-loading');
  const error = document.getElementById('tide-error');

  // Hide loading/error
  loading.style.display = 'none';
  error.style.display = 'none';

  // Save current station and reset day offset
  tideDataStore.setCurrentStation(stationKey);
  tideDataStore.setDayOffset(0);

  // Get station data
  const currentStation = tideDataStore.getCurrentObservation(stationKey);
  const highlowStation = tideDataStore.getHighLow(stationKey);

  // Update station name in all locations
  const stationName = STATION_DISPLAY_NAMES[stationKey] || stationKey;
  document.getElementById('station-name').textContent = stationName;
  document.getElementById('highlow-station-name').textContent = stationName;
  document.getElementById('chart-station-name').textContent = stationName;

  // Display station metadata
  displayStationMetadata(stationKey, tideDataStore);

  // Display sunlight times for this station (for current day)
  displaySunlightCallback(stationKey, 0);

  // Get full station object for display functions
  const fullStation = {
    observation: tideDataStore.getCurrentObservation(stationKey),
    prediction_now: tideDataStore.getCurrentPrediction(stationKey),
    tide_offset: tideDataStore.getTideOffset(stationKey)
  };

  // Display current observation
  displayCurrentObservation(fullStation, stationKey, tideDataStore);

  // Display current prediction
  displayCurrentPrediction(fullStation, stationKey, tideDataStore);

  // Display high/low table (for current day)
  displayHighLowTable(highlowStation, 0);

  // Show the section first (so chart can measure properly)
  section.style.display = 'block';

  // Setup day navigation buttons
  setupDayNavigationCallback();

  // Display tide chart after section is visible (must be before storm surge card for geodetic stations)
  displayTideChartCallback(stationKey, 0);

  // Display storm surge AFTER chart so geodetic residuals are available
  displayStormSurge(fullStation, stationKey, tideDataStore);
}

/**
 * Display station metadata (type, code, coordinates, location)
 *
 * @param {string} stationKey - Station identifier
 * @param {Object} tideDataStore - Tide data store instance
 * @returns {void}
 */
export function displayStationMetadata(stationKey, tideDataStore) {
  const container = document.getElementById('station-metadata');

  const metadata = tideDataStore.getStationMetadata(stationKey);
  if (!metadata) {
    container.innerHTML = '';
    return;
  }

  // Check if station has observations by looking at series array
  const hasObservations = metadata.series && metadata.series.includes('wlo');

  // Determine badge based on observation capability
  const typeClass = hasObservations ? 'station-type-permanent' : 'station-type-temporary';
  const typeLabel = hasObservations ? '📡 Real-Time Observations' : '📊 Predictions Only';

  container.innerHTML = `
    <div class="metadata-item">
      <span class="station-type-badge ${typeClass}">${typeLabel}</span>
    </div>
    <div class="metadata-item">
      <strong>Code:</strong> <span>${metadata.code}</span>
    </div>
    <div class="metadata-item">
      <strong>📍 Coordinates:</strong> <span>${metadata.lat.toFixed(4)}°N, ${Math.abs(metadata.lon).toFixed(4)}°W</span>
    </div>
    <div class="metadata-item">
      <strong>Location:</strong> <span>${metadata.location}</span>
    </div>
  `;
}

/**
 * Display current observation
 *
 * @param {Object} station - Station data object
 * @param {string} stationKey - Station identifier
 * @param {Object} tideDataStore - Tide data store instance
 * @returns {void}
 */
export function displayCurrentObservation(station, stationKey, tideDataStore) {
  const container = document.getElementById('current-observation');

  if (!station || !station.observation || station.observation.value === null) {
    // Check if this station has observations at all (in timeseries data)
    const timeseries = tideDataStore.getTimeseries(stationKey);
    const hasObservations = timeseries?.has_observations || false;

    if (!hasObservations) {
      container.innerHTML = `
        <div style="padding: 1rem; background: #fff3e0; border-radius: 4px; border-left: 4px solid #ff9800;">
          <div style="font-weight: bold; color: #f57c00; margin-bottom: 0.5rem;">
            📊 Predictions Only
          </div>
          <div style="color: #666; font-size: 0.9rem;">
            This station provides astronomical tide predictions but does not have real-time water level sensors.
          </div>
        </div>
      `;
    } else {
      container.innerHTML = '<p style="color: #999;">No recent observation available</p>';
    }
    return;
  }

  const obs = station.observation;
  let observedLevel = obs.value;
  const obsTime = new Date(obs.time);
  const timeStr = formatTime(obsTime);
  const ageStr = getAgeString(obsTime);
  const isStale = obs.stale || false;

  // For geodetic stations that calibrate observations, show calibrated value
  const methodology = getGeodeticMethodology(stationKey);
  const geodeticOffset = getCurrentGeodeticOffset(stationKey, tideDataStore.tideTimeseriesData);

  let calibrationNote = '';
  if (methodology === 'calibrate_observation' && geodeticOffset !== null) {
    observedLevel = obs.value + geodeticOffset;
    calibrationNote = `
      <div style="color: #1976d2; margin-top: 0.5rem; font-size: 0.85rem; font-style: italic;">
        📏 Calibrated (offset: ${geodeticOffset >= 0 ? '+' : ''}${geodeticOffset.toFixed(3)}m)
      </div>
    `;
  }

  container.innerHTML = `
    <div style="font-size: 1.5rem; font-weight: bold; color: ${isStale ? '#e53935' : '#43a047'};">
      ${observedLevel.toFixed(2)} m
    </div>
    <div style="color: #666; margin-top: 0.25rem; font-size: 0.9rem;">
      at ${timeStr}
      <span style="color: ${isStale ? '#e53935' : '#999'};">(${ageStr})</span>
    </div>
    ${isStale ? '<div style="color: #e53935; margin-top: 0.25rem; font-size: 0.85rem;">⚠ Data may be stale</div>' : ''}
    ${calibrationNote}
  `;
}

/**
 * Display current prediction
 *
 * @param {Object} station - Station data object
 * @param {string} stationKey - Station identifier
 * @param {Object} tideDataStore - Tide data store instance
 * @returns {void}
 */
export function displayCurrentPrediction(station, stationKey, tideDataStore) {
  const container = document.getElementById('current-prediction');

  if (!station || !station.prediction_now || station.prediction_now.value === null) {
    container.innerHTML = '<p style="color: #999;">No prediction available</p>';
    return;
  }

  const pred = station.prediction_now;
  let tideLevel = pred.value;
  const predTime = new Date(pred.time);
  const timeStr = formatTime(predTime);

  // For geodetic stations that calibrate predictions, show calibrated value
  const methodology = getGeodeticMethodology(stationKey);
  const geodeticOffset = getCurrentGeodeticOffset(stationKey, tideDataStore.tideTimeseriesData);

  let calibrationNote = '';
  if (methodology === 'calibrate_prediction' && geodeticOffset !== null) {
    tideLevel = pred.value + geodeticOffset;
    calibrationNote = `
      <div style="color: #1976d2; margin-top: 0.5rem; font-size: 0.85rem; font-style: italic;">
        📏 Calibrated (offset: ${geodeticOffset >= 0 ? '+' : ''}${geodeticOffset.toFixed(3)}m)
      </div>
    `;
  }

  // Determine tide direction (rising, falling, slack)
  let tideDirection = '';
  let tideArrow = '';
  if (pred.trend) {
    if (pred.trend === 'rising') {
      tideDirection = 'Rising';
      tideArrow = '↗️';
    } else if (pred.trend === 'falling') {
      tideDirection = 'Falling';
      tideArrow = '↘️';
    } else if (pred.trend === 'slack') {
      tideDirection = 'Slack';
      tideArrow = '→';
    }
  }

  // Get next high/low event
  let nextEventHtml = '';
  const highlowData = tideDataStore.getHighLow(stationKey);
  if (highlowData && highlowData.events) {
    const now = Date.now();
    const futureEvents = highlowData.events.filter(e => new Date(e.time).getTime() > now);
    if (futureEvents.length > 0) {
      const nextEvent = futureEvents[0];

      // Only display if we have valid event data
      if (nextEvent.time && nextEvent.type && nextEvent.value != null) {
        const eventTime = new Date(nextEvent.time);
        const eventTimeStr = eventTime.toLocaleTimeString('en-US', {
          hour: '2-digit',
          minute: '2-digit',
          hour12: false,
          timeZone: 'America/Vancouver'
        });
        const eventType = nextEvent.type === 'high' ? 'High' : 'Low';
        const eventHeight = nextEvent.value.toFixed(2);

        // Calculate time remaining
        const msUntil = eventTime.getTime() - Date.now();
        const minutesUntil = Math.floor(msUntil / 60000);
        const hoursUntil = Math.floor(minutesUntil / 60);
        const remainingMinutes = minutesUntil % 60;

        let timeUntilStr = '';
        if (hoursUntil > 0) {
          timeUntilStr = `in ${hoursUntil}h ${remainingMinutes}m`;
        } else if (minutesUntil > 0) {
          timeUntilStr = `in ${minutesUntil}m`;
        } else {
          timeUntilStr = 'now';
        }

        // Try to find storm surge forecast at this time for combined water level
        let combinedWaterLevel = null;
        const combinedData = tideDataStore.getCombinedWaterLevel(stationKey);
        if (combinedData && combinedData.forecast) {
          // Find forecast entry closest to the event time
          const eventTimeTs = eventTime.getTime();
          let closestForecast = null;
          let minDiff = Infinity;

          combinedData.forecast.forEach(f => {
            const forecastTime = new Date(f.time).getTime();
            const diff = Math.abs(forecastTime - eventTimeTs);
            if (diff < minDiff && diff < 900000) { // Within 15 minutes
              minDiff = diff;
              closestForecast = f;
            }
          });

          if (closestForecast && closestForecast.total_water_level_m != null) {
            combinedWaterLevel = closestForecast.total_water_level_m.toFixed(2);
          }
        }

        nextEventHtml = `
          <div style="margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid #eee; font-size: 0.85rem;">
            <div style="color: #666;">
              Next ${eventType} Tide: <strong style="color: #0077be;">${eventHeight} m</strong>
              ${combinedWaterLevel ? `<span style="color: #00897b; font-weight: 600;">(${combinedWaterLevel} m total)</span>` : ''}
              <span style="color: #43a047; font-weight: 600;">${timeUntilStr}</span>
              <span style="color: #999;">(${eventTimeStr})</span>
            </div>
          </div>
        `;
      }
    }
  }

  container.innerHTML = `
    <div>
      <div style="font-size: 1.5rem; font-weight: bold; color: #0077be;">
        ${tideLevel.toFixed(2)} m ${tideArrow}
      </div>
      <div style="color: #666; margin-top: 0.25rem; font-size: 0.9rem;">
        at ${timeStr}${tideDirection ? ` <span style="color: #0077be;">(${tideDirection})</span>` : ''}
      </div>
      ${calibrationNote}
    </div>
    ${nextEventHtml}
  `;
}

/**
 * Generate Today's Peak Tide Forecast HTML section
 * @param {Object} tideDataStore - Tide data store instance
 * @param {string} stationKey - Station identifier
 * @returns {string} HTML string for peak forecast, or empty string if not available
 */
function generatePeakForecastHtml(tideDataStore, stationKey) {
  const combinedData = tideDataStore.getCombinedWaterLevel(stationKey);
  if (!combinedData || !combinedData.forecast || combinedData.forecast.length === 0) {
    return '';
  }

  // Get today's date range in Pacific time
  const now = new Date();
  const pacificNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/Vancouver' }));
  const todayStart = new Date(pacificNow);
  todayStart.setHours(0, 0, 0, 0);
  const todayEnd = new Date(pacificNow);
  todayEnd.setHours(23, 59, 59, 999);

  // Filter forecast for today only and find peak
  let todayPeak = null;
  combinedData.forecast.forEach(entry => {
    const entryTime = new Date(entry.time);
    const pacificEntryTime = new Date(entryTime.toLocaleString('en-US', { timeZone: 'America/Vancouver' }));

    // Check if this entry is today
    if (pacificEntryTime >= todayStart && pacificEntryTime <= todayEnd) {
      if (!todayPeak || entry.total_water_level_m > todayPeak.total_water_level_m) {
        todayPeak = entry;
      }
    }
  });

  if (!todayPeak) {
    return '';
  }

  const peakTime = new Date(todayPeak.time);
  const peakTimeStr = peakTime.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'America/Vancouver'
  });

  return `
    <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #ddd;">
      <div style="font-size: 0.9rem; color: #666; margin-bottom: 0.75rem;">
        <strong>Today's Peak Tide Forecast</strong>
      </div>
      <div style="font-size: 0.95rem; line-height: 1.6;">
        Today's forecasted peak water level is
        <strong style="color: #00897b; font-size: 1.1rem;">${todayPeak.total_water_level_m.toFixed(2)} m</strong>
        with a storm surge of
        <strong style="color: #9c27b0;">${todayPeak.storm_surge_m >= 0 ? '+' : ''}${todayPeak.storm_surge_m.toFixed(3)} m</strong>
        at <strong style="color: #0077be;">${peakTimeStr}</strong>.
      </div>
    </div>
  `;
}

/**
 * Display storm surge / tide offset
 *
 * @param {Object} station - Station data object
 * @param {string} stationKey - Station identifier
 * @param {Object} tideDataStore - Tide data store instance
 * @returns {void}
 */
export function displayStormSurge(station, stationKey, tideDataStore) {
  const container = document.getElementById('storm-surge');

  // ===================================================================
  // GEODETIC STATIONS (Surrey/FlowWorks data - CGVD28 datum)
  // ===================================================================
  // Geodetic stations use Surrey's pre-calculated residuals (taken at face value)
  // This data must NEVER mix with DFO data
  if (isGeodeticStation(stationKey)) {
    const geodeticResiduals = tideDataStore.getGeodeticResiduals();

    if (geodeticResiduals.length > 0) {
      // Get the last (most recent) residual from the chart
      const lastResidual = geodeticResiduals[geodeticResiduals.length - 1];
      const [residualTime, residualValue] = lastResidual;

      const residualStr = residualValue >= 0 ? `+${residualValue.toFixed(3)}` : residualValue.toFixed(3);
      const color = Math.abs(residualValue) > 0.3 ? '#e53935' : (Math.abs(residualValue) > 0.15 ? '#ff9800' : '#43a047');
      const residualTimeStr = formatTime(residualTime);

      // Get ECCC forecast for comparison (optional)
      const forecastSurge = station?.prediction_now?.surge;
      let forecastHtml = '';
      if (forecastSurge !== null && forecastSurge !== undefined) {
        const forecastStr = forecastSurge >= 0 ? `+${forecastSurge.toFixed(3)}` : forecastSurge.toFixed(3);
        forecastHtml = `
          <div style="color: #666; margin-top: 0.75rem; font-size: 0.9rem; padding-top: 0.75rem; border-top: 1px solid #eee;">
            <strong>ECCC Storm Surge:</strong> <span style="color: #9c27b0; font-weight: 600;">${forecastStr} m</span>
          </div>
        `;
      }

      // Get peak forecast if available
      const peakForecastHtml = generatePeakForecastHtml(tideDataStore, stationKey);

      container.innerHTML = `
        <div style="font-size: 1.5rem; font-weight: bold; color: ${color};">
          ${residualStr} m
        </div>
        <div style="color: #666; margin-top: 0.25rem; font-size: 0.9rem;">
          Residual
        </div>
        <div style="color: #999; margin-top: 0.25rem; font-size: 0.85rem;">
          at ${residualTimeStr}
        </div>
        ${forecastHtml}
        <div style="color: #999; margin-top: 0.5rem; font-size: 0.8rem;">
          Latest value from chart
        </div>
        ${peakForecastHtml}
      `;
      return;
    } else {
      // Geodetic station but no residuals available
      container.innerHTML = `
        <div style="font-size: 1rem; color: #e53935;">
          No residual data available
        </div>
        <div style="color: #666; margin-top: 0.5rem; font-size: 0.85rem;">
          Check if Surrey observations and residuals are available
        </div>
      `;
      return;
    }
  }

  // ===================================================================
  // DFO STATIONS (Chart Datum)
  // ===================================================================
  // Check if we have tide_offset (observed - predicted with matched timestamps)
  if (station && station.tide_offset && station.tide_offset.value !== null) {
    const offset = station.tide_offset.value;
    const offsetStr = offset >= 0 ? `+${offset.toFixed(2)}` : offset.toFixed(2);
    const color = Math.abs(offset) > 0.3 ? '#e53935' : (Math.abs(offset) > 0.15 ? '#ff9800' : '#43a047');

    // Format the calculation time
    const calcTime = new Date(station.tide_offset.observation_time);
    const calcTimeStr = formatTime(calcTime);

    // Check if we also have ECCC forecast surge for comparison
    const forecastSurge = station.observation?.surge || station.prediction_now?.surge;
    let forecastHtml = '';
    if (forecastSurge !== null && forecastSurge !== undefined) {
      const forecastStr = forecastSurge >= 0 ? `+${forecastSurge.toFixed(3)}` : forecastSurge.toFixed(3);
      forecastHtml = `
        <div style="color: #666; margin-top: 0.5rem; font-size: 0.85rem;">
          ECCC Forecast: <strong style="color: #ff9800;">${forecastStr} m</strong>
        </div>
      `;
    }

    // Get peak forecast if available
    const peakForecastHtml = generatePeakForecastHtml(tideDataStore, stationKey);

    container.innerHTML = `
      <div style="font-size: 1.5rem; font-weight: bold; color: ${color};">
        ${offsetStr} m
      </div>
      <div style="color: #666; margin-top: 0.25rem; font-size: 0.9rem;">
        Observed - Predicted
      </div>
      <div style="color: #999; margin-top: 0.25rem; font-size: 0.85rem;">
        Calculated at ${calcTimeStr}
      </div>
      ${forecastHtml}
      <div style="color: #999; margin-top: 0.25rem; font-size: 0.8rem;">
        ${station.tide_offset.description}
      </div>
      ${peakForecastHtml}
    `;
    return;
  }

  // Fall back to storm surge forecast from combined water level data
  let surge = null;
  let surgeTime = null;

  // First try to get current surge from combined water level data
  const combinedData = tideDataStore.getCombinedWaterLevel(stationKey);
  if (combinedData && combinedData.forecast) {
    // Find forecast closest to now
    const now = new Date();
    let closestForecast = null;
    let minDiff = Infinity;

    for (const point of combinedData.forecast) {
      const pointTime = new Date(point.time);
      const diff = Math.abs(pointTime - now);
      if (diff < minDiff) {
        minDiff = diff;
        closestForecast = point;
      }
    }

    if (closestForecast && closestForecast.storm_surge_m !== null && closestForecast.storm_surge_m !== undefined) {
      surge = closestForecast.storm_surge_m;
      surgeTime = new Date(closestForecast.time);
    }
  }

  // Fall back to prediction_now.surge if available
  if (!surge && station && station.prediction_now && station.prediction_now.surge !== null && station.prediction_now.surge !== undefined) {
    surge = station.prediction_now.surge;
    surgeTime = new Date(station.prediction_now.time);
  }

  if (surge === null) {
    container.innerHTML = '<p style="color: #999;">No storm surge data available</p>';
    return;
  }

  const timeStr = formatTime(surgeTime);

  // Get peak forecast if available
  const peakHtml = generatePeakForecastHtml(tideDataStore, stationKey);

  container.innerHTML = `
    <div>
      <div style="font-size: 1.5rem; font-weight: bold; color: #ff9800;">
        ${surge >= 0 ? '+' : ''}${surge.toFixed(3)} m
      </div>
      <div style="color: #666; margin-top: 0.25rem; font-size: 0.9rem;">
        Current (at ${timeStr})
      </div>
    </div>
    ${peakHtml}
  `;
}

/**
 * Display high/low tide table
 *
 * @param {Object} station - High/low station data
 * @param {number} dayOffset - Day offset (0=today, 1=tomorrow, 2=day after)
 * @returns {void}
 */
export function displayHighLowTable(station, dayOffset = 0) {
  const tbody = document.querySelector('#highlow-table tbody');

  if (!station || !station.events || station.events.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="color: #999; text-align: center;">No high/low data available</td></tr>';
    return;
  }

  // Calculate target day in Pacific timezone
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

  // Create a date for midnight Pacific time on the current Pacific day
  const pacificMidnight = new Date(year, month - 1, day);

  // Add day offset
  pacificMidnight.setDate(pacificMidnight.getDate() + dayOffset);

  // Format as YYYY-MM-DD
  const targetYear = pacificMidnight.getFullYear();
  const targetMonth = String(pacificMidnight.getMonth() + 1).padStart(2, '0');
  const targetDay = String(pacificMidnight.getDate()).padStart(2, '0');
  const targetDateStr = `${targetYear}-${targetMonth}-${targetDay}`;

  // Filter events for the target day
  const eventsForDay = station.events.filter(event => {
    // event.date is in format "YYYY-MM-DD" (Pacific time)
    return event.date === targetDateStr;
  });

  if (eventsForDay.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="color: #999; text-align: center;">No high/low data available for this day</td></tr>';
    return;
  }

  // Sort events by time
  const events = [...eventsForDay].sort((a, b) => new Date(a.time) - new Date(b.time));

  // Build table rows
  tbody.innerHTML = events.map(event => {
    const timeStr = event.time_display; // Use pre-formatted time from JSON
    const height = event.value.toFixed(2);
    const type = event.type.charAt(0).toUpperCase() + event.type.slice(1);
    const typeColor = event.type === 'high' ? '#0077be' : '#e53935';

    return `
      <tr>
        <td style="font-weight: bold;">${timeStr}</td>
        <td>${height} m</td>
        <td style="color: ${typeColor}; font-weight: bold;">${type}</td>
      </tr>
    `;
  }).join('');
}
