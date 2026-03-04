/* -----------------------------
   Wind Stations Module
   Displays current wind conditions table and 24hr trends chart
   ----------------------------- */

// Helper: Fetch with timeout
async function fetchWithTimeout(url, timeout = 5000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(id);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } catch (error) {
    clearTimeout(id);
    throw error;
  }
}

// Helper: Convert degrees to cardinal direction
function degreesToCardinal(degrees) {
  if (degrees == null) return null;
  const directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}

// Helper: Get directional arrow (SVG, rotated to exact degrees)
// Based on buoy page implementation - provides infinite precision
function getDirectionalArrow(degrees, arrowType = 'wind') {
  if (degrees == null || degrees === '—') return '';

  // Meteorological convention: direction indicates WHERE wind is COMING FROM
  // Arrow rotation: wind arrow points down by default, rotates to show direction wind is blowing TO
  const rotation = degrees; // Wind arrow points down, so rotate by degrees directly

  // SVG wind arrow pointing down
  const svg = `<svg width="16" height="16" viewBox="0 0 16 16"><path d="M8 2v12m0 0l-3-3m3 3l3-3" stroke="#004b7c" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`;

  return `<span style="display:inline-block;transform:rotate(${rotation}deg);margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

// Helper: Format timestamp to local time
function formatTimestamp(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString("en-US", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  }).replace(',', '');
}

// Global chart instance
let windChart = null;
let windTimeseriesData = null;
let allStationsList = []; // Store all stations
let currentSort = { column: null, ascending: true };
let currentWindTimeRange = 24; // Default to 24 hours

/**
 * Filter wind timeseries data to specified time range (hours)
 */
function filterWindTimeseriesData(data, hours) {
  if (!data) return data;

  const now = new Date();
  const cutoff = new Date(now - hours * 60 * 60 * 1000);

  // Deep copy and filter each station's timeseries
  const filtered = {};

  Object.keys(data).forEach(stationId => {
    if (stationId === '_meta') {
      filtered[stationId] = data[stationId];
      return;
    }

    const station = data[stationId];
    filtered[stationId] = {
      name: station.name,
      isBuoy: station.isBuoy,
      timeseries: {}
    };

    // Handle both buoy format (with .data) and wind station format (direct arrays)
    Object.keys(station.timeseries || {}).forEach(metricKey => {
      const metric = station.timeseries[metricKey];

      if (Array.isArray(metric)) {
        // Wind station format: direct array
        filtered[stationId].timeseries[metricKey] = metric.filter(point => new Date(point.time) >= cutoff);
      } else if (metric.data && Array.isArray(metric.data)) {
        // Buoy format: {data: [...], name, unit}
        filtered[stationId].timeseries[metricKey] = {
          name: metric.name,
          unit: metric.unit,
          data: metric.data.filter(point => new Date(point.time) >= cutoff)
        };
      }
    });
  });

  return filtered;
}

/**
 * Initialize sortable table functionality
 */
function initializeSortableTable() {
  const headers = document.querySelectorAll('#wind-conditions-table th.sortable');

  headers.forEach(header => {
    header.style.cursor = 'pointer';
    header.style.userSelect = 'none';

    header.addEventListener('click', () => {
      const column = header.dataset.column;
      const type = header.dataset.type;

      // Toggle sort direction if clicking same column
      if (currentSort.column === column) {
        currentSort.ascending = !currentSort.ascending;
      } else {
        currentSort.column = column;
        currentSort.ascending = true;
      }

      sortTable(column, type, currentSort.ascending);
      updateSortIndicators(header);
    });
  });
}

/**
 * Sort table by column
 */
function sortTable(column, type, ascending) {
  const table = document.getElementById('wind-conditions-table');
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));

  rows.sort((a, b) => {
    let aVal = a.dataset[column];
    let bVal = b.dataset[column];

    // Handle empty values
    if (!aVal && !bVal) return 0;
    if (!aVal) return 1;
    if (!bVal) return -1;

    // Compare based on type
    let comparison = 0;
    if (type === 'number') {
      comparison = parseFloat(aVal) - parseFloat(bVal);
    } else if (type === 'date') {
      comparison = new Date(aVal) - new Date(bVal);
    } else {
      // String comparison
      comparison = aVal.toLowerCase().localeCompare(bVal.toLowerCase());
    }

    return ascending ? comparison : -comparison;
  });

  // Re-append rows in sorted order
  rows.forEach(row => tbody.appendChild(row));
}

/**
 * Update sort direction indicators
 */
function updateSortIndicators(activeHeader) {
  // Clear all indicators and remove sorting class
  document.querySelectorAll('#wind-conditions-table th.sortable').forEach(header => {
    header.classList.remove('sorting');
    const indicator = header.querySelector('.sort-indicator');
    indicator.textContent = '';
  });

  // Set active indicator
  activeHeader.classList.add('sorting');
  const indicator = activeHeader.querySelector('.sort-indicator');
  indicator.textContent = currentSort.ascending ? '▲' : '▼';
}

/**
 * Load and display current wind conditions table
 */
async function loadWindTable() {
  try {
    // Load stations metadata, wind stations, and buoy data
    const [stationsMetadata, windData, buoyData] = await Promise.all([
      fetchWithTimeout(`/data/stations.json?t=${Date.now()}`),
      fetchWithTimeout(`/data/latest_wind.json?t=${Date.now()}`),
      fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`)
    ]);

    const table = document.getElementById("wind-conditions-table");
    if (!table) return;

    // Combine wind stations and buoys
    const allStations = [];
    const offlineStations = []; // Stations with data > 4 hours old

    // Add wind stations
    Object.entries(windData)
      .filter(([key]) => key !== '_meta')
      .forEach(([id, station]) => {
        // Calculate data age
        const obsTime = station.observation_time ? new Date(station.observation_time) : null;
        const ageHours = obsTime ? (Date.now() - obsTime.getTime()) / (1000 * 60 * 60) : Infinity;

        const stationData = {
          name: station.name + ' 💨',
          wind_speed_kt: station.wind_speed_kt != null ? Math.round(station.wind_speed_kt) : null,
          wind_gust_kt: station.wind_gust_kt != null ? Math.round(station.wind_gust_kt) : null,
          wind_direction: station.wind_direction_deg || station.wind_direction,
          wind_direction_cardinal: station.wind_direction_cardinal,
          air_temp_c: station.air_temp_c,
          pressure_hpa: station.pressure_hpa,
          observation_time: station.observation_time,
          ageHours: ageHours,
          stale: ageHours >= 2 && ageHours < 4, // Mark as stale if 2-4 hours old
          type: 'land'
        };

        if (ageHours >= 4) {
          // Data is too old - add to offline list
          offlineStations.push([id, stationData]);
        } else {
          // Data is fresh enough - add to main table
          allStations.push([id, stationData]);
        }
      });

    // Add buoys (with wind data)
    Object.entries(buoyData)
      .filter(([key]) => key !== '_meta')
      .forEach(([id, buoy]) => {
        // Only add buoys that have wind data
        const buoyWindDir = buoy.wind_direction_deg || buoy.wind_direction;
        if (buoy.wind_speed != null || buoyWindDir != null) {
          // Use field-specific timestamp for wind if available, otherwise use main observation_time
          let windObsTime = buoy.observation_time;
          if (buoy.field_times && (buoy.field_times.wind_speed || buoy.field_times.wind_direction)) {
            // Use the most recent wind-related field timestamp
            windObsTime = buoy.field_times.wind_speed || buoy.field_times.wind_direction;
          }

          // Calculate data age
          const obsTime = windObsTime ? new Date(windObsTime) : null;
          const ageHours = obsTime ? (Date.now() - obsTime.getTime()) / (1000 * 60 * 60) : Infinity;

          // Determine icon based on station type from metadata
          const stationMeta = stationsMetadata.buoys?.[id];
          const isWindStation = stationMeta?.type === 'wind_monitoring_station' ||
                                stationMeta?.type === 'weather_station' ||
                                stationMeta?.type === 'c_man_station' ||
                                stationMeta?.type === 'land_station';
          const icon = isWindStation ? ' 💨' : ' 🌊';

          const stationData = {
            name: buoy.name + icon,
            wind_speed_kt: buoy.wind_speed != null ? Math.round(buoy.wind_speed) : null,
            wind_gust_kt: buoy.wind_gust != null ? Math.round(buoy.wind_gust) : null,
            wind_direction: buoy.wind_direction_deg || buoy.wind_direction,
            wind_direction_cardinal: buoy.wind_direction_cardinal,
            air_temp_c: buoy.air_temp,
            pressure_hpa: buoy.pressure,
            observation_time: windObsTime,
            ageHours: ageHours,
            stale: ageHours >= 2 && ageHours < 4, // Mark as stale if 2-4 hours old
            type: isWindStation ? 'land' : 'buoy'
          };

          if (ageHours >= 4) {
            // Data is too old - add to offline list
            offlineStations.push([id, stationData]);
          } else {
            // Data is fresh enough - add to main table
            allStations.push([id, stationData]);
          }
        }
      });

    // Sort all stations by name
    const stations = allStations;
    stations.sort((a, b) => a[1].name.localeCompare(b[1].name));

    // Station-specific source links (used in table and offline list)
    const sourceLinks = {
      // Environment Canada Marine Stations
      'CWGT': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06100&stationID=WGT',
      'CWGB': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=WGB',
      'CWEL': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06200&stationID=WEL',
      'CWSB': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06300&stationID=WSB',
      'CVTF': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06500&stationID=VTF',
      'CWVF': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06600&stationID=WVF',
      'CWEZ': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06700&stationID=WEZ',
      'CWQK': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06800&stationID=WQK',
      // Environment Canada Airports
      'CYVR': 'https://spaces.navcanada.ca/workspace/aeroview/CYVR',
      'CZBB': 'https://spaces.navcanada.ca/workspace/aeroview/CZBB',
      // Environment Canada Buoys
      '4600146': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46146',
      '4600304': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46304',
      '4600303': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46303',
      '4600131': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46131',
      '4600206': 'https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46206',
      // NOAA Buoys and Stations
      '46087': 'https://www.ndbc.noaa.gov/station_page.php?station=46087',
      '46088': 'https://www.ndbc.noaa.gov/station_page.php?station=46088',
      '46267': 'https://www.ndbc.noaa.gov/station_page.php?station=46267',
      'CPMW1': 'https://www.ndbc.noaa.gov/station_page.php?station=cpmw1',
      'SISW1': 'https://www.ndbc.noaa.gov/station_page.php?station=sisw1',
      // NOAA NWS Airports
      'KBLI': 'https://www.weather.gov/wrh/timeseries?site=KBLI',
      'KORS': 'https://www.weather.gov/wrh/timeseries?site=KORS',
      // Municipal/Other
      'whiterock_east': 'https://www.whiterockcity.ca/1000/Weather-Station',
      'JERICHO': 'https://jsca.bc.ca/services/weather/'
    };

    let tableHTML = `
      <thead>
        <tr>
          <th class="sortable" data-column="name" data-type="string">Station <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="wind_direction" data-type="number">Direction <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="wind_speed_kt" data-type="number">Speed (kt) <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="wind_gust_kt" data-type="number">Gust (kt) <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="air_temp_c" data-type="number">Temp (°C) <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="pressure_hpa" data-type="number">Pressure (hPa) <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="observation_time" data-type="date">Updated <span class="sort-indicator"></span></th>
          <th>View:</th>
        </tr>
      </thead>
      <tbody>
    `;

    stations.forEach(([id, station]) => {
      const rowClass = station.stale ? 'class="stale"' : '';
      // Round wind speeds to integers
      const windSpeed = station.wind_speed_kt != null ? Math.round(station.wind_speed_kt) : '—';
      const windGust = station.wind_gust_kt != null ? Math.round(station.wind_gust_kt) : '—';
      // Show arrow, cardinal direction, and degrees
      const direction = station.wind_direction != null
        ? `${station.wind_direction_cardinal || degreesToCardinal(station.wind_direction)} (${station.wind_direction}°) ${getDirectionalArrow(station.wind_direction)}`
        : '—';
      const temp = station.air_temp_c != null ? station.air_temp_c.toFixed(1) : '—';
      const pressure = station.pressure_hpa != null ? station.pressure_hpa.toFixed(1) : '—';
      const updated = formatTimestamp(station.observation_time);

      // Determine source badge and link
      let sourceBadge = '';
      if (sourceLinks[id]) {
        if (id.startsWith('4600')) {
          // Environment Canada buoys (4600xxx)
          sourceBadge = `<br><a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="font-size: 0.75em; color: #006400; text-decoration: none;">🇨🇦 Env Canada 🔗</a>`;
        } else if (id.startsWith('46') || id === 'CPMW1' || id === 'SISW1') {
          // NOAA buoys/stations (46xxx but not 4600xxx)
          sourceBadge = `<br><a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="font-size: 0.75em; color: #003087; text-decoration: none;">🇺🇸 NOAA 🔗</a>`;
        } else if (id.startsWith('K')) {
          // NOAA NWS airports (KXXX)
          sourceBadge = `<br><a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="font-size: 0.75em; color: #003087; text-decoration: none;">🇺🇸 NOAA 🔗</a>`;
        } else if (id === 'CRPILE' || id === 'CRCHAN' || id === 'COLEB') {
          // Surrey FlowWorks (no public link)
          sourceBadge = '<br><span style="font-size: 0.75em; color: #006837;">🏛️ Surrey</span>';
        } else if (id === 'whiterock_east') {
          // White Rock City
          sourceBadge = `<br><a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="font-size: 0.75em; color: #0066cc; text-decoration: none;">🏛️ White Rock 🔗</a>`;
        } else if (id === 'JERICHO') {
          // Jericho Sailing Centre
          sourceBadge = `<br><a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="font-size: 0.75em; color: #2563eb; text-decoration: none;">⛵ JSCA 🔗</a>`;
        } else if (id.startsWith('C')) {
          // Environment Canada weather stations
          sourceBadge = `<br><a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="font-size: 0.75em; color: #006400; text-decoration: none;">🇨🇦 Env Canada 🔗</a>`;
        }
      }

      tableHTML += `
        <tr ${rowClass}
            data-name="${station.name}"
            data-wind_speed_kt="${station.wind_speed_kt || ''}"
            data-wind_gust_kt="${station.wind_gust_kt || ''}"
            data-wind_direction="${station.wind_direction || ''}"
            data-air_temp_c="${station.air_temp_c || ''}"
            data-pressure_hpa="${station.pressure_hpa || ''}"
            data-observation_time="${station.observation_time}">
          <td><strong>${station.name}</strong>${sourceBadge}</td>
          <td>${direction}</td>
          <td>${windSpeed}</td>
          <td>${windGust}</td>
          <td>${temp}</td>
          <td>${pressure}</td>
          <td>${updated}</td>
          <td style="white-space: nowrap;">
            <a href="#map-section" onclick="showStationOnMap('${id}'); return false;" style="color: #0077be; text-decoration: none; cursor: pointer; margin-right: 0.5rem;" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">Map</a>
            <span style="color: #ccc;">/</span>
            <a href="#wind-chart-section" onclick="viewStationChart('${id}'); return false;" style="color: #0077be; text-decoration: none; cursor: pointer; margin-left: 0.5rem;" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">Chart</a>
          </td>
        </tr>
      `;
    });

    tableHTML += '</tbody>';
    table.innerHTML = tableHTML;

    // Add sort functionality
    initializeSortableTable();

    // Default sort by wind speed (descending) to show strongest winds first
    const speedHeader = document.querySelector('#wind-conditions-table th[data-column="wind_speed_kt"]');
    if (speedHeader) {
      currentSort.column = 'wind_speed_kt';
      currentSort.ascending = false; // Descending to show highest first
      sortTable('wind_speed_kt', 'number', false);
      updateSortIndicators(speedHeader);
    }

    // Display offline stations list (data > 4 hours old)
    const offlineListContainer = document.getElementById('offline-stations-list');
    if (offlineListContainer && offlineStations.length > 0) {
      // Sort offline stations by name
      offlineStations.sort((a, b) => a[1].name.localeCompare(b[1].name));

      let offlineHTML = '<div style="margin-top: 1rem; padding: 1rem; background: #fff9e6; border-left: 3px solid #f59e0b; border-radius: 4px;">';
      offlineHTML += '<h3 style="margin: 0 0 0.5rem 0; font-size: 1rem; color: #92400e;">Stations with Stale Data (>4 hours)</h3>';
      offlineHTML += '<p style="margin: 0 0 0.75rem 0; font-size: 0.9rem; color: #78350f;">The following stations have not reported wind data in over 4 hours:</p>';

      // Use single column on mobile, 2 columns on desktop
      const isMobile = window.innerWidth < 768;
      const columnStyle = isMobile ? '' : 'columns: 2; column-gap: 2rem;';
      offlineHTML += `<ul style="margin: 0; padding-left: 1.5rem; ${columnStyle}">`;

      offlineStations.forEach(([id, station]) => {
        const hours = Math.floor(station.ageHours);
        const minutes = Math.round((station.ageHours - hours) * 60);
        let ageText = '';
        if (hours > 0) {
          ageText = `${hours}h`;
          if (minutes > 0) ageText += ` ${minutes}m`;
        } else {
          ageText = `${minutes}m`;
        }

        // Add source link if available
        let stationLink = station.name.replace(' 💨', '').replace(' 🌊', '');
        if (sourceLinks[id]) {
          stationLink = `<a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="color: #0066cc; text-decoration: none;">${stationLink}</a>`;
        }

        offlineHTML += `<li style="margin-bottom: 0.25rem; break-inside: avoid;"><strong>${stationLink}</strong> (${ageText} ago)</li>`;
      });

      offlineHTML += '</ul></div>';
      offlineListContainer.innerHTML = offlineHTML;
    } else if (offlineListContainer) {
      offlineListContainer.innerHTML = '';
    }

    // Update footer timestamp (use wind data timestamp)
    const timestamp = document.getElementById("timestamp");
    if (timestamp && windData._meta) {
      timestamp.textContent = `Updated: ${formatTimestamp(windData._meta.generated_utc)}`;
    }

  } catch (error) {
    console.error('Error loading wind table:', error);
    const table = document.getElementById("wind-conditions-table");
    if (table) {
      table.innerHTML = '<tbody><tr><td colspan="7" style="text-align: center; color: #e53935; padding: 2rem;">Error loading wind data</td></tr></tbody>';
    }
  }
}

/**
 * Populate station dropdown (always shows all stations)
 */
function populateStationDropdown() {
  const select = document.getElementById("wind-station-select");
  if (!select || !allStationsList) return;

  select.innerHTML = '';

  // Populate dropdown with all stations
  allStationsList.forEach(([id, station]) => {
    const option = document.createElement('option');
    option.value = id;
    option.textContent = station.name;
    select.appendChild(option);
  });
}

/**
 * Load wind timeseries data and populate station selector
 */
async function loadWindTimeseries() {
  try {
    // Load both wind station and buoy timeseries data
    const [windData, buoyData] = await Promise.all([
      fetchWithTimeout(`/data/wind_timeseries_48hr.json?t=${Date.now()}`),
      fetchWithTimeout(`/data/buoy_timeseries_48h.json?t=${Date.now()}`)
    ]);

    // Merge wind and buoy data
    windTimeseriesData = { ...windData };

    // Add buoys that have wind data
    Object.entries(buoyData)
      .filter(([key]) => key !== '_meta')
      .forEach(([id, buoy]) => {
        // Check if buoy has wind speed data
        if (buoy.timeseries && buoy.timeseries.wind_speed &&
            buoy.timeseries.wind_speed.data && buoy.timeseries.wind_speed.data.length > 0) {
          windTimeseriesData[id] = {
            name: buoy.name + ' 🌊',
            timeseries: buoy.timeseries,
            isBuoy: true // Flag to handle different data structure
          };
        }
      });

    const select = document.getElementById("wind-station-select");
    const searchInput = document.getElementById("wind-station-search");
    if (!select) return;

    // Get all stations (exclude _meta)
    allStationsList = Object.entries(windTimeseriesData)
      .filter(([key]) => key !== '_meta')
      .sort((a, b) => a[1].name.localeCompare(b[1].name));

    // Populate dropdown with all stations
    populateStationDropdown();

    // Set default selection (first station)
    if (allStationsList.length > 0) {
      select.value = allStationsList[0][0];
      renderWindChart(allStationsList[0][0]);
      renderWind24HourTable(allStationsList[0][0]);
    }

    // Add change listener to dropdown
    select.addEventListener('change', (e) => {
      renderWindChart(e.target.value);
      renderWind24HourTable(e.target.value);
    });

    // Add "jump to" search listener
    if (searchInput) {
      searchInput.addEventListener('input', (e) => {
        const searchText = e.target.value.toLowerCase();
        if (!searchText) return;

        // Find first matching station (by name or ID)
        const match = allStationsList.find(([id, station]) =>
          station.name.toLowerCase().includes(searchText) ||
          id.toLowerCase().includes(searchText)
        );

        if (match) {
          // Select the matching station in dropdown
          select.value = match[0];
          // Trigger chart and table update
          renderWindChart(match[0]);
          renderWind24HourTable(match[0]);
        }
      });
    }

  } catch (error) {
    console.error('Error loading wind timeseries:', error);
    const select = document.getElementById("wind-station-select");
    if (select) {
      select.innerHTML = '<option value="">Error loading stations</option>';
    }
  }
}

/**
 * Create wind direction arrow data for scatter series
 */
function createWindDirectionArrows(windDirectionTimes, windSpeedData, windGustData) {
  if (!windDirectionTimes || windDirectionTimes.length === 0) return { arrowData: [], maxValue: null };

  // Find maximum wind speed/gust to position arrows at top
  const allSpeeds = [...windSpeedData, ...windGustData]
    .filter(v => v != null && !isNaN(v));

  const maxSpeed = allSpeeds.length > 0 ? Math.max(...allSpeeds) : 20;
  const arrowYPosition = maxSpeed * 1.05; // Position arrows 5% above max value

  const arrowData = [];

  // Responsive sampling based on data density and screen size
  // For 24h of 10-min data (144 points), sample intelligently:
  // Mobile (< 600px): every 6 hours (~36 points → 4 arrows)
  // Desktop: every 3 hours (~18 points → 8 arrows)
  const isMobile = window.innerWidth < 600;
  const hoursInterval = isMobile ? 6 : 3;

  // Calculate approximate points per hour (assuming roughly uniform spacing)
  const dataSpanHours = windDirectionTimes.length > 1
    ? (new Date(windDirectionTimes[windDirectionTimes.length - 1].time) - new Date(windDirectionTimes[0].time)) / (1000 * 60 * 60)
    : 24;
  const pointsPerHour = windDirectionTimes.length / dataSpanHours;
  const sampleInterval = Math.max(1, Math.round(hoursInterval * pointsPerHour));

  for (let i = 0; i < windDirectionTimes.length; i += sampleInterval) {
    const dirPoint = windDirectionTimes[i];
    if (!dirPoint || dirPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value; // Meteorological direction (coming FROM)

    // Arrow SVG points DOWN by default
    // Wind direction indicates where wind comes FROM, arrow shows where it's blowing TO
    // ECharts rotates counter-clockwise, so negate to get clockwise rotation
    // 0° North wind → -0° = arrow points down, 90° East → -90° = arrow points left
    arrowData.push({
      value: [timestamp, arrowYPosition],
      symbolRotate: -direction,
      itemStyle: {
        color: '#004b7c',
        opacity: 0.7
      }
    });
  }

  return { arrowData, maxValue: arrowYPosition };
}

/**
 * View chart for a specific station (from table link)
 */
function viewStationChart(stationId) {
  const select = document.getElementById('wind-station-select');
  if (!select) return;

  // Select the station in dropdown
  select.value = stationId;

  // Render the chart and table
  renderWindChart(stationId);
  renderWind24HourTable(stationId);

  // Scroll to chart section
  const chartSection = document.getElementById('wind-chart-section');
  if (chartSection) {
    chartSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

/**
 * Show station on map (from table link)
 */
function showStationOnMap(stationId) {
  // Scroll to map section
  const mapSection = document.getElementById('map-section');
  if (mapSection) {
    mapSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  // Use the winds-map module to center on the station
  // The map is initialized in winds-map.js
  setTimeout(() => {
    if (window.windsMap && window.windsMap.focusStation) {
      window.windsMap.focusStation(stationId);
    }
  }, 500); // Small delay to allow smooth scroll to complete
}

// Make functions globally accessible
window.viewStationChart = viewStationChart;
window.showStationOnMap = showStationOnMap;

/**
 * Render wind data table for selected station (24hr or 48hr based on currentWindTimeRange)
 */
function renderWind24HourTable(stationId) {
  if (!windTimeseriesData || !stationId) return;

  // Filter data based on current time range
  const filteredData = filterWindTimeseriesData(windTimeseriesData, currentWindTimeRange);
  const station = filteredData[stationId];
  if (!station) return;

  const table = document.getElementById('wind-24hr-table');
  if (!table) return;

  // Extract timeseries data
  const timeseries = station.timeseries;
  const isBuoy = station.isBuoy;

  // Get data arrays
  const windSpeedArray = isBuoy && timeseries.wind_speed?.data ? timeseries.wind_speed.data : (timeseries.wind_speed || []);
  const windGustArray = isBuoy && timeseries.wind_gust?.data ? timeseries.wind_gust.data : (timeseries.wind_gust || []);
  const windDirArray = isBuoy && timeseries.wind_direction?.data ? timeseries.wind_direction.data : (timeseries.wind_direction || []);
  const airTempArray = isBuoy && timeseries.air_temp?.data ? timeseries.air_temp.data : (timeseries.air_temp || []);
  const pressureArray = isBuoy && timeseries.pressure?.data ? timeseries.pressure.data : (timeseries.pressure || []);

  // Create a merged dataset by time
  const dataByTime = new Map();

  // Add wind speeds
  windSpeedArray.forEach(point => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).speed = point.value;
  });

  // Add wind gusts
  windGustArray.forEach(point => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).gust = point.value;
  });

  // Add wind directions
  windDirArray.forEach(point => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).direction = point.value;
  });

  // Add air temperature
  airTempArray.forEach(point => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).temp = point.value;
  });

  // Add pressure
  pressureArray.forEach(point => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).pressure = point.value;
  });

  // Sort by time (newest first)
  const sortedTimes = Array.from(dataByTime.keys()).sort((a, b) => new Date(b) - new Date(a));

  // Downsample to hourly data (only show :00 minutes to keep table manageable)
  const hourlyTimes = sortedTimes.filter(time => {
    const date = new Date(time);
    return date.getMinutes() === 0;
  });

  // Build table HTML
  let tableHTML = `
    <thead>
      <tr>
        <th>Time</th>
        <th>Direction</th>
        <th>Wind Speed (kt)</th>
        <th>Gust (kt)</th>
        <th class="hide-mobile">Temp (°C)</th>
        <th class="hide-mobile">Pressure (hPa)</th>
      </tr>
    </thead>
    <tbody>
  `;

  if (hourlyTimes.length === 0) {
    tableHTML += '<tr><td colspan="6" style="text-align: center; padding: 2rem;">No data available</td></tr>';
  } else {
    const DEFAULT_VISIBLE_ROWS = 12;

    hourlyTimes.forEach((time, index) => {
      const data = dataByTime.get(time);
      const formattedTime = formatTimestamp(time);
      const speed = data.speed != null ? Math.round(data.speed) : '—';
      const gust = data.gust != null ? Math.round(data.gust) : '—';
      const temp = data.temp != null ? data.temp.toFixed(1) : '—';
      const pressure = data.pressure != null ? data.pressure.toFixed(1) : '—';

      let direction = '—';
      if (data.direction != null) {
        const cardinal = degreesToCardinal(data.direction);
        const arrow = getDirectionalArrow(data.direction);
        direction = `${cardinal} (${Math.round(data.direction)}°) ${arrow}`;
      }

      // Add 'collapsed-row' class to rows beyond the default visible count
      const rowClass = index >= DEFAULT_VISIBLE_ROWS ? ' class="collapsed-row"' : '';

      tableHTML += `
        <tr${rowClass}>
          <td>${formattedTime}</td>
          <td>${direction}</td>
          <td>${speed}</td>
          <td>${gust}</td>
          <td class="hide-mobile">${temp}</td>
          <td class="hide-mobile">${pressure}</td>
        </tr>
      `;
    });

    // Add expand/collapse toggle button if there are more rows than the default
    if (hourlyTimes.length > DEFAULT_VISIBLE_ROWS) {
      tableHTML += `
        <tr id="toggle-row-wind-24hr">
          <td colspan="6" style="text-align: center; padding: 1rem; background: #f5f9fc; cursor: pointer; border-bottom: none;">
            <button onclick="toggleWind24hrRows()" style="padding: 0.5rem 1.5rem; background: #0077be; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.95rem; font-weight: 600; transition: background 0.2s;" onmouseover="this.style.background='#005a94'" onmouseout="this.style.background='#0077be'">
              ▼ Show More Rows
            </button>
          </td>
        </tr>
      `;
    }
  }

  tableHTML += '</tbody>';
  table.innerHTML = tableHTML;
}

/**
 * Set time range for wind charts and update display
 */
function setWindTimeRange(hours) {
  currentWindTimeRange = hours;

  // Update ALL button states (sync all toggle buttons on page)
  document.querySelectorAll('.wind-time-range-btn').forEach(btn => {
    if (parseInt(btn.dataset.windHours) === hours) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });

  // Update section headers to show current time range
  updateWindTimeRangeLabels();

  // Re-render current station chart and table
  const selectedStation = document.getElementById('wind-station-select')?.value;
  if (selectedStation) {
    renderWindChart(selectedStation);
    renderWind24HourTable(selectedStation);
  }
}

/**
 * Update all wind time range labels on the page
 */
function updateWindTimeRangeLabels() {
  // Update section headers
  const chartSectionH2 = document.querySelector('#wind-chart-section h2');
  if (chartSectionH2) {
    chartSectionH2.textContent = `${currentWindTimeRange}-Hour Wind Trends`;
  }

  const tableSectionH2 = document.querySelector('#wind-data-table-section h2');
  if (tableSectionH2) {
    tableSectionH2.textContent = `${currentWindTimeRange}-Hour Wind Data`;
  }
}

/**
 * Render wind chart for selected station
 */
function renderWindChart(stationId) {
  if (!windTimeseriesData || !stationId) return;

  // Filter data based on current time range
  const filteredData = filterWindTimeseriesData(windTimeseriesData, currentWindTimeRange);
  const station = filteredData[stationId];
  if (!station) return;

  const chartContainer = document.getElementById('wind-trend-chart');
  if (!chartContainer) return;

  // Initialize chart if needed
  if (!windChart) {
    windChart = echarts.init(chartContainer);
  }

  // Extract timeseries data (handle both wind station and buoy formats)
  const timeseries = station.timeseries;
  const isBuoy = station.isBuoy;

  // Buoys have {data: [...], name, unit} structure, wind stations have simple arrays
  const windSpeedArray = isBuoy && timeseries.wind_speed?.data ? timeseries.wind_speed.data : (timeseries.wind_speed || []);
  const windGustArray = isBuoy && timeseries.wind_gust?.data ? timeseries.wind_gust.data : (timeseries.wind_gust || []);
  const windDirArray = isBuoy && timeseries.wind_direction?.data ? timeseries.wind_direction.data : (timeseries.wind_direction || []);

  const windSpeedData = windSpeedArray.map(p => ({ time: p.time, value: p.value }));
  const windGustData = windGustArray.map(p => ({ time: p.time, value: p.value }));
  const windDirTimes = windDirArray;

  // Create direction arrow data
  const { arrowData, maxValue } = createWindDirectionArrows(
    windDirTimes,
    windSpeedData.map(d => d.value),
    windGustData.map(d => d.value)
  );

  // Calculate y-axis max to ensure arrows are visible at top
  const yAxisMax = maxValue ? Math.ceil(maxValue * 1.1) : null;

  // Build legend data
  const legendData = ["Wind Speed", "Wind Gust"];
  if (arrowData.length > 0) {
    legendData.push("Wind Direction");
  }

  // Chart configuration
  const option = {
    backgroundColor: '#ffffff',
    title: {
      text: `${station.name.replace(' 💨', '').replace(' 🌊', '')} - Wind Conditions`,
      left: 'center',
      textStyle: {
        fontSize: 18,
        fontWeight: 600,
        color: '#004b7c'
      }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = new Date(params[0].value[0]).toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver"
        }).replace(',', '');
        let res = `<b>${time}</b><br/>`;

        params.forEach((p) => {
          if (p.seriesName === "Wind Direction") return; // Skip arrow series
          if (p.value && p.value[1] != null) {
            res += `${p.marker} ${p.seriesName}: ${Math.round(p.value[1])} kt<br/>`;
          }
        });

        // Add wind direction to tooltip if available
        const timestamp = new Date(params[0].value[0]).getTime();
        const dirPoint = windDirTimes.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000);
        if (dirPoint && dirPoint.value != null) {
          const dir = Math.round(dirPoint.value);
          const compass = degreesToCardinal(dir);
          res += `🧭 Direction: ${dir}° (${compass})<br/>`;
        }

        return res;
      }
    },
    legend: {
      data: legendData,
      top: 35
    },
    grid: {
      left: '8%',
      right: '5%',
      bottom: '15%',
      top: '20%',
      containLabel: true
    },
    xAxis: {
      type: 'time',
      axisLabel: {
        rotate: 45,
        formatter: (value) => {
          const date = new Date(value);
          return date.toLocaleString("en-US", {
            month: "numeric",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
            timeZone: "America/Vancouver"
          }).replace(',', '');
        }
      }
    },
    yAxis: {
      type: 'value',
      name: 'Speed (kt)',
      max: yAxisMax
    },
    series: [
      {
        name: 'Wind Speed',
        type: 'line',
        data: windSpeedData.map(d => [new Date(d.time).getTime(), d.value]),
        smooth: true,
        lineStyle: {
          width: 2,
          color: '#fb8c00'
        },
        itemStyle: {
          color: '#fb8c00'
        },
        areaStyle: {
          opacity: 0.1
        }
      },
      {
        name: 'Wind Gust',
        type: 'scatter',
        data: windGustData.map(d => [new Date(d.time).getTime(), d.value]),
        symbol: 'circle',
        symbolSize: 6,
        itemStyle: {
          color: '#e53935'
        }
      },
      {
        name: 'Wind Direction',
        type: 'scatter',
        data: arrowData,
        symbol: 'path://M0,10 L-4,-10 L0,-8 L4,-10 Z', // Centered arrow pointing DOWN
        symbolSize: 16,
        symbolRotate: function(params) {
          return arrowData[params.dataIndex]?.symbolRotate || 0;
        },
        itemStyle: {
          color: function(params) {
            return arrowData[params.dataIndex]?.itemStyle?.color || '#004b7c';
          },
          opacity: function(params) {
            return arrowData[params.dataIndex]?.itemStyle?.opacity || 0.7;
          }
        },
        silent: true,
        z: 2
      }
    ]
  };

  windChart.setOption(option);
}

/**
 * Select a station in the dropdown and display its chart
 * Called from map popups and URL hash navigation
 */
function selectStationAndShowChart(stationId) {
  const select = document.getElementById('wind-station-select');
  const chartSection = document.getElementById('wind-chart-section');

  if (!select || !chartSection) {
    console.warn('Station selector or chart section not found');
    return;
  }

  // Wait for timeseries data to load if needed
  const attemptSelection = (retryCount = 0) => {
    if (!windTimeseriesData || Object.keys(windTimeseriesData).length === 0) {
      if (retryCount < 10) {
        setTimeout(() => attemptSelection(retryCount + 1), 300);
        return;
      }
      console.warn('Wind timeseries data not loaded');
      return;
    }

    // Check if station exists
    if (!windTimeseriesData[stationId]) {
      console.warn(`Station ${stationId} not found in timeseries data`);
      return;
    }

    // Select the station
    select.value = stationId;

    // Render the chart and table
    renderWindChart(stationId);
    renderWind24HourTable(stationId);

    // Scroll to chart section
    setTimeout(() => {
      chartSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 100);
  };

  attemptSelection();
}

// Make function globally accessible
window.selectStationAndShowChart = selectStationAndShowChart;

/**
 * Check URL hash for station to display
 * Called on page load to handle deep links from map popups
 */
/**
 * Toggle visibility of extra rows in wind 24hr table
 */
function toggleWind24hrRows() {
  const table = document.getElementById('wind-24hr-table');
  if (!table) return;

  const collapsedRows = table.querySelectorAll('.collapsed-row');
  const toggleButton = table.querySelector('#toggle-row-wind-24hr button');

  if (!toggleButton || collapsedRows.length === 0) return;

  // Check actual computed display value (CSS or inline)
  const firstRowStyle = window.getComputedStyle(collapsedRows[0]);
  const isCurrentlyHidden = firstRowStyle.display === 'none';

  // Toggle display for all collapsed rows
  collapsedRows.forEach(row => {
    row.style.display = isCurrentlyHidden ? 'table-row' : 'none';
  });

  // Update button text
  if (isCurrentlyHidden) {
    toggleButton.innerHTML = '▲ Show Less Rows';
  } else {
    toggleButton.innerHTML = '▼ Show More Rows';
  }
}

function checkHashForWindStation() {
  const hash = window.location.hash;

  if (hash.startsWith('#wind-')) {
    const stationId = hash.substring(6); // Remove '#wind-'
    // Short delay to ensure data starts loading, then retry logic in selectStationAndShowChart kicks in
    setTimeout(() => {
      selectStationAndShowChart(stationId);
    }, 500);
  }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  loadWindTable();
  loadWindTimeseries();
  updateWindTimeRangeLabels(); // Set initial labels to 24-Hour

  // Check for wind station in URL hash
  checkHashForWindStation();

  // Handle window resize
  window.addEventListener('resize', () => {
    if (windChart) {
      windChart.resize();
    }
  });
});
