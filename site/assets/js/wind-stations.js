/* -----------------------------
   Wind Stations Module
   Displays current wind conditions table and 24hr trends chart
   ----------------------------- */

function setSafeHTML(element, html) {
  if (!element) return;
  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

// Helper: Convert degrees to cardinal direction (also used by winds-map.js via global scope)
function degreesToCardinal(degrees) {
  if (degrees == null) return null;
  const directions = [
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
  ];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}

// Helper: Get directional arrow (SVG, rotated to exact degrees)
// Based on buoy page implementation - provides infinite precision
function getDirectionalArrow(degrees, arrowType = "wind") {
  if (degrees == null || degrees === "—") return "";

  // Meteorological convention: direction indicates WHERE wind is COMING FROM
  // Arrow rotation: wind arrow points down by default, rotates to show direction wind is blowing TO
  const rotation = degrees; // Wind arrow points down, so rotate by degrees directly

  // SVG wind arrow pointing down
  const svg = `<svg width="16" height="16" viewBox="0 0 16 16"><path d="M8 2v12m0 0l-3-3m3 3l3-3" stroke="#004b7c" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`;

  return `<span style="display:inline-block;transform:rotate(${rotation}deg);margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

// Helper: Format timestamp to local time (time first, then date)
function formatTimestamp(isoString) {
  const date = new Date(isoString);
  const time = date.toLocaleString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
  const day = date.toLocaleString("en-US", {
    month: "numeric",
    day: "numeric",
    timeZone: "America/Vancouver",
  });
  return `${time} ${day}`;
}

// Helper: Format timestamp to time only (for mobile)
function formatTimeOnly(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
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

  Object.keys(data).forEach((stationId) => {
    if (stationId === "_meta") {
      filtered[stationId] = data[stationId];
      return;
    }

    const station = data[stationId];
    filtered[stationId] = {
      name: station.name,
      timeseries: {},
    };

    // All timeseries are now normalized to flat arrays by wind-data.js
    Object.keys(station.timeseries || {}).forEach((metricKey) => {
      const metric = station.timeseries[metricKey];
      if (Array.isArray(metric)) {
        filtered[stationId].timeseries[metricKey] = metric.filter(
          (point) => new Date(point.time) >= cutoff,
        );
      }
    });
  });

  return filtered;
}

/**
 * Initialize sortable table functionality
 */
function initializeSortableTable() {
  const headers = document.querySelectorAll("#wind-conditions-table th.sortable");

  headers.forEach((header) => {
    header.style.cursor = "pointer";
    header.style.userSelect = "none";

    header.addEventListener("click", () => {
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
  const table = document.getElementById("wind-conditions-table");
  const tbody = table.querySelector("tbody");
  const rows = Array.from(tbody.querySelectorAll("tr"));

  rows.sort((a, b) => {
    let aVal = a.dataset[column];
    let bVal = b.dataset[column];

    // Handle empty values
    if (!aVal && !bVal) return 0;
    if (!aVal) return 1;
    if (!bVal) return -1;

    // Compare based on type
    let comparison = 0;
    if (type === "number") {
      comparison = parseFloat(aVal) - parseFloat(bVal);
    } else if (type === "date") {
      comparison = new Date(aVal) - new Date(bVal);
    } else {
      // String comparison
      comparison = aVal.toLowerCase().localeCompare(bVal.toLowerCase());
    }

    return ascending ? comparison : -comparison;
  });

  // Re-append rows in sorted order
  rows.forEach((row) => tbody.appendChild(row));
}

/**
 * Update sort direction indicators
 */
function updateSortIndicators(activeHeader) {
  // Clear all indicators and remove sorting class
  document.querySelectorAll("#wind-conditions-table th.sortable").forEach((header) => {
    header.classList.remove("sorting");
    const indicator = header.querySelector(".sort-indicator");
    indicator.textContent = "";
  });

  // Set active indicator
  activeHeader.classList.add("sorting");
  const indicator = activeHeader.querySelector(".sort-indicator");
  indicator.textContent = currentSort.ascending ? "▲" : "▼";
}

/**
 * Load and display current wind conditions table
 */
async function loadWindTable() {
  try {
    // Wait for shared data store (fetched once by wind-data.js)
    await window.windData.ready;
    const latestAll = window.windData.latestAll;

    const table = document.getElementById("wind-conditions-table");
    if (!table) return;

    // Classify stations into active vs offline by data age
    const allStations = [];
    const offlineStations = [];

    Object.entries(latestAll)
      .filter(([key]) => key !== "_meta")
      .forEach(([id, station]) => {
        const obsTime = station.observation_time ? new Date(station.observation_time) : null;
        const ageHours = obsTime ? (Date.now() - obsTime.getTime()) / (1000 * 60 * 60) : Infinity;

        const displayName = station._isWindType ? station.name : station.name + " \u{1F30A}";

        const stationData = {
          name: displayName,
          wind_speed_kt: station.wind_speed_kt != null ? Math.round(station.wind_speed_kt) : null,
          wind_gust_kt: station.wind_gust_kt != null ? Math.round(station.wind_gust_kt) : null,
          wind_direction: station.wind_direction_deg,
          wind_direction_cardinal: station.wind_direction_cardinal,
          air_temp_c: station.air_temp_c,
          pressure_hpa: station.pressure_hpa,
          observation_time: station.observation_time,
          ageHours: ageHours,
          stale: ageHours >= 2 && ageHours < 4,
          type: station._sourceType,
        };

        if (ageHours >= 4) {
          offlineStations.push([id, stationData]);
        } else {
          allStations.push([id, stationData]);
        }
      });

    // Sort all stations by name
    const stations = allStations;
    stations.sort((a, b) => a[1].name.localeCompare(b[1].name));

    // Station-specific source links (used in table and offline list)
    const sourceLinks = {
      // Environment Canada Marine Stations
      CWAS: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06300&stationID=WAS",
      CWGT: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06100&stationID=WGT",
      CWGB: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=WGB",
      CWEL: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06200&stationID=WEL",
      CWSB: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06300&stationID=WSB",
      CVTF: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06500&stationID=VTF",
      CWVF: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06600&stationID=WVF",
      CWEZ: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06700&stationID=WEZ",
      CWQK: "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06800&stationID=WQK",
      // Environment Canada Airports
      CYVR: "https://spaces.navcanada.ca/workspace/aeroview/CYVR",
      CZBB: "https://spaces.navcanada.ca/workspace/aeroview/CZBB",
      // Environment Canada Buoys
      4600146:
        "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46146",
      4600304:
        "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46304",
      4600303:
        "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46303",
      4600131:
        "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46131",
      4600206:
        "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46206",
      // NOAA Buoys and Stations
      46087: "https://www.ndbc.noaa.gov/station_page.php?station=46087",
      46088: "https://www.ndbc.noaa.gov/station_page.php?station=46088",
      46267: "https://www.ndbc.noaa.gov/station_page.php?station=46267",
      CPMW1: "https://www.ndbc.noaa.gov/station_page.php?station=cpmw1",
      SISW1: "https://www.ndbc.noaa.gov/station_page.php?station=sisw1",
      // NOAA NWS Airports
      KBLI: "https://www.weather.gov/wrh/timeseries?site=KBLI",
      KORS: "https://www.weather.gov/wrh/timeseries?site=KORS",
      // Municipal/Other
      whiterock_east: "https://www.whiterockcity.ca/1000/Weather-Station",
      JERICHO: "https://jsca.bc.ca/services/weather/",
    };

    // Short names for mobile display
    const shortNames = {
      "Bellingham International Airport": "Bellingham",
      "Southern Georgia Strait": "S. Georgia",
      "Colebrook Pump House": "Colebrook",
      "Jericho Sailing Centre": "Jericho",
      "Crescent Channel Ocean": "Crescent Ch.",
      "White Rock East Beach": "White Rock",
      "Orcas Island Airport": "Orcas Island",
      "Boundary Bay Airport": "Boundary Bay",
      "Crescent Beach Ocean": "Crescent Bch.",
      "Entrance Island": "Entrance Is.",
      "La Perouse Bank": "La Perouse",
      "Point Atkinson": "Pt. Atkinson",
      "Sisters Islets": "Sisters Is.",
      "YVR Airport": "YVR",
      Tsawwassen: "Tsawwassen",
    };

    let tableHTML = `
      <thead>
        <tr>
          <th class="sortable" data-column="name" data-type="string">Station <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="observation_time" data-type="date">Updated <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="wind_direction" data-type="number"><span class="hide-mobile">Direction</span><span class="show-mobile">Dir</span> <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="wind_speed_kt" data-type="number"><span class="hide-mobile">Speed (kt)</span><span class="show-mobile">kt</span> <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="wind_gust_kt" data-type="number"><span class="hide-mobile">Gust (kt)</span><span class="show-mobile">Gst</span> <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="air_temp_c" data-type="number">Temp (°C) <span class="sort-indicator"></span></th>
          <th class="sortable" data-column="pressure_hpa" data-type="number">Pressure (hPa) <span class="sort-indicator"></span></th>
          <th>View:</th>
        </tr>
      </thead>
      <tbody>
    `;

    stations.forEach(([id, station]) => {
      const rowClass = station.stale ? 'class="stale"' : "";
      // Round wind speeds to integers
      const windSpeed = station.wind_speed_kt != null ? Math.round(station.wind_speed_kt) : "—";
      const windGust = station.wind_gust_kt != null ? Math.round(station.wind_gust_kt) : "—";
      // Show arrow + cardinal direction (degrees in tooltip)
      const cardinal =
        station.wind_direction != null
          ? station.wind_direction_cardinal || degreesToCardinal(station.wind_direction)
          : null;
      const direction =
        cardinal != null
          ? `${getDirectionalArrow(station.wind_direction)} ${cardinal}<span class="hide-mobile"> (${station.wind_direction}°)</span>`
          : "—";
      const temp = station.air_temp_c != null ? station.air_temp_c.toFixed(1) : "—";
      const pressure = station.pressure_hpa != null ? station.pressure_hpa.toFixed(1) : "—";
      const updated = formatTimestamp(station.observation_time);

      // Determine source link and flag
      const sourceLink = sourceLinks[id] || null;
      let flag = "";
      if (id.startsWith("4600") || (id.startsWith("C") && id !== "CPMW1" && id !== "COLEB")) {
        flag = "🇨🇦";
      } else if (id.startsWith("46") || id === "CPMW1" || id === "SISW1" || id.startsWith("K")) {
        flag = "🇺🇸";
      } else if (id === "CRPILE" || id === "CRCHAN" || id === "COLEB") {
        flag = "🏛️";
      } else if (id === "whiterock_east") {
        flag = "🏛️";
      } else if (id === "JERICHO") {
        flag = "⛵";
      }
      const flagSpan = flag
        ? ` <span class="hide-mobile" style="font-size: 0.8em;">${flag}</span>`
        : "";

      const baseName = station.name.replace(" 🌊", "");
      const mobileName = shortNames[baseName] || station.name;

      tableHTML += `
        <tr ${rowClass}
            data-name="${station.name}"
            data-wind_speed_kt="${station.wind_speed_kt || ""}"
            data-wind_gust_kt="${station.wind_gust_kt || ""}"
            data-wind_direction="${station.wind_direction || ""}"
            data-air_temp_c="${station.air_temp_c || ""}"
            data-pressure_hpa="${station.pressure_hpa || ""}"
            data-observation_time="${station.observation_time}">
          <td>${sourceLink ? `<a href="${sourceLink}" target="_blank" rel="noopener" style="color: inherit; text-decoration: none;"><strong><span class="hide-mobile">${station.name}</span><span class="show-mobile">${mobileName}</span></strong>${flagSpan}</a>` : `<strong><span class="hide-mobile">${station.name}</span><span class="show-mobile">${mobileName}</span></strong>${flagSpan}`}</td>
          <td style="white-space: nowrap;"><span class="hide-mobile">${updated}</span><span class="show-mobile">${formatTimeOnly(station.observation_time)}</span></td>
          <td style="white-space: nowrap;">${direction}</td>
          <td>${windSpeed}</td>
          <td>${windGust}</td>
          <td>${temp}</td>
          <td>${pressure}</td>
          <td style="white-space: nowrap;">
            <a href="#map-section" class="wind-table-action-link" data-action="map" data-station-id="${id}" style="color: var(--color-primary); text-decoration: none; cursor: pointer; margin-right: 0.5rem;">Map</a>
            <span style="color: var(--color-border);">/</span>
            <a href="#wind-chart-section" class="wind-table-action-link" data-action="chart" data-station-id="${id}" style="color: var(--color-primary); text-decoration: none; cursor: pointer; margin-left: 0.5rem;">Chart</a>
          </td>
        </tr>
      `;
    });

    tableHTML += "</tbody>";
    table.innerHTML = tableHTML;

    // Add sort functionality
    initializeSortableTable();

    // Default sort by wind speed (descending) to show strongest winds first
    const speedHeader = document.querySelector(
      '#wind-conditions-table th[data-column="wind_speed_kt"]',
    );
    if (speedHeader) {
      currentSort.column = "wind_speed_kt";
      currentSort.ascending = false; // Descending to show highest first
      sortTable("wind_speed_kt", "number", false);
      updateSortIndicators(speedHeader);
    }

    // Display offline stations list (data > 4 hours old)
    const offlineListContainer = document.getElementById("offline-stations-list");
    if (offlineListContainer && offlineStations.length > 0) {
      // Sort offline stations by name
      offlineStations.sort((a, b) => a[1].name.localeCompare(b[1].name));

      let offlineHTML =
        '<div style="margin-top: 1rem; padding: 1rem; background: var(--color-callout-warning-bg); border-left: 3px solid var(--color-status-warning); border-radius: 4px;">';
      offlineHTML +=
        '<h3 style="margin: 0 0 0.5rem 0; font-size: 1rem; color: var(--color-warning-text);">Stations with Stale Data (>4 hours)</h3>';
      offlineHTML +=
        '<p style="margin: 0 0 0.75rem 0; font-size: 0.9rem; color: var(--color-warning-text);">The following stations have not reported wind data in over 4 hours:</p>';

      // Use single column on mobile, 2 columns on desktop
      const isMobile = window.innerWidth < 768;
      const columnStyle = isMobile ? "" : "columns: 2; column-gap: 2rem;";
      offlineHTML += `<ul style="margin: 0; padding-left: 1.5rem; ${columnStyle}">`;

      offlineStations.forEach(([id, station]) => {
        const hours = Math.floor(station.ageHours);
        const minutes = Math.round((station.ageHours - hours) * 60);
        let ageText = "";
        if (hours > 0) {
          ageText = `${hours}h`;
          if (minutes > 0) ageText += ` ${minutes}m`;
        } else {
          ageText = `${minutes}m`;
        }

        // Add source link if available
        let stationLink = station.name.replace(" 🌊", "");
        if (sourceLinks[id]) {
          stationLink = `<a href="${sourceLinks[id]}" target="_blank" rel="noopener" style="color: var(--color-accent-blue); text-decoration: none;">${stationLink}</a>`;
        }

        offlineHTML += `<li style="margin-bottom: 0.25rem; break-inside: avoid;"><strong>${stationLink}</strong> (${ageText} ago)</li>`;
      });

      offlineHTML += "</ul></div>";
      setSafeHTML(offlineListContainer, offlineHTML);
    } else if (offlineListContainer) {
      offlineListContainer.innerHTML = "";
    }

    // Update footer timestamp (use wind data timestamp)
    const timestamp = document.getElementById("timestamp");
    if (timestamp && windData._meta) {
      timestamp.textContent = `Updated: ${formatTimestamp(windData._meta.generated_utc)}`;
    }
  } catch (error) {
    console.error("Error loading wind table:", error);
    const table = document.getElementById("wind-conditions-table");
    if (table) {
      table.innerHTML =
        '<tbody><tr><td colspan="7" style="text-align: center; color: var(--color-error-text); padding: 2rem;">Error loading wind data</td></tr></tbody>';
    }
  }
}

/**
 * Populate station dropdown (always shows all stations)
 */
function populateStationDropdown() {
  const select = document.getElementById("wind-station-select");
  if (!select || !allStationsList) return;

  select.innerHTML = "";

  // Populate dropdown with all stations
  allStationsList.forEach(([id, station]) => {
    const option = document.createElement("option");
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
    // Wait for shared data store (fetched and normalized by wind-data.js)
    await window.windData.ready;
    windTimeseriesData = window.windData.timeseries;

    const select = document.getElementById("wind-station-select");
    const searchInput = document.getElementById("wind-station-search");
    if (!select) return;

    // Get all stations (exclude _meta)
    allStationsList = Object.entries(windTimeseriesData)
      .filter(([key]) => key !== "_meta")
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
    select.addEventListener("change", (e) => {
      renderWindChart(e.target.value);
      renderWind24HourTable(e.target.value);
    });

    // Add "jump to" search listener
    if (searchInput) {
      searchInput.addEventListener("input", (e) => {
        const searchText = e.target.value.toLowerCase();
        if (!searchText) return;

        // Find first matching station (by name or ID)
        const match = allStationsList.find(
          ([id, station]) =>
            station.name.toLowerCase().includes(searchText) ||
            id.toLowerCase().includes(searchText),
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
    console.error("Error loading wind timeseries:", error);
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
  if (!windDirectionTimes || windDirectionTimes.length === 0)
    return { arrowData: [], maxValue: null };

  // Find maximum wind speed/gust to position arrows at top
  const allSpeeds = [...windSpeedData, ...windGustData].filter((v) => v != null && !isNaN(v));

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
  const dataSpanHours =
    windDirectionTimes.length > 1
      ? (new Date(windDirectionTimes[windDirectionTimes.length - 1].time) -
          new Date(windDirectionTimes[0].time)) /
        (1000 * 60 * 60)
      : 24;
  const pointsPerHour = windDirectionTimes.length / dataSpanHours;
  // For sparse data (< 8 points), show every point rather than risk showing 0-1 arrows
  const maxArrows = isMobile ? 4 : 8;
  const sampleInterval =
    windDirectionTimes.length <= maxArrows
      ? 1
      : Math.max(1, Math.round(hoursInterval * pointsPerHour));

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
        color: getChartThemeColors().marker,
        opacity: 0.7,
      },
    });
  }

  return { arrowData, maxValue: arrowYPosition };
}

/**
 * View chart for a specific station (from table link)
 */
function viewStationChart(stationId) {
  const select = document.getElementById("wind-station-select");
  if (!select) return;

  // Select the station in dropdown
  select.value = stationId;

  // Render the chart and table
  renderWindChart(stationId);
  renderWind24HourTable(stationId);

  // Scroll to chart section
  const chartSection = document.getElementById("wind-chart-section");
  if (chartSection) {
    chartSection.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

/**
 * Show station on map (from table link)
 */
function showStationOnMap(stationId) {
  // Scroll to map section
  const mapSection = document.getElementById("map-section");
  if (mapSection) {
    mapSection.scrollIntoView({ behavior: "smooth", block: "start" });
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

  const table = document.getElementById("wind-24hr-table");
  if (!table) return;

  // Update station name heading above the table
  const stationNameEl = document.getElementById("wind-24hr-station-name");
  if (stationNameEl) {
    stationNameEl.textContent = station.name.replace(" \u{1F30A}", "");
    stationNameEl.style.display = "block";
  }

  // Extract timeseries data (all normalized to flat arrays by wind-data.js)
  const timeseries = station.timeseries;
  const windSpeedArray = timeseries.wind_speed || [];
  const windGustArray = timeseries.wind_gust || [];
  const windDirArray = timeseries.wind_direction || [];
  const airTempArray = timeseries.air_temp || [];
  const pressureArray = timeseries.pressure || [];

  // Create a merged dataset by time
  const dataByTime = new Map();

  // Add wind speeds
  windSpeedArray.forEach((point) => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).speed = point.value;
  });

  // Add wind gusts
  windGustArray.forEach((point) => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).gust = point.value;
  });

  // Add wind directions
  windDirArray.forEach((point) => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).direction = point.value;
  });

  // Add air temperature
  airTempArray.forEach((point) => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).temp = point.value;
  });

  // Add pressure
  pressureArray.forEach((point) => {
    if (!dataByTime.has(point.time)) {
      dataByTime.set(point.time, {});
    }
    dataByTime.get(point.time).pressure = point.value;
  });

  // Sort by time (newest first)
  const sortedTimes = Array.from(dataByTime.keys()).sort((a, b) => new Date(b) - new Date(a));

  // Downsample to hourly data (only show :00 minutes to keep table manageable)
  const hourlyTimes = sortedTimes.filter((time) => {
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

  let shouldAddToggleRow = false;

  if (hourlyTimes.length === 0) {
    tableHTML +=
      '<tr><td colspan="6" style="text-align: center; padding: 2rem;">No data available</td></tr>';
  } else {
    const DEFAULT_VISIBLE_ROWS = 12;

    hourlyTimes.forEach((time, index) => {
      const data = dataByTime.get(time);
      const formattedTime = formatTimestamp(time);
      const mobileTime = formatTimeOnly(time);
      const speed = data.speed != null ? Math.round(data.speed) : "—";
      const gust = data.gust != null ? Math.round(data.gust) : "—";
      const temp = data.temp != null ? data.temp.toFixed(1) : "—";
      const pressure = data.pressure != null ? data.pressure.toFixed(1) : "—";

      let direction = "—";
      if (data.direction != null) {
        const cardinal = degreesToCardinal(data.direction);
        const arrow = getDirectionalArrow(data.direction);
        direction = `${cardinal} (${Math.round(data.direction)}°) ${arrow}`;
      }

      // Add 'collapsed-row' class to rows beyond the default visible count
      const rowClass = index >= DEFAULT_VISIBLE_ROWS ? ' class="collapsed-row"' : "";

      tableHTML += `
        <tr${rowClass}>
          <td style="white-space: nowrap;"><span class="hide-mobile">${formattedTime}</span><span class="show-mobile">${mobileTime}</span></td>
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
      shouldAddToggleRow = true;
    }
  }

  tableHTML += "</tbody>";
  table.innerHTML = tableHTML;

  if (shouldAddToggleRow) {
    const tbody = table.querySelector("tbody");
    if (tbody) {
      const toggleRow = document.createElement("tr");
      toggleRow.id = "toggle-row-wind-24hr";

      const toggleCell = document.createElement("td");
      toggleCell.colSpan = 6;
      toggleCell.style.textAlign = "center";
      toggleCell.style.padding = "1rem";
      toggleCell.style.background = "var(--color-surface-light)";
      toggleCell.style.cursor = "pointer";
      toggleCell.style.borderBottom = "none";

      const toggleButton = document.createElement("button");
      toggleButton.type = "button";
      toggleButton.className = "wind-24hr-toggle-btn";
      toggleButton.textContent = "▼ Show More Rows";
      toggleButton.addEventListener("click", toggleWind24hrRows);

      toggleCell.appendChild(toggleButton);
      toggleRow.appendChild(toggleCell);
      tbody.appendChild(toggleRow);
    }
  }
}

/**
 * Set time range for wind charts and update display
 */
function setWindTimeRange(hours) {
  currentWindTimeRange = hours;

  // Update ALL button states (sync all toggle buttons on page)
  document.querySelectorAll(".wind-time-range-btn").forEach((btn) => {
    if (parseInt(btn.dataset.windHours) === hours) {
      btn.classList.add("active");
    } else {
      btn.classList.remove("active");
    }
  });

  // Update section headers to show current time range
  updateWindTimeRangeLabels();

  // Re-render current station chart and table
  const selectedStation = document.getElementById("wind-station-select")?.value;
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
  const chartSectionH2 = document.querySelector("#wind-chart-section h2");
  if (chartSectionH2) {
    chartSectionH2.textContent = `${currentWindTimeRange}-Hour Wind Trends`;
  }

  const tableSectionH2 = document.querySelector("#wind-data-table-section h2");
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

  const chartContainer = document.getElementById("wind-trend-chart");
  if (!chartContainer) return;

  // Initialize chart if needed
  if (!windChart) {
    windChart = echarts.init(chartContainer);
  }

  // Extract timeseries data (all normalized to flat arrays by wind-data.js)
  const timeseries = station.timeseries;
  const windSpeedArray = timeseries.wind_speed || [];
  const windGustArray = timeseries.wind_gust || [];
  const windDirArray = timeseries.wind_direction || [];

  const windSpeedData = windSpeedArray.map((p) => ({ time: p.time, value: p.value }));
  const windGustData = windGustArray.map((p) => ({ time: p.time, value: p.value }));
  const windDirTimes = windDirArray;

  // Create direction arrow data
  const { arrowData, maxValue } = createWindDirectionArrows(
    windDirTimes,
    windSpeedData.map((d) => d.value),
    windGustData.map((d) => d.value),
  );

  // Calculate y-axis max to ensure arrows are visible at top
  const yAxisMax = maxValue ? Math.ceil(maxValue * 1.1) : null;

  // Build legend data
  const legendData = ["Wind Speed", "Wind Gust"];
  if (arrowData.length > 0) {
    legendData.push("Wind Direction");
  }

  // Get theme-aware colors
  const tc = getChartThemeColors();

  // Chart configuration
  const option = {
    backgroundColor: tc.background,
    textStyle: { color: tc.text },
    title: {
      text: `${station.name.replace(" 🌊", "")} - Wind Conditions`,
      left: "center",
      textStyle: {
        fontSize: 18,
        fontWeight: 600,
        color: tc.text,
      },
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = new Date(params[0].value[0])
          .toLocaleString("en-US", {
            month: "numeric",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
            timeZone: "America/Vancouver",
          })
          .replace(",", "");
        let res = `<b>${time}</b><br/>`;

        params.forEach((p) => {
          if (p.seriesName === "Wind Direction") return; // Skip arrow series
          if (p.value && p.value[1] != null) {
            res += `${p.marker} ${p.seriesName}: ${Math.round(p.value[1])} kt<br/>`;
          }
        });

        // Add wind direction to tooltip if available
        const timestamp = new Date(params[0].value[0]).getTime();
        const dirPoint = windDirTimes.find(
          (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
        );
        if (dirPoint && dirPoint.value != null) {
          const dir = Math.round(dirPoint.value);
          const compass = degreesToCardinal(dir);
          res += `🧭 Direction: ${dir}° (${compass})<br/>`;
        }

        return res;
      },
    },
    legend: {
      data: legendData,
      top: 35,
      textStyle: { color: tc.text },
    },
    grid: {
      left: "8%",
      right: "5%",
      bottom: "15%",
      top: "20%",
      containLabel: true,
    },
    xAxis: {
      type: "time",
      axisLabel: {
        rotate: 45,
        color: tc.mutedText,
        formatter: (value) => {
          const date = new Date(value);
          return date
            .toLocaleString("en-US", {
              month: "numeric",
              day: "numeric",
              hour: "2-digit",
              minute: "2-digit",
              hour12: false,
              timeZone: "America/Vancouver",
            })
            .replace(",", "");
        },
      },
      axisLine: { lineStyle: { color: tc.axisLine } },
      splitLine: { show: true, lineStyle: { color: tc.gridLine } },
    },
    yAxis: {
      type: "value",
      name: "Speed (kt)",
      max: yAxisMax,
      axisLabel: { color: tc.mutedText },
      nameTextStyle: { color: tc.text },
      axisLine: { lineStyle: { color: tc.axisLine } },
      splitLine: { lineStyle: { color: tc.gridLine } },
    },
    series: [
      {
        name: "Wind Speed",
        type: "line",
        data: windSpeedData.map((d) => [new Date(d.time).getTime(), d.value]),
        smooth: true,
        lineStyle: {
          width: 2,
          color: tc.series.secondary,
        },
        itemStyle: {
          color: tc.series.secondary,
        },
        areaStyle: {
          opacity: 0.1,
        },
      },
      {
        name: "Wind Gust",
        type: "scatter",
        data: windGustData.map((d) => [new Date(d.time).getTime(), d.value]),
        symbol: "circle",
        symbolSize: 6,
        itemStyle: {
          color: tc.negative,
        },
      },
      {
        name: "Wind Direction",
        type: "scatter",
        data: arrowData,
        symbol: DIRECTION_ARROW_PATH,
        symbolSize: 16,
        symbolRotate: function (params) {
          return arrowData[params.dataIndex]?.symbolRotate || 0;
        },
        itemStyle: {
          color: function (params) {
            return arrowData[params.dataIndex]?.itemStyle?.color || tc.marker;
          },
          opacity: function (params) {
            return arrowData[params.dataIndex]?.itemStyle?.opacity || 0.7;
          },
        },
        silent: true,
        z: 2,
      },
    ],
  };

  windChart.setOption(option);
}

/**
 * Select a station in the dropdown and display its chart
 * Called from map popups and URL hash navigation
 */
async function selectStationAndShowChart(stationId) {
  const select = document.getElementById("wind-station-select");
  const chartSection = document.getElementById("wind-chart-section");

  if (!select || !chartSection) {
    console.warn("Station selector or chart section not found");
    return;
  }

  // Wait for shared data to be ready (no more retry polling)
  await window.windData.ready;

  if (!windTimeseriesData || !windTimeseriesData[stationId]) {
    console.warn(`Station ${stationId} not found in timeseries data`);
    return;
  }

  select.value = stationId;
  renderWindChart(stationId);
  renderWind24HourTable(stationId);

  setTimeout(() => {
    chartSection.scrollIntoView({ behavior: "smooth", block: "start" });
  }, 100);
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
  const table = document.getElementById("wind-24hr-table");
  if (!table) return;

  const collapsedRows = table.querySelectorAll(".collapsed-row");
  const toggleButton = table.querySelector(".wind-24hr-toggle-btn");

  if (!toggleButton || collapsedRows.length === 0) return;

  // Check actual computed display value (CSS or inline)
  const firstRowStyle = window.getComputedStyle(collapsedRows[0]);
  const isCurrentlyHidden = firstRowStyle.display === "none";

  // Toggle display for all collapsed rows
  collapsedRows.forEach((row) => {
    row.style.display = isCurrentlyHidden ? "table-row" : "none";
  });

  // Update button text
  toggleButton.textContent = isCurrentlyHidden ? "▲ Show Less Rows" : "▼ Show More Rows";
}

function checkHashForWindStation() {
  const hash = window.location.hash;

  if (hash.startsWith("#wind-")) {
    const stationId = hash.substring(6); // Remove '#wind-'
    // Short delay to ensure data starts loading, then retry logic in selectStationAndShowChart kicks in
    setTimeout(() => {
      selectStationAndShowChart(stationId);
    }, 500);
  }
}

// Event delegation for time range buttons and table Map/Chart links (replaces onclick= for CSP compliance)
document.addEventListener("click", function (e) {
  var btn = e.target.closest(".wind-time-range-btn");
  if (btn) {
    var hours = parseInt(btn.dataset.windHours, 10);
    if (hours) setWindTimeRange(hours);
    return;
  }

  var link = e.target.closest(".wind-table-action-link");
  if (link) {
    e.preventDefault();
    var stationId = link.dataset.stationId;
    var action = link.dataset.action;
    if (action === "map") {
      showStationOnMap(stationId);
    } else if (action === "chart") {
      viewStationChart(stationId);
    }
  }
});

// Initialize on page load
document.addEventListener("DOMContentLoaded", () => {
  loadWindTable();
  loadWindTimeseries();
  updateWindTimeRangeLabels(); // Set initial labels to 24-Hour

  // Check for wind station in URL hash
  checkHashForWindStation();

  // Handle window resize
  window.addEventListener("resize", () => {
    if (windChart) {
      windChart.resize();
    }
  });

  // Re-render chart on theme change
  if (typeof registerChartThemeListener === "function") {
    registerChartThemeListener(() => {
      const selectedStation = document.getElementById("wind-station-select")?.value;
      if (selectedStation && windChart) {
        renderWindChart(selectedStation);
      }
    });
  }
});
