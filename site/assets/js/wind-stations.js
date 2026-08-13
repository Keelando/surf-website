/* -----------------------------
   Wind Stations Module (ES module)
   Displays current wind conditions table and 24hr trends chart

   Chart helpers (degreesToCardinal, getDirectionalArrow,
   createWindDirectionArrowData, theme/grid/tooltip config, echarts) still
   come from classic scripts loaded before this one.
   ----------------------------- */

import { formatMonthDayTime, formatTimeHM, formatTimeWithDate } from "./shared/format-time.js";
import { DIRECTION_ARROW_PATH } from "./shared/markers.js";
import { windData } from "./wind-data.js";

function setSafeHTML(element, html) {
  if (!element) return;
  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

/** Escape a string for safe interpolation into an HTML attribute. */
function escapeAttr(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// --- Constants ---
const STALE_THRESHOLD_HOURS = 2; // Data older than this is dimmed
const OFFLINE_THRESHOLD_HOURS = 4; // Data older than this moves to offline list
const SCROLL_SETTLE_DELAY_MS = 500; // Delay after smooth-scroll before focusing
const TOOLTIP_TIME_TOLERANCE_MS = 1800000; // 30 min — snap tooltip to nearest direction point

// Global chart instance
let windChart = null;
let windTimeseriesData = null;
let allStationsList = []; // Store all stations
let currentSort = { column: null, ascending: true };
let currentWindTimeRange = 24; // Default to 24 hours

/**
 * Look up station metadata (source_url, short_name, flag) from stations.json
 */
function getStationMeta(id) {
  const s = windData.stations;
  return s?.wind?.[id] || s?.buoys?.[id] || {};
}

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
    header.removeAttribute("aria-sort");
    const indicator = header.querySelector(".sort-indicator");
    indicator.textContent = "";
  });

  // Set active indicator
  activeHeader.classList.add("sorting");
  activeHeader.setAttribute("aria-sort", currentSort.ascending ? "ascending" : "descending");
  const indicator = activeHeader.querySelector(".sort-indicator");
  indicator.textContent = currentSort.ascending ? "▲" : "▼";
}

/**
 * Classify latest station data into active vs offline lists by data age
 * @param {Object} latestAll - All latest station readings keyed by ID
 * @returns {{ active: Array, offline: Array }} Station tuples [id, stationData]
 */
function classifyStations(latestAll) {
  const active = [];
  const offline = [];

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
        stale: ageHours >= STALE_THRESHOLD_HOURS && ageHours < OFFLINE_THRESHOLD_HOURS,
        type: station._sourceType,
      };

      if (ageHours >= OFFLINE_THRESHOLD_HOURS) {
        offline.push([id, stationData]);
      } else {
        active.push([id, stationData]);
      }
    });

  active.sort((a, b) => a[1].name.localeCompare(b[1].name));
  return { active, offline };
}

/**
 * Render the offline stations callout box
 * @param {Array} offlineStations - Station tuples [id, stationData] with ageHours
 */
function renderOfflineStationsList(offlineStations) {
  const container = document.getElementById("offline-stations-list");
  if (!container) return;

  if (offlineStations.length === 0) {
    container.innerHTML = "";
    return;
  }

  offlineStations.sort((a, b) => a[1].name.localeCompare(b[1].name));

  let html = '<div class="offline-callout">';
  html += "<h3>Stations with Stale Data (>4 hours)</h3>";
  html += "<p>The following stations have not reported wind data in over 4 hours:</p>";
  html += "<ul>";

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

    const offlineMeta = getStationMeta(id);
    let stationLink = station.name.replace(" 🌊", "");
    if (offlineMeta.source_url) {
      stationLink = `<a href="${offlineMeta.source_url}" target="_blank" rel="noopener">${stationLink}</a>`;
    }

    html += `<li><strong>${stationLink}</strong> (${ageText} ago)</li>`;
  });

  html += "</ul></div>";
  setSafeHTML(container, html);
}

/**
 * Load and display current wind conditions table
 */
async function loadWindTable() {
  try {
    // Wait for shared data store (fetched once by wind-data.js)
    await windData.ready;
    const latestAll = windData.latestAll;

    const table = document.getElementById("wind-conditions-table");
    if (!table) return;

    const { active: stations, offline: offlineStations } = classifyStations(latestAll);

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
      const updated = formatTimeWithDate(station.observation_time);

      // Look up source link, flag, and short name from stations.json
      const meta = getStationMeta(id);
      const sourceLink = meta.source_url || null;
      const flag = meta.flag || "";
      const flagSpan = flag
        ? ` <span class="hide-mobile" style="font-size: 0.8em;">${flag}</span>`
        : "";

      const mobileName = meta.short_name || station.name;

      // Stations with a `caveat` in stations.json get an asterisk that links
      // down to the footnote below the table (e.g. CWLM sits 65 m up a hill,
      // so its pressure reads low next to the sea-level stations). The title
      // answers on hover; the anchor is what works without a pointer.
      const caveatMark = meta.caveat
        ? ` <sup class="station-caveat-mark"><a href="#caveat-${escapeAttr(id)}" title="${escapeAttr(meta.caveat)}" aria-label="Footnote: why ${escapeAttr(station.name)} readings differ">*</a></sup>`
        : "";

      tableHTML += `
        <tr ${rowClass}
            data-name="${station.name}"
            data-wind_speed_kt="${station.wind_speed_kt || ""}"
            data-wind_gust_kt="${station.wind_gust_kt || ""}"
            data-wind_direction="${station.wind_direction || ""}"
            data-air_temp_c="${station.air_temp_c || ""}"
            data-pressure_hpa="${station.pressure_hpa || ""}"
            data-observation_time="${station.observation_time}">
          <td>${sourceLink ? `<a href="${sourceLink}" target="_blank" rel="noopener" style="color: inherit; text-decoration: none;"><strong><span class="hide-mobile">${station.name}</span><span class="show-mobile">${mobileName}</span></strong>${flagSpan}</a>` : `<strong><span class="hide-mobile">${station.name}</span><span class="show-mobile">${mobileName}</span></strong>${flagSpan}`}${caveatMark}</td>
          <td class="wind-table-actions"><span class="hide-mobile">${updated}</span><span class="show-mobile">${formatTimeHM(station.observation_time)}</span></td>
          <td class="wind-table-actions">${direction}</td>
          <td>${windSpeed}</td>
          <td>${windGust}</td>
          <td>${temp}</td>
          <td>${pressure}</td>
          <td class="wind-table-actions">
            <a href="#map-section" class="wind-table-action-link" data-action="map" data-station-id="${id}">Map</a>
            <span class="wind-table-action-separator">/</span>
            <a href="#wind-chart-section" class="wind-table-action-link" data-action="chart" data-station-id="${id}">Chart</a>
          </td>
        </tr>
      `;
    });

    tableHTML += "</tbody>";
    table.innerHTML = tableHTML;

    renderCaveatFootnotes(stations);

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

    renderOfflineStationsList(offlineStations);

    // (Dead footer-timestamp updater removed 2026-07-16: it targeted
    // #timestamp, which no longer exists in any page, and checked
    // windData._meta, which never existed on the store.)
  } catch (error) {
    console.error("Error loading wind table:", error);
    const table = document.getElementById("wind-conditions-table");
    if (table) {
      table.innerHTML =
        '<tbody><tr><td colspan="7" class="table-message-cell">Error loading wind data</td></tr></tbody>';
    }
  }
}

/**
 * Render the "*" footnotes under the conditions table — one line per visible
 * station that carries a `caveat` in stations.json. Driven entirely by the
 * registry, so adding a caveat to a station needs no change here.
 */
function renderCaveatFootnotes(stations) {
  const container = document.getElementById("wind-table-footnotes");
  if (!container) return;

  const caveats = stations
    .map(([id, station]) => [getStationMeta(id).caveat, station.name, id])
    .filter(([caveat]) => caveat);

  if (caveats.length === 0) {
    container.innerHTML = "";
    return;
  }

  // tabindex="-1" so the asterisk anchor moves focus here, not just scroll —
  // otherwise a keyboard user jumps but their focus stays up in the table.
  setSafeHTML(
    container,
    caveats
      .map(
        ([caveat, name, id]) =>
          `<p class="station-caveat" id="caveat-${id}" tabindex="-1">* <strong>${name}</strong> — ${caveat}</p>`,
      )
      .join(""),
  );
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
    await windData.ready;
    windTimeseriesData = windData.timeseries;

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

// createWindDirectionArrowData provided by chart-utils-v4.js (loaded earlier)

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

  // Ask winds-map.js to center on the station (after scroll settles)
  setTimeout(() => {
    document.dispatchEvent(new CustomEvent("winds:focus-station", { detail: { stationId } }));
  }, SCROLL_SETTLE_DELAY_MS);
}

// Make functions globally accessible
// Listen for map popup requests to show a station chart (dispatched by winds-map.js)
document.addEventListener("winds:select-station", (e) => {
  const stationId = e.detail?.stationId;
  if (stationId) selectStationAndShowChart(stationId);
});

/**
 * Merge multiple timeseries arrays into a single Map keyed by timestamp
 * @param {Array<[Array, string]>} fields - Pairs of [dataArray, fieldName]
 * @returns {Map<string, Object>} Map from ISO time string to { fieldName: value, ... }
 */
function mergeTimeseriesByTime(fields) {
  const byTime = new Map();
  for (const [dataArray, fieldName] of fields) {
    for (const point of dataArray) {
      if (!byTime.has(point.time)) {
        byTime.set(point.time, {});
      }
      byTime.get(point.time)[fieldName] = point.value;
    }
  }
  return byTime;
}

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

  // Extract and merge timeseries data (all normalized to flat arrays by wind-data.js)
  const ts = station.timeseries;
  const dataByTime = mergeTimeseriesByTime([
    [ts.wind_speed || [], "speed"],
    [ts.wind_gust || [], "gust"],
    [ts.wind_direction || [], "direction"],
    [ts.air_temp || [], "temp"],
    [ts.pressure || [], "pressure"],
  ]);

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
    tableHTML += '<tr><td colspan="6" class="table-message-cell">No data available</td></tr>';
  } else {
    const DEFAULT_VISIBLE_ROWS = 12;

    hourlyTimes.forEach((time, index) => {
      const data = dataByTime.get(time);
      const formattedTime = formatTimeWithDate(time);
      const mobileTime = formatTimeHM(time);
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
          <td class="wind-table-actions"><span class="hide-mobile">${formattedTime}</span><span class="show-mobile">${mobileTime}</span></td>
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
      toggleCell.className = "wind-24hr-toggle-cell";

      const toggleButton = document.createElement("button");
      toggleButton.type = "button";
      toggleButton.className = "wind-24hr-toggle-btn";
      toggleButton.textContent = "▼ Show More Rows";
      toggleButton.setAttribute("aria-expanded", "false");
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
    const isActive = parseInt(btn.dataset.windHours) === hours;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-pressed", isActive);
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
 * Render wind chart for selected station (winds page).
 * For the buoy page wind chart, see renderBuoyWindChart() in wind-chart-v4.js.
 */
function renderWindChart(stationId) {
  if (!windTimeseriesData || !stationId) return;

  // Filter data based on current time range
  const filteredData = filterWindTimeseriesData(windTimeseriesData, currentWindTimeRange);
  const station = filteredData[stationId];
  if (!station) return;

  const chartContainer = document.getElementById("wind-trend-chart");
  if (!chartContainer) return;

  try {
    // Initialize chart if needed
    if (!windChart) {
      windChart = echarts.init(chartContainer);
    }

    // Extract timeseries data (all normalized to flat arrays by wind-data.js)
    const timeseries = station.timeseries;
    const windSpeedData = timeseries.wind_speed || [];
    const windGustData = timeseries.wind_gust || [];
    const windDirData = timeseries.wind_direction || [];

    // Create direction arrow data
    const { arrowData, maxValue } = createWindDirectionArrowData(
      windDirData,
      windSpeedData,
      windGustData,
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

    windChart.setOption({
      backgroundColor: tc.background,
      textStyle: { color: tc.text },
      title: {
        text: `${station.name.replace(" 🌊", "")} - Wind Conditions`,
        left: "center",
        textStyle: {
          fontSize: window.innerWidth < 600 ? 12 : 14,
          color: tc.text,
        },
      },
      tooltip: {
        ...getMobileOptimizedTooltipConfig(),
        formatter: (params) => {
          if (!params || params.length === 0) return "";
          const time = formatMonthDayTime(new Date(params[0].value[0]).toISOString());
          let res = `<b>${time}</b><br/>`;

          params.forEach((p) => {
            if (p.seriesName === "Wind Direction") return;
            if (p.value && p.value[1] != null) {
              res += `${p.marker} ${p.seriesName}: ${Math.round(p.value[1])} kt<br/>`;
            }
          });

          // Add wind direction to tooltip if available
          const timestamp = new Date(params[0].value[0]).getTime();
          const dirPoint = windDirData.find(
            (d) => Math.abs(new Date(d.time).getTime() - timestamp) < TOOLTIP_TIME_TOLERANCE_MS,
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
        bottom: getResponsiveLegendBottom(),
        textStyle: { color: tc.text },
      },
      grid: getResponsiveGridConfig(false),
      xAxis: {
        type: "time",
        axisLabel: {
          fontSize: window.innerWidth < 600 ? 9 : 10,
          rotate: window.innerWidth < 600 ? 30 : 0,
          formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
          hideOverlap: true,
          margin: 10,
          color: tc.mutedText,
        },
        axisTick: { show: true },
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
          data: sanitizeSeriesData(windSpeedData),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: tc.series.secondary },
          areaStyle: tc.isDark ? { opacity: 0 } : { opacity: 0.1 },
        },
        {
          name: "Wind Gust",
          type: "scatter",
          data: sanitizeSeriesData(windGustData),
          symbol: "circle",
          symbolSize: 6,
          itemStyle: { color: tc.negative },
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
    });
  } catch (error) {
    showChartError("wind-trend-chart", "Wind Chart", error);
  }
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
  await windData.ready;

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

  // Update button text and aria state
  toggleButton.textContent = isCurrentlyHidden ? "▲ Show Less Rows" : "▼ Show More Rows";
  toggleButton.setAttribute("aria-expanded", isCurrentlyHidden);
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

// Initialize on page load (module scripts are deferred, so the DOM is parsed)
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
