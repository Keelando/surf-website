/* -----------------------------
   Wind Stations Module (ES module)
   The winds page: the station list on the summary (built by wind-list.js),
   and one station's history chart with its hourly table.

   Chart helpers (degreesToCardinal, getDirectionalArrow,
   createWindDirectionArrowData, theme/grid/tooltip config, echarts) still
   come from classic scripts loaded before this one.
   ----------------------------- */

import { formatMonthDayTime, formatTimeHM, formatTimeWithDate } from "./shared/format-time.js";
import { DIRECTION_ARROW_PATH } from "./shared/markers.js";
import { setSafeHTML } from "./shared/safe-html.js";
import { airTempFootnote, airTempIsProxy, airTempLabel } from "./shared/station-meta.js";
import { windData } from "./wind-data.js";
import {
  buildWindDetail,
  buildWindList,
  DEFAULT_SORT,
  nextSort,
  regionGroups,
  windEntry,
} from "./wind-list.js";

// --- Constants ---
const SCROLL_SETTLE_DELAY_MS = 500; // Delay after smooth-scroll before focusing
const TOOLTIP_TIME_TOLERANCE_MS = 1800000; // 30 min — snap tooltip to nearest direction point

// Global chart instance
let windChart = null;
let windTimeseriesData = null;
// The timeseries the chart, hourly table and station picker were last drawn from.
let shownTimeseriesAt = 0;
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

// -----------------------------
// The station list
// -----------------------------

// How often the list fetches a fresh snapshot: the home page's rhythm (main.js).
const REFRESH_MS = 5 * 60 * 1000;
// The reader's last pick of view, remembered per browser (a convenience only:
// without storage the list opens as "all").
const VIEW_KEY = "winds-list-view";
// Region slugs the reader folded away in region view, remembered the same way.
const COLLAPSED_KEY = "winds-collapsed-regions";

let listView = readView();
let collapsedRegions = readCollapsed();
let listSort = { ...DEFAULT_SORT };
let entriesById = {};

function readView() {
  try {
    return localStorage.getItem(VIEW_KEY) === "region" ? "region" : "all";
  } catch {
    return "all";
  }
}

function readCollapsed() {
  try {
    const saved = JSON.parse(localStorage.getItem(COLLAPSED_KEY) || "[]");
    return new Set(Array.isArray(saved) ? saved : []);
  } catch {
    return new Set();
  }
}

function saveCollapsed() {
  try {
    localStorage.setItem(COLLAPSED_KEY, JSON.stringify([...collapsedRegions]));
  } catch {
    // Storage unavailable: the folds last for this visit.
  }
}

/** Fold or unfold one region in place; the list is not rebuilt. */
function toggleRegion(button) {
  const slug = button.dataset.region;
  const rows = document.getElementById(`wl-rows-${slug}`);
  if (!rows) return;
  const open = button.getAttribute("aria-expanded") !== "true";
  button.setAttribute("aria-expanded", String(open));
  rows.hidden = !open;
  if (open) collapsedRegions.delete(slug);
  else collapsedRegions.add(slug);
  saveCollapsed();
}

function saveView(view) {
  try {
    localStorage.setItem(VIEW_KEY, view);
  } catch {
    // Private mode or blocked storage: the choice lasts for this visit.
  }
}

/** Every station in the store, as wind-list.js rows (keys starting "_" are not stations). */
function currentEntries(now = Date.now()) {
  const latestAll = windData.latestAll || {};
  const registry = windData.stations || {};
  return Object.entries(latestAll)
    .filter(([id]) => !id.startsWith("_"))
    .map(([id, s]) => windEntry(id, s, registry.wind?.[id] || registry.buoys?.[id] || {}, now));
}

/** Rebuild the list, keeping open rows and focus. */
function renderWindList() {
  const container = document.getElementById("wind-list");
  if (!container || !windData.latestAll) return;
  const open = [...container.querySelectorAll('.wl-row[aria-expanded="true"]')].map(
    (row) => row.closest(".wl-item").dataset.stationId,
  );
  const active = document.activeElement;
  const focusedRow = active?.closest?.("#wind-list .wl-item")?.dataset.stationId;
  const focusedSort = active?.closest?.("#wind-list .wl-sort")?.dataset.sort;
  const focusedRegion = active?.closest?.("#wind-list .wl-region-toggle")?.dataset.region;

  const entries = currentEntries();
  entriesById = Object.fromEntries(entries.map((e) => [e.id, e]));
  setSafeHTML(
    container,
    buildWindList(entries, { view: listView, sort: listSort, collapsed: collapsedRegions }),
  );
  container.removeAttribute("aria-busy");

  for (const id of open) toggleWindRow(id, true);
  if (focusedSort) container.querySelector(`.wl-sort[data-sort="${focusedSort}"]`)?.focus();
  else if (focusedRegion)
    container.querySelector(`.wl-region-toggle[data-region="${focusedRegion}"]`)?.focus();
  else if (focusedRow) document.querySelector(`#wind-row-${focusedRow} .wl-row`)?.focus();
}

/** Open or close one station's row; its panel is built each time it opens. */
function toggleWindRow(id, open) {
  const item = document.getElementById(`wind-row-${id}`);
  const row = item?.querySelector(".wl-row");
  const panel = item?.querySelector(".wl-detail");
  if (!row || !panel) return;
  const opening = open ?? row.getAttribute("aria-expanded") !== "true";
  if (opening && entriesById[id]) setSafeHTML(panel, buildWindDetail(entriesById[id]));
  panel.hidden = !opening;
  row.setAttribute("aria-expanded", String(opening));
}

/**
 * Once a minute: fetch a fresh snapshot when the last one is due for
 * replacing (the store then fires winds:data-updated, which rebuilds the
 * list), then bring every row's age up to the clock. A row whose state
 * changes (on time to late, say) shows different things, so that rebuilds
 * the list too. Fetch first: a tab the browser froze wakes with its clock on
 * time but its copy old, and ticking the old copy would mark every station
 * down (the home page's lesson, main.js).
 */
async function tick() {
  if (Date.now() - windData.loadedAt >= REFRESH_MS) await windData.reload();
  const container = document.getElementById("wind-list");
  if (!container) return;
  for (const e of currentEntries()) {
    const item = document.getElementById(`wind-row-${e.id}`);
    const row = item?.querySelector(".wl-row");
    if (!row) continue;
    if (!row.classList.contains(`wl-row--${e.fresh.state}`)) {
      renderWindList();
      return;
    }
    const text = item.querySelector(".sl-age-text");
    if (text) text.textContent = e.fresh.text.split(" · ")[0].replace(/ ago$/, "");
  }
}

async function loadWindList() {
  await windData.ready;
  const container = document.getElementById("wind-list");
  if (!container) return;
  if (!windData.latestAll) {
    container.removeAttribute("aria-busy");
    setSafeHTML(
      container,
      `<p class="sl-error">Couldn't load the latest readings. Trying again every minute.</p>`,
    );
    return;
  }
  renderWindList();
}

function setListView(view) {
  listView = view;
  saveView(view);
  document.querySelectorAll(".wl-view-btn").forEach((btn) => {
    const active = btn.dataset.view === view;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-pressed", String(active));
  });
  renderWindList();
}

document.getElementById("wind-list")?.addEventListener("click", (event) => {
  const region = event.target.closest(".wl-region-toggle");
  if (region) {
    toggleRegion(region);
    return;
  }
  const sort = event.target.closest(".wl-sort");
  if (sort) {
    listSort = nextSort(listSort, sort.dataset.sort);
    renderWindList();
    return;
  }
  const item = event.target.closest(".wl-item");
  if (!item) return;
  const id = item.dataset.stationId;
  if (event.target.closest(".wl-row")) {
    toggleWindRow(id);
    return;
  }
  const action = event.target.closest("[data-action]")?.dataset.action;
  if (action === "chart") viewStationChart(id);
  else if (action === "map") showStationOnMap(id);
  else if (action === "close") {
    toggleWindRow(id, false);
    item.querySelector(".wl-row")?.focus();
  }
});

document.querySelectorAll(".wl-view-btn").forEach((btn) => {
  btn.addEventListener("click", () => setListView(btn.dataset.view));
});
// The markup starts on "all"; show the remembered view's button as pressed.
if (listView !== "all") {
  document.querySelectorAll(".wl-view-btn").forEach((btn) => {
    const active = btn.dataset.view === listView;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-pressed", String(active));
  });
}

/**
 * The chart's station picker, grouped by region like the list (same order),
 * stations by name within each.
 */
function populateStationDropdown() {
  const select = document.getElementById("wind-station-select");
  if (!select || !windTimeseriesData) return;
  const registry = windData.stations || {};
  const stations = Object.entries(windTimeseriesData)
    .filter(([id]) => !id.startsWith("_"))
    .map(([id, station]) => {
      const meta = registry.wind?.[id] || registry.buoys?.[id] || {};
      // The list's name for it (the timeseries export shortens some: "Ballenas").
      const name = windData.latestAll?.[id]?.name || meta.name || station.name || id;
      return { id, name: name.replace(" \u{1F30A}", ""), region: meta.region ?? "" };
    });
  const selected = select.value;
  select.textContent = "";
  for (const group of regionGroups(stations, { key: "name", dir: "asc" })) {
    const optgroup = document.createElement("optgroup");
    optgroup.label = group.label;
    for (const station of group.entries) {
      const option = document.createElement("option");
      option.value = station.id;
      option.textContent = station.name;
      optgroup.appendChild(option);
    }
    select.appendChild(optgroup);
  }
  if (selected && windTimeseriesData[selected]) select.value = selected;
}

/**
 * Load wind timeseries data and populate station selector
 */
async function loadWindTimeseries() {
  try {
    // Wait for shared data store (fetched and normalized by wind-data.js)
    await windData.ready;
    windTimeseriesData = windData.timeseries;
    shownTimeseriesAt = windData.timeseriesLoadedAt;

    const select = document.getElementById("wind-station-select");
    if (!select) return;

    populateStationDropdown();

    // The first station of the first region, unless a link already chose one.
    if (!select.value) select.selectedIndex = 0;
    if (select.value) {
      renderWindChart(select.value);
      renderWind24HourTable(select.value);
    }

    select.addEventListener("change", (e) => {
      renderWindChart(e.target.value);
      renderWind24HourTable(e.target.value);
    });
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
  const focus = () =>
    document.dispatchEvent(new CustomEvent("winds:focus-station", { detail: { stationId } }));
  const map = document.getElementById("winds-map");
  const box = map?.getBoundingClientRect();
  // Beside the list on a wide screen the map is already in view: pan it at once.
  if (box && box.top >= 0 && box.bottom <= window.innerHeight) {
    focus();
    return;
  }
  document.getElementById("map-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
  // Ask winds-map.js to center on the station once the scroll settles.
  setTimeout(focus, SCROLL_SETTLE_DELAY_MS);
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

  // A temperature that isn't the open air's (Colebrook's enclosure) gets the
  // registry's label, an asterisk on the column and its values, and the
  // footnote under the table.
  const meta = windData.stations?.wind?.[stationId] || windData.stations?.buoys?.[stationId];
  const proxyTemp = airTempIsProxy(meta);
  const star = proxyTemp ? `<span class="wl-proxy-star" aria-hidden="true">*</span>` : "";
  const note = document.getElementById("wind-hourly-note");
  if (note) {
    note.textContent = airTempFootnote(meta) ?? "";
    note.hidden = !proxyTemp;
  }

  // Build table HTML
  let tableHTML = `
    <thead>
      <tr>
        <th>Time</th>
        <th>Direction</th>
        <th>Wind Speed (kt)</th>
        <th>Gust (kt)</th>
        <th class="hide-mobile">${proxyTemp ? airTempLabel(meta) : "Temp"} (°C)${star}</th>
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
      const temp = data.temp != null ? `${data.temp.toFixed(1)}${star}` : "—";
      const pressure = data.pressure != null ? data.pressure.toFixed(1) : "—";

      let direction = "—";
      if (data.direction != null) {
        const cardinal = degreesToCardinal(data.direction);
        const arrow = getDirectionalArrow(data.direction);
        // Arrow first and degrees on wide screens only, as in the list: on a
        // phone "ESE (102°) ↖" wrapped onto two lines.
        direction = `${arrow} ${cardinal}<span class="hide-mobile"> (${Math.round(data.direction)}°)</span>`;
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

  // Re-render current station chart and table
  const selectedStation = document.getElementById("wind-station-select")?.value;
  if (selectedStation) {
    renderWindChart(selectedStation);
    renderWind24HourTable(selectedStation);
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
        // The list's name for it: the timeseries export shortens some ("Ballenas").
        text: `${(windData.latestAll?.[stationId]?.name || station.name).replace(" 🌊", "")} - Wind Conditions`,
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
        nameTextStyle: { color: tc.text, align: "left" },
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
    rescaleYAxisOnLegendToggle(windChart, { topSeries: ["Wind Direction"] });
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

/**
 * A #wind-<id> link (the home map's "View on Winds Page") opens that
 * station's row in the list and picks it for the chart below, as a #buoy-
 * link opens its row on the home page.
 */
async function checkHashForWindStation() {
  const hash = window.location.hash;
  if (!hash.startsWith("#wind-")) return;
  const stationId = decodeURIComponent(hash.substring(6));
  await windData.ready;
  // The list renders on the same promise; let it land first.
  await new Promise((resolve) => requestAnimationFrame(resolve));
  const folded = document
    .getElementById(`wind-row-${stationId}`)
    ?.closest(".wl-region")
    ?.querySelector('.wl-region-toggle[aria-expanded="false"]');
  if (folded) toggleRegion(folded);
  toggleWindRow(stationId, true);
  document.getElementById(`wind-row-${stationId}`)?.scrollIntoView({ block: "center" });
  const select = document.getElementById("wind-station-select");
  if (select && windTimeseriesData?.[stationId]) {
    select.value = stationId;
    renderWindChart(stationId);
    renderWind24HourTable(stationId);
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
loadWindList();
loadWindTimeseries();

// The list and chart catch up on the home page's rhythm: once a minute, and at
// once when the page comes back into view or out of the browser's page cache.
setInterval(tick, 60 * 1000);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") tick();
});
window.addEventListener("pageshow", (e) => {
  if (e.persisted) tick();
});
document.addEventListener("winds:data-updated", () => {
  renderWindList();
  // Those three read the timeseries, which refresh only every half hour
  // (wind-data.js), so most updates leave them alone. When they do redraw,
  // "Show More Rows" stays open and a dropdown the reader is in is left be.
  if (windData.timeseriesLoadedAt === shownTimeseriesAt) return;
  shownTimeseriesAt = windData.timeseriesLoadedAt;
  windTimeseriesData = windData.timeseries;
  const select = document.getElementById("wind-station-select");
  if (select && document.activeElement !== select) populateStationDropdown();
  const selected = select?.value;
  if (selected) {
    const moreOpen =
      document
        .querySelector("#wind-24hr-table .wind-24hr-toggle-btn")
        ?.getAttribute("aria-expanded") === "true";
    renderWindChart(selected);
    renderWind24HourTable(selected);
    if (moreOpen) toggleWind24hrRows();
  }
});

// The hourly table opens under the chart.
const hourlyToggle = document.querySelector(".wl-hourly-toggle");
hourlyToggle?.addEventListener("click", () => {
  const panel = document.getElementById("wind-hourly");
  if (!panel) return;
  panel.hidden = !panel.hidden;
  hourlyToggle.setAttribute("aria-expanded", String(!panel.hidden));
  hourlyToggle.textContent = panel.hidden ? "Hourly table" : "Hide hourly table";
});

// Check for wind station in URL hash, on load and when a link on the page changes it.
checkHashForWindStation();
window.addEventListener("hashchange", checkHashForWindStation);

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
