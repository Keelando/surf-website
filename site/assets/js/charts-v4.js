/* -----------------------------
   Charts Orchestrator (ES module)
   Main coordination for all chart modules

   Chart helpers (fetchWithTimeout, registerChartThemeListener, echarts)
   still come from classic scripts loaded before the entry point.
   ----------------------------- */

import { renderComparisonChart } from "./comparison-chart-v4.js";
import { caveatFootnote } from "./shared/station-meta.js";
import { renderTemperatureChart } from "./temperature-chart-v4.js";
import { renderWaveChart } from "./wave-chart-v4.js";
import { generateWaveHeightTable } from "./wave-table-v4.js";
import { renderBuoyWindChart } from "./wind-chart-v4.js";

let chartData = null;
// stations.json `buoys`, from main.js (setStationMeta): the charts need it for
// the asterisk rule, and main.js has already fetched it.
let stationMeta = {};
let waveChart, windChart, tempChart, waveComparisonChart;
let currentTimeRange = 24; // Default to 24 hours
let detachThemeListener = null;

/**
 * Filter timeseries data to specified time range (hours)
 */
function filterTimeseriesData(data, hours) {
  if (!data) return data;

  const now = new Date();
  const cutoff = new Date(now - hours * 60 * 60 * 1000);

  // Deep copy and filter each buoy's timeseries
  const filtered = {};

  Object.keys(data).forEach((buoyId) => {
    if (buoyId === "_meta") {
      filtered[buoyId] = data[buoyId];
      return;
    }

    const buoy = data[buoyId];
    filtered[buoyId] = {
      name: buoy.name,
      location: buoy.location,
      timeseries: {},
    };

    // Filter each metric's data array
    Object.keys(buoy.timeseries || {}).forEach((metricKey) => {
      const metric = buoy.timeseries[metricKey];
      filtered[buoyId].timeseries[metricKey] = {
        name: metric.name,
        unit: metric.unit,
        data: (metric.data || []).filter((point) => new Date(point.time) >= cutoff),
      };
    });
  });

  return filtered;
}

// One download of buoy_timeseries_48h.json (~1.4 MB, ~65 KB gzipped) shared
// by the charts and every card's history table. The history button used to
// fetch its own cache-busted copy on each click, which was most of why the
// table was slow to appear.
const TIMESERIES_MAX_AGE_MS = 10 * 60 * 1000;
let timeseriesPromise = null;
let timeseriesFetchedAt = 0;

/**
 * The 48 h buoy timeseries, fetched at most once per TIMESERIES_MAX_AGE_MS.
 * A tab left open past that gets a fresh copy on its next request.
 *
 * @returns {Promise<Object>}
 */
export function getBuoyTimeseries() {
  if (!timeseriesPromise || Date.now() - timeseriesFetchedAt > TIMESERIES_MAX_AGE_MS) {
    timeseriesFetchedAt = Date.now();
    timeseriesPromise = fetchWithTimeout(`/data/buoy_timeseries_48h.json?t=${Date.now()}`);
    // A failed fetch must not be cached for ten minutes.
    timeseriesPromise.catch(() => {
      timeseriesPromise = null;
    });
  }
  return timeseriesPromise;
}

/**
 * Load chart data from JSON file and initialize
 */
async function loadChartsData() {
  try {
    chartData = await getBuoyTimeseries();

    initCharts();
    const selectedBuoy = document.getElementById("chart-buoy-select").value;
    updateCharts(selectedBuoy);
    generateWaveHeightTable(chartData);
    updateTimeRangeLabels(); // Set initial labels to 24-Hour
  } catch (err) {
    logger.error("Charts", "Error loading chart data", err);
  }
}

/**
 * Initialize all chart instances and event listeners
 */
function initCharts() {
  waveChart = echarts.init(document.getElementById("wave-chart"));
  windChart = echarts.init(document.getElementById("wind-chart"));
  tempChart = echarts.init(document.getElementById("temp-chart"));
  waveComparisonChart = echarts.init(document.getElementById("wave-comparison-chart"));

  document.getElementById("chart-buoy-select").addEventListener("change", (e) => {
    updateCharts(e.target.value);
    updateActiveBuoyIndicator(e.target.value);
  });

  const selectedBuoy = document.getElementById("chart-buoy-select").value;
  updateActiveBuoyIndicator(selectedBuoy);

  window.addEventListener("resize", () => {
    [waveChart, windChart, tempChart].forEach((chart) => chart.resize());
    waveComparisonChart.resize();
  });

  renderComparisonChart(waveComparisonChart, chartData);
  ensureChartThemeListener();
}

/**
 * Update active buoy indicator text
 */
function updateActiveBuoyIndicator(buoyId) {
  const indicator = document.getElementById("active-buoy-indicator");
  if (!indicator || !chartData) return;
  indicator.textContent = `📊 Viewing: ${stationName(buoyId)}`;
  indicator.classList.add("active");
}

// The channels each per-station chart plots. A chart with none of them in the
// time range collapses to a one-line note instead of an empty 400-500px
// frame (the Crescent stations' wave chart, 2026-10-31, sensors silent).
const CHART_CHANNELS = {
  wave: ["wave_height_sig", "swell_height", "wind_wave_height"],
  wind: ["wind_speed", "wind_gust"],
  temp: ["air_temp", "sea_temp"],
};

function hasPoints(buoy, fields) {
  return fields.some((field) =>
    buoy.timeseries?.[field]?.data?.some((point) => point.value != null),
  );
}

/**
 * Show a chart's frame, or in its place a line saying it has nothing to plot.
 * A hidden chart isn't drawn (chart-utils' lazy wrapper waits for it to be
 * on screen), so this costs nothing while the note shows.
 *
 * @returns {boolean} Whether to draw the chart.
 */
function chartOrNote(chart, containerId, hasData, note) {
  const container = document.getElementById(containerId);
  let empty = document.getElementById(`${containerId}-empty`);
  if (!empty) {
    empty = document.createElement("p");
    empty.id = `${containerId}-empty`;
    empty.className = "chart-empty";
    container.after(empty);
  }
  empty.textContent = note;
  empty.hidden = hasData;
  container.hidden = !hasData;
  if (hasData) chart.resize();
  return hasData;
}

/**
 * The caveat footnote under a chart (the asterisk rule), created on first use
 * and hidden when the station's caveat doesn't concern what the chart plots.
 */
function caveatNoteUnder(containerId, footnote) {
  const container = document.getElementById(containerId);
  if (!container) return;
  let note = document.getElementById(`${containerId}-caveat`);
  if (!note && !footnote) return;
  if (!note) {
    note = document.createElement("p");
    note.id = `${containerId}-caveat`;
    note.className = "caveat-footnote chart-caveat";
    container.after(note);
  }
  note.textContent = footnote ?? "";
  note.hidden = !footnote || container.hidden;
}

/**
 * The buoys' registry entries, once main.js has them. Redraws the selected
 * station's charts if they were drawn before it arrived.
 */
export function setStationMeta(meta) {
  stationMeta = meta || {};
  const selectedBuoy = document.getElementById("chart-buoy-select")?.value;
  if (selectedBuoy && chartData?.[selectedBuoy]) updateCharts(selectedBuoy);
}

/**
 * A station's display name: from the timeseries, else the registry, else the
 * dropdown's own label.
 */
function stationName(buoyId) {
  if (chartData?.[buoyId]?.name) return chartData[buoyId].name;
  if (stationMeta[buoyId]?.name) return stationMeta[buoyId].name;
  const option = document.querySelector(`#chart-buoy-select option[value="${buoyId}"]`);
  return option?.textContent.trim() || buoyId;
}

/**
 * Update all charts for the selected buoy
 */
function updateCharts(buoyId) {
  if (!chartData) return;

  // Filter data based on current time range
  const filteredData = filterTimeseriesData(chartData, currentTimeRange);
  // A station with nothing in the 48 h file (offline buoy) still gets its
  // notes; bailing out here left the previous station's charts on screen
  // under the new station's name (Southern Georgia Strait, 2026-11-13).
  const buoy = filteredData[buoyId] ?? { name: stationName(buoyId), timeseries: {} };
  const since = `in the last ${currentTimeRange} hours`;

  // Render each chart using the dedicated modules
  const hasWaves = hasPoints(buoy, CHART_CHANNELS.wave);
  if (
    chartOrNote(waveChart, "wave-chart", hasWaves, `No wave readings from ${buoy.name} ${since}.`)
  ) {
    renderWaveChart(waveChart, buoy, buoyId);
  } else {
    // NOAA's second wave chart (periods) goes with the first.
    const periodChart = document.getElementById("wave-period-chart");
    if (periodChart) periodChart.style.display = "none";
  }
  if (
    chartOrNote(
      windChart,
      "wind-chart",
      hasPoints(buoy, CHART_CHANNELS.wind),
      `No wind readings from ${buoy.name} ${since}.`,
    )
  ) {
    renderBuoyWindChart(windChart, buoy);
  }
  if (
    chartOrNote(
      tempChart,
      "temp-chart",
      hasPoints(buoy, CHART_CHANNELS.temp),
      `No temperature readings from ${buoy.name} ${since}.`,
    )
  ) {
    renderTemperatureChart(tempChart, buoy, stationMeta[buoyId]);
  }
  caveatNoteUnder("temp-chart", caveatFootnote(stationMeta[buoyId], "air_temp", "sea_temp"));
  renderComparisonChart(waveComparisonChart, filteredData);
}
function ensureChartThemeListener() {
  if (detachThemeListener) return;
  detachThemeListener = registerChartThemeListener(() => {
    const selectedBuoy = document.getElementById("chart-buoy-select")?.value;
    if (selectedBuoy && chartData && chartData[selectedBuoy]) {
      updateCharts(selectedBuoy);
    }
  });
}

/**
 * Set time range and update all charts
 */
export function setTimeRange(hours) {
  currentTimeRange = hours;

  // Update ALL button states (sync all toggle buttons on page)
  document.querySelectorAll(".time-range-btn").forEach((btn) => {
    const active = parseInt(btn.dataset.hours) === hours;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-pressed", String(active));
  });

  // Update section headers to show current time range
  updateTimeRangeLabels();

  // Filter data based on new time range
  const filteredData = filterTimeseriesData(chartData, currentTimeRange);

  // Re-render current buoy charts
  const selectedBuoy = document.getElementById("chart-buoy-select")?.value;
  if (selectedBuoy) {
    updateCharts(selectedBuoy);
  }

  // Re-generate wave height table with filtered data
  generateWaveHeightTable(filteredData);
}

/**
 * Update all time range labels on the page
 */
function updateTimeRangeLabels() {
  // Update section headers
  const chartSectionH2 = document.querySelector("#charts-section h2");
  if (chartSectionH2) {
    chartSectionH2.textContent = `${currentTimeRange}-Hour Trends`;
  }

  const tableSectionH2 = document.querySelector("#wave-height-table-section h2");
  if (tableSectionH2) {
    tableSectionH2.textContent = `${currentTimeRange}-Hour Wave Height Summary`;
  }
}

/**
 * Apply a user-defined wave threshold line to the comparison chart
 */
export function applyWaveThreshold() {
  const input = document.getElementById("wave-threshold-input");
  const val = parseFloat(input.value);
  if (isNaN(val) || val <= 0 || val > 10) return;

  localStorage.setItem("waveThreshold", val);
  const status = document.getElementById("wave-threshold-status");
  if (status) status.textContent = `Threshold set: ${val}m`;

  const filteredData = filterTimeseriesData(chartData, currentTimeRange);
  renderComparisonChart(waveComparisonChart, filteredData);
}

/**
 * Clear the user-defined wave threshold
 */
export function clearWaveThreshold() {
  localStorage.removeItem("waveThreshold");
  const input = document.getElementById("wave-threshold-input");
  if (input) input.value = "";
  const status = document.getElementById("wave-threshold-status");
  if (status) status.textContent = "";

  const filteredData = filterTimeseriesData(chartData, currentTimeRange);
  renderComparisonChart(waveComparisonChart, filteredData);
}

/**
 * Restore threshold input from localStorage on page load
 */
function initThresholdControl() {
  const stored = localStorage.getItem("waveThreshold");
  if (!stored) return;
  const input = document.getElementById("wave-threshold-input");
  if (input) input.value = stored;
  const status = document.getElementById("wave-threshold-status");
  if (status) status.textContent = `Threshold set: ${stored}m`;
}

// Wait for HTMX to load footer (which contains timestamp element) before initializing
document.addEventListener(
  "htmx:load",
  function () {
    loadChartsData();
    initThresholdControl();
  },
  { once: true },
);

// Auto-refresh every 15 minutes
setInterval(loadChartsData, 15 * 60 * 1000);
