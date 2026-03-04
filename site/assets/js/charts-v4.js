/* -----------------------------
   Charts Orchestrator
   Main coordination for all chart modules
   ----------------------------- */

let chartData = null;
let waveChart, windChart, tempChart, waveComparisonChart;
let currentTimeRange = 24; // Default to 24 hours

/**
 * Filter timeseries data to specified time range (hours)
 */
function filterTimeseriesData(data, hours) {
  if (!data) return data;

  const now = new Date();
  const cutoff = new Date(now - hours * 60 * 60 * 1000);

  // Deep copy and filter each buoy's timeseries
  const filtered = {};

  Object.keys(data).forEach(buoyId => {
    if (buoyId === '_meta') {
      filtered[buoyId] = data[buoyId];
      return;
    }

    const buoy = data[buoyId];
    filtered[buoyId] = {
      name: buoy.name,
      location: buoy.location,
      timeseries: {}
    };

    // Filter each metric's data array
    Object.keys(buoy.timeseries || {}).forEach(metricKey => {
      const metric = buoy.timeseries[metricKey];
      filtered[buoyId].timeseries[metricKey] = {
        name: metric.name,
        unit: metric.unit,
        data: (metric.data || []).filter(point => new Date(point.time) >= cutoff)
      };
    });
  });

  return filtered;
}

/**
 * Load chart data from JSON file and initialize
 */
async function loadChartsData() {
  try {
    chartData = await fetchWithTimeout(`/data/buoy_timeseries_48h.json?t=${Date.now()}`);

    if (chartData._meta?.generated_utc) {
      const dataTime = new Date(chartData._meta.generated_utc);
      const timestampEl = document.getElementById("timestamp");
      if (timestampEl) {
        timestampEl.textContent = `Chart data updated: ${dataTime.toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver",
        })}`;
      }
    }

    initCharts();
    const selectedBuoy = document.getElementById("chart-buoy-select").value;
    updateCharts(selectedBuoy);
    generateWaveHeightTable(chartData);
    updateTimeRangeLabels(); // Set initial labels to 24-Hour
  } catch (err) {
    logger.error("Charts", "Error loading chart data", err);
    const timestampEl = document.getElementById("timestamp");
    if (timestampEl) {
      timestampEl.textContent = "⚠️ Error loading chart data";
    }
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

  document
    .getElementById("chart-buoy-select")
    .addEventListener("change", (e) => {
      updateCharts(e.target.value);
      updateActiveBuoyIndicator(e.target.value);
    });

  const selectedBuoy = document.getElementById("chart-buoy-select").value;
  updateActiveBuoyIndicator(selectedBuoy);

  window.addEventListener("resize", () => {
    [waveChart, windChart, tempChart].forEach(chart => chart.resize());
    waveComparisonChart.resize();
  });

  renderComparisonChart(waveComparisonChart, chartData);
}

/**
 * Update active buoy indicator text
 */
function updateActiveBuoyIndicator(buoyId) {
  const indicator = document.getElementById("active-buoy-indicator");
  if (!indicator || !chartData || !chartData[buoyId]) return;
  indicator.textContent = `📊 Viewing: ${chartData[buoyId].name}`;
  indicator.classList.add("active");
}

/**
 * Update all charts for the selected buoy
 */
function updateCharts(buoyId) {
  if (!chartData || !chartData[buoyId]) {
    logger.warn("Charts", `No data for buoy ${buoyId}`);
    return;
  }

  // Filter data based on current time range
  const filteredData = filterTimeseriesData(chartData, currentTimeRange);
  const buoy = filteredData[buoyId];

  // Render each chart using the dedicated modules
  renderWaveChart(waveChart, buoy, buoyId);
  renderWindChart(windChart, buoy);
  renderTemperatureChart(tempChart, buoy);
  renderComparisonChart(waveComparisonChart, filteredData);
}

/**
 * Set time range and update all charts
 */
function setTimeRange(hours) {
  currentTimeRange = hours;

  // Update ALL button states (sync all toggle buttons on page)
  document.querySelectorAll('.time-range-btn').forEach(btn => {
    if (parseInt(btn.dataset.hours) === hours) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
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
  const chartSectionH2 = document.querySelector('#charts-section h2');
  if (chartSectionH2) {
    chartSectionH2.textContent = `${currentTimeRange}-Hour Trends`;
  }

  const tableSectionH2 = document.querySelector('#wave-height-table-section h2');
  if (tableSectionH2) {
    tableSectionH2.textContent = `${currentTimeRange}-Hour Wave Height Summary`;
  }
}

// Wait for HTMX to load footer (which contains timestamp element) before initializing
document.addEventListener('htmx:load', function() {
  loadChartsData();
}, { once: true });

// Auto-refresh every 15 minutes
setInterval(loadChartsData, 15 * 60 * 1000);
