/* -----------------------------
   Storm Surge Chart - Multi-Station Selector
   ----------------------------- */

let surgeChart = null;
let surgeData = null;

// Station display order
const STATION_ORDER = [
  "Point_Atkinson",
  "Crescent_Channel_Ocean",     // Surrey - reuses Crescent_Beach_Channel forecast
  "Crescent_Beach_Ocean",       // Surrey - reuses Crescent_Beach_Channel forecast
  "Campbell_River",
  "Neah_Bay",
  "New_Dungeness",
  "Tofino"
];

async function loadStormSurgeData() {
  try {
    surgeData = await fetchWithTimeout(`/data/storm_surge/combined_forecast.json?t=${Date.now()}`);
    
    // Initialize the selector if not already done
    initStationSelector();
    
    // Load the default station (Point Atkinson)
    const selectedStation = document.getElementById("surge-station-select")?.value || "Point_Atkinson";
    updateSurgeChart(selectedStation);

  } catch (err) {
    logger.error("StormSurgeChart", "Error loading storm surge data", err);
    const container = document.getElementById("surge-chart");
    if (container) {
      container.innerHTML = '<p style="text-align:center;color:#999;">⚠️ Storm surge data unavailable</p>';
    }
  }
}

function initStationSelector() {
  const selector = document.getElementById("surge-station-select");
  if (!selector || selector.dataset.initialized) return;

  // Clear existing options
  selector.innerHTML = "";

  // Add options for each station
  STATION_ORDER.forEach(stationId => {
    // Surrey stations reuse Crescent_Beach_Channel forecast
    const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
      ? "Crescent_Beach_Channel"
      : stationId;

    const station = surgeData.stations?.[forecastStationId];
    if (station) {
      const option = document.createElement("option");
      option.value = stationId;

      // Use proper display name for Surrey stations
      let displayName = station.station_name;
      if (stationId === "Crescent_Beach_Ocean") {
        displayName = "Crescent Beach Ocean";
      } else if (stationId === "Crescent_Channel_Ocean") {
        displayName = "Crescent Channel Ocean";
      }

      option.textContent = displayName;
      selector.appendChild(option);
    }
  });

  // Add change event listener
  selector.addEventListener("change", (e) => {
    updateSurgeChart(e.target.value);
    updateActiveStationIndicator(e.target.value);
  });

  selector.dataset.initialized = "true";

  // Update indicator
  updateActiveStationIndicator(selector.value);
}

function updateActiveStationIndicator(stationId) {
  const indicator = document.getElementById("active-surge-indicator");
  if (!indicator) return;

  // Surrey stations reuse Crescent_Beach_Channel forecast
  const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
    ? "Crescent_Beach_Channel"
    : stationId;

  if (!surgeData?.stations?.[forecastStationId]) return;

  const station = surgeData.stations[forecastStationId];

  // Use proper display name for Surrey stations
  let displayName = station.station_name;
  if (stationId === "Crescent_Beach_Ocean") {
    displayName = "Crescent Beach Ocean";
  } else if (stationId === "Crescent_Channel_Ocean") {
    displayName = "Crescent Channel Ocean";
  }

  indicator.textContent = `📍 Viewing: ${displayName}`;
  indicator.classList.add("active");
}

function updateSurgeChart(stationId) {
  // Surrey stations reuse Crescent_Beach_Channel forecast
  const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
    ? "Crescent_Beach_Channel"
    : stationId;

  if (!surgeData?.stations?.[forecastStationId]) {
    logger.warn("StormSurgeChart", `No data found for station: ${stationId}`);
    return;
  }

  const station = surgeData.stations[forecastStationId];

  // Create display station object (don't mutate original)
  let displayName = station.station_name;
  if (stationId === "Crescent_Beach_Ocean") {
    displayName = "Crescent Beach Ocean";
  } else if (stationId === "Crescent_Channel_Ocean") {
    displayName = "Crescent Channel Ocean";
  }

  if (!station.forecast || Object.keys(station.forecast).length === 0) {
    logger.warn("StormSurgeChart", `No forecast data for ${stationId}`);
    return;
  }

  // Prepare data
  const times = [];
  const values = [];
  
  Object.entries(station.forecast)
    .sort(([a], [b]) => new Date(a) - new Date(b))
    .forEach(([timeStr, value]) => {
      times.push(timeStr);
      values.push(value);
    });

  // Initialize chart if needed
  if (!surgeChart) {
    surgeChart = echarts.init(document.getElementById("surge-chart"));
    window.addEventListener("resize", () => surgeChart.resize());
  }

  // Calculate y-axis range for better visualization
  const maxVal = Math.max(...values);
  const minVal = Math.min(...values);
  const range = maxVal - minVal;
  const padding = range * 0.1;
  const yMin = Math.floor((minVal - padding) * 10) / 10;
  const yMax = Math.ceil((maxVal + padding) * 10) / 10;

  // Set chart options
  surgeChart.setOption({
    title: {
      text: `${displayName} - Storm Surge Forecast`,
      left: "center",
      textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14 }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        const idx = params[0].dataIndex;
        const time = new Date(times[idx]).toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver"
        });
        const value = params[0].value;
        const sign = value >= 0 ? "+" : "";
        return `<b>${time}</b><br/>Storm Surge: ${sign}${value.toFixed(2)} m`;
      }
    },
    grid: {
      left: window.innerWidth < 600 ? "8%" : "10%",
      right: window.innerWidth < 600 ? "8%" : "10%",
      bottom: "22%",
      top: "15%",
      containLabel: true
    },
    xAxis: {
      type: "category",
      data: times,
      axisLabel: {
        interval: (index) => {
          // Show labels only at midnight Pacific Time
          const d = new Date(times[index]);
          const hour = parseInt(d.toLocaleString("en-US", { hour: "2-digit", hour12: false, timeZone: "America/Vancouver" }));
          return hour === 0;
        },
        formatter: (value, index) => {
          const d = new Date(value);
          const day = d.toLocaleString("en-US", { day: "2-digit", timeZone: "America/Vancouver" });
          const month = d.toLocaleString("en-US", { month: "short", timeZone: "America/Vancouver" });
          return index === 0 ? `${month} ${day}` : day;
        },
        rotate: window.innerWidth < 600 ? 45 : 0,
        fontSize: 10,
        hideOverlap: true,
        margin: 10
      },
      axisTick: { show: true, alignWithLabel: true },
      splitLine: {
        show: true,
        lineStyle: { color: "#eee" },
        interval: (index) => {
          // Show gridlines only at midnight Pacific Time
          const d = new Date(times[index]);
          const hour = parseInt(d.toLocaleString("en-US", { hour: "2-digit", hour12: false, timeZone: "America/Vancouver" }));
          return hour === 0;
        }
      }
    },
    yAxis: {
      type: "value",
      name: "Surge (m)",
      min: yMin,
      max: yMax,
      axisLabel: {
        formatter: (value) => {
          const sign = value >= 0 ? "+" : "";
          return `${sign}${value.toFixed(1)}`;
        }
      },
      splitLine: { show: true, lineStyle: { color: "#eee" } }
    },
    series: [{
      name: "Storm Surge",
      type: "line",
      data: values,
      smooth: true,
      symbol: "none",
      itemStyle: { color: "#0077be" },
      areaStyle: {
        color: {
          type: "linear",
          x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: "rgba(0, 119, 190, 0.3)" },
            { offset: 1, color: "rgba(0, 119, 190, 0.05)" }
          ]
        }
      },
      markLine: {
        silent: true,
        symbol: "none",
        lineStyle: { type: "dashed", color: "#999", width: 1 },
        label: { 
          show: true, 
          position: "end",
          formatter: "Sea Level"
        },
        data: [{ yAxis: 0 }]
      }
    }]
  });

  // Update metadata display
  updateMetadata(station, times, values, displayName);

  logger.info("StormSurgeChart", `Loaded ${values.length} hours of storm surge forecast for ${displayName}`);
}

function updateMetadata(station, times, values, displayName = null) {
  const metaEl = document.getElementById("surge-metadata");
  if (!metaEl) return;

  const generatedTime = new Date(surgeData.generated_utc);
  const firstForecast = new Date(times[0]);
  const lastForecast = new Date(times[times.length - 1]);

  const formatDate = (date) => date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
    timeZoneName: "short"
  });

  // Extract model run time (00Z or 12Z format)
  let modelRunDisplay = "";
  if (surgeData.model_run_time) {
    const runStr = surgeData.model_run_time;
    const modelRunTime = new Date(runStr.endsWith('Z') || runStr.includes('+') ? runStr : runStr + 'Z');
    const hourUTC = modelRunTime.getUTCHours();
    const dateStr = modelRunTime.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      timeZone: "UTC"
    });
    modelRunDisplay = `${dateStr} ${hourUTC.toString().padStart(2, '0')}Z`;
  }

  // Use displayName if provided, otherwise use station.station_name
  const stationName = displayName || station.station_name;

  metaEl.innerHTML = `
    <strong>Station:</strong> ${stationName}<br/>
    <strong>Location:</strong> ${station.location.lat.toFixed(4)}°N, ${Math.abs(station.location.lon).toFixed(4)}°W<br/>
    <strong>Model:</strong> GDSPS (Global Deterministic Storm Surge Prediction System)<br/>
    ${modelRunDisplay ? `<strong>Model Run:</strong> ${modelRunDisplay}<br/>` : ''}
    <strong>Data Retrieved:</strong> ${formatDate(generatedTime)}<br/>
    <strong>Forecast Period:</strong> ${formatDate(firstForecast)} to ${formatDate(lastForecast)}<br/>
    <strong>Resolution:</strong> ${values.length} hours (1-hour intervals)
  `;
}

// Load on page load
loadStormSurgeData();

// Refresh every 2 hours
setInterval(loadStormSurgeData, 2 * 60 * 60 * 1000);

