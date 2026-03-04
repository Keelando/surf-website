/* ======================================
   Storm Surge Page - Forecast & Hindcast
   ====================================== */

let forecastChart = null;
let hindcastChart = null;
let forecastData = null;
let hindcastData = null;
let observedSurgeData = null;

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

// Calculate minimum date for hindcast: 11 days back from today (12 days total including today)
// Extended to match backend export range
function getHindcastMinDate() {
  const now = new Date();
  const pacificNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/Vancouver' }));
  // Start of today Pacific
  const todayMidnight = new Date(pacificNow);
  todayMidnight.setHours(0, 0, 0, 0);
  // 11 days back from midnight today
  const minDate = new Date(todayMidnight);
  minDate.setDate(minDate.getDate() - 11);
  // Return in YYYY-MM-DD format
  return minDate.toISOString().split('T')[0];
}

/* ======================================
   Forecast Section
   ====================================== */

async function loadForecastData() {
  try {
    forecastData = await fetchWithTimeout(`/data/storm_surge/combined_forecast.json?t=${Date.now()}`);

    initForecastSelector();
    const selectedStation = document.getElementById("forecast-station-select")?.value || "Point_Atkinson";
    updateForecastChart(selectedStation);

  } catch (err) {
    logger.error("StormSurge", "Error loading forecast data", err);
    const container = document.getElementById("forecast-chart");
    if (container) {
      container.innerHTML = '<p style="text-align:center;color:#999;">⚠️ Forecast data unavailable</p>';
    }
  }
}

function initForecastSelector() {
  const selector = document.getElementById("forecast-station-select");
  if (!selector || selector.dataset.initialized) return;

  selector.innerHTML = "";

  STATION_ORDER.forEach(stationId => {
    // Surrey stations reuse Crescent_Beach_Channel forecast
    const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
      ? "Crescent_Beach_Channel"
      : stationId;

    const station = forecastData.stations?.[forecastStationId];
    if (station) {
      const option = document.createElement("option");
      option.value = stationId;

      // Check if station has observed surge data available
      const hasObservedSurge = observedSurgeData?.stations?.[stationId];
      const indicator = hasObservedSurge ? ' 📡' : '';

      // Use proper display name for Surrey stations
      let displayName = station.station_name;
      if (stationId === "Crescent_Beach_Ocean") {
        displayName = "Crescent Beach Ocean";
      } else if (stationId === "Crescent_Channel_Ocean") {
        displayName = "Crescent Channel Ocean";
      }

      option.textContent = displayName + indicator;
      selector.appendChild(option);
    }
  });

  selector.addEventListener("change", (e) => {
    updateForecastChart(e.target.value);
    updateStationIndicator("forecast-station-indicator", e.target.value);
  });

  selector.dataset.initialized = "true";
  updateStationIndicator("forecast-station-indicator", selector.value);
}

function updateStationIndicator(elementId, stationId) {
  const indicator = document.getElementById(elementId);
  if (!indicator) return;

  // Surrey stations reuse Crescent_Beach_Channel forecast
  const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
    ? "Crescent_Beach_Channel"
    : stationId;

  let stationName = "";
  if (elementId.includes("forecast") && forecastData?.stations?.[forecastStationId]) {
    stationName = forecastData.stations[forecastStationId].station_name;
  } else if (elementId.includes("hindcast") && hindcastData?.stations?.[forecastStationId]) {
    stationName = hindcastData.stations[forecastStationId].station_name;
  }

  // Use proper display name for Surrey stations
  if (stationId === "Crescent_Beach_Ocean") {
    stationName = "Crescent Beach Ocean";
  } else if (stationId === "Crescent_Channel_Ocean") {
    stationName = "Crescent Channel Ocean";
  }

  if (stationName) {
    indicator.textContent = `📍 Viewing: ${stationName}`;
  }
}

function updatePeakToday(stationId) {
  const display = document.getElementById("peak-surge-display");
  const peakTodayValue = document.getElementById("peak-today-value");
  const peakTodayTime = document.getElementById("peak-today-time");
  const peak3DayValue = document.getElementById("peak-3day-value");
  const peak3DayTime = document.getElementById("peak-3day-time");
  const peak7DayValue = document.getElementById("peak-7day-value");
  const peak7DayTime = document.getElementById("peak-7day-time");

  if (!display) return;

  const station = forecastData?.stations?.[stationId];
  if (!station?.forecast) {
    display.style.display = "none";
    return;
  }

  // Get current time in Pacific
  const now = new Date();
  const pacificNow = new Date(now.toLocaleString("en-US", { timeZone: "America/Vancouver" }));

  // Define time ranges (in hours from now)
  // 0-24hr, 24-72hrs, 72-156hrs
  const ranges = [
    {
      startHours: 0,
      endHours: 24,
      valueEl: peakTodayValue,
      timeEl: peakTodayTime,
      label: "Next 24 Hours"
    },
    {
      startHours: 24,
      endHours: 72,
      valueEl: peak3DayValue,
      timeEl: peak3DayTime,
      label: "24-72 Hours"
    },
    {
      startHours: 72,
      endHours: 168,
      valueEl: peak7DayValue,
      timeEl: peak7DayTime,
      label: "72-168 Hours"
    }
  ];

  // Find peak for each range
  ranges.forEach(range => {
    const rangeStart = new Date(now.getTime() + range.startHours * 60 * 60 * 1000);
    const rangeEnd = new Date(now.getTime() + range.endHours * 60 * 60 * 1000);

    let peakSurge = null;
    let peakTimeStr = null;

    Object.entries(station.forecast).forEach(([timeStr, value]) => {
      const forecastTime = new Date(timeStr);

      if (forecastTime >= rangeStart && forecastTime < rangeEnd) {
        if (peakSurge === null || Math.abs(value) > Math.abs(peakSurge)) {
          peakSurge = value;
          peakTimeStr = timeStr;
        }
      }
    });

    // Store peak data in range object for return
    range.peakSurge = peakSurge;
    range.peakTimeStr = peakTimeStr;

    // Display peak if found
    if (peakSurge !== null && peakTimeStr && range.valueEl && range.timeEl) {
      const sign = peakSurge >= 0 ? "+" : "";
      range.valueEl.textContent = `${sign}${peakSurge.toFixed(2)} m`;

      const peakDate = new Date(peakTimeStr);
      const timeFormatted = peakDate.toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
        timeZone: "America/Vancouver"
      });
      range.timeEl.textContent = timeFormatted;
    } else if (range.valueEl && range.timeEl) {
      range.valueEl.textContent = "—";
      range.timeEl.textContent = "No data";
    }
  });

  display.style.display = "block";

  // Return peak data for chart markers
  return ranges.map(range => ({
    time: range.peakTimeStr,
    value: range.peakSurge,
    label: range.label
  })).filter(p => p.time && p.value !== null);
}

function updateForecastChart(stationId) {
  // Surrey stations reuse Crescent_Beach_Channel forecast (same area or exact same location)
  let forecastStationId = stationId;
  let displayName = null;

  if (stationId === "Crescent_Beach_Ocean") {
    forecastStationId = "Crescent_Beach_Channel";
    displayName = "Crescent Beach Ocean";
  } else if (stationId === "Crescent_Channel_Ocean") {
    forecastStationId = "Crescent_Beach_Channel";
    displayName = "Crescent Channel Ocean";
  }

  if (!forecastData?.stations?.[forecastStationId]) {
    logger.warn("StormSurge", `No forecast data found for station: ${stationId}`);
    return;
  }

  const station = forecastData.stations[forecastStationId];

  // Create display station object (don't mutate original)
  const displayStation = displayName ? {
    ...station,
    station_name: displayName
  } : station;

  if (!station.forecast || Object.keys(station.forecast).length === 0) {
    logger.warn("StormSurge", `No forecast data for ${stationId}`);
    return;
  }

  // Update peak display and get peak data for markers (use forecastStationId for lookup)
  const peakData = updatePeakToday(forecastStationId);

  // Prepare data
  const forecastData_series = [];

  Object.entries(station.forecast)
    .sort(([a], [b]) => new Date(a) - new Date(b))
    .forEach(([timeStr, value]) => {
      forecastData_series.push([timeStr, value]);
    });

  // Initialize chart if needed
  if (!forecastChart) {
    forecastChart = echarts.init(document.getElementById("forecast-chart"));
    window.addEventListener("resize", () => forecastChart.resize());
  }

  // Calculate y-axis range
  const values = forecastData_series.map(d => d[1]);
  const maxVal = Math.max(...values);
  const minVal = Math.min(...values);
  const range = maxVal - minVal;
  const padding = Math.max(range * 0.2, 0.1); // At least 0.1m padding
  const yMin = Math.floor((minVal - padding) * 10) / 10;
  const yMax = Math.ceil((maxVal + padding) * 10) / 10;

  // Calculate midnight boundaries in Pacific timezone for gridlines
  const midnightLines = [];
  if (forecastData_series.length > 0) {
    const firstTime = new Date(forecastData_series[0][0]);
    const lastTime = new Date(forecastData_series[forecastData_series.length - 1][0]);

    // Helper: Get midnight Pacific as UTC for a given UTC date
    function getMidnightPacificAsUTC(utcDate) {
      const year = parseInt(utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", year: 'numeric' }));
      const month = parseInt(utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", month: 'numeric' }));
      const day = parseInt(utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", day: 'numeric' }));

      // Midnight Pacific is either 07:00 or 08:00 UTC depending on DST
      // Try 08:00 UTC first (PST)
      let testDate = new Date(Date.UTC(year, month - 1, day, 8, 0, 0, 0));
      let testHour = parseInt(testDate.toLocaleString("en-US", {
        timeZone: "America/Vancouver",
        hour: 'numeric',
        hour12: false
      }));

      if (testHour === 0) return testDate;

      // Try 07:00 UTC (PDT)
      testDate = new Date(Date.UTC(year, month - 1, day, 7, 0, 0, 0));
      return testDate;
    }

    // Get first midnight Pacific after start time
    let currentDate = new Date(firstTime);
    let currentMidnight = getMidnightPacificAsUTC(currentDate);

    // If this midnight is before our start, move to next day
    if (currentMidnight <= firstTime) {
      currentDate.setUTCDate(currentDate.getUTCDate() + 1);
      currentMidnight = getMidnightPacificAsUTC(currentDate);
    }

    // Add vertical lines for each midnight up to the last time
    while (currentMidnight <= lastTime) {
      midnightLines.push({
        xAxis: currentMidnight.toISOString(),
        lineStyle: { color: '#ddd', type: 'solid', width: 1 },
        label: { show: false }
      });

      // Move to next day
      currentDate.setUTCDate(currentDate.getUTCDate() + 1);
      currentMidnight = getMidnightPacificAsUTC(currentDate);
    }
  }

  // Prepare series array
  const series = [];

  // Prepare markPoint data for peak surge values
  const markPointData = [];
  const peakLabels = []; // For legend
  if (peakData && peakData.length > 0) {
    peakData.forEach((peak) => {
      if (peak.time && peak.value !== null) {
        markPointData.push({
          coord: [peak.time, peak.value],
          itemStyle: {
            color: '#ff4444',
            borderColor: '#fff',
            borderWidth: 2
          }
        });
        // Collect labels for legend
        const sign = peak.value >= 0 ? '+' : '';
        peakLabels.push(`${peak.label}: ${sign}${peak.value.toFixed(2)}m`);
      }
    });
  }

  // Add forecast series
  series.push({
    name: "Storm Surge Forecast",
    type: "line",
    data: forecastData_series,
    smooth: true,
    symbol: "none",
    legendHoverLink: false,
    itemStyle: { color: "#0077be" },
    lineStyle: { width: 2 },
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
      data: [
        // Sea level line (horizontal)
        {
          yAxis: 0,
          lineStyle: { type: "dashed", color: "#999", width: 1 },
          label: {
            show: true,
            position: "end",
            formatter: "Sea Level"
          }
        },
        // Midnight gridlines (vertical)
        ...midnightLines
      ]
    },
    markPoint: markPointData.length > 0 ? {
      data: markPointData,
      symbol: 'circle',
      symbolSize: 10
    } : undefined
  });

  // Set chart options (notMerge: true to replace all data when switching stations)
  forecastChart.setOption({
    title: {
      text: window.innerWidth < 600 ? displayStation.station_name : `${displayStation.station_name} - Surge Forecast`,
      subtext: window.innerWidth < 600 ? 'Surge Forecast' : '',
      left: "center",
      textStyle: {
        fontSize: window.innerWidth < 600 ? 11 : 14,
        fontWeight: 'bold',
        overflow: 'truncate',
        width: window.innerWidth < 600 ? window.innerWidth - 40 : null
      },
      subtextStyle: {
        fontSize: 10,
        color: '#666'
      }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = new Date(params[0].data[0]).toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver",
          timeZoneName: "short"
        });
        let tooltip = `<b>${time}</b><br/>`;
        params.forEach(param => {
          const value = param.data[1];
          const sign = value >= 0 ? "+" : "";
          tooltip += `${param.marker} ${param.seriesName}: ${sign}${value.toFixed(3)} m<br/>`;
        });
        return tooltip;
      }
    },
    grid: {
      left: window.innerWidth < 600 ? "8%" : "8%",
      right: window.innerWidth < 600 ? "4%" : "6%",
      bottom: peakLabels.length > 0 ? "25%" : "15%", // More space when showing peak labels
      top: "15%",
      containLabel: true
    },
    xAxis: {
      type: "time",
      axisLabel: {
        formatter: (value) => {
          const d = new Date(value);
          return d.toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            timeZone: "America/Vancouver"
          });
        },
        rotate: window.innerWidth < 600 ? 30 : 0,
        fontSize: 10,
        hideOverlap: true
      },
      axisTick: { show: true },
      splitLine: { show: true, lineStyle: { color: "#eee" } }
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
    legend: {
      show: peakLabels.length > 0,
      data: peakLabels.length > 0 ? [
        {
          name: 'Storm Surge Forecast',
          icon: 'path://M0,0 L100,0'  // Just a line, no marker
        },
        {
          name: `🔴 Peaks: ${peakLabels.join(' | ')}`,
          icon: 'circle',
          itemStyle: { color: '#ff4444', borderColor: '#fff', borderWidth: 2 }
        }
      ] : ['Storm Surge Forecast'],
      bottom: 5,
      left: 'center',
      right: 20,
      textStyle: { fontSize: window.innerWidth < 600 ? 8 : 10 },
      itemGap: window.innerWidth < 600 ? 8 : 15,
      padding: [5, 10]
    },
    series: series
  }, true); // notMerge: true to prevent old data from persisting

  // Update metadata
  updateForecastMetadata(displayStation, forecastData_series.map(d => d[0]), values);

  logger.info("StormSurge", `Loaded ${values.length} hours of forecast for ${displayStation.station_name}`);
}

function updateForecastMetadata(station, times, values) {
  const metaEl = document.getElementById("forecast-metadata");
  if (!metaEl) return;

  const generatedTime = new Date(forecastData.generated_utc);
  const firstForecast = new Date(times[0]);
  const lastForecast = new Date(times[times.length - 1]);

  const maxSurge = Math.max(...values);
  const minSurge = Math.min(...values);
  const maxTime = times[values.indexOf(maxSurge)];
  const minTime = times[values.indexOf(minSurge)];

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
  if (forecastData.model_run_time) {
    const runStr = forecastData.model_run_time;
    const modelRunTime = new Date(runStr.endsWith('Z') || runStr.includes('+') ? runStr : runStr + 'Z');
    const hourUTC = modelRunTime.getUTCHours();
    const dateStr = modelRunTime.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      timeZone: "UTC"
    });
    modelRunDisplay = `${dateStr} ${hourUTC.toString().padStart(2, '0')}Z`;
  }

  metaEl.innerHTML = `
    <strong>Station:</strong> ${station.station_name}<br/>
    <strong>Location:</strong> ${station.location.lat.toFixed(4)}°N, ${Math.abs(station.location.lon).toFixed(4)}°W<br/>
    ${modelRunDisplay ? `<strong>Model Run:</strong> ${modelRunDisplay}<br/>` : ''}
    <strong>Data Retrieved:</strong> ${formatDate(generatedTime)}<br/>
    <strong>Forecast Period:</strong> ${formatDate(firstForecast)} to ${formatDate(lastForecast)}<br/>
    <strong>Resolution:</strong> ${values.length} hours (1-hour intervals)<br/>
    <strong>Peak High:</strong> +${maxSurge.toFixed(3)} m at ${formatDate(new Date(maxTime))}<br/>
    <strong>Peak Low:</strong> ${minSurge.toFixed(3)} m at ${formatDate(new Date(minTime))}
  `;
}

/* ======================================
   Observed Surge Data
   ====================================== */

async function loadObservedSurgeData() {
  try {
    observedSurgeData = await fetchWithTimeout(`/data/storm_surge/observed_surge.json?t=${Date.now()}`);
    logger.info("StormSurge", `Loaded observed surge data for ${Object.keys(observedSurgeData.stations || {}).length} stations`);
  } catch (err) {
    logger.warn("StormSurge", "Observed surge data not available", err.message);
    observedSurgeData = null;
  }
}

/* ======================================
   Hindcast Section

   Displays 12 days of storm surge predictions (48h lead time) vs. 10 days of observations.
   See /docs/HINDCAST_METHODOLOGY.md for detailed methodology.
   ====================================== */

async function loadHindcastData() {
  try {
    hindcastData = await fetchWithTimeout(`/data/storm_surge/hindcast.json?t=${Date.now()}`);

    initHindcastSelector();
    const selectedStation = document.getElementById("hindcast-station-select")?.value || "Point_Atkinson";
    updateHindcastChart(selectedStation);

  } catch (err) {
    logger.error("StormSurge", "Error loading hindcast data", err);
    const container = document.getElementById("hindcast-chart");
    if (container) {
      container.innerHTML = '<p style="text-align:center;color:#999;">⚠️ Hindcast data unavailable</p>';
    }
  }
}

function initHindcastSelector() {
  const selector = document.getElementById("hindcast-station-select");
  if (!selector || selector.dataset.initialized) return;

  selector.innerHTML = "";

  // Only show stations that have hindcast data
  STATION_ORDER.forEach(stationId => {
    // Surrey stations reuse Crescent_Beach_Channel forecast
    const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
      ? "Crescent_Beach_Channel"
      : stationId;

    const station = hindcastData.stations?.[forecastStationId];
    if (station && station.hindcast && station.hindcast.length > 0) {
      const option = document.createElement("option");
      option.value = stationId;

      // Check if station has observed surge data available
      const hasObservedSurge = observedSurgeData?.stations?.[stationId];
      const indicator = hasObservedSurge ? ' 📡' : '';

      // Use proper display name for Surrey stations
      let displayName = station.station_name;
      if (stationId === "Crescent_Beach_Ocean") {
        displayName = "Crescent Beach Ocean";
      } else if (stationId === "Crescent_Channel_Ocean") {
        displayName = "Crescent Channel Ocean";
      }

      option.textContent = displayName + indicator;
      selector.appendChild(option);
    }
  });

  selector.addEventListener("change", (e) => {
    updateHindcastChart(e.target.value);
    updateStationIndicator("hindcast-station-indicator", e.target.value);
  });

  selector.dataset.initialized = "true";
  updateStationIndicator("hindcast-station-indicator", selector.value);
}

function updateHindcastChart(stationId) {
  // Surrey stations reuse Crescent_Beach_Channel hindcast
  const forecastStationId = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean")
    ? "Crescent_Beach_Channel"
    : stationId;

  if (!hindcastData?.stations?.[forecastStationId]) {
    logger.warn("StormSurge", `No hindcast data found for station: ${stationId}`);
    return;
  }

  const station = hindcastData.stations[forecastStationId];

  // Create display station object (don't mutate original)
  const displayStation = (stationId === "Crescent_Beach_Ocean" || stationId === "Crescent_Channel_Ocean") ? {
    ...station,
    station_name: stationId === "Crescent_Beach_Ocean" ? "Crescent Beach Ocean" : "Crescent Channel Ocean"
  } : station;

  if (!station.hindcast || station.hindcast.length === 0) {
    const container = document.getElementById("hindcast-chart");
    if (container) {
      container.innerHTML = '<p style="text-align:center;color:#999;">No hindcast data available for this station yet. Data accumulates over time.</p>';
    }
    return;
  }

  // Prepare data - group by forecast date
  // Filter out data before the minimum date (9 days back from today)
  const minDate = getHindcastMinDate();

  // Calculate midnight tonight (Pacific time) - only show up to today
  const now = new Date();
  const pacificNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/Vancouver' }));
  const midnightTonight = new Date(pacificNow);
  midnightTonight.setHours(23, 59, 59, 999);
  const midnightTonightUTC = new Date(midnightTonight.toLocaleString('en-US', { timeZone: 'UTC' }));

  const forecastDates = {};

  station.hindcast.forEach(point => {
    const date = point.forecast_date;
    const pointTime = new Date(point.time);

    // Skip data before the minimum date
    if (date < minDate) {
      return;
    }

    // Skip data beyond midnight tonight (Pacific) - only show today
    if (pointTime > midnightTonightUTC) {
      return;
    }

    if (!forecastDates[date]) {
      forecastDates[date] = {
        times: [],
        values: []
      };
    }
    forecastDates[date].times.push(point.time);
    forecastDates[date].values.push(point.value);
  });

  // Sort dates
  const sortedDates = Object.keys(forecastDates).sort();

  // Check if we have any data after filtering
  if (sortedDates.length === 0) {
    const container = document.getElementById("hindcast-chart");
    if (container) {
      container.innerHTML = `<p style="text-align:center;color:#999;">No hindcast data available for this station from ${minDate} onwards. Data accumulates over time.</p>`;
    }
    return;
  }

  // Prepare series for each forecast date
  const series = sortedDates.map((date, index) => {
    const data = forecastDates[date];
    const color = getColorForIndex(index, sortedDates.length);

    return {
      name: '', // Don't show individual forecast dates in legend
      type: "line",
      data: data.times.map((time, i) => [time, data.values[i]]),
      smooth: true,
      symbol: "circle",
      symbolSize: 4,
      itemStyle: { color: color },
      lineStyle: { width: 2 }
    };
  });

  // Add observed surge data if available for this station
  if (observedSurgeData?.stations?.[stationId]) {
    const obsStation = observedSurgeData.stations[stationId];
    const obsData = obsStation.data.map(d => [d.time, d.observed_surge_m]);

    series.push({
      name: "Observed Surge (Actual)",
      type: "line",
      data: obsData,
      smooth: false,
      symbol: "circle",
      symbolSize: 3,
      itemStyle: { color: "#000000" },
      lineStyle: { width: 3, type: "solid" },
      z: 10 // Render on top
    });

    logger.debug("StormSurge", `Added ${obsData.length} observed surge points`);
  }

  // Get all unique times for x-axis (only from filtered data)
  const allTimes = [...new Set(
    station.hindcast
      .filter(p => p.forecast_date >= minDate)
      .map(p => p.time)
  )].sort();

  // Calculate midnight boundaries in Pacific timezone for gridlines
  const midnightLines = [];
  if (allTimes.length > 0) {
    const firstTime = new Date(allTimes[0]);
    const lastTime = new Date(allTimes[allTimes.length - 1]);

    // Helper: Get midnight Pacific as UTC for a given UTC date
    function getMidnightPacificAsUTC(utcDate) {
      const year = parseInt(utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", year: 'numeric' }));
      const month = parseInt(utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", month: 'numeric' }));
      const day = parseInt(utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", day: 'numeric' }));

      // Midnight Pacific is either 07:00 or 08:00 UTC depending on DST
      // Try 08:00 UTC first (PST)
      let testDate = new Date(Date.UTC(year, month - 1, day, 8, 0, 0, 0));
      let testHour = parseInt(testDate.toLocaleString("en-US", {
        timeZone: "America/Vancouver",
        hour: 'numeric',
        hour12: false
      }));

      if (testHour === 0) return testDate;

      // Try 07:00 UTC (PDT)
      testDate = new Date(Date.UTC(year, month - 1, day, 7, 0, 0, 0));
      return testDate;
    }

    // Get first midnight Pacific after start time
    let currentDate = new Date(firstTime);
    let currentMidnight = getMidnightPacificAsUTC(currentDate);

    // If this midnight is before our start, move to next day
    if (currentMidnight <= firstTime) {
      currentDate.setUTCDate(currentDate.getUTCDate() + 1);
      currentMidnight = getMidnightPacificAsUTC(currentDate);
    }

    // Add vertical lines for each midnight up to the last time
    while (currentMidnight <= lastTime) {
      midnightLines.push({
        xAxis: currentMidnight.toISOString(),
        lineStyle: { color: '#ddd', type: 'solid', width: 1 },
        label: { show: false }
      });

      // Move to next day
      currentDate.setUTCDate(currentDate.getUTCDate() + 1);
      currentMidnight = getMidnightPacificAsUTC(currentDate);
    }
  }

  // Initialize chart if needed
  if (!hindcastChart) {
    hindcastChart = echarts.init(document.getElementById("hindcast-chart"));
    window.addEventListener("resize", () => hindcastChart.resize());
  }

  // Set chart options (notMerge: true to replace all data when switching stations)
  hindcastChart.setOption({
    title: {
      text: window.innerWidth < 600
        ? `${displayStation.station_name} - Hindcast`
        : `${displayStation.station_name} - Hindcast Comparison (48h Predictions)`,
      subtext: window.innerWidth < 600
        ? 'Observed (black) vs Forecast runs (colored)'
        : 'Black line = Tide offset observations | Colored lines = Historical forecast runs',
      left: "center",
      textStyle: {
        fontSize: window.innerWidth < 600 ? 11 : 14,
        fontWeight: 'bold',
        overflow: 'truncate',
        width: window.innerWidth < 600 ? window.innerWidth - 40 : null
      },
      subtextStyle: { fontSize: 10, color: '#666' }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";

        const time = new Date(params[0].data[0]).toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver"
        });

        let tooltip = `<b>${time}</b><br/>`;
        params.forEach(param => {
          const value = param.data[1];
          const sign = value >= 0 ? "+" : "";
          tooltip += `${param.marker} ${param.seriesName}: ${sign}${value.toFixed(3)} m<br/>`;
        });

        return tooltip;
      }
    },
    legend: {
      show: true,
      bottom: 0,
      left: "center",
      data: ['Observed Surge (Actual)'], // Only show observed surge in legend
      type: "plain"
    },
    grid: {
      left: window.innerWidth < 600 ? "8%" : "8%",
      right: window.innerWidth < 600 ? "4%" : "6%",
      bottom: "18%",
      top: "15%",
      containLabel: true
    },
    xAxis: {
      type: "time",
      axisLabel: {
        formatter: (value) => {
          const d = new Date(value);
          return d.toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            timeZone: "America/Vancouver"
          });
        },
        rotate: window.innerWidth < 600 ? 45 : 0,
        fontSize: 10
      },
      splitLine: { show: true, lineStyle: { color: "#eee" } }
    },
    yAxis: {
      type: "value",
      name: "Surge (m)",
      axisLabel: {
        formatter: (value) => {
          const sign = value >= 0 ? "+" : "";
          return `${sign}${value.toFixed(2)}`;
        }
      },
      splitLine: { show: true, lineStyle: { color: "#eee" } }
    },
    series: series.concat([{
      // Zero reference line with midnight gridlines
      name: "Sea Level",
      type: "line",
      data: allTimes.map(t => [t, 0]),
      lineStyle: { type: "dashed", color: "#999", width: 1 },
      symbol: "none",
      showSymbol: false,
      silent: true,
      markLine: midnightLines.length > 0 ? {
        silent: true,
        symbol: "none",
        data: midnightLines
      } : undefined
    }])
  }, true); // notMerge: true to prevent old data from persisting

  // Update metadata
  updateHindcastMetadata(displayStation);

  logger.info("StormSurge", `Loaded hindcast data for ${displayStation.station_name} (${sortedDates.length} forecast dates)`);
}

function getColorForIndex(index, total) {
  const colors = [
    "#e53935", // red
    "#1e88e5", // blue
    "#43a047", // green
    "#fb8c00", // orange
    "#8e24aa", // purple
    "#00acc1", // cyan
    "#fdd835", // yellow
    "#6d4c41", // brown
    "#546e7a", // blue-grey
    "#f06292"  // pink
  ];
  return colors[index % colors.length];
}

function updateHindcastMetadata(station) {
  const metaEl = document.getElementById("hindcast-metadata");
  if (!metaEl) return;

  const generatedTime = new Date(hindcastData.generated_utc);
  const daysAvailable = hindcastData.actual_days_available || 0;

  const formatDate = (date) => date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
    timeZoneName: "short"
  });

  // Extract model run time if available (hindcast data uses 12Z runs)
  let modelRunDisplay = "12Z model run";
  if (hindcastData.model_run_time) {
    const runStr = hindcastData.model_run_time;
    const modelRunTime = new Date(runStr.endsWith('Z') || runStr.includes('+') ? runStr : runStr + 'Z');
    const hourUTC = modelRunTime.getUTCHours();
    const dateStr = modelRunTime.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      timeZone: "UTC"
    });
    modelRunDisplay = `${dateStr} ${hourUTC.toString().padStart(2, '0')}Z`;
  }

  metaEl.innerHTML = `
    <strong>Station:</strong> ${station.station_name}<br/>
    <strong>Location:</strong> ${station.location.lat.toFixed(4)}°N, ${Math.abs(station.location.lon).toFixed(4)}°W<br/>
    <strong>Data Retrieved:</strong> ${formatDate(generatedTime)}<br/>
    <strong>Forecast Horizon:</strong> ${hindcastData.forecast_horizon_hours || 48} hours ahead<br/>
    <strong>Historical Days:</strong> ${daysAvailable} day${daysAvailable !== 1 ? 's' : ''} (max ${hindcastData.max_days_back || 10})<br/>
    <strong>Collection Time:</strong> ${modelRunDisplay}
  `;
}

/* ======================================
   Page Initialization
   ====================================== */

function initPage() {
  // Update timestamp in footer
  const timestampEl = document.getElementById("timestamp");
  if (timestampEl) {
    timestampEl.textContent = new Date().toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "America/Vancouver",
      timeZoneName: "short"
    });
  }

  // Load all datasets (observed surge first, then charts)
  loadObservedSurgeData().then(() => {
    loadForecastData();
    loadHindcastData();
  });
}

// Initialize on page load
document.addEventListener("DOMContentLoaded", initPage);

// Refresh data every 2 hours
setInterval(() => {
  loadObservedSurgeData().then(() => {
    loadForecastData();
    loadHindcastData();
  });
}, 2 * 60 * 60 * 1000);

/* ======================================
   Show on Map Navigation Functions
   ====================================== */

// Show selected surge station on map from forecast selector
function showSelectedForecastSurgeOnMap() {
  const select = document.getElementById('forecast-station-select');
  if (!select || !select.value) return;

  // Navigate to index.html with surge station in hash
  window.location.href = `/#surge-${select.value}`;
}

// Show selected surge station on map from hindcast selector
function showSelectedHindcastSurgeOnMap() {
  const select = document.getElementById('hindcast-station-select');
  if (!select || !select.value) return;

  // Navigate to index.html with surge station in hash
  window.location.href = `/#surge-${select.value}`;
}

// Make functions globally accessible
window.showSelectedForecastSurgeOnMap = showSelectedForecastSurgeOnMap;
window.showSelectedHindcastSurgeOnMap = showSelectedHindcastSurgeOnMap;
