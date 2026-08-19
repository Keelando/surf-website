/* ======================================
   Storm Surge Page - Forecast & Verification (ES module)

   Chart helpers (fetchWithTimeout, getChartThemeColors,
   getMobileOptimizedTooltipConfig, registerChartThemeListener, echarts)
   still come from classic scripts loaded before this one.
   ====================================== */

import {
  formatModelRunTime,
  formatMonthDayTime,
  formatMonthDayTimeTZ,
} from "./shared/format-time.js";

const charts = { forecast: null, verification: null };
const themeListeners = { forecast: null, verification: null };
let forecastData = null;
let verificationData = null;
let observedSurgeData = null;

// Station display order
const STATION_ORDER = [
  "Point_Atkinson",
  "Crescent_Channel_Ocean", // Surrey - reuses Crescent_Beach_Channel forecast
  "Crescent_Beach_Ocean", // Surrey - reuses Crescent_Beach_Channel forecast
  "Campbell_River",
  "Neah_Bay",
  "New_Dungeness",
  "Tofino",
];

// Surrey stations reuse the Crescent_Beach_Channel forecast (same area or
// exact same location) under their own display names.
const SURREY_DISPLAY_NAMES = {
  Crescent_Beach_Ocean: "Crescent Beach Ocean",
  Crescent_Channel_Ocean: "Crescent Channel Ocean",
};

function getForecastStationId(stationId) {
  return SURREY_DISPLAY_NAMES[stationId] ? "Crescent_Beach_Channel" : stationId;
}

function getDisplayName(stationId, fallback) {
  return SURREY_DISPLAY_NAMES[stationId] || fallback;
}

function setSafeHTML(element, html) {
  if (!element) return;

  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

// Calculate minimum date for the verification window: 11 days back from today (12 days total including today)
// Extended to match backend export range
function getVerificationMinDate() {
  const now = new Date();
  const pacificNow = new Date(now.toLocaleString("en-US", { timeZone: "America/Vancouver" }));
  // Start of today Pacific
  const todayMidnight = new Date(pacificNow);
  todayMidnight.setHours(0, 0, 0, 0);
  // 11 days back from midnight today
  const minDate = new Date(todayMidnight);
  minDate.setDate(minDate.getDate() - 11);
  // Return in YYYY-MM-DD format
  return minDate.toISOString().split("T")[0];
}

// Helper: convert a UTC date to the corresponding midnight in the Pacific timezone
function getMidnightPacificAsUTC(utcDate) {
  const year = parseInt(
    utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", year: "numeric" }),
    10,
  );
  const month = parseInt(
    utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", month: "numeric" }),
    10,
  );
  const day = parseInt(
    utcDate.toLocaleString("en-US", { timeZone: "America/Vancouver", day: "numeric" }),
    10,
  );

  // Midnight Pacific is either 07:00 or 08:00 UTC depending on DST
  let testDate = new Date(Date.UTC(year, month - 1, day, 8, 0, 0, 0));
  const testHour = parseInt(
    testDate.toLocaleString("en-US", {
      timeZone: "America/Vancouver",
      hour: "numeric",
      hour12: false,
    }),
    10,
  );

  if (testHour === 0) {
    return testDate;
  }

  // Try 07:00 UTC (PDT)
  return new Date(Date.UTC(year, month - 1, day, 7, 0, 0, 0));
}

// Vertical markLine entries at each Pacific midnight between two times
function buildMidnightMarkLines(firstTime, lastTime, gridColor) {
  const lines = [];
  const currentDate = new Date(firstTime);
  let currentMidnight = getMidnightPacificAsUTC(currentDate);

  // If this midnight is before our start, move to next day
  if (currentMidnight <= firstTime) {
    currentDate.setUTCDate(currentDate.getUTCDate() + 1);
    currentMidnight = getMidnightPacificAsUTC(currentDate);
  }

  while (currentMidnight <= lastTime) {
    lines.push({
      xAxis: currentMidnight.toISOString(),
      lineStyle: { color: gridColor, type: "solid", width: 1 },
      label: { show: false },
    });

    currentDate.setUTCDate(currentDate.getUTCDate() + 1);
    currentMidnight = getMidnightPacificAsUTC(currentDate);
  }

  return lines;
}

/* ======================================
   Shared chart scaffolding — everything the forecast and verification charts
   do identically, with the real differences passed in as options.
   Series + legend construction stays per-chart.
   ====================================== */

function signed(value, digits) {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
}

function showChartMessage(containerId, message) {
  const container = document.getElementById(containerId);
  if (container) {
    setSafeHTML(
      container,
      `<p style="text-align:center;color:var(--color-text-muted,#999);">${message}</p>`,
    );
  }
}

// Look up a station in a dataset and apply Surrey display-name overrides.
// Returns null (with a warning) when the dataset has no entry for it.
function resolveStation(data, stationId, kind) {
  const forecastStationId = getForecastStationId(stationId);
  const station = data?.stations?.[forecastStationId];
  if (!station) {
    logger.warn("StormSurge", `No ${kind} data found for station: ${stationId}`);
    return null;
  }
  const displayName = getDisplayName(stationId, null);
  return {
    forecastStationId,
    station,
    // Override the display name without mutating the original
    displayStation: displayName ? { ...station, station_name: displayName } : station,
  };
}

function ensureChart(key, elementId) {
  if (!charts[key]) {
    charts[key] = echarts.init(document.getElementById(elementId));
    window.addEventListener("resize", () => charts[key].resize());
  }
  return charts[key];
}

function ensureThemeRefresh(key, selectorId, update) {
  if (themeListeners[key]) return;
  themeListeners[key] = registerChartThemeListener(() => {
    update(document.getElementById(selectorId)?.value || "Point_Atkinson");
  });
}

function initStationSelector({ selectorId, indicatorId, data, hasData, onChange }) {
  const selector = document.getElementById(selectorId);
  if (!selector || selector.dataset.initialized) return;

  selector.textContent = "";

  STATION_ORDER.forEach((stationId) => {
    const station = data.stations?.[getForecastStationId(stationId)];
    if (station && hasData(station)) {
      const option = document.createElement("option");
      option.value = stationId;

      // 📡 marks stations with observed surge data available
      const indicator = observedSurgeData?.stations?.[stationId] ? " 📡" : "";
      option.textContent = getDisplayName(stationId, station.station_name) + indicator;
      selector.appendChild(option);
    }
  });

  selector.addEventListener("change", (e) => {
    onChange(e.target.value);
    updateStationIndicator(indicatorId, e.target.value, data);
  });

  selector.dataset.initialized = "true";
  updateStationIndicator(indicatorId, selector.value, data);
}

function baseSurgeChartOption(theme, opts) {
  const mobile = window.innerWidth < 600;
  return {
    backgroundColor: theme.background,
    textStyle: { color: theme.text },
    title: {
      text: mobile ? opts.mobileTitle : opts.title,
      subtext: mobile ? opts.mobileSubtext : opts.subtext,
      left: "center",
      textStyle: {
        fontSize: mobile ? 11 : 14,
        fontWeight: "bold",
        overflow: "truncate",
        width: mobile ? window.innerWidth - 40 : null,
        color: theme.text,
      },
      subtextStyle: { fontSize: 10, color: theme.mutedText },
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        let tooltip = `<b>${opts.tooltipTime(params[0].data[0])}</b><br/>`;
        params.forEach((param) => {
          tooltip += `${param.marker} ${param.seriesName}: ${signed(param.data[1], 3)} m<br/>`;
        });
        return tooltip;
      },
    },
    grid: {
      left: "8%",
      right: mobile ? "4%" : "6%",
      bottom: opts.gridBottom,
      top: "15%",
      containLabel: true,
    },
    xAxis: {
      type: "time",
      axisLabel: {
        formatter: (value) =>
          new Date(value).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            timeZone: "America/Vancouver",
          }),
        rotate: mobile ? opts.mobileXRotate : 0,
        fontSize: 10,
        hideOverlap: opts.xHideOverlap,
        color: theme.mutedText,
      },
      axisTick: { show: true },
      axisLine: { lineStyle: { color: theme.axisLine } },
      splitLine: { show: true, lineStyle: { color: theme.gridLine } },
    },
    yAxis: {
      type: "value",
      name: "Surge (m)",
      ...opts.yRange,
      axisLabel: {
        formatter: (value) => signed(value, opts.yDigits),
        color: theme.mutedText,
      },
      nameTextStyle: { color: theme.text },
      axisLine: { lineStyle: { color: theme.axisLine } },
      splitLine: { show: true, lineStyle: { color: theme.gridLine } },
    },
  };
}

/* ======================================
   Forecast Section
   ====================================== */

async function loadForecastData() {
  try {
    forecastData = await fetchWithTimeout(
      `/data/storm_surge/combined_forecast.json?t=${Date.now()}`,
    );

    initStationSelector({
      selectorId: "forecast-station-select",
      indicatorId: "forecast-station-indicator",
      data: forecastData,
      hasData: () => true,
      onChange: updateForecastChart,
    });
    const selectedStation =
      document.getElementById("forecast-station-select")?.value || "Point_Atkinson";
    updateForecastChart(selectedStation);
  } catch (err) {
    logger.error("StormSurge", "Error loading forecast data", err);
    showChartMessage("forecast-chart", "⚠️ Forecast data unavailable");
  }
}

function updateStationIndicator(elementId, stationId, data) {
  const indicator = document.getElementById(elementId);
  if (!indicator) return;

  const stationName = getDisplayName(
    stationId,
    data?.stations?.[getForecastStationId(stationId)]?.station_name || "",
  );

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

  const now = new Date();

  // Define time ranges (in hours from now)
  // 0-24hr, 24-72hrs, 72-156hrs
  const ranges = [
    {
      startHours: 0,
      endHours: 24,
      valueEl: peakTodayValue,
      timeEl: peakTodayTime,
      label: "Next 24 Hours",
    },
    {
      startHours: 24,
      endHours: 72,
      valueEl: peak3DayValue,
      timeEl: peak3DayTime,
      label: "24-72 Hours",
    },
    {
      startHours: 72,
      endHours: 168,
      valueEl: peak7DayValue,
      timeEl: peak7DayTime,
      label: "72-168 Hours",
    },
  ];

  // Find peak for each range
  ranges.forEach((range) => {
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
      range.timeEl.textContent = formatMonthDayTime(peakTimeStr);
    } else if (range.valueEl && range.timeEl) {
      range.valueEl.textContent = "—";
      range.timeEl.textContent = "No data";
    }
  });

  display.style.display = "block";

  // Return peak data for chart markers
  return ranges
    .map((range) => ({
      time: range.peakTimeStr,
      value: range.peakSurge,
      label: range.label,
    }))
    .filter((p) => p.time && p.value !== null);
}

function updateForecastChart(stationId) {
  const resolved = resolveStation(forecastData, stationId, "forecast");
  if (!resolved) return;
  const { forecastStationId, station, displayStation } = resolved;

  if (!station.forecast || Object.keys(station.forecast).length === 0) {
    logger.warn("StormSurge", `No forecast data for ${stationId}`);
    return;
  }

  // Update peak display and get peak data for markers (use forecastStationId for lookup)
  const peakData = updatePeakToday(forecastStationId);

  const theme = getChartThemeColors();
  const colors = theme.series || {};
  const mutedText = theme.mutedText;
  const gridColor = theme.gridLine;
  const gradientTop = theme.isDark ? "rgba(92, 198, 255, 0.35)" : "rgba(0, 119, 190, 0.3)";
  const gradientBottom = theme.isDark ? "rgba(92, 198, 255, 0.05)" : "rgba(0, 119, 190, 0.05)";
  const peakColor = theme.negative;

  // Prepare data
  const forecastData_series = [];

  Object.entries(station.forecast)
    .sort(([a], [b]) => new Date(a) - new Date(b))
    .forEach(([timeStr, value]) => {
      forecastData_series.push([timeStr, value]);
    });

  // Calculate y-axis range
  const values = forecastData_series.map((d) => d[1]);
  const maxVal = Math.max(...values);
  const minVal = Math.min(...values);
  const range = maxVal - minVal;
  const padding = Math.max(range * 0.2, 0.1); // At least 0.1m padding
  const yMin = Math.floor((minVal - padding) * 10) / 10;
  const yMax = Math.ceil((maxVal + padding) * 10) / 10;

  // Calculate midnight boundaries in Pacific timezone for gridlines
  const midnightLines =
    forecastData_series.length > 0
      ? buildMidnightMarkLines(
          new Date(forecastData_series[0][0]),
          new Date(forecastData_series[forecastData_series.length - 1][0]),
          gridColor,
        )
      : [];

  // Prepare series array
  const series = [];

  // Peak markers on the line. The values themselves are read off the
  // ⚡ Peak Forecasts card above the chart, which also carries their times —
  // these dots just say *where* on the curve each one falls.
  const markPointData = [];
  if (peakData && peakData.length > 0) {
    peakData.forEach((peak) => {
      if (peak.time && peak.value !== null) {
        markPointData.push({
          coord: [peak.time, peak.value],
          itemStyle: {
            color: peakColor,
            borderColor: theme.background,
            borderWidth: 2,
          },
        });
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
    itemStyle: { color: colors.primary },
    lineStyle: { width: 2 },
    areaStyle: {
      color: {
        type: "linear",
        x: 0,
        y: 0,
        x2: 0,
        y2: 1,
        colorStops: [
          { offset: 0, color: gradientTop },
          { offset: 1, color: gradientBottom },
        ],
      },
    },
    markLine: {
      silent: true,
      symbol: "none",
      data: [
        // Sea level line (horizontal)
        {
          yAxis: 0,
          lineStyle: { type: "dashed", color: mutedText, width: 1 },
          label: {
            show: true,
            position: "end",
            formatter: "Sea Level",
            color: mutedText,
          },
        },
        // Midnight gridlines (vertical)
        ...midnightLines,
      ],
    },
    markPoint:
      markPointData.length > 0
        ? {
            data: markPointData,
            symbol: "circle",
            symbolSize: 10,
          }
        : undefined,
  });

  // Set chart options (notMerge: true to replace all data when switching stations)
  ensureChart("forecast", "forecast-chart").setOption(
    {
      ...baseSurgeChartOption(theme, {
        title: `${displayStation.station_name} - Surge Forecast`,
        mobileTitle: displayStation.station_name,
        subtext: "",
        mobileSubtext: "Surge Forecast",
        tooltipTime: formatMonthDayTimeTZ,
        gridBottom: "15%",
        mobileXRotate: 30,
        xHideOverlap: true,
        yDigits: 1,
        yRange: { min: yMin, max: yMax },
      }),
      // No legend on this chart. It carried one series and said nothing the
      // page wasn't already saying twice over:
      //
      //   - "Storm Surge Forecast" repeated the chart title immediately above
      //     it ("<station> - Surge Forecast"), on a chart with exactly one
      //     series — so it could not disambiguate anything either.
      //   - "🔴 Peaks: …" was a fake legend entry used as a text label, and it
      //     repeated the ⚡ Peak Forecasts card directly above the chart, which
      //     gives the same three figures *with their times*.
      //
      // It also laid out badly: `left: "center"` and `right: 20` are
      // contradictory, so the text drifted into the x-axis labels, and
      // gridBottom was pushed to 25% to reserve a band for it — which is where
      // the empty gap under the chart came from. Removing it takes the gap with
      // it. The verification chart below keeps its legend: two series there,
      // and telling them apart is the whole point.
      legend: { show: false },
      series: series,
    },
    true,
  ); // notMerge: true to prevent old data from persisting

  // Update metadata
  updateForecastMetadata(
    displayStation,
    forecastData_series.map((d) => d[0]),
    values,
  );
  ensureThemeRefresh("forecast", "forecast-station-select", updateForecastChart);

  logger.info(
    "StormSurge",
    `Loaded ${values.length} hours of forecast for ${displayStation.station_name}`,
  );
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

  // Model run time (00Z or 12Z format)
  const modelRunDisplay = formatModelRunTime(forecastData.model_run_time);

  setSafeHTML(
    metaEl,
    `
    <strong>Station:</strong> ${station.station_name}<br/>
    <strong>Location:</strong> ${station.location.lat.toFixed(4)}°N, ${Math.abs(station.location.lon).toFixed(4)}°W<br/>
    ${modelRunDisplay ? `<strong>Model Run:</strong> ${modelRunDisplay}<br/>` : ""}
    <strong>Data Retrieved:</strong> ${formatMonthDayTimeTZ(generatedTime)}<br/>
    <strong>Forecast Period:</strong> ${formatMonthDayTimeTZ(firstForecast)} to ${formatMonthDayTimeTZ(lastForecast)}<br/>
    <strong>Resolution:</strong> ${values.length} hours (1-hour intervals)<br/>
    <strong>Peak High:</strong> +${maxSurge.toFixed(3)} m at ${formatMonthDayTimeTZ(maxTime)}<br/>
    <strong>Peak Low:</strong> ${minSurge.toFixed(3)} m at ${formatMonthDayTimeTZ(minTime)}
  `,
  );
}

/* ======================================
   Observed Surge Data
   ====================================== */

async function loadObservedSurgeData() {
  try {
    observedSurgeData = await fetchWithTimeout(
      `/data/storm_surge/observed_surge.json?t=${Date.now()}`,
    );
    logger.info(
      "StormSurge",
      `Loaded observed surge data for ${Object.keys(observedSurgeData.stations || {}).length} stations`,
    );
  } catch (err) {
    logger.warn("StormSurge", "Observed surge data not available", err.message);
    observedSurgeData = null;
  }
}

/* ======================================
   Forecast Verification Section

   Displays 12 days of storm surge predictions (48h lead time) vs. 10 days of observations.
   See /docs/VERIFICATION_METHODOLOGY.md for detailed methodology.
   ====================================== */

async function loadVerificationData() {
  try {
    verificationData = await fetchWithTimeout(
      `/data/storm_surge/verification.json?t=${Date.now()}`,
    );

    initStationSelector({
      selectorId: "verification-station-select",
      indicatorId: "verification-station-indicator",
      data: verificationData,
      // Only show stations that have verification data
      hasData: (station) => station.verification && station.verification.length > 0,
      onChange: updateVerificationChart,
    });
    const selectedStation =
      document.getElementById("verification-station-select")?.value || "Point_Atkinson";
    updateVerificationChart(selectedStation);
  } catch (err) {
    logger.error("StormSurge", "Error loading verification data", err);
    showChartMessage("verification-chart", "⚠️ Verification data unavailable");
  }
}

function updateVerificationChart(stationId) {
  const resolved = resolveStation(verificationData, stationId, "verification");
  if (!resolved) return;
  const { station, displayStation } = resolved;

  if (!station.verification || station.verification.length === 0) {
    showChartMessage(
      "verification-chart",
      "No verification data available for this station yet. Data accumulates over time.",
    );
    return;
  }

  const theme = getChartThemeColors();
  const textColor = theme.text;
  const mutedText = theme.mutedText;
  const gridColor = theme.gridLine;

  // Prepare data - group by forecast date
  // Filter out data before the minimum date (9 days back from today)
  const minDate = getVerificationMinDate();

  // Calculate midnight tonight (Pacific time) - only show up to today
  const now = new Date();
  const pacificNow = new Date(now.toLocaleString("en-US", { timeZone: "America/Vancouver" }));
  const midnightTonight = new Date(pacificNow);
  midnightTonight.setHours(23, 59, 59, 999);
  const midnightTonightUTC = new Date(midnightTonight.toLocaleString("en-US", { timeZone: "UTC" }));

  const forecastDates = {};

  station.verification.forEach((point) => {
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
        values: [],
      };
    }
    forecastDates[date].times.push(point.time);
    forecastDates[date].values.push(point.value);
  });

  // Sort dates
  const sortedDates = Object.keys(forecastDates).sort();

  // Check if we have any data after filtering
  if (sortedDates.length === 0) {
    showChartMessage(
      "verification-chart",
      `No verification data available for this station from ${minDate} onwards. Data accumulates over time.`,
    );
    return;
  }

  // Prepare series for each forecast date
  const series = sortedDates.map((date, index) => {
    const data = forecastDates[date];
    const color = getColorForIndex(index, sortedDates.length, theme);

    return {
      name: index === 0 ? "Forecast" : "",
      type: "line",
      data: data.times.map((time, i) => [time, data.values[i]]),
      smooth: true,
      symbol: "circle",
      symbolSize: 4,
      itemStyle: { color: color },
      lineStyle: { width: 2 },
    };
  });

  // Add observed surge data if available for this station
  if (observedSurgeData?.stations?.[stationId]) {
    const obsStation = observedSurgeData.stations[stationId];
    const obsData = obsStation.data.map((d) => [d.time, d.observed_surge_m]);

    series.push({
      name: "Observed Surge (Actual)",
      type: "line",
      data: obsData,
      smooth: false,
      symbol: "circle",
      symbolSize: 3,
      itemStyle: { color: textColor },
      lineStyle: { width: 3, type: "solid", color: textColor },
      z: 10, // Render on top
    });

    logger.debug("StormSurge", `Added ${obsData.length} observed surge points`);
  }

  // Get all unique times for x-axis (only from filtered data)
  const allTimes = [
    ...new Set(station.verification.filter((p) => p.forecast_date >= minDate).map((p) => p.time)),
  ].sort();

  // Calculate midnight boundaries in Pacific timezone for gridlines
  const midnightLines =
    allTimes.length > 0
      ? buildMidnightMarkLines(
          new Date(allTimes[0]),
          new Date(allTimes[allTimes.length - 1]),
          gridColor,
        )
      : [];

  // Set chart options (notMerge: true to replace all data when switching stations)
  ensureChart("verification", "verification-chart").setOption(
    {
      ...baseSurgeChartOption(theme, {
        title: `${displayStation.station_name} — forecasts issued 56-79 h ahead`,
        mobileTitle: `${displayStation.station_name} — verification`,
        subtext: "Black line = Tide offset observations | Colored lines = Historical forecast runs",
        mobileSubtext: "Observed (black) vs Forecast runs (colored)",
        tooltipTime: formatMonthDayTime,
        gridBottom: "18%",
        mobileXRotate: 45,
        xHideOverlap: false,
        yDigits: 2,
      }),
      legend: {
        show: true,
        bottom: 0,
        left: "center",
        data: ["Forecast", "Observed Surge (Actual)"],
        type: "plain",
        textStyle: { color: textColor },
      },
      series: series.concat([
        {
          // Zero reference line with midnight gridlines
          name: "Sea Level",
          type: "line",
          data: allTimes.map((t) => [t, 0]),
          lineStyle: { type: "dashed", color: mutedText, width: 1 },
          symbol: "none",
          showSymbol: false,
          silent: true,
          markLine:
            midnightLines.length > 0
              ? {
                  silent: true,
                  symbol: "none",
                  data: midnightLines,
                }
              : undefined,
        },
      ]),
    },
    true,
  ); // notMerge: true to prevent old data from persisting

  // Update metadata
  updateVerificationMetadata(displayStation);
  ensureThemeRefresh("verification", "verification-station-select", updateVerificationChart);

  logger.info(
    "StormSurge",
    `Loaded verification data for ${displayStation.station_name} (${sortedDates.length} forecast dates)`,
  );
}

function getColorForIndex(index, total, theme) {
  const lightPalette = [
    "#e53935",
    "#1e88e5",
    "#43a047",
    "#fb8c00",
    "#8e24aa",
    "#00acc1",
    "#fdd835",
    "#6d4c41",
    "#546e7a",
    "#f06292",
  ];
  const darkPalette = [
    "#f87171",
    "#60a5fa",
    "#4ade80",
    "#fbbf24",
    "#c084fc",
    "#5eead4",
    "#fde047",
    "#f97316",
    "#38bdf8",
    "#f472b6",
  ];
  const palette = theme?.isDark ? darkPalette : lightPalette;
  return palette[index % palette.length];
}

function updateVerificationMetadata(station) {
  const metaEl = document.getElementById("verification-metadata");
  if (!metaEl) return;

  const generatedTime = new Date(verificationData.generated_utc);
  const daysAvailable = verificationData.actual_days_available || 0;

  // The archived run is 00Z (ARCHIVED_RUN_HOUR in fetch_storm_surge.py).
  // The old fallback here said 12Z, which was never the run being archived.
  const modelRunDisplay = formatModelRunTime(verificationData.model_run_time) || "00Z model run";

  setSafeHTML(
    metaEl,
    `
    <strong>Station:</strong> ${station.station_name}<br/>
    <strong>Location:</strong> ${station.location.lat.toFixed(4)}°N, ${Math.abs(station.location.lon).toFixed(4)}°W<br/>
    <strong>Data Retrieved:</strong> ${formatMonthDayTimeTZ(generatedTime)}<br/>
    <strong>Forecast Horizon:</strong> ${verificationData.forecast_horizon_hours || "56-79"} hours ahead<br/>
    <strong>Historical Days:</strong> ${daysAvailable} day${daysAvailable !== 1 ? "s" : ""} (max ${verificationData.max_days_back || 10})<br/>
    <strong>Collection Time:</strong> ${modelRunDisplay}
  `,
  );
}

/* ======================================
   Page Initialization
   ====================================== */

function loadAllData() {
  // Load all datasets (observed surge first, then charts)
  loadObservedSurgeData().then(() => {
    loadForecastData();
    loadVerificationData();
  });
}

// Initialize on page load
document.addEventListener("DOMContentLoaded", loadAllData);

// Refresh data every 2 hours
setInterval(loadAllData, 2 * 60 * 60 * 1000);

/* ======================================
   Show on Map Navigation Functions
   ====================================== */

function getIndexPathWithHash(hash) {
  const basePath = window.location.pathname.replace(/[^/]*$/, "");
  const normalizedBase = basePath.endsWith("/") ? basePath : `${basePath}/`;
  return `${normalizedBase}index.html${hash}`;
}

// Navigate to the index map with the selected surge station highlighted.
// Event listeners replace onclick= attributes (CSP compliance).
function wireShowOnMapButton(buttonId, selectorId) {
  const btn = document.getElementById(buttonId);
  if (!btn) return;

  btn.addEventListener("click", (event) => {
    event.preventDefault();
    const select = document.getElementById(selectorId);
    if (!select || !select.value) return;

    window.location.href = getIndexPathWithHash(`#surge-${select.value}`);
  });
}

wireShowOnMapButton("show-forecast-surge-on-map-btn", "forecast-station-select");
wireShowOnMapButton("show-verification-surge-on-map-btn", "verification-station-select");
