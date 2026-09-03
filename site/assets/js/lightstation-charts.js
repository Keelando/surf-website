/* -----------------------------
   Lightstation Charts Module (ES module)
   Displays 24hr wind speed and wave height trends

   Chart helpers (fetchWithTimeout, getChartThemeColors,
   getMobileOptimizedTooltipConfig, registerChartThemeListener, echarts)
   still come from classic scripts loaded before this one.
   ----------------------------- */

import { formatDayMonthNumeric, formatNumericDayTime, formatTimeHM } from "./shared/format-time.js";
import { setSafeHTML } from "./shared/safe-html.js";

/**
 * Lightkeepers report wind direction as a full compass word ("SOUTHEAST").
 * Spelled out, that single cell is the widest thing in the 24-hour table and
 * is what pushes every phone-width row onto a second line — so the table (and
 * only the table; the map popups and cards have room) shows the abbreviation.
 */
const WIND_DIRECTION_ABBR = {
  NORTH: "N",
  NORTHEAST: "NE",
  EAST: "E",
  SOUTHEAST: "SE",
  SOUTH: "S",
  SOUTHWEST: "SW",
  WEST: "W",
  NORTHWEST: "NW",
};

/**
 * "SOUTHEAST" → "SE"; anything unrecognised is passed through unchanged so a
 * new vocabulary word from upstream still shows up rather than vanishing.
 */
function abbreviateDirection(direction) {
  if (!direction) return "";
  return WIND_DIRECTION_ABBR[String(direction).toUpperCase()] || direction;
}

/**
 * "MODERATE" → "Moderate". The feed shouts every sea-state and swell word;
 * title case reads as a value rather than a warning.
 */
function titleCase(word) {
  const text = String(word);
  return text.charAt(0).toUpperCase() + text.slice(1).toLowerCase();
}

/**
 * True on phone-width screens, where the charts need shorter titles, one-line
 * axis labels and an unrotated axis name to stay legible.
 */
function isNarrowChart() {
  return window.innerWidth < 600;
}

/**
 * Shared option fragments for the two 24-hour charts, which are identical in
 * layout and differ only in series. Every one of these was a phone-only
 * legibility bug before it was a helper — see the comments on each.
 */

/** Title block: wraps to the canvas instead of being clipped, and shrinks on a phone. */
function lightstationTitle(text, chart, textColor) {
  const narrow = isNarrowChart();
  return {
    text,
    left: "center",
    textStyle: {
      fontSize: narrow ? 14 : 18,
      fontWeight: 600,
      color: textColor,
      // A centred one-line title longer than the canvas is clipped at both
      // ends — "MERRY ISLAND - Sea State (Wave Heig" — so let it wrap to the
      // chart's own width instead.
      width: chartWidth(chart),
      overflow: "break",
    },
  };
}

/**
 * Where the legend and plot area start.
 *
 * A wrapped two-line title is ~34 px at the phone font size and the legend sat
 * at a fixed `top: 35`, so the title's second line was printed straight
 * through it. Both anchors move down together on a narrow screen.
 */
function lightstationLegendTop() {
  return isNarrowChart() ? 52 : 35;
}

/**
 * X-axis tick labels.
 *
 * Wide: two lines, "9/3" over "13:40". On a phone that stacked pair is wider
 * than the ~55 px between ticks and `hideOverlap` alone was not enough — so a
 * narrow screen gets a single-line clock, with the date shown in place of
 * "00:00" at the midnight tick to keep the day boundary readable.
 */
function lightstationTimeAxisLabel(mutedText) {
  const narrow = isNarrowChart();
  return {
    formatter: (value) => {
      const date = new Date(value);
      if (!narrow) return formatNumericDayTime(date).replace(" ", "\n");
      const clock = formatTimeHM(date);
      return clock === "00:00" ? formatDayMonthNumeric(date) : clock;
    },
    fontSize: narrow ? 11 : 12,
    color: mutedText,
    hideOverlap: true,
  };
}

/**
 * Y-axis name.
 *
 * Rotated in the middle of the axis it needs `nameGap` px of margin outside
 * the tick labels; with `grid.left: 30` and one-character labels ("0"…"6")
 * there was nowhere near 40 px on a phone and "Wave Height (ft)" was sliced
 * off at the canvas edge. Narrow screens get it unrotated above the axis,
 * where it cannot be clipped.
 */
function lightstationValueAxisName(name, textColor) {
  const narrow = isNarrowChart();
  return {
    name,
    nameLocation: narrow ? "end" : "middle",
    nameRotate: narrow ? 0 : 90,
    nameGap: narrow ? 12 : 40,
    nameTextStyle: {
      fontSize: narrow ? 11 : 13,
      fontWeight: 600,
      color: textColor,
      align: narrow ? "left" : "center",
    },
  };
}

/** Plot area, with room reserved for whichever y-axis name style is in play. */
function lightstationGrid() {
  const narrow = isNarrowChart();
  return {
    // containLabel reserves the tick labels; `left` then only has to leave
    // room for the rotated axis name outside them, and `right` comes from the
    // site-wide gutter. The old 15%/4% pair spent 120 px of a 800 px chart on
    // blank margin. On a phone the name sits above the axis instead, so there
    // is nothing to reserve at the left.
    //
    // 44, not 30: with containLabel the axis line sits at left + label width,
    // and the name is nameGap (40) back from there — so a chart whose labels
    // are one character wide ("0"…"6", i.e. the wave chart) put the name at
    // x≈2 and sliced it off against the canvas edge. 44 clears the widest
    // nameGap with the narrowest labels.
    left: narrow ? getChartSideGutters().left : 44,
    right: getChartSideGutters().right,
    // 22% of a 400 px canvas is 88 px of margin under the plot for a 30 px
    // dataZoom slider and one line of tick labels. Pixels on a phone, where
    // that overhead is a fifth of the chart.
    bottom: narrow ? 62 : "22%",
    top: narrow ? 96 : "20%",
    containLabel: true,
  };
}

// Global chart instances
let windSpeedChart = null;
let waveHeightChart = null;
let lightstationTimeseriesData = null;
let allLightstations = [];
let currentLightstationStation = null;
let detachLightstationThemeListener = null;

/**
 * Load lightstation timeseries data and populate station selector
 */
async function loadLightstationTimeseries() {
  try {
    // Load timeseries data (past 24hr only)
    const data = await fetchWithTimeout(`/data/lightstation_timeseries_24hr.json?t=${Date.now()}`);
    lightstationTimeseriesData = data;

    // Load ALL lightstations from stations.json (including those without recent data)
    const stationsData = await fetchWithTimeout("/data/stations.json");
    const lightstationsMeta = stationsData.lightstations || {};

    const select = document.getElementById("lightstation-station-select");
    const searchInput = document.getElementById("lightstation-station-search");
    if (!select) return;

    // Build combined list: all stations from metadata, with timeseries data if available
    allLightstations = Object.entries(lightstationsMeta)
      .map(([id, meta]) => {
        const stationKey = meta.name.toUpperCase();
        const hasData = lightstationTimeseriesData[stationKey] !== undefined;

        return [
          stationKey,
          {
            name: stationKey,
            region: meta.region,
            hasRecentData: hasData,
          },
        ];
      })
      .sort((a, b) => a[1].name.localeCompare(b[1].name));

    // Populate dropdown
    populateLightstationDropdown();

    // Set default selection (Merry Island, or first station if not found)
    if (allLightstations.length > 0) {
      // Try to find Merry Island
      const merryIsland = allLightstations.find(
        ([id, station]) => station.name === "MERRY ISLAND" || id === "MERRY ISLAND",
      );

      const defaultStation = merryIsland ? merryIsland[0] : allLightstations[0][0];
      select.value = defaultStation;
      renderLightstationCharts(defaultStation);
    }

    // Add change listener to dropdown
    select.addEventListener("change", (e) => {
      renderLightstationCharts(e.target.value);
    });

    // Add search listener
    if (searchInput) {
      searchInput.addEventListener("input", (e) => {
        const searchText = e.target.value.toLowerCase();
        if (!searchText) return;

        // Find first matching station
        const match = allLightstations.find(
          ([id, station]) =>
            station.name.toLowerCase().includes(searchText) ||
            id.toLowerCase().includes(searchText),
        );

        if (match) {
          select.value = match[0];
          renderLightstationCharts(match[0]);
        }
      });
    }
  } catch (error) {
    console.error("Error loading lightstation timeseries:", error);
    const select = document.getElementById("lightstation-station-select");
    if (select) {
      setSafeHTML(select, '<option value="">Error loading stations</option>');
    }
  }
}

/**
 * Populate station dropdown grouped by region
 */
function populateLightstationDropdown() {
  const select = document.getElementById("lightstation-station-select");
  if (!select || !allLightstations) return;

  select.textContent = "";

  // Group stations by region
  const regionGroups = {};
  allLightstations.forEach(([id, station]) => {
    const region = station.region || "Other";
    if (!regionGroups[region]) {
      regionGroups[region] = [];
    }
    regionGroups[region].push([id, station]);
  });

  // Define region order
  const regionOrder = [
    "STRAIT OF GEORGIA",
    "JUAN DE FUCA STRAIT",
    "WEST COAST VANCOUVER ISLAND",
    "CENTRAL COAST",
    "HECATE STRAIT",
  ];

  // Create optgroups for each region
  regionOrder.forEach((regionName) => {
    if (!regionGroups[regionName]) return;

    const optgroup = document.createElement("optgroup");
    optgroup.label = regionName;

    regionGroups[regionName].forEach(([id, station]) => {
      const option = document.createElement("option");
      option.value = id;
      // Add indicator if station doesn't have recent data
      const dataIndicator = station.hasRecentData ? "" : " (no recent data)";
      option.textContent = station.name + dataIndicator;
      optgroup.appendChild(option);
    });

    select.appendChild(optgroup);
  });
}

/**
 * Render both wind and wave charts for selected station
 */
export function renderLightstationCharts(stationName) {
  if (!stationName) return;

  const station = lightstationTimeseriesData ? lightstationTimeseriesData[stationName] : null;
  currentLightstationStation = stationName;
  ensureLightstationThemeListener();

  // Update 24-hour reports title with station name
  const title = document.getElementById("lightstation-24hr-title");
  if (title) {
    title.textContent = `24-Hour Reports: ${stationName}`;
  }

  if (!station) {
    // No recent data - show message
    showNoDataMessage(stationName);
    return;
  }

  renderWindSpeedChart(stationName, station);
  renderWaveHeightChart(stationName, station);
  render24HourTable(stationName, station);
}

/**
 * Show "no data available" message in charts and table
 */
function showNoDataMessage(stationName) {
  const tbody = document.getElementById("lightstation-24hr-body");
  if (tbody) {
    setSafeHTML(
      tbody,
      '<tr><td colspan="5" class="ls-table-empty ls-table-empty-alert">⚠️ No data from the past 24 hours</td></tr>',
    );
  }

  // Clear charts
  if (windSpeedChart) {
    windSpeedChart.clear();
    windSpeedChart.setOption({
      title: {
        text: `${stationName} - Wind Speed`,
        subtext: "No data from the past 24 hours",
        left: "center",
        textStyle: { fontSize: 18, fontWeight: 600, color: "var(--color-primary-dark,#004b7c)" },
        subtextStyle: { fontSize: 14, color: "var(--color-accent-red,#e53e3e)" },
      },
    });
  }

  if (waveHeightChart) {
    waveHeightChart.clear();
    waveHeightChart.setOption({
      title: {
        text: `${stationName} - Sea State`,
        subtext: "No data from the past 24 hours",
        left: "center",
        textStyle: { fontSize: 18, fontWeight: 600, color: "var(--color-primary-dark,#004b7c)" },
        subtextStyle: { fontSize: 14, color: "var(--color-accent-red,#e53e3e)" },
      },
    });
  }
}

/**
 * Width available to a chart's title, in pixels.
 *
 * ECharts does not wrap title text on its own; it needs an explicit width, and
 * the only honest source for that is the instance's current canvas.
 *
 * @param {Object} chart - ECharts instance
 * @returns {number} Usable title width
 */
function chartWidth(chart) {
  return Math.max(120, (chart?.getWidth?.() || 320) - 16);
}

/**
 * Select a station in the dropdown, render its charts, and scroll to the
 * data section. Called from page cards (by name) and map popups (by ID).
 * Moved here from lightstation-page.js/lightstation-map.js, which each
 * carried a near-identical copy.
 */
export function viewLightstationChart(stationName) {
  const select = document.getElementById("lightstation-station-select");
  if (!select) return;

  // Check if station exists in timeseries data
  if (!lightstationTimeseriesData || !lightstationTimeseriesData[stationName]) {
    // Station doesn't have 24hr data - show alert instead of scrolling
    alert(
      `${stationName} does not have data from the past 24 hours.\n\nMost recent observation may be older than 24 hours.`,
    );
    return;
  }

  // Select the station in dropdown
  select.value = stationName;
  renderLightstationCharts(stationName);

  // Scroll to data table section (top of the tables/charts area)
  const tableSection = document.getElementById("lightstation-data-table-section");
  if (tableSection) {
    tableSection.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

/**
 * ID-flavoured variant for map popups: "CHROME_ISLAND" → "CHROME ISLAND".
 */
export function viewLightstationDataById(lightstationId) {
  viewLightstationChart(lightstationId.replace(/_/g, " "));
}

/**
 * Render 24-hour data table for selected station
 */
function render24HourTable(stationName, station) {
  const tbody = document.getElementById("lightstation-24hr-body");
  if (!tbody) return;

  const timeseries = station.timeseries;

  // Get all unique timestamps from all data series
  const timestamps = new Set();

  ["wind_speed_kt", "sea_height_ft", "swell_intensity", "sea_condition"].forEach((field) => {
    if (timeseries[field]) {
      timeseries[field].forEach((point) => timestamps.add(point.time));
    }
  });

  // Convert to array and sort by time (newest first)
  const sortedTimes = Array.from(timestamps).sort((a, b) => new Date(b) - new Date(a));

  if (sortedTimes.length === 0) {
    setSafeHTML(
      tbody,
      '<tr><td colspan="5" class="ls-table-empty">No data available for this station</td></tr>',
    );
    return;
  }

  // Build table rows
  let tableHTML = "";
  sortedTimes.forEach((time) => {
    // Find data for this timestamp
    const windData = timeseries.wind_speed_kt?.find((p) => p.time === time);
    const seaData = timeseries.sea_height_ft?.find((p) => p.time === time);
    const swellData = timeseries.swell_intensity?.find((p) => p.time === time);
    const conditionData = timeseries.sea_condition?.find((p) => p.time === time);
    const directionData = timeseries.wind_direction?.find((p) => p.time === time);

    // Format timestamp
    const formattedTime = formatNumericDayTime(time);

    // Build wind text. "(gusting)" spelled out is a whole extra line on a
    // phone, so it becomes a "G" flag with the long form on hover/AT.
    let windText = "—";
    if (windData && windData.value !== null) {
      const direction = abbreviateDirection(directionData ? directionData.value : "");
      const gusting = windData.gusting ? ' <abbr title="gusting">G</abbr>' : "";
      windText = `${direction} ${Math.round(windData.value)} kt${gusting}`.trim();
    }

    // Sea state and condition each get their own cell. Concatenating them
    // ("3 ft - MODERATE") made the widest column on the page while leaving
    // Conditions permanently empty — two short cells fit where one long one
    // did not.
    let seaText = "—";
    if (seaData && seaData.value !== null) {
      seaText = `${seaData.value} ft`;
    }

    let swellText = "—";
    if (swellData && swellData.value) {
      swellText = titleCase(swellData.value);
    }

    let conditionsText = "—";
    if (conditionData && conditionData.value) {
      conditionsText = titleCase(conditionData.value);
    }

    // Zebra striping is `.data-table tbody tr:nth-child(even)` in style-v4.css,
    // not a per-row background written out here.
    tableHTML += `
      <tr>
        <td class="ls-col-time">${formattedTime}</td>
        <td class="ls-col-wind">${windText}</td>
        <td class="ls-col-sea">${seaText}</td>
        <td class="ls-col-swell">${swellText}</td>
        <td class="ls-col-cond">${conditionsText}</td>
      </tr>
    `;
  });

  // Table HTML is built from our own data (not user input), so bypass
  // DOMPurify which strips <tr>/<td> tags outside a <table> context.
  tbody.innerHTML = tableHTML;
}

/**
 * Render wind speed chart
 */
function renderWindSpeedChart(stationName, station) {
  const chartContainer = document.getElementById("lightstation-wind-chart");
  if (!chartContainer) return;

  // Initialize chart if needed
  if (!windSpeedChart) {
    windSpeedChart = echarts.init(chartContainer);
  }

  const timeseries = station.timeseries;
  const windSpeedData = timeseries.wind_speed_kt || [];
  const theme = getChartThemeColors();
  const colors = theme.series;
  const textColor = theme.text;
  const mutedText = theme.mutedText;
  const axisColor = theme.axisLine;
  const gridColor = theme.gridLine;

  // Separate gusting vs non-gusting for visual distinction
  const normalSpeedData = windSpeedData
    .filter((p) => !p.gusting)
    .map((p) => [new Date(p.time).getTime(), p.value]);

  const gustingSpeedData = windSpeedData
    .filter((p) => p.gusting)
    .map((p) => [new Date(p.time).getTime(), p.value]);

  const option = {
    backgroundColor: theme.background,
    textStyle: { color: textColor },
    title: {
      ...lightstationTitle(`${station.name} - Wind Speed`, windSpeedChart, textColor),
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = formatNumericDayTime(new Date(params[0].value[0]));

        let tooltipText = `<strong>${time}</strong><br/>`;
        params.forEach((param) => {
          if (param.value && param.value[1] != null) {
            tooltipText += `${param.marker} ${param.seriesName}: ${Math.round(param.value[1])} kt<br/>`;
          }
        });
        return tooltipText;
      },
    },
    legend: {
      data: ["Wind Speed", "Gusting"],
      top: lightstationLegendTop(),
      textStyle: {
        fontSize: 14,
        color: textColor,
      },
    },
    grid: lightstationGrid(),
    xAxis: {
      type: "time",
      boundaryGap: false,
      splitLine: {
        show: true,
        lineStyle: {
          color: gridColor,
          type: "dashed",
        },
      },
      axisLabel: lightstationTimeAxisLabel(mutedText),
      axisLine: { lineStyle: { color: axisColor } },
    },
    yAxis: {
      type: "value",
      ...lightstationValueAxisName("Wind Speed (kt)", textColor),
      min: 0,
      axisLabel: { color: mutedText },
      axisLine: { lineStyle: { color: axisColor } },
      splitLine: { lineStyle: { color: gridColor } },
    },
    dataZoom: [
      {
        type: "inside",
        start: 0,
        end: 100,
      },
      {
        type: "slider",
        start: 0,
        end: 100,
        height: 30,
        bottom: "2%",
      },
    ],
    series: [
      {
        name: "Wind Speed",
        type: "line",
        data: normalSpeedData,
        smooth: true,
        lineStyle: {
          width: 2,
          color: colors.primary,
        },
        itemStyle: {
          color: colors.primary,
          borderColor: theme.symbolBorderColor,
        },
        symbol: "circle",
        symbolSize: 6,
        emphasis: {
          focus: "series",
        },
      },
      {
        name: "Gusting",
        type: "scatter",
        data: gustingSpeedData,
        itemStyle: {
          color: theme.negative,
        },
        symbol: "diamond",
        symbolSize: 8,
        emphasis: {
          focus: "series",
        },
      },
    ],
  };

  windSpeedChart.setOption(option);
}

/**
 * Render wave height chart
 */
function renderWaveHeightChart(stationName, station) {
  const chartContainer = document.getElementById("lightstation-wave-chart");
  if (!chartContainer) return;

  // Initialize chart if needed
  if (!waveHeightChart) {
    waveHeightChart = echarts.init(chartContainer);
  }

  const timeseries = station.timeseries;
  const waveData = timeseries.sea_height_ft || [];
  const theme = getChartThemeColors();
  const colors = theme.series;
  const textColor = theme.text;
  const mutedText = theme.mutedText;
  const axisColor = theme.axisLine;
  const gridColor = theme.gridLine;
  const gradientTop = theme.isDark ? "rgba(94, 234, 212, 0.25)" : "rgba(56, 161, 105, 0.3)";
  const gradientBottom = theme.isDark ? "rgba(94, 234, 212, 0.06)" : "rgba(56, 161, 105, 0.05)";

  if (waveData.length === 0) {
    // Show "no data" message
    waveHeightChart.clear();
    waveHeightChart.setOption({
      backgroundColor: theme.background,
      title: {
        text: `${station.name} - Sea State`,
        subtext: "No wave height data available",
        left: "center",
        textStyle: {
          fontSize: 18,
          fontWeight: 600,
          color: textColor,
        },
        subtextStyle: {
          fontSize: 14,
          color: mutedText,
        },
      },
    });
    return;
  }

  // Prepare data for ECharts
  const heightData = waveData.map((p) => [new Date(p.time).getTime(), p.value]);

  const option = {
    backgroundColor: theme.background,
    textStyle: { color: textColor },
    title: {
      // "Sea State (Wave Height)" says the same thing twice and, spelled out,
      // wraps to three lines on a phone; the axis name carries the units.
      ...lightstationTitle(
        isNarrowChart()
          ? `${station.name} - Wave Height`
          : `${station.name} - Sea State (Wave Height)`,
        waveHeightChart,
        textColor,
      ),
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = formatNumericDayTime(new Date(params[0].value[0]));

        let tooltipText = `<strong>${time}</strong><br/>`;
        params.forEach((param) => {
          if (param.value && param.value[1] != null) {
            tooltipText += `${param.marker} ${param.seriesName}: ${param.value[1]} ft<br/>`;
          }
        });
        return tooltipText;
      },
    },
    legend: {
      data: ["Wave Height"],
      top: lightstationLegendTop(),
      textStyle: {
        fontSize: 14,
        color: textColor,
      },
    },
    grid: lightstationGrid(),
    xAxis: {
      type: "time",
      boundaryGap: false,
      splitLine: {
        show: true,
        lineStyle: {
          color: gridColor,
          type: "dashed",
        },
      },
      axisLabel: lightstationTimeAxisLabel(mutedText),
      axisLine: { lineStyle: { color: axisColor } },
    },
    yAxis: {
      type: "value",
      ...lightstationValueAxisName("Wave Height (ft)", textColor),
      min: 0,
      axisLabel: { color: mutedText },
      axisLine: { lineStyle: { color: axisColor } },
      splitLine: { lineStyle: { color: gridColor } },
    },
    dataZoom: [
      {
        type: "inside",
        start: 0,
        end: 100,
      },
      {
        type: "slider",
        start: 0,
        end: 100,
        height: 30,
        bottom: "2%",
      },
    ],
    series: [
      {
        name: "Wave Height",
        type: "line",
        data: heightData,
        smooth: true,
        lineStyle: {
          width: 3,
          color: colors.quaternary,
        },
        itemStyle: {
          color: colors.quaternary,
          borderColor: theme.symbolBorderColor,
        },
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
        symbol: "circle",
        symbolSize: 6,
        emphasis: {
          focus: "series",
        },
      },
    ],
  };

  waveHeightChart.setOption(option);
}

// Initialize on page load (module scripts are deferred, so the DOM is
// parsed; the "show on map" button listener lives in lightstation-page.js)
loadLightstationTimeseries();

// Handle window resize. The wave chart's title wraps at a pixel width taken
// from the canvas, so a resize has to re-render it, not just resize it —
// otherwise a phone rotated to landscape keeps the narrow wrap point.
window.addEventListener("resize", () => {
  if (windSpeedChart) windSpeedChart.resize();
  if (waveHeightChart) waveHeightChart.resize();
  if (currentLightstationStation) renderLightstationCharts(currentLightstationStation);
});

function ensureLightstationThemeListener() {
  if (detachLightstationThemeListener) return;
  detachLightstationThemeListener = registerChartThemeListener(() => {
    if (currentLightstationStation) {
      renderLightstationCharts(currentLightstationStation);
    }
  });
}
