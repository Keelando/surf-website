/* -----------------------------
   RDWPS Wave Forecast (ES module entry point)

   UNDER DEVELOPMENT. Previewing at the bottom of forecasts.html — see
   docs/project/FORECAST_UPGRADE.md. Halibut Bank only while we get a feel
   for how the model behaves; validation against the buoy is inconclusive
   until autumn, so nothing here is promoted or linked yet.

   Data: /data/wave_forecast/<station>.json, from
   scripts/fetch/fetch_wave_forecast.py (Environment Canada GeoMet WMS).

   Chart helpers (fetchWithTimeout, getChartThemeColors, degreesToCardinal,
   echarts, logger) come from classic scripts loaded before this entry point.
   ----------------------------- */

import { formatModelRunTime, formatMonthDayTimeTZ } from "./shared/format-time.js";
import { DIRECTION_ARROW_PATH } from "./shared/markers.js";

function setSafeHTML(element, html) {
  if (!element) return;
  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

const STATION_ID = "4600146"; // Halibut Bank

// Wave heights in the Strait rarely trouble a metre outside a winter
// southeasterly, and the summer forecasts sit under 0.2 m. Auto-scaling to
// the data would render a flat calm as a dramatic mountain range, so the
// axis holds a fixed 0–1 m unless the forecast exceeds it.
const MIN_HEIGHT_AXIS_MAX = 1.0;

// Hours of the table shown before the reader asks for more.
const DEFAULT_VISIBLE_HOURS = 12;

let waveChart = null;
let detachWaveThemeListener = null;
let visibleHours = DEFAULT_VISIBLE_HOURS;

/**
 * Convert the forecast object into a sorted array of rows.
 *
 * The payload is keyed by ISO timestamp rather than being an array, so
 * ordering is not guaranteed by the JSON — sort explicitly.
 *
 * @param {Object} forecast - The `forecast` object from the payload
 * @returns {Array<Object>} Rows with a parsed Date, sorted by valid time
 */
function toSortedRows(forecast) {
  return Object.entries(forecast)
    .map(([validTime, values]) => ({
      validTime,
      time: new Date(validTime),
      waveHeight: values.wave_height ?? null,
      peakPeriod: values.peak_period ?? null,
      waveDirection: values.wave_direction ?? null,
      // Absent on any step where RDWPS reports no wind sea. Masked steps are
      // dropped by the fetcher rather than written as zero, so a missing
      // value here means "no wind-wave partition", not "flat".
      windWaveHeight: values.wind_wave_height ?? null,
    }))
    .filter((row) => !isNaN(row.time))
    .sort((a, b) => a.time - b.time);
}

/**
 * Upper bound for the wave-height axis.
 *
 * @param {Array<Object>} rows
 * @returns {number} Axis maximum in metres
 */
function heightAxisMax(rows) {
  const peak = rows.reduce((max, row) => Math.max(max, row.waveHeight ?? 0), 0);
  if (peak <= MIN_HEIGHT_AXIS_MAX) return MIN_HEIGHT_AXIS_MAX;
  // Round up to the next half metre so the line never touches the frame.
  return Math.ceil((peak * 1.1) / 0.5) * 0.5;
}

/**
 * Build the direction-arrow scatter points, as on the buoy wave chart.
 *
 * Sampled by elapsed time rather than by array index, which is where this
 * departs from `createWaveDirectionArrowData` in wave-chart-v4.js. That one
 * steps a fixed number of points because buoy observations are evenly
 * spaced; these steps taper from hourly to 3-hourly at the 24-hour mark, so
 * an index stride would thin out to one arrow per 9 hours across the back
 * half of the forecast.
 *
 * @param {Array<Object>} rows
 * @param {number} axisMax - Height axis maximum, for vertical placement
 * @param {Object} colors - Resolved theme palette
 * @returns {Array<Object>} ECharts scatter points carrying symbolRotate
 */
function createDirectionArrows(rows, axisMax, colors) {
  // Match the buoy chart's density: every 3 h, or 6 h where a narrow screen
  // would otherwise overlap them.
  const intervalMs = (window.innerWidth < 600 ? 6 : 3) * 60 * 60 * 1000;

  // Ride near the top of the axis rather than just above the data. The axis
  // is pinned to a 1 m floor, so tracking the peak would drop the arrows
  // into the middle of an empty chart every calm summer day.
  const arrowY = axisMax * 0.92;

  const arrows = [];
  let lastStamp = null;

  for (const row of rows) {
    if (row.waveDirection === null) continue;

    const stamp = row.time.getTime();
    if (lastStamp !== null && stamp - lastStamp < intervalMs) continue;

    arrows.push({
      value: [stamp, arrowY],
      // Meteorological direction is where waves come FROM; the arrow points
      // where they are going, hence the negation.
      symbolRotate: calculateArrowRotation(row.waveDirection),
      itemStyle: { color: colors.marker, opacity: 0.75 },
    });
    lastStamp = stamp;
  }

  return arrows;
}

/**
 * Format a direction as "SE (136°)".
 *
 * Meteorological convention throughout this site: the direction waves are
 * coming FROM.
 *
 * @param {number|null} degrees
 * @returns {string}
 */
function formatDirection(degrees) {
  if (degrees === null || degrees === undefined) return "—";
  const cardinal = typeof degreesToCardinal === "function" ? degreesToCardinal(degrees) : "";
  return `${cardinal} (${Math.round(degrees)}°)`;
}

/**
 * Format a Pacific-time axis/table label, "Sun 14:00".
 *
 * @param {Date} date
 * @returns {string}
 */
function formatStepLabel(date) {
  return date.toLocaleString("en-US", {
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
}

/**
 * Render the forecast chart.
 *
 * A time axis, not a category axis: the fetch tapers from hourly to
 * 3-hourly at the 24-hour mark, and evenly spacing those steps would
 * stretch the back half of the forecast to look like the front half.
 *
 * @param {Array<Object>} rows
 * @returns {void}
 */
function renderChart(rows) {
  const el = document.getElementById("wave-forecast-chart");
  if (!el || typeof echarts === "undefined") return;

  const colors = getChartThemeColors();

  if (!waveChart) {
    waveChart = echarts.init(el);
    window.addEventListener("resize", () => waveChart.resize());
  }

  const heightSeries = rows.map((row) => [row.time.getTime(), row.waveHeight]);
  const periodSeries = rows.map((row) => [row.time.getTime(), row.peakPeriod]);

  const axisMax = heightAxisMax(rows);
  const arrows = createDirectionArrows(rows, axisMax, colors);

  waveChart.setOption(
    {
      backgroundColor: "transparent",
      textStyle: { color: colors.text },
      // Grid, legend and tooltip all come from the shared responsive helpers,
      // so this chart breaks at the same widths as the buoy wave charts.
      grid: getResponsiveGridConfig(false),
      legend: {
        data: ["Significant wave height", "Peak period", "Wave direction"],
        bottom: getResponsiveLegendBottom(),
        textStyle: { color: colors.mutedText },
      },
      tooltip: {
        ...getMobileOptimizedTooltipConfig(),
        formatter: (params) => {
          const stamp = params[0]?.value?.[0];
          const row = rows.find((r) => r.time.getTime() === stamp);
          if (!row) return "";
          return `<strong>${formatStepLabel(row.time)}</strong><br/>
                  Height: ${row.waveHeight?.toFixed(2) ?? "—"} m<br/>
                  Peak period: ${row.peakPeriod?.toFixed(1) ?? "—"} s<br/>
                  From: ${formatDirection(row.waveDirection)}`;
        },
      },
      xAxis: {
        type: "time",
        axisLabel: {
          // Same treatment as the buoy wave chart. `hideOverlap` is what
          // actually stops the labels colliding — a 48 h span at 33 steps
          // asks for far more ticks than fit, and ECharts will happily draw
          // them on top of each other otherwise. The tilt buys room for the
          // ones that survive on a narrow screen.
          fontSize: window.innerWidth < 600 ? 9 : 10,
          rotate: window.innerWidth < 600 ? 30 : 0,
          formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
          hideOverlap: true,
          margin: 10,
          color: colors.mutedText,
        },
        axisTick: { show: true },
        axisLine: { lineStyle: { color: colors.axisLine } },
        splitLine: { show: true, lineStyle: { color: colors.gridLine } },
      },
      // Axis furniture tinted to match its series, as on the buoy charts, so
      // which axis a line belongs to is readable without the legend.
      yAxis: [
        {
          type: "value",
          name: "Height (m)",
          position: "left",
          min: 0,
          max: axisMax,
          nameTextStyle: { color: colors.series.primary },
          axisLine: { lineStyle: { color: colors.series.primary } },
          axisLabel: { color: colors.mutedText },
          splitLine: { lineStyle: { color: colors.gridLine } },
        },
        {
          type: "value",
          name: "Period (s)",
          position: "right",
          min: 0,
          nameTextStyle: { color: colors.series.secondary },
          axisLine: { lineStyle: { color: colors.series.secondary } },
          axisLabel: { color: colors.mutedText },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: "Significant wave height",
          type: "line",
          data: heightSeries,
          yAxisIndex: 0,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2.5, color: colors.series.primary },
          areaStyle: { opacity: 0.15, color: colors.series.primary },
        },
        {
          name: "Peak period",
          type: "line",
          data: periodSeries,
          yAxisIndex: 1,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2, color: colors.series.secondary, type: "dashed" },
        },
        {
          name: "Wave direction",
          type: "scatter",
          data: arrows,
          yAxisIndex: 0,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: (value, params) => arrows[params.dataIndex]?.symbolRotate ?? 0,
          // The axis tooltip already reports direction on every step; letting
          // the arrows answer as well would double the line up.
          tooltip: { show: false },
          silent: true,
          z: 2,
        },
      ],
    },
    // Replace wholesale on theme change rather than merging, so stale
    // palette values can't survive into the new option set.
    { notMerge: true },
  );
}

/**
 * Hours of forecast covered by a row set.
 *
 * @param {Array<Object>} rows
 * @returns {number}
 */
function spanHours(rows) {
  if (rows.length < 2) return 0;
  return (rows[rows.length - 1].time - rows[0].time) / (1000 * 60 * 60);
}

/**
 * The leading slice of rows falling within `hours` of the first step.
 *
 * Filtered on elapsed time rather than row count, so the window means the
 * same thing either side of the 24-hour point where steps taper from
 * hourly to 3-hourly.
 *
 * @param {Array<Object>} rows
 * @param {number} hours
 * @returns {Array<Object>}
 */
function rowsWithin(rows, hours) {
  if (rows.length === 0) return [];
  const cutoff = rows[0].time.getTime() + hours * 60 * 60 * 1000;
  return rows.filter((row) => row.time.getTime() <= cutoff);
}

/**
 * Render the tabular forecast, showing `visibleHours` of it.
 *
 * The full 48 hours is 33 rows, which buries the chart and the provenance
 * block below it. It previously lived in a fixed-height scrolling box, but
 * an inner scrollbar is easy to miss entirely — the table simply looks
 * shorter than the chart above it. Explicit expand controls say how much
 * more there is.
 *
 * @param {Array<Object>} rows - Every forecast step
 * @returns {void}
 */
function renderTable(rows) {
  const container = document.getElementById("wave-forecast-table");
  if (!container) return;

  const total = spanHours(rows);
  const shown = rowsWithin(rows, visibleHours);
  const atFullExtent = shown.length >= rows.length;

  const body = shown
    .map(
      (row) => `
        <tr>
          <th scope="row">${formatStepLabel(row.time)}</th>
          <td>${row.waveHeight?.toFixed(2) ?? "—"}</td>
          <td>${row.peakPeriod?.toFixed(1) ?? "—"}</td>
          <td>${formatDirection(row.waveDirection)}</td>
          <td>${row.windWaveHeight?.toFixed(2) ?? "—"}</td>
        </tr>`,
    )
    .join("");

  setSafeHTML(
    container,
    `
    <table class="wave-forecast-table">
      <thead>
        <tr>
          <th scope="col">Time (PT)</th>
          <th scope="col">Height (m)</th>
          <th scope="col">Period (s)</th>
          <th scope="col">From</th>
          <th scope="col">Wind wave (m)</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
    <div class="wave-forecast-controls">
      <span class="wave-forecast-extent">
        Showing ${atFullExtent ? "all" : `the first ${Math.round(Math.min(visibleHours, total))} h`}
        of ${Math.round(total)} h — ${shown.length} of ${rows.length} steps
      </span>
      <span class="wave-forecast-buttons">
        ${
          atFullExtent
            ? ""
            : `<button type="button" class="wave-forecast-btn" data-expand="12">+12 hours</button>
               <button type="button" class="wave-forecast-btn" data-expand="24">+24 hours</button>
               <button type="button" class="wave-forecast-btn" data-expand="all">Show all</button>`
        }
        ${
          visibleHours > DEFAULT_VISIBLE_HOURS
            ? '<button type="button" class="wave-forecast-btn wave-forecast-btn-quiet" data-collapse="1">Collapse</button>'
            : ""
        }
      </span>
    </div>`,
  );

  for (const button of container.querySelectorAll("[data-expand]")) {
    button.addEventListener("click", () => {
      const step = button.dataset.expand;
      visibleHours = step === "all" ? total : visibleHours + Number(step);
      renderTable(rows);
    });
  }

  const collapse = container.querySelector("[data-collapse]");
  if (collapse) {
    collapse.addEventListener("click", () => {
      visibleHours = DEFAULT_VISIBLE_HOURS;
      renderTable(rows);
    });
  }
}

/**
 * Render the provenance block under the chart.
 *
 * @param {Object} payload
 * @param {Array<Object>} rows
 * @returns {void}
 */
function renderMetadata(payload, rows) {
  const el = document.getElementById("wave-forecast-metadata");
  if (!el || rows.length === 0) return;

  const runDisplay = formatModelRunTime(payload.model_run_time);
  const first = rows[0].time;
  const last = rows[rows.length - 1].time;

  setSafeHTML(
    el,
    `
    <strong>Station:</strong> ${payload.station_name} (${payload.station_id})<br/>
    <strong>Location:</strong> ${payload.location.lat.toFixed(3)}°N, ${Math.abs(payload.location.lon).toFixed(3)}°W<br/>
    <strong>Model:</strong> ${payload.model}<br/>
    ${runDisplay ? `<strong>Model Run:</strong> ${runDisplay}<br/>` : ""}
    <strong>Data Retrieved:</strong> ${formatMonthDayTimeTZ(new Date(payload.generated_utc))}<br/>
    <strong>Forecast Period:</strong> ${formatMonthDayTimeTZ(first)} to ${formatMonthDayTimeTZ(last)}<br/>
    <strong>Resolution:</strong> ${rows.length} steps — hourly to 24 h, then 3-hourly`,
  );
}

/**
 * Load the forecast and render every view.
 *
 * @returns {Promise<void>}
 */
async function loadWaveForecast() {
  const section = document.getElementById("wave-forecast-section");
  if (!section) return;

  try {
    const payload = await fetchWithTimeout(
      `/data/wave_forecast/${STATION_ID}.json?t=${Date.now()}`,
    );

    const rows = toSortedRows(payload.forecast || {});
    if (rows.length === 0) throw new Error("No forecast steps in payload");

    renderChart(rows);
    renderTable(rows);
    renderMetadata(payload, rows);

    if (!detachWaveThemeListener) {
      detachWaveThemeListener = registerChartThemeListener(() => renderChart(rows));
    }
  } catch (err) {
    logger.error("WaveForecast", "Error loading wave forecast", err);
    const container = document.getElementById("wave-forecast-table");
    if (container) {
      setSafeHTML(
        container,
        '<p style="text-align:center;color:var(--color-text-muted,#999);">⚠️ Wave forecast unavailable</p>',
      );
    }
  }
}

loadWaveForecast();

// The fetch runs 4x/day; a refresh every 2 hours keeps a long-open tab current.
setInterval(loadWaveForecast, 2 * 60 * 60 * 1000);
