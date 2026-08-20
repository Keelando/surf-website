/* -----------------------------
   Wave and Wind Forecast (ES module entry point)

   Waves are RDWPS, wind is HRDPS — the two arrive in one payload but they are
   not one model, so each chart names its own source (see `models` in the
   payload and the header of scripts/fetch/fetch_wave_forecast.py).

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

// Halibut Bank is the default because it is the reference station: an EC buoy
// reports the same spot, so it is the one forecast a reader can immediately
// check against something measured.
const DEFAULT_STATION_ID = "4600146";

// Wave heights in the Strait rarely trouble a metre outside a winter
// southeasterly, and the summer forecasts sit under 0.2 m. Auto-scaling to
// the data would render a flat calm as a dramatic mountain range, so the
// axis holds a fixed 0–1 m unless the forecast exceeds it.
const MIN_HEIGHT_AXIS_MAX = 1.0;

// Hours of the table shown before the reader asks for more. The table runs at
// 3-hourly spacing, so this is 8 rows rather than 24.
const DEFAULT_VISIBLE_HOURS = 24;

// Wind axis floor, knots. Same reasoning as the height floor: a calm summer
// afternoon of 4 kt breezes should look calm, not like a working breeze.
const MIN_WIND_AXIS_MAX = 15;

// Table cadence. The chart keeps every step the fetch stored.
const TABLE_STEP_HOURS = 3;

// The fetch stores km/h (site-wide storage unit); every display is knots.
const KMH_TO_KNOTS = 0.539957;

// Which view the segmented control has selected. Waves is the default: it is
// the forecast the page exists for, and the wind is the input behind it.
const MODE_WAVES = "waves";
const MODE_WIND = "wind";
let currentMode = MODE_WAVES;

let waveChart = null;
let windChart = null;
let verificationWaveChart = null;
let verificationWindChart = null;
let detachWaveThemeListener = null;
let visibleHours = DEFAULT_VISIBLE_HOURS;
let currentStationId = DEFAULT_STATION_ID;
// Held at module scope rather than closed over at load time: the theme
// listener is registered once and outlives any number of station switches, so
// a captured row set would redraw the previous station's chart on a theme flip.
let currentRows = [];
// Same reasoning for the verification payload: one fetch per station switch, read
// again on every mode switch and every theme flip.
let currentVerification = null;

/**
 * Convert a stored km/h wind value to the knots the site displays.
 *
 * @param {number|null|undefined} kmh
 * @returns {number|null}
 */
function toKnots(kmh) {
  return kmh === null || kmh === undefined ? null : kmh * KMH_TO_KNOTS;
}

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
      windSpeed: toKnots(values.wind_speed),
      windDirection: values.wind_direction ?? null,
      // Absent at most steps by design: HRDPS only diagnoses a gust where
      // there is one, so a value being present is itself the signal rather
      // than a gust of zero.
      windGust: toKnots(values.wind_gust),
    }))
    .filter((row) => !isNaN(row.time))
    .sort((a, b) => a.time - b.time);
}

/**
 * Thin rows to a 3-hourly cadence for the table.
 *
 * The fetch stores hourly steps out to 24 h, and the chart draws every one of
 * them — a line can carry that density, a table of numbers cannot. Thinning
 * here rather than at the fetch keeps the hourly series in the database and on
 * the chart, and makes the table read at one cadence across the whole 48 h
 * instead of changing gear at the 24-hour taper point.
 *
 * These are *sampled* steps, not averages. Only wave height would survive
 * averaging: peak period is a modal value (the mean of a 4 s wind chop and an
 * 11 s swell is 7.5 s, an interval that occurs in neither), direction is
 * circular (a plain mean across 350° and 010° gives 180°, the exact opposite),
 * and wind-wave height is absent wherever the model reports no wind sea, so a
 * bucket mean would silently mix "calm" with "not applicable". Every number in
 * the table is therefore a value the model actually produced.
 *
 * Measured off the first step's lead time rather than the wall-clock hour, so
 * it lines up with the fetcher's own taper (see lib/forecast_steps.py).
 *
 * @param {Array<Object>} rows - Every forecast step, sorted by valid time
 * @returns {Array<Object>} Rows at 3-hourly spacing
 */
function toThreeHourly(rows) {
  if (rows.length === 0) return [];
  const start = rows[0].time.getTime();
  return rows.filter((row) => {
    const lead = (row.time.getTime() - start) / (60 * 60 * 1000);
    return Number.isInteger(lead) ? lead % TABLE_STEP_HOURS === 0 : false;
  });
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
 * Upper bound for the wind-speed axis.
 *
 * Covers gusts as well as the sustained wind, since the gust scatter is drawn
 * against the same axis, and leaves headroom for the direction arrows that
 * ride near the top.
 *
 * @param {Array<Object>} rows
 * @returns {number} Axis maximum in knots
 */
function windAxisMax(rows) {
  const peak = rows.reduce((max, row) => Math.max(max, row.windSpeed ?? 0, row.windGust ?? 0), 0);
  if (peak <= MIN_WIND_AXIS_MAX) return MIN_WIND_AXIS_MAX;
  // Round up to the next 5 kt so the arrows clear the data.
  return Math.ceil((peak * 1.15) / 5) * 5;
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
 * @param {number} axisMax - Axis maximum of the chart, for vertical placement
 * @param {Object} colors - Resolved theme palette
 * @param {string} field - Row property holding the direction, in degrees
 * @returns {Array<Object>} ECharts scatter points carrying symbolRotate
 */
function createDirectionArrows(rows, axisMax, colors, field = "waveDirection") {
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
    if (row[field] === null) continue;

    const stamp = row.time.getTime();
    if (lastStamp !== null && stamp - lastStamp < intervalMs) continue;

    arrows.push({
      value: [stamp, arrowY],
      // Meteorological direction is where the waves or wind come FROM; the
      // arrow points where they are going, which is what the helper handles.
      symbolRotate: calculateArrowRotation(row[field]),
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
          // The legend swatch is drawn from itemStyle, not lineStyle. Without
          // this it falls back to the default ECharts palette and the legend
          // disagrees with the line it labels.
          itemStyle: { color: colors.series.primary },
        },
        {
          name: "Peak period",
          type: "line",
          data: periodSeries,
          yAxisIndex: 1,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2, color: colors.series.secondary, type: "dashed" },
          itemStyle: { color: colors.series.secondary },
        },
        {
          name: "Wave direction",
          type: "scatter",
          data: arrows,
          yAxisIndex: 0,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: (value, params) => arrows[params.dataIndex]?.symbolRotate ?? 0,
          // Per-point itemStyle (set in createDirectionArrows) styles the
          // arrows; the legend reads only the series-level one.
          itemStyle: { color: colors.marker, opacity: 0.75 },
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
 * Render the wind forecast chart.
 *
 * A chart of its own rather than a third and fourth series on the wave chart:
 * the wind is a different model (HRDPS, not RDWPS), and stacking a knots axis
 * onto a chart that already carries height, period and wave-direction arrows
 * would leave neither readable. Same time axis and same furniture, so the two
 * read as one forecast stacked vertically.
 *
 * @param {Array<Object>} rows
 * @returns {void}
 */
function renderWindChart(rows) {
  const el = document.getElementById("wind-forecast-chart");
  if (!el || typeof echarts === "undefined") return;

  const colors = getChartThemeColors();

  if (!windChart) {
    windChart = echarts.init(el);
    window.addEventListener("resize", () => windChart.resize());
  }

  const speedSeries = rows.map((row) => [row.time.getTime(), row.windSpeed]);
  // Gusts are masked at most hours, so this series is deliberately sparse —
  // scatter rather than a line, which would draw a connecting segment across
  // the hours the model reported no gust at all.
  const gustSeries = rows
    .filter((row) => row.windGust !== null)
    .map((row) => [row.time.getTime(), row.windGust]);

  const axisMax = windAxisMax(rows);
  const arrows = createDirectionArrows(rows, axisMax, colors, "windDirection");

  windChart.setOption(
    {
      backgroundColor: "transparent",
      textStyle: { color: colors.text },
      grid: getResponsiveGridConfig(false),
      legend: {
        data: ["Wind speed", "Gust", "Wind direction"],
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
                  Wind: ${row.windSpeed?.toFixed(1) ?? "—"} kt<br/>
                  Gust: ${row.windGust !== null ? `${row.windGust.toFixed(1)} kt` : "none forecast"}<br/>
                  From: ${formatDirection(row.windDirection)}`;
        },
      },
      xAxis: {
        type: "time",
        axisLabel: {
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
      yAxis: {
        type: "value",
        name: "Speed (kt)",
        min: 0,
        max: axisMax,
        nameTextStyle: { color: colors.series.secondary },
        axisLine: { lineStyle: { color: colors.series.secondary } },
        axisLabel: { color: colors.mutedText },
        splitLine: { lineStyle: { color: colors.gridLine } },
      },
      series: [
        {
          name: "Wind speed",
          // Matches the buoy wind chart: orange line, red gust points.
          type: "line",
          data: speedSeries,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2.5, color: colors.series.secondary },
          areaStyle: { opacity: colors.isDark ? 0 : 0.12, color: colors.series.secondary },
          itemStyle: { color: colors.series.secondary },
        },
        {
          name: "Gust",
          type: "scatter",
          data: gustSeries,
          symbol: "circle",
          symbolSize: 6,
          itemStyle: { color: colors.negative },
        },
        {
          name: "Wind direction",
          type: "scatter",
          data: arrows,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: (value, params) => arrows[params.dataIndex]?.symbolRotate ?? 0,
          // Per-point itemStyle (set in createDirectionArrows) styles the
          // arrows; the legend reads only the series-level one.
          itemStyle: { color: colors.marker, opacity: 0.75 },
          tooltip: { show: false },
          silent: true,
          z: 2,
        },
      ],
    },
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
 * Render the tabular forecast at 3-hourly spacing, showing `visibleHours` of it.
 *
 * Takes every step and thins it here rather than receiving a thinned set, so
 * the caller keeps one row list and the chart keeps the hourly detail. The
 * full 48 hours is 17 rows at this cadence, which still buries the chart and
 * the provenance block below it, so the expand controls stay — an inner
 * scrollbar is easy to miss entirely, and the table then just looks shorter
 * than the chart above it.
 *
 * @param {Array<Object>} allRows - Every forecast step
 * @returns {void}
 */
/**
 * Column definitions per mode: header text and the cell for a row.
 *
 * Held as data rather than two template literals so the table markup, its
 * header, and its column count cannot drift apart — a header added without a
 * matching cell is the classic way a table like this goes crooked.
 *
 * Wind wave height stays on the waves side: it is a wave partition (the part
 * of the sea being raised by the local wind right now), not a wind reading.
 */
const TABLE_COLUMNS = {
  [MODE_WAVES]: [
    { header: "Height (m)", cell: (row) => row.waveHeight?.toFixed(2) ?? "—" },
    { header: "Period (s)", cell: (row) => row.peakPeriod?.toFixed(1) ?? "—" },
    { header: "From", cell: (row) => formatDirection(row.waveDirection) },
    { header: "Wind wave (m)", cell: (row) => row.windWaveHeight?.toFixed(2) ?? "—" },
  ],
  [MODE_WIND]: [
    { header: "Wind (kt)", cell: (row) => row.windSpeed?.toFixed(0) ?? "—" },
    { header: "Gust (kt)", cell: (row) => (row.windGust !== null ? row.windGust.toFixed(0) : "—") },
    { header: "From", cell: (row) => formatDirection(row.windDirection) },
  ],
};

function renderTable(allRows) {
  const container = document.getElementById("wave-forecast-table");
  if (!container) return;

  const rows = toThreeHourly(allRows);
  const total = spanHours(rows);
  const shown = rowsWithin(rows, visibleHours);
  const atFullExtent = shown.length >= rows.length;

  const columns = TABLE_COLUMNS[currentMode];
  const body = shown
    .map(
      (row) => `
        <tr>
          <th scope="row">${formatStepLabel(row.time)}</th>
          ${columns.map((column) => `<td>${column.cell(row)}</td>`).join("")}
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
          ${columns.map((column) => `<th scope="col">${column.header}</th>`).join("")}
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
               <button type="button" class="wave-forecast-btn" data-expand="all">Show all 48 h</button>`
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
      renderTable(allRows);
    });
  }

  const collapse = container.querySelector("[data-collapse]");
  if (collapse) {
    collapse.addEventListener("click", () => {
      visibleHours = DEFAULT_VISIBLE_HOURS;
      renderTable(allRows);
    });
  }
}

/**
 * Draw whichever chart the active mode calls for.
 *
 * Only the visible one is drawn, and it is drawn every time it becomes
 * visible. ECharts measures its container at init, so initialising a chart
 * inside a `hidden` panel yields a zero-sized canvas that no later resize
 * fully recovers — deferring the init until the panel is shown avoids that
 * entirely. Redrawing on every switch also means a theme change while a chart
 * was hidden cannot leave stale palette colours behind.
 *
 * @param {Array<Object>} rows
 * @returns {void}
 */
function renderForecastMode(rows) {
  if (rows.length === 0) return;
  if (currentMode === MODE_WIND) {
    renderWindChart(rows);
    windChart?.resize();
  } else {
    renderChart(rows);
    waveChart?.resize();
  }
}

/**
 * Switch the section between the waves and wind views.
 *
 * Every copy of the control is updated, not just the one that was clicked —
 * there are two on the page (above the chart, below the table) and they must
 * never disagree about what is being shown.
 *
 * @param {string} mode - MODE_WAVES or MODE_WIND
 * @returns {void}
 */
function setForecastMode(mode) {
  currentMode = mode === MODE_WIND ? MODE_WIND : MODE_WAVES;

  for (const button of document.querySelectorAll(".forecast-mode-btn")) {
    const isActive = button.dataset.forecastMode === currentMode;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  }

  for (const panel of document.querySelectorAll(".forecast-panel")) {
    panel.hidden = panel.dataset.panel !== currentMode;
  }

  renderForecastMode(currentRows);
  renderTable(currentRows);
  renderVerificationMode();
}

/**
 * Wire both copies of the segmented control.
 *
 * Bound once at init rather than per render: the buttons are static markup in
 * forecasts.html, so nothing here replaces them.
 *
 * @returns {void}
 */
function initModeToggle() {
  for (const button of document.querySelectorAll(".forecast-mode-btn")) {
    button.addEventListener("click", () => setForecastMode(button.dataset.forecastMode));
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

  const first = rows[0].time;
  const last = rows[rows.length - 1].time;

  // One line per model rather than a single "Model Run": waves and wind come
  // from different models on the same 00/06/12/18Z cadence, and a fetch can
  // legitimately catch different runs of each. `models` is the payload's own
  // provenance list; the top-level `model` fields describe the wave model only.
  const models = Array.isArray(payload.models)
    ? payload.models
    : [{ name: payload.model, run_time: payload.model_run_time }];
  const modelLines = models
    .map((model) => {
      const run = formatModelRunTime(model.run_time);
      return `<strong>Model:</strong> ${model.name}${run ? ` — run ${run}` : ""}<br/>`;
    })
    .join("");

  setSafeHTML(
    el,
    `
    <strong>Station:</strong> ${payload.station_name} (${payload.station_id})<br/>
    <strong>Location:</strong> ${payload.location.lat.toFixed(3)}°N, ${Math.abs(payload.location.lon).toFixed(3)}°W<br/>
    ${modelLines}
    <strong>Data Retrieved:</strong> ${formatMonthDayTimeTZ(new Date(payload.generated_utc))}<br/>
    <strong>Forecast Period:</strong> ${formatMonthDayTimeTZ(first)} to ${formatMonthDayTimeTZ(last)}<br/>
    <strong>Resolution:</strong> ${rows.length} steps — hourly to 24 h, then 3-hourly;
    the table below samples every ${TABLE_STEP_HOURS} h`,
  );
}

/* -----------------------------
   Forecast verification — the last 48 h, forecast against observation.

   The charts above look forward; these look back at the same station, so the
   reader can judge the forecast on the only evidence that counts. The model
   line is a stitched ~24 h-lead band (see
   scripts/export/export_forecast_verification.py): for each past hour, the newest
   run that was still at least ~19 h ahead. It is NOT one run's output, and it
   is not a flat 24 h — leads cycle 19→24, so a small step at a six-hourly seam
   is two runs disagreeing, which is the honest picture.
   ----------------------------- */

/**
 * Pull one variable out of the verification payload as ECharts series data.
 *
 * @param {string} variable - Payload series key, e.g. "wave_height"
 * @param {function(number): number|null} [convert] - Unit conversion per value
 * @returns {{forecast: Array, observed: Array}} `[timestamp, value]` pairs,
 *   with the forecast points carrying their own lead hours for the tooltip
 */
function verificationSeries(variable, convert) {
  const series = currentVerification?.series?.[variable];
  const scale = convert || ((value) => value);
  const points = (list, extra) =>
    (list || []).map((point) => {
      const row = [new Date(point.time).getTime(), scale(point.value)];
      return extra ? { value: row, leadHours: point.lead_hours } : row;
    });
  return {
    forecast: points(series?.forecast, true),
    observed: points(series?.observed, false),
  };
}

/**
 * Nearest point in a series to a timestamp, for the shared tooltip.
 *
 * The two series are deliberately not resampled onto a common axis — the buoy
 * reports hourly and CRPILE every 10 minutes, and thinning either to match the
 * other would throw away real detail. That leaves most hovered instants with a
 * point in one series and not the other, so the tooltip looks each up itself
 * rather than relying on what ECharts happens to have at that exact x.
 *
 * @param {Array} points
 * @param {number} timestamp
 * @param {number} toleranceMs
 * @returns {{value: number, leadHours: number|undefined}|null}
 */
function nearestPoint(points, timestamp, toleranceMs) {
  let best = null;
  for (const point of points) {
    const row = Array.isArray(point) ? point : point.value;
    const gap = Math.abs(row[0] - timestamp);
    if (row[1] === null || gap > toleranceMs) continue;
    if (!best || gap < best.gap) {
      best = { gap, value: row[1], leadHours: Array.isArray(point) ? undefined : point.leadHours };
    }
  }
  return best;
}

/**
 * Shared option set for both verification charts.
 *
 * Observed is a solid line, forecast a dashed one — the same visual grammar as
 * the storm-surge verification chart, where dashed always means "this was a
 * prediction".
 *
 * @param {Object} config
 * @returns {Object} An ECharts option object
 */
function verificationOption({ label, unit, series, decimals, axisMax }) {
  const colors = getChartThemeColors();
  // Sentence case in the legend, title case on the axis: "Observed Wind speed"
  // reads as a proper noun.
  const observedName = `Observed ${label.toLowerCase()}`;
  const forecastName = `Forecast (~24 h ahead)`;

  return {
    backgroundColor: "transparent",
    textStyle: { color: colors.text },
    grid: getResponsiveGridConfig(false),
    legend: {
      data: [observedName, forecastName],
      bottom: getResponsiveLegendBottom(),
      textStyle: { color: colors.mutedText },
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        const stamp = params[0]?.axisValue ?? params[0]?.value?.[0];
        if (stamp === undefined) return "";
        // 30 minutes: wide enough to catch the hourly buoy against an on-the-
        // hour forecast, narrow enough that it can't reach the next step.
        const observed = nearestPoint(series.observed, stamp, 30 * 60 * 1000);
        const forecast = nearestPoint(series.forecast, stamp, 30 * 60 * 1000);
        const show = (point) => (point ? `${point.value.toFixed(decimals)} ${unit}` : "—");

        let error = "";
        if (observed && forecast) {
          const delta = forecast.value - observed.value;
          const sign = delta >= 0 ? "+" : "−";
          error = `<br/>Model error: ${sign}${Math.abs(delta).toFixed(decimals)} ${unit}`;
        }
        const lead = forecast?.leadHours ? `<br/>Lead time: ${forecast.leadHours} h` : "";

        return `<strong>${formatStepLabel(new Date(stamp))}</strong><br/>
                Observed: ${show(observed)}<br/>
                Forecast: ${show(forecast)}${error}${lead}`;
      },
    },
    xAxis: {
      type: "time",
      axisLabel: {
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
    yAxis: {
      type: "value",
      name: `${label} (${unit})`,
      min: 0,
      max: axisMax,
      nameTextStyle: { color: colors.mutedText },
      axisLine: { lineStyle: { color: colors.axisLine } },
      axisLabel: { color: colors.mutedText },
      splitLine: { lineStyle: { color: colors.gridLine } },
    },
    series: [
      {
        name: observedName,
        type: "line",
        data: series.observed,
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2.5, color: colors.series.primary },
        areaStyle: { opacity: 0.12, color: colors.series.primary },
        itemStyle: { color: colors.series.primary },
      },
      {
        name: forecastName,
        type: "line",
        data: series.forecast,
        smooth: true,
        showSymbol: false,
        // Dashed and unfilled: a prediction, drawn over the record of what
        // actually happened rather than competing with it.
        lineStyle: { width: 2, color: colors.series.secondary, type: "dashed" },
        itemStyle: { color: colors.series.secondary },
      },
    ],
  };
}

/**
 * Largest value across both verification series, floored so a calm spell stays calm.
 *
 * Rounded up to a whole `step` so the axis lands on a readable number — an
 * auto-fitted max prints labels like "16.9 kt", which tells the reader nothing
 * and makes two stations' charts hard to compare at a glance.
 *
 * @param {Object} series
 * @param {number} floor - Minimum axis top, so flat calm looks flat
 * @param {number} step - Round the top up to a multiple of this
 * @returns {number}
 */
function verificationAxisMax(series, floor, step) {
  const values = [...series.observed, ...series.forecast]
    .map((point) => (Array.isArray(point) ? point[1] : point.value[1]))
    .filter((value) => value !== null && value !== undefined);
  const peak = values.length > 0 ? Math.max(...values) : 0;
  const headroom = Math.ceil((peak * 1.15) / step) * step;
  // Floating-point multiples of 0.5 need rounding back to a clean decimal.
  return Math.round(Math.max(floor, headroom) * 100) / 100;
}

/**
 * Draw the verification chart for the active mode.
 *
 * Only significant wave height and sustained wind are charted, one comparison
 * per view. Adding period or direction would put four lines on a chart whose
 * entire job is to make one gap easy to see. Gusts are left out for a stronger
 * reason: HRDPS masks the gust at most hours, so the forecast side would be a
 * near-empty line beside a continuous observed one, which reads as the model
 * predicting calm rather than the model not being asked.
 *
 * Same deferred-init rule as the forecast charts: ECharts cannot size itself
 * inside a `hidden` panel, so each chart is created only once its panel shows.
 *
 * @returns {void}
 */
function renderVerificationMode() {
  if (!currentVerification || typeof echarts === "undefined") return;

  if (currentMode === MODE_WIND) {
    const el = document.getElementById("wind-verification-chart");
    if (!el) return;
    if (!verificationWindChart) {
      verificationWindChart = echarts.init(el);
      window.addEventListener("resize", () => verificationWindChart.resize());
    }
    const series = verificationSeries("wind_speed", toKnots);
    verificationWindChart.setOption(
      verificationOption({
        label: "Wind speed",
        unit: "kt",
        series,
        decimals: 1,
        // 5 kt steps: the granularity anyone actually thinks in on the water.
        axisMax: verificationAxisMax(series, MIN_WIND_AXIS_MAX, 5),
      }),
      { notMerge: true },
    );
    verificationWindChart.resize();
    return;
  }

  const el = document.getElementById("wave-verification-chart");
  if (!el) return;
  if (!verificationWaveChart) {
    verificationWaveChart = echarts.init(el);
    window.addEventListener("resize", () => verificationWaveChart.resize());
  }
  const series = verificationSeries("wave_height");
  verificationWaveChart.setOption(
    verificationOption({
      label: "Wave height",
      unit: "m",
      series,
      decimals: 2,
      // 0.5 m steps, matching how the forward wave chart scales.
      axisMax: verificationAxisMax(series, MIN_HEIGHT_AXIS_MAX, 0.5),
    }),
    { notMerge: true },
  );
  verificationWaveChart.resize();
}

/**
 * Name the window the verification panel covers, in the heading itself.
 *
 * The exporter owns the window length, so the heading reads it from the
 * payload rather than restating 48 h in the markup — a heading that had to be
 * hand-edited whenever the export changed is a heading that would eventually
 * be wrong. Hours up to three days, days beyond that, because "last 168 hours"
 * is not how anyone thinks about a week.
 *
 * @returns {void}
 */
function renderVerificationHeading() {
  const heading = document.getElementById("forecast-verification-heading");
  const hours = currentVerification?.window_hours;
  if (!heading || !hours) return;

  const span =
    hours <= 72
      ? `${hours} hour${hours === 1 ? "" : "s"}`
      : (() => {
          const days = Math.round(hours / 24);
          return `${days} day${days === 1 ? "" : "s"}`;
        })();

  heading.textContent = `Forecast Verification — Last ${span}`;
}

/**
 * Caption the verification panel: what the dashed line actually is.
 *
 * The lead band comes from the payload rather than being written here — the
 * exporter decides the policy, and a page that hardcoded "24 hours" would
 * quietly start lying the moment the fetch cadence or taper changed.
 *
 * @returns {void}
 */
function renderVerificationMetadata() {
  const el = document.getElementById("forecast-verification-metadata");
  if (!el || !currentVerification) return;

  renderVerificationHeading();

  const band = currentVerification.lead_band || {};
  const min = band.min_hours ?? 19;
  const max = band.max_hours ?? 24;

  setSafeHTML(
    el,
    `<strong>How to read this:</strong> the dashed line is what the model said
     about a day ahead — for each hour, the most recent run that was still
     ${min}–${max} hours away. Because runs come every six hours, the lead time
     sawtooths between ${min} and ${max} h, so a small step every six hours is
     two runs disagreeing, not a data glitch. Hover any point for its exact lead
     time and the model's error.<br/>
     <strong>Window:</strong> last ${currentVerification.window_hours} h &nbsp;·&nbsp;
     <strong>Updated:</strong> ${formatMonthDayTimeTZ(new Date(currentVerification.generated_utc))}`,
  );
}

/**
 * Load the verification series for `currentStationId`.
 *
 * Failure is a warning, not an error: the verification panel is an extra panel below the
 * forecast, and a station whose archive is younger than the window legitimately
 * has no file yet. The console-error suite treats `error` as a page failure.
 *
 * @returns {Promise<void>}
 */
async function loadVerification() {
  const section = document.getElementById("forecast-verification");
  if (!section) return;

  try {
    currentVerification = await fetchWithTimeout(
      `/data/wave_forecast/verification/${currentStationId}.json?t=${Date.now()}`,
    );
    section.hidden = false;
    renderVerificationMode();
    renderVerificationMetadata();
  } catch (err) {
    logger.warn("WaveForecast", "Verification data unavailable for this station", err);
    currentVerification = null;
    section.hidden = true;
  }
}

/**
 * Build the station picker from the fetcher's index.
 *
 * Hidden when only one station has a forecast, so the control appears the
 * moment a second point is added and not before. The list comes from
 * index.json rather than being written out here — the fetcher already knows
 * which stations it produced, and duplicating that list in JavaScript is how
 * it goes stale.
 *
 * @param {Array<Object>} stations - `stations` from index.json
 * @returns {void}
 */
function renderStationPicker(stations) {
  const container = document.getElementById("wave-forecast-station");
  if (!container) return;

  if (stations.length < 2) {
    setSafeHTML(container, "");
    return;
  }

  const options = stations
    .map(
      (station) =>
        `<option value="${station.station_id}"${
          station.station_id === currentStationId ? " selected" : ""
        }>${station.name}</option>`,
    )
    .join("");

  setSafeHTML(
    container,
    `<label for="wave-forecast-station-select">Station</label>
     <select id="wave-forecast-station-select">${options}</select>`,
  );

  container.querySelector("select").addEventListener("change", (event) => {
    currentStationId = event.target.value;
    // A fresh station starts collapsed: carrying an expanded table across a
    // switch would show one station's window on another station's data.
    visibleHours = DEFAULT_VISIBLE_HOURS;
    loadWaveForecast();
  });
}

/**
 * Set the section heading to the station being shown, and point the map link
 * at it.
 *
 * The map link uses the same `/?station=<id>#map-section` form the webcam
 * cards and buoy cards use — the home-page map keys its markers by the station
 * id in stations.json, which is the id the forecast files are named for.
 *
 * @param {string} stationName
 * @param {string} stationId
 * @returns {void}
 */
function renderHeading(stationName, stationId) {
  const heading = document.getElementById("wave-forecast-heading");
  // Just the station name: the "Point Forecast" <h2> above already says what
  // kind of forecast this is, so repeating it here only added noise.
  if (heading) heading.textContent = stationName;

  const mapLink = document.getElementById("wave-forecast-map-link");
  if (mapLink && stationId) {
    mapLink.href = `/?station=${encodeURIComponent(stationId)}#map-section`;
  }
}

/**
 * Fetch the station index.
 *
 * Fetch only — it does not touch `currentStationId` or render anything, so it
 * can run alongside the forecast load rather than in front of it.
 *
 * A missing or broken index is not fatal: the default station's forecast still
 * renders, just without a picker. That keeps the page working if the index
 * write is the thing that failed.
 *
 * @returns {Promise<Array<Object>>} The stations offered, empty if unavailable
 */
async function loadStationIndex() {
  try {
    const index = await fetchWithTimeout(`/data/wave_forecast/index.json?t=${Date.now()}`);
    return index.stations || [];
  } catch (err) {
    // warn, not error: a missing index is a handled condition, and the
    // console-error suite treats error as a page failure.
    logger.warn("WaveForecast", "Station index unavailable, using default station", err);
    return [];
  }
}

/**
 * Load the forecast for `currentStationId` and render every view.
 *
 * @returns {Promise<void>}
 */
async function loadWaveForecast() {
  const section = document.getElementById("wave-forecast-section");
  if (!section) return;

  try {
    const payload = await fetchWithTimeout(
      `/data/wave_forecast/${currentStationId}.json?t=${Date.now()}`,
    );

    const rows = toSortedRows(payload.forecast || {});
    if (rows.length === 0) throw new Error("No forecast steps in payload");

    currentRows = rows;
    renderHeading(payload.station_name, payload.station_id || currentStationId);
    renderForecastMode(rows);
    renderTable(rows);
    renderMetadata(payload, rows);

    if (!detachWaveThemeListener) {
      // Only the visible chart — the hidden one is redrawn from scratch the
      // moment it is shown, so there is nothing stale to repaint.
      detachWaveThemeListener = registerChartThemeListener(() => {
        renderForecastMode(currentRows);
        renderVerificationMode();
      });
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

/**
 * Station id requested by the URL hash, if any.
 *
 * `#wave-<id>` is what the buoy cards on the home page link to. Namespaced
 * rather than a bare id so it cannot collide with an element id on the page —
 * forecasts.html has several other anchors.
 *
 * @returns {string|null}
 */
function stationFromHash() {
  const match = /^#wave-(.+)$/.exec(window.location.hash);
  return match ? decodeURIComponent(match[1]) : null;
}

/**
 * Apply the hash, if it names a station we have a forecast for.
 *
 * Validated against the index rather than trusted: an unknown id would
 * otherwise send the page to fetch a file that does not exist and render the
 * error state, when falling back to the default station is the better answer.
 *
 * @param {Array<Object>} stations
 * @returns {boolean} Whether the hash selected a station
 */
function applyHashStation(stations) {
  const requested = stationFromHash();
  if (!requested) return false;
  if (!stations.some((station) => station.station_id === requested)) {
    logger.warn("WaveForecast", `No forecast for station "${requested}" from URL hash`);
    return false;
  }
  currentStationId = requested;
  return true;
}

async function initWaveForecast() {
  // Take the hash at its word and start the forecast fetch immediately. The
  // index only decides which stations the *picker* offers — nothing the chart
  // needs — so awaiting it first put a whole network round trip in front of
  // the one thing the reader came for. Worse, `fetchWithTimeout` retries three
  // times with backoff, so a missing index.json (before the fetcher had ever
  // written one) delayed the chart by several seconds rather than failing fast.
  const requested = stationFromHash();
  if (requested) currentStationId = requested;

  initModeToggle();

  const forecastLoad = loadWaveForecast();
  const stations = await loadStationIndex();

  // Correct the optimistic guess only if it was actually wrong: an unknown
  // station from the hash, or a default with no forecast this run.
  const offered = (id) => stations.some((station) => station.station_id === id);
  if (stations.length > 0 && !offered(currentStationId)) {
    if (requested) {
      logger.warn("WaveForecast", `No forecast for station "${requested}" from URL hash`);
    }
    currentStationId = stations[0].station_id;
    await forecastLoad; // let the failing load settle before replacing its output
    await loadWaveForecast();
  } else {
    await forecastLoad;
  }

  // Only once the station is settled: the verification panel is below the fold,
  // so it is not worth fetching for a station the index is about to correct.
  await loadVerification();

  renderStationPicker(stations);

  // Arriving at #wave-<id> from another page scrolls to the section, but a
  // same-page hash change does not re-run any of the above.
  window.addEventListener("hashchange", async () => {
    if (!applyHashStation(stations)) return;
    visibleHours = DEFAULT_VISIBLE_HOURS;
    renderStationPicker(stations);
    await loadWaveForecast();
    await loadVerification();
  });
}

initWaveForecast();

// The fetch runs 4x/day; a refresh every 2 hours keeps a long-open tab current.
setInterval(loadWaveForecast, 2 * 60 * 60 * 1000);

// The verification panel moves with the instruments, not the model — its observed half is
// re-exported every 10 minutes — so it refreshes on its own, faster clock.
setInterval(loadVerification, 15 * 60 * 1000);
