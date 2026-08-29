/* -----------------------------
   Chart Utilities
   Shared formatting and config functions

   Classic script: these top-level declarations are globals consumed by the
   other classic page scripts (and, until the ES-module migration finishes,
   by module pages too). The directive below tells ESLint they're exported.
   ----------------------------- */
/* exported calculateArrowRotation, degreesToCardinal,
   getDirectionalArrow, createWindDirectionArrowData, fetchWithTimeout,
   sanitizeSeriesData, registerChartThemeListener, formatCompactTimeLabel,
   getResponsiveGridConfig, getResponsiveLegendBottom, getLegendRowCount, showChartError,
   getMobileOptimizedTooltipConfig */

/* =============================================================================
   DIRECTION ARROW DEFINITIONS (CENTRALIZED)
   The arrow symbol path lives in shared/markers.js (DIRECTION_ARROW_PATH);
   rotation convention below applies to it everywhere.
   ============================================================================= */

/**
 * Calculate arrow rotation for direction display
 *
 * ALWAYS USE THIS FUNCTION - DO NOT rotate by direction directly!
 *
 * @param {number} direction - Direction in degrees (meteorological: coming FROM)
 * @returns {number} Rotation angle in degrees (NEGATED for counter-clockwise)
 *
 * Why negative? Meteorological direction = source bearing (where FROM)
 * Arrow shows destination (where TO) = opposite direction = negate the value
 */
function calculateArrowRotation(direction) {
  return -direction;
}

/* =============================================================================
   SHARED UTILITY FUNCTIONS
   Deduplicated helpers used across multiple page scripts
   ============================================================================= */

/**
 * Convert degrees to 16-point cardinal direction
 * @param {number} degrees - Direction in degrees (0-360)
 * @returns {string|null} Cardinal direction (N, NNE, NE, ...) or null
 */
function degreesToCardinal(degrees) {
  if (degrees == null) return null;
  const directions = [
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
  ];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}

/**
 * Create rotated directional arrow (inline SVG)
 * Supports wind (↓ default) and wave (→ default) arrow types.
 * Uses CSS variable for color so it works in both light and dark themes.
 *
 * @param {number} degrees - Meteorological direction (where wind/waves come FROM)
 * @param {string} arrowType - "wind" or "wave"
 * @returns {string} HTML string with rotated SVG arrow
 */
function getDirectionalArrow(degrees, arrowType = "wind") {
  if (degrees == null || degrees === "—") return "";

  const rotation = arrowType === "wind" ? degrees : degrees + 90;

  const svg =
    arrowType === "wind"
      ? `<svg width="16" height="16" viewBox="0 0 16 16" style="color: var(--color-primary-dark, #004b7c);"><path d="M8 2v12m0 0l-3-3m3 3l3-3" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`
      : `<svg width="16" height="16" viewBox="0 0 16 16" style="color: var(--color-primary-dark, #004b7c);"><path d="M2 8h12m0 0l-3-3m3 3l-3 3" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`;

  return `<span aria-hidden="true" style="display:inline-block;transform:rotate(${rotation}deg);margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

/**
 * Create wind direction arrow data for ECharts scatter series.
 * Positions arrows at the top of the chart, sampled responsively.
 * Handles sparse data gracefully (shows every point if fewer than maxArrows).
 *
 * @param {Array} windDirectionData - Array of {time, value} direction points
 * @param {Array} windSpeedData - Array of {time, value} speed points (for y-axis max)
 * @param {Array} windGustData - Array of {time, value} gust points (for y-axis max)
 * @param {string} [colorOverride] - Optional color override (defaults to theme marker color)
 * @returns {Object} { arrowData, maxValue } for ECharts series and y-axis scaling
 */
function createWindDirectionArrowData(
  windDirectionData,
  windSpeedData,
  windGustData,
  colorOverride,
) {
  if (!windDirectionData || windDirectionData.length === 0)
    return { arrowData: [], maxValue: null };

  // Find maximum wind speed/gust to position arrows at top
  const allSpeeds = [...windSpeedData, ...windGustData]
    .map((d) => (typeof d === "object" ? d.value : d))
    .filter((v) => v != null && !isNaN(v));

  const maxSpeed = allSpeeds.length > 0 ? Math.max(...allSpeeds) : 20;
  const arrowYPosition = maxSpeed * 1.05;

  const arrowData = [];
  const arrowColor = colorOverride || getChartThemeColors().marker;

  // Responsive sampling based on data density and screen size
  const isMobile = window.innerWidth < 600;
  const hoursInterval = isMobile ? 6 : 3;
  const maxArrows = isMobile ? 4 : 8;

  // Calculate sample interval from data density
  const dataSpanHours =
    windDirectionData.length > 1
      ? (new Date(windDirectionData[windDirectionData.length - 1].time) -
          new Date(windDirectionData[0].time)) /
        (1000 * 60 * 60)
      : 24;
  const pointsPerHour = windDirectionData.length / dataSpanHours;

  // For sparse data (fewer points than maxArrows), show every point
  const sampleInterval =
    windDirectionData.length <= maxArrows
      ? 1
      : Math.max(1, Math.round(hoursInterval * pointsPerHour));

  for (let i = 0; i < windDirectionData.length; i += sampleInterval) {
    const dirPoint = windDirectionData[i];
    if (!dirPoint || dirPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value;

    arrowData.push({
      value: [timestamp, arrowYPosition],
      symbolRotate: -direction,
      itemStyle: {
        color: arrowColor,
        opacity: 0.7,
      },
    });
  }

  return { arrowData, maxValue: arrowYPosition };
}

/**
 * Fetch with timeout and retry logic
 * @param {string} url - URL to fetch
 * @param {Object} options - Fetch options plus timeout/retry config
 * @param {number} options.timeout - Timeout in milliseconds (default: 10000)
 * @param {number} options.maxRetries - Maximum retry attempts (default: 3)
 * @param {number} options.retryDelay - Base delay between retries in ms (default: 1000)
 * @returns {Promise<any>} Parsed JSON response
 */
async function fetchWithTimeout(url, options = {}) {
  const timeout = options.timeout || 10000;
  const maxRetries = options.maxRetries || 3;
  const retryDelay = options.retryDelay || 1000;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);

      const fetchOptions = { ...options };
      delete fetchOptions.timeout;
      delete fetchOptions.maxRetries;
      delete fetchOptions.retryDelay;
      fetchOptions.signal = controller.signal;

      const response = await fetch(url, fetchOptions);
      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      if (attempt === maxRetries) {
        logger.error("ChartUtils", `Fetch failed after ${maxRetries} attempts`, error);
        throw error;
      }

      const delay = retryDelay * attempt;
      logger.warn("ChartUtils", `Fetch attempt ${attempt} failed, retrying in ${delay}ms...`, {
        message: error.message,
      });
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

/**
 * Sanitize series data for ECharts
 * Converts invalid values (null, NaN, "MM") to [timestamp, null]
 */
function sanitizeSeriesData(dataArray) {
  return dataArray.map((d) => {
    const y = parseFloat(d.value);
    if (isNaN(y) || d.value == null || d.value === "MM") {
      return [new Date(d.time).getTime(), null];
    }
    return [new Date(d.time).getTime(), y];
  });
}

/**
 * Determine current theme ("light" or "dark")
 */
function getCurrentThemeMode() {
  return document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
}

/**
 * Get palette and typography tokens for charts based on current theme
 */
function getChartThemeColors() {
  const mode = getCurrentThemeMode();
  const isDark = mode === "dark";

  return {
    mode,
    isDark,
    background: isDark ? "#162232" : "#ffffff",
    text: isDark ? "#e5edf5" : "#1a202c",
    mutedText: isDark ? "#9fb3c8" : "#4a5568",
    axisLine: isDark ? "#3f4f67" : "#d0d7de",
    gridLine: isDark ? "rgba(255, 255, 255, 0.04)" : "#e7edf3",
    tooltipBg: isDark ? "rgba(10, 18, 31, 0.95)" : "rgba(255, 255, 255, 0.95)",
    tooltipText: isDark ? "#e6edf5" : "#1f2933",
    tooltipBorder: isDark ? "#4dbdff" : "#004b7c",
    marker: isDark ? "#4dbdff" : "#004b7c",
    series: {
      primary: "#1e88e5",
      secondary: "#fb8c00",
      tertiary: "#999999",
      quaternary: "#43a047",
      quinary: "#8e24aa",
      senary: "#66bb6a",
    },
    symbolBorderColor: isDark ? "#111111" : "#ffffff",
    positive: isDark ? "#4ade80" : "#2e7d32",
    negative: isDark ? "#f87171" : "#c53030",
  };
}

/**
 * Listen for theme changes and invoke callback
 * @param {Function} callback
 * @returns {Function} Cleanup function
 */
function registerChartThemeListener(callback) {
  if (typeof callback !== "function") {
    return () => {};
  }

  const handler = () => callback(getChartThemeColors());
  window.addEventListener("themechange", handler);
  return () => window.removeEventListener("themechange", handler);
}

/**
 * Format time as compact label for chart axes
 * Example: "Mon 14h"
 */
function formatCompactTimeLabel(isoString) {
  const d = new Date(isoString);
  const dayOfWeek = d.toLocaleString("en-US", {
    weekday: "short",
    timeZone: "America/Vancouver",
  });
  const hour = d.toLocaleString("en-US", {
    hour: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
  return `${dayOfWeek} ${hour}h`;
}

/**
 * Horizontal gutters for a chart grid, in pixels — the single source of truth
 * for how much width every chart on the site gives away at its left and right
 * edges.
 *
 * Pixels, not percent, and deliberately small. These sit *outside* the axis
 * labels (every caller pairs them with `containLabel: true`, which already
 * reserves the label's own width), so they are pure margin. A percentage
 * spends the most where there is the least to spend: 8% each side of a 360 px
 * phone chart is 58 px of blank paper, while the same 8% on a 1200 px desktop
 * chart is a gutter nobody minds. A fixed handful of pixels keeps the plot
 * area as wide as the screen allows and still leaves room for the first and
 * last x-axis label to overhang.
 *
 * @returns {{left: number, right: number}}
 */
function getChartSideGutters() {
  return window.innerWidth < 600 ? { left: 4, right: 10 } : { left: 8, right: 16 };
}

/**
 * How many rows a horizontal legend wraps onto.
 *
 * ECharts lays a bottom legend out left to right and wraps when it runs out of
 * chart width, then anchors the whole block by its BOTTOM edge — so every
 * extra row grows upward, into the x-axis labels. Nothing else in an option
 * set reserves space for that, which is how a third legend entry starts
 * writing over the labels on a phone.
 *
 * Measuring the text the way ECharts does (symbol + gap + label at the legend
 * font) is enough to know the row count; the grid can then be told to end
 * above the legend instead of behind it.
 *
 * @param {string[]} labels - Legend entries, in order
 * @param {number} chartWidth - Rendered chart width in px
 * @returns {number} Row count, at least 1
 */
function getLegendRowCount(labels, chartWidth) {
  // ECharts defaults: 25 px symbol, 5 px symbol-to-text gap, 10 px between
  // entries, 12 px legend text.
  const SYMBOL = 30;
  const ITEM_GAP = 10;

  if (!chartWidth || !labels || labels.length === 0) return 1;

  const canvas =
    getLegendRowCount._canvas || (getLegendRowCount._canvas = document.createElement("canvas"));
  const ctx = canvas.getContext("2d");
  ctx.font = "12px sans-serif";

  let rows = 1;
  let used = 0;
  for (const text of labels) {
    const width = SYMBOL + ctx.measureText(text).width;
    if (used > 0 && used + ITEM_GAP + width > chartWidth) {
      rows += 1;
      used = width;
    } else {
      used += (used > 0 ? ITEM_GAP : 0) + width;
    }
  }
  return rows;
}

/**
 * Get responsive grid configuration based on screen width
 *
 * Pass `legendData` and the rendered `size` and the bottom band is measured
 * rather than guessed: it ends above however many rows the legend wraps onto,
 * so the legend always sits BELOW the x-axis labels instead of over them. The
 * percentage bands below are the floor — this only ever widens them. Callers
 * that omit those arguments keep the old single-row behaviour.
 *
 * @param {boolean} isComparisonChart - Whether this is for the comparison chart (needs more bottom space)
 * @param {string[]} [legendData] - Legend entries, if the chart has a bottom legend
 * @param {{width: number, height: number}} [size] - Rendered chart size in px
 */
function getResponsiveGridConfig(isComparisonChart = false, legendData = null, size = null) {
  const width = window.innerWidth;
  const { left, right } = getChartSideGutters();

  if (width < 600) {
    return withLegendRoom({
      left,
      right,
      top: "15%",
      bottom: isComparisonChart ? "28%" : "22%",
      containLabel: true,
    });
  } else if (width < 1000) {
    return withLegendRoom({
      left,
      right,
      top: "12%",
      bottom: isComparisonChart ? "20%" : "16%",
      containLabel: true,
    });
  } else {
    return withLegendRoom({
      left,
      right,
      top: "10%",
      bottom: isComparisonChart ? "16%" : "10%",
      containLabel: true,
    });
  }

  /**
   * Widen `bottom` to clear the legend, in px, when the caller told us enough
   * to work it out.
   *
   * @param {Object} grid
   * @returns {Object} The same grid, with a bottom that clears the legend
   */
  function withLegendRoom(grid) {
    if (!legendData || !size?.height) return grid;

    // A legend row is 14 px of symbol on 12 px text, so 22 px holds one with
    // its line spacing. 8 px of clearance is enough to clear the labels'
    // descenders — any more and the legend drifts away from the chart it
    // belongs to, which on a 360 px-tall canvas is height the plot wants back.
    const ROW_PX = 22;
    const CLEARANCE_PX = 8;

    const legendBottomPx = (parseFloat(getResponsiveLegendBottom()) / 100) * size.height;
    const rows = getLegendRowCount(legendData, size.width);
    const needed = Math.round(legendBottomPx + rows * ROW_PX + CLEARANCE_PX);

    const current =
      typeof grid.bottom === "string" ? (parseFloat(grid.bottom) / 100) * size.height : grid.bottom;
    return { ...grid, bottom: Math.max(Math.round(current), needed) };
  }
}

/**
 * Get responsive legend bottom position based on screen width
 * Scales proportionally with grid bottom spacing for consistent visual gap
 */
function getResponsiveLegendBottom() {
  const width = window.innerWidth;

  if (width < 600) {
    return "10%"; // Mobile: higher up to stay closer to rotated labels
  } else if (width < 1000) {
    return "7%"; // Medium: middle ground
  } else {
    return "5%"; // Desktop: works well per user feedback
  }
}

/**
 * Display error message in chart container
 * @param {HTMLElement|string} container - DOM element or element ID
 * @param {string} chartName - Name of the chart for error message
 * @param {Error} error - The error object
 */
function showChartError(container, chartName, error) {
  const element = typeof container === "string" ? document.getElementById(container) : container;

  if (!element) {
    logger.error("ChartUtils", `Chart container not found: ${container}`);
    return;
  }

  logger.error("ChartUtils", `Error rendering ${chartName}`, error);

  element.innerHTML = `
    <div style="display: flex; align-items: center; justify-content: center; height: 100%; min-height: 200px; padding: 2rem; text-align: center;">
      <div>
        <div style="color: var(--color-accent-red, #e53935); font-size: 1.2rem; margin-bottom: 0.5rem;">⚠️ Chart Error</div>
        <div style="color: var(--color-text-muted, #666); font-size: 0.9rem;">Unable to load ${chartName}</div>
        <div style="color: var(--color-text-light, #999); font-size: 0.8rem; margin-top: 0.5rem;">Check console for details</div>
      </div>
    </div>
  `;
}

/**
 * Get mobile-optimized tooltip configuration for ECharts
 * Improves touch interaction and prevents tooltip from going off-screen
 * @returns {Object} ECharts tooltip configuration
 */
function getMobileOptimizedTooltipConfig() {
  const isMobile = window.innerWidth < 768;
  const theme = getChartThemeColors();

  return {
    trigger: "axis",
    confine: true, // Keep tooltip within chart bounds
    axisPointer: {
      type: "line", // Show only vertical x-axis line (cleaner on mobile)
      label: {
        backgroundColor: theme.tooltipBorder,
        color: theme.tooltipText,
      },
      lineStyle: {
        color: theme.tooltipBorder,
        width: isMobile ? 2 : 1,
        type: "solid",
      },
    },
    // Smart positioning: avoid finger/cursor on mobile
    position: function (point, params, dom, rect, size) {
      if (!isMobile) {
        // Desktop: use default positioning
        return null;
      }

      // Mobile: position tooltip to avoid being covered by finger
      const tooltipWidth = size.contentSize[0];
      const tooltipHeight = size.contentSize[1];
      const chartWidth = size.viewSize[0];
      const chartHeight = size.viewSize[1];
      const fingerOffset = 60; // Offset to clear finger (assuming ~40-50px finger diameter + margin)

      let x = point[0];
      let y = point[1];

      // Horizontal positioning: try right first, then left if no room
      if (x + tooltipWidth + 20 > chartWidth) {
        x = Math.max(10, point[0] - tooltipWidth - 20); // Place to left with margin
      } else {
        x = point[0] + 20; // Place to right with margin
      }

      // Vertical positioning: ALWAYS try above finger first (most important for mobile UX)
      if (point[1] - tooltipHeight - fingerOffset >= 0) {
        // Enough room above - place tooltip above finger
        y = point[1] - tooltipHeight - fingerOffset;
      } else if (point[1] + fingerOffset + tooltipHeight <= chartHeight) {
        // Not enough room above, but room below - place below finger
        y = point[1] + fingerOffset;
      } else {
        // Constrained space - place at top of chart (best compromise)
        y = 10;
      }

      return [x, y];
    },
    // Enhanced touch behavior
    renderMode: "html",
    className: "echarts-tooltip-mobile",
    backgroundColor: theme.tooltipBg,
    borderColor: theme.tooltipBorder,
    borderWidth: 1,
    textStyle: {
      color: theme.tooltipText,
      fontSize: isMobile ? 12 : 14,
    },
    padding: isMobile ? 8 : 12,
  };
}

/* =============================================================================
   ACCESSIBILITY: CHART TEXT ALTERNATIVES
   ============================================================================= */

// Every chart gets ECharts' generated text description (aria.enabled builds an
// aria-label from the series data) without each page opting in: wrap init once
// so instances inject the default into any option that doesn't set its own.
// This file loads right after the echarts vendor script on every chart page;
// pages without charts (forecasts) don't load echarts at all, hence the guard.
if (typeof echarts !== "undefined") {
  const echartsInit = echarts.init.bind(echarts);
  echarts.init = (...initArgs) => {
    const chart = echartsInit(...initArgs);
    const setOption = chart.setOption.bind(chart);
    chart.setOption = (option, ...rest) =>
      setOption({ aria: { enabled: true }, ...option }, ...rest);
    return chart;
  };
}
