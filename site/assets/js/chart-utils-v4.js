/* -----------------------------
   Chart Utilities
   Shared formatting and config functions
   ----------------------------- */

/* =============================================================================
   DIRECTION ARROW DEFINITIONS (CENTRALIZED)
   All direction markers across the frontend use these definitions
   ============================================================================= */

/**
 * ECharts arrow symbol path - points DOWNWARD at 0° rotation
 * Skinny notched arrow design
 *
 * CRITICAL: Arrow shows where wind/waves are TRAVELING TO, not coming from
 * Meteorological convention: direction value = where wind/waves COME FROM
 * Therefore: arrow must point OPPOSITE to the bearing (hence negative rotation)
 *
 * Philosophy: Arrow defaults pointing DOWN, rotates COUNTER-CLOCKWISE (negated degrees)
 *   0° = FROM NORTH → arrow points down (traveling south) → rotation: -0° = 0°
 *   90° = FROM EAST → arrow points left (traveling west) → rotation: -90°
 *   180° = FROM SOUTH → arrow points up (traveling north) → rotation: -180°
 *   270° = FROM WEST → arrow points right (traveling east) → rotation: -270°
 */
const DIRECTION_ARROW_PATH = "path://M0,15 L-3,-5 L0,0 L3,-5 Z";

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

  return `<span style="display:inline-block;transform:rotate(${rotation}deg);margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

/**
 * Format ISO timestamp to "HH:MM M/D" in Pacific time
 * @param {string} isoString - ISO 8601 timestamp
 * @returns {string} Formatted time string
 */
function formatTimestamp(isoString) {
  const date = new Date(isoString);
  const time = date.toLocaleString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
  const day = date.toLocaleString("en-US", {
    month: "numeric",
    day: "numeric",
    timeZone: "America/Vancouver",
  });
  return `${time} ${day}`;
}

/**
 * Format ISO timestamp to "HH:MM" in Pacific time (for mobile)
 * @param {string} isoString - ISO 8601 timestamp
 * @returns {string} Formatted time string
 */
function formatTimeOnly(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
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
 * Format time for tooltips and detailed displays
 * Example: "Oct 25 14:30"
 */
function formatTimeAxis(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  });
}

/**
 * Get responsive grid configuration based on screen width
 * @param {boolean} isComparisonChart - Whether this is for the comparison chart (needs more bottom space)
 */
function getResponsiveGridConfig(isComparisonChart = false) {
  const width = window.innerWidth;

  if (width < 600) {
    return {
      left: "8%",
      right: "8%",
      top: "15%",
      bottom: isComparisonChart ? "28%" : "22%",
      containLabel: true,
    };
  } else if (width < 1000) {
    return {
      left: "10%",
      right: "10%",
      top: "12%",
      bottom: isComparisonChart ? "20%" : "16%",
      containLabel: true,
    };
  } else {
    return {
      left: "8%",
      right: "8%",
      top: "10%",
      bottom: isComparisonChart ? "16%" : "10%",
      containLabel: true,
    };
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
 * Safely render chart with error handling
 * @param {Function} renderFn - Chart rendering function
 * @param {HTMLElement|string} container - DOM element or element ID
 * @param {string} chartName - Name of the chart for error messages
 * @param {...any} args - Arguments to pass to renderFn
 */
function safeRenderChart(renderFn, container, chartName, ...args) {
  try {
    renderFn(...args);
  } catch (error) {
    showChartError(container, chartName, error);
  }
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
