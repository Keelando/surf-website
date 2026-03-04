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
const DIRECTION_ARROW_PATH = 'path://M0,15 L-3,-5 L0,0 L3,-5 Z';

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
        logger.error('ChartUtils', `Fetch failed after ${maxRetries} attempts`, error);
        throw error;
      }

      const delay = retryDelay * attempt;
      logger.warn('ChartUtils', `Fetch attempt ${attempt} failed, retrying in ${delay}ms...`, { message: error.message });
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Sanitize series data for ECharts
 * Converts invalid values (null, NaN, "MM") to [timestamp, null]
 */
function sanitizeSeriesData(dataArray) {
  return dataArray.map(d => {
    const y = parseFloat(d.value);
    if (isNaN(y) || d.value == null || d.value === "MM") {
      return [new Date(d.time).getTime(), null];
    }
    return [new Date(d.time).getTime(), y];
  });
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
      left: '8%',
      right: '8%',
      top: '15%',
      bottom: isComparisonChart ? '28%' : '22%',
      containLabel: true
    };
  } else if (width < 1000) {
    return {
      left: '10%',
      right: '10%',
      top: '12%',
      bottom: isComparisonChart ? '20%' : '16%',
      containLabel: true
    };
  } else {
    return {
      left: '8%',
      right: '8%',
      top: '10%',
      bottom: isComparisonChart ? '16%' : '10%',
      containLabel: true
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
    return '10%';  // Mobile: higher up to stay closer to rotated labels
  } else if (width < 1000) {
    return '7%';   // Medium: middle ground
  } else {
    return '5%';   // Desktop: works well per user feedback
  }
}

/**
 * Display error message in chart container
 * @param {HTMLElement|string} container - DOM element or element ID
 * @param {string} chartName - Name of the chart for error message
 * @param {Error} error - The error object
 */
function showChartError(container, chartName, error) {
  const element = typeof container === 'string'
    ? document.getElementById(container)
    : container;

  if (!element) {
    logger.error('ChartUtils', `Chart container not found: ${container}`);
    return;
  }

  logger.error('ChartUtils', `Error rendering ${chartName}`, error);

  element.innerHTML = `
    <div style="display: flex; align-items: center; justify-content: center; height: 100%; min-height: 200px; padding: 2rem; text-align: center;">
      <div>
        <div style="color: #e53935; font-size: 1.2rem; margin-bottom: 0.5rem;">⚠️ Chart Error</div>
        <div style="color: #666; font-size: 0.9rem;">Unable to load ${chartName}</div>
        <div style="color: #999; font-size: 0.8rem; margin-top: 0.5rem;">Check console for details</div>
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

  return {
    trigger: 'axis',
    confine: true, // Keep tooltip within chart bounds
    axisPointer: {
      type: 'line', // Show only vertical x-axis line (cleaner on mobile)
      label: {
        backgroundColor: '#004b7c'
      },
      lineStyle: {
        color: '#004b7c',
        width: isMobile ? 2 : 1,
        type: 'solid'
      }
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
    renderMode: 'html',
    className: 'echarts-tooltip-mobile',
    backgroundColor: 'rgba(255, 255, 255, 0.95)',
    borderColor: '#004b7c',
    borderWidth: 1,
    textStyle: {
      color: '#333',
      fontSize: isMobile ? 12 : 14
    },
    padding: isMobile ? 8 : 12
  };
}
