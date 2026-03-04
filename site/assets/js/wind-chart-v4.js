/* -----------------------------
   Wind Chart Module
   Handles wind speed and gust visualization with direction arrows
   ----------------------------- */

/**
 * Create wind direction arrow data for scatter series
 * @param {Array} windDirectionData - Array of {time, value} wind direction points
 * @param {Array} windSpeedData - Array of {time, value} wind speed points for max calculation
 * @param {Array} windGustData - Array of {time, value} wind gust points for max calculation
 * @returns {Object} Object with arrowData and maxValue for y-axis scaling
 */
function createWindDirectionArrowData(windDirectionData, windSpeedData, windGustData) {
  if (!windDirectionData || windDirectionData.length === 0) return { arrowData: [], maxValue: null };

  // Find maximum wind speed/gust to position arrows at top
  const allSpeeds = [...windSpeedData, ...windGustData]
    .map(d => d.value)
    .filter(v => v != null && !isNaN(v));

  const maxSpeed = allSpeeds.length > 0 ? Math.max(...allSpeeds) : 20;
  const arrowYPosition = maxSpeed * 1.05; // Position arrows 5% above max value

  const arrowData = [];

  // Responsive sampling: fewer arrows on mobile to prevent overlap
  // Mobile (< 600px): every 6 hours, Desktop: every 3 hours
  const sampleInterval = window.innerWidth < 600 ? 6 : 3;

  for (let i = 0; i < windDirectionData.length; i += sampleInterval) {
    const dirPoint = windDirectionData[i];
    if (!dirPoint || dirPoint.value == null) continue;

    // Find corresponding wind speed for this time (for validation)
    const speedPoint = windSpeedData.find(s => s.time === dirPoint.time);
    if (!speedPoint || speedPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value; // Meteorological direction (coming FROM)

    // Position arrows in a straight line at top
    // Arrow SVG points DOWN by default
    // Wind direction indicates where wind comes FROM, arrow shows where it's blowing TO
    // ECharts rotates counter-clockwise, so negate to get clockwise rotation
    // 0° North wind → -0° = arrow points down, 90° East → -90° = arrow points left
    arrowData.push({
      value: [timestamp, arrowYPosition],
      symbolRotate: -direction,
      itemStyle: {
        color: '#004b7c',
        opacity: 0.7
      }
    });
  }

  return { arrowData, maxValue: arrowYPosition };
}

/**
 * Render wind chart for the selected buoy
 * @param {Object} windChart - ECharts instance for wind chart
 * @param {Object} buoy - Buoy data including name and timeseries
 */
function renderWindChart(windChart, buoy) {
  try {
    const ts = buoy.timeseries;
  const windSpeedData = ts.wind_speed?.data || [];
  const windGustData = ts.wind_gust?.data || [];
  const windDirectionData = ts.wind_direction?.data || [];

  // Create direction arrow data (returns object with arrowData and maxValue)
  const { arrowData, maxValue } = createWindDirectionArrowData(windDirectionData, windSpeedData, windGustData);

  // Calculate y-axis max to ensure arrows are visible at top
  const yAxisMax = maxValue ? Math.ceil(maxValue * 1.1) : null;

  // Build legend data array
  const legendData = ["Wind Speed", "Wind Gust"];
  if (arrowData.length > 0) {
    legendData.push("Wind Direction");
  }

  windChart.setOption({
    title: {
      text: `${buoy.name} - Wind Conditions`,
      left: "center",
      textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14 }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = formatTimeAxis(new Date(params[0].value[0]).toISOString());
        let res = `<b>${time}</b><br/>`;
        params.forEach((p) => {
          if (p.seriesName === "Wind Direction") return; // Skip arrow series in tooltip
          if (p.value[1] != null) {
            res += `${p.marker} ${p.seriesName}: ${p.value[1]} kt<br/>`;
          }
        });

        // Add wind direction to tooltip if available
        const timestamp = new Date(params[0].value[0]).getTime();
        const dirPoint = windDirectionData.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000); // Within 30 min
        if (dirPoint && dirPoint.value != null) {
          const dir = Math.round(dirPoint.value);
          const compass = degreesToCompass(dir);
          res += `🧭 Direction: ${dir}° (${compass})<br/>`;
        }

        return res;
      },
    },
    legend: { data: legendData, bottom: getResponsiveLegendBottom() },
    grid: getResponsiveGridConfig(false),
    xAxis: {
      type: "time",
      axisLabel: {
        fontSize: window.innerWidth < 600 ? 9 : 10,
        rotate: window.innerWidth < 600 ? 30 : 0,
        formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
        hideOverlap: true,
        margin: 10
      },
      axisTick: { show: true },
      splitLine: { show: true, lineStyle: { color: "#eee" } },
    },
    yAxis: {
      type: "value",
      name: "Speed (kt)",
      max: yAxisMax // Set max to accommodate arrows at top
    },
    series: [
      {
        name: "Wind Speed",
        type: "line",
        data: sanitizeSeriesData(windSpeedData),
        smooth: true,
        connectNulls: false,
        itemStyle: { color: "#fb8c00" },
        areaStyle: { opacity: 0.1 },
      },
      {
        name: "Wind Gust",
        type: "scatter",
        data: sanitizeSeriesData(windGustData),
        symbol: "circle",
        symbolSize: 6,
        itemStyle: { color: "#e53935" },
      },
      {
        name: "Wind Direction",
        type: "scatter",
        data: arrowData,
        symbol: DIRECTION_ARROW_PATH,
        symbolSize: 16,
        symbolRotate: function(dataIndex) {
          // Read rotation from data point
          return arrowData[dataIndex]?.symbolRotate || 0;
        },
        itemStyle: {
          color: function(params) {
            // Read color from data point
            return arrowData[params.dataIndex]?.itemStyle?.color || '#004b7c';
          },
          opacity: function(params) {
            // Read opacity from data point
            return arrowData[params.dataIndex]?.itemStyle?.opacity || 1.0;
          }
        },
        silent: true, // Don't trigger mouse events
        z: 2 // Render on top of lines
      }
    ]
  });
  } catch (error) {
    showChartError('wind-chart', 'Wind Chart', error);
  }
}

/**
 * Convert degrees to compass direction
 * @param {number} degrees - Wind direction in degrees
 * @returns {string} Compass direction (N, NE, E, etc.)
 */
function degreesToCompass(degrees) {
  const directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}
