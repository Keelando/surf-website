/* -----------------------------
   Comparison Chart Module
   Handles multi-buoy wave height comparison
   ----------------------------- */

/**
 * Downsample high-frequency data to hourly intervals
 * Keeps the data point closest to the top of each hour
 * @param {Array} data - Array of {time, value} objects
 * @returns {Array} Downsampled array with one point per hour
 */
function downsampleToHourly(data) {
  if (!data || data.length === 0) return [];

  const hourlyBuckets = {};

  // Group points by hour
  for (const point of data) {
    const timestamp = new Date(point.time);
    // Round down to the hour
    const hourKey = new Date(timestamp.getFullYear(), timestamp.getMonth(),
                             timestamp.getDate(), timestamp.getHours(), 0, 0, 0).getTime();

    // Keep the point closest to the top of the hour
    if (!hourlyBuckets[hourKey]) {
      hourlyBuckets[hourKey] = point;
    } else {
      const existingTime = new Date(hourlyBuckets[hourKey].time);
      const existingOffset = Math.abs(existingTime.getMinutes() * 60 + existingTime.getSeconds());
      const newOffset = Math.abs(timestamp.getMinutes() * 60 + timestamp.getSeconds());

      if (newOffset < existingOffset) {
        hourlyBuckets[hourKey] = point;
      }
    }
  }

  // Convert back to array and sort by time
  return Object.values(hourlyBuckets).sort((a, b) => new Date(a.time) - new Date(b.time));
}

/**
 * Render comparison chart showing all Canadian buoys
 * @param {Object} waveComparisonChart - ECharts instance for comparison chart
 * @param {Object} chartData - Full chart data object with all buoys
 */
function renderComparisonChart(waveComparisonChart, chartData) {
  try {
    if (!chartData) {
      logger.warn("ComparisonChart", "No chart data available");
      return;
    }

    const buoyOrder = ["4600146", "4600304", "4600303", "4600131", "CRPILE"];
  const buoyColors = {
    "4600146": "#1e88e5",
    "4600304": "#43a047",
    "4600303": "#fb8c00",
    "4600131": "#e53935",
    "CRPILE": "#9c27b0",
  };

  const series = buoyOrder
    .map((buoyId) => {
      const buoy = chartData[buoyId];
      if (!buoy?.timeseries?.wave_height_sig) return null;

      let data = buoy.timeseries.wave_height_sig.data || [];

      // Downsample high-frequency buoys to hourly for better chart performance
      if (buoyId === "CRPILE") {
        data = downsampleToHourly(data);
        logger.debug("ComparisonChart", `Downsampled ${buoy.name} from high-frequency to hourly (${data.length} points)`);
      }

      return {
        name: buoy.name,
        type: "line",
        data: sanitizeSeriesData(data),
        smooth: true,
        connectNulls: false,
        itemStyle: { color: buoyColors[buoyId] },
        emphasis: { focus: "series" },
        markLine: {
          symbol: "none",
          data: [
            {
              yAxis: 0.7,
              lineStyle: { type: "dashed", color: "orange", width: 1 },
              label: { formatter: "0.7m" }
            },
            {
              yAxis: 1.2,
              lineStyle: { type: "dashed", color: "red", width: 1 },
              label: { formatter: "1.2m" }
            }
          ]
        }
      };
    })
    .filter(Boolean);

  waveComparisonChart.setOption({
    title: {
      text: "Sig Wave Height (All)",
      left: "center",
      textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14 }
    },

    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params?.length) return "";
        const time = formatTimeAxis(new Date(params[0].value[0]).toISOString());
        let res = `<b>${time}</b><br/>`;
        for (const p of params) {
          if (p.value[1] != null) {
            res += `${p.marker} ${p.seriesName}: ${p.value[1]} m<br/>`;
          }
        }
        return res;
      },
    },

    legend: {
      data: buoyOrder.map((id) => chartData[id]?.name).filter(Boolean),
      bottom: "3%",  // Fixed lower position for comparison chart (multi-row legend needs more space)
    },

    grid: getResponsiveGridConfig(true),

    xAxis: {
      type: "time",
      axisLabel: {
        fontSize: window.innerWidth < 600 ? 9 : 10,
        rotate: window.innerWidth < 600 ? 30 : 0,
        formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
        hideOverlap: true,
        margin: 10,
      },
      axisTick: { show: true },
      splitLine: { show: true, lineStyle: { color: "#eee" } },
    },

    yAxis: {
      type: "value",
      name: "Wave Height (m)",
      min: 0,
      max: (value) => {
        const rawMax = value.max || 1;
        const padded = Math.ceil(rawMax * 1.01 * 10) / 10;
        return Math.max(1, padded);
      },
      scale: false,
      boundaryGap: [0, 0],
      splitLine: { show: true, lineStyle: { color: "#eee" } },
    },

    series: series,
  });
  } catch (error) {
    showChartError('wave-comparison-chart', 'Wave Comparison Chart', error);
  }
}
