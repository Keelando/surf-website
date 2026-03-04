/* -----------------------------
   Wave Chart Module
   Handles wave height and period visualization
   Uses centralized arrow definitions from chart-utils-v4.js
   ----------------------------- */

/**
 * Create wave direction arrow data for scatter series
 * @param {Array} waveDirectionData - Array of {time, value} wave direction points
 * @param {Array} waveHeightData - Array of {time, value} wave height points for max calculation
 * @returns {Object} Object with arrowData and maxValue for y-axis scaling
 */
function createWaveDirectionArrowData(waveDirectionData, waveHeightData) {
  if (!waveDirectionData || waveDirectionData.length === 0) return { arrowData: [], maxValue: null };

  // Find maximum wave height to position arrows at top
  const allHeights = waveHeightData
    .map(d => d.value)
    .filter(v => v != null && !isNaN(v));

  const maxHeight = allHeights.length > 0 ? Math.max(...allHeights) : 2;
  const arrowYPosition = maxHeight * 1.05; // Position arrows 5% above max value

  const arrowData = [];

  // Responsive sampling: fewer arrows on mobile to prevent overlap
  // Mobile (< 600px): every 6 hours, Desktop: every 3 hours
  const sampleInterval = window.innerWidth < 600 ? 6 : 3;

  for (let i = 0; i < waveDirectionData.length; i += sampleInterval) {
    const dirPoint = waveDirectionData[i];
    if (!dirPoint || dirPoint.value == null) continue;

    // Find corresponding wave height for this time (for validation)
    const heightPoint = waveHeightData.find(h => h.time === dirPoint.time);
    if (!heightPoint || heightPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value; // Wave direction (coming FROM)

    arrowData.push({
      value: [timestamp, arrowYPosition],
      symbolRotate: calculateArrowRotation(direction),
      itemStyle: {
        color: '#1e88e5',
        opacity: 0.7
      }
    });
  }

  return { arrowData, maxValue: arrowYPosition };
}

/**
 * Render wave chart for the selected buoy
 * Special handling for NOAA buoys (46087 Neah Bay, 46088 New Dungeness) with dual charts
 * @param {Object} waveChart - ECharts instance for main wave chart
 * @param {Object} buoy - Buoy data including name and timeseries
 * @param {string} buoyId - Buoy identifier
 */
function renderWaveChart(waveChart, buoy, buoyId) {
  try {
    const ts = buoy.timeseries;

    if (buoyId === "46087" || buoyId === "46088") {
      // NOAA BUOYS (Neah Bay & New Dungeness) - Dual charts with spectral wave separation
      renderSpectralCharts(waveChart, buoy, ts);
    } else {
      // ALL OTHER BUOYS - Standard single wave chart
      renderStandardWaveChart(waveChart, buoy, buoyId, ts);
    }
  } catch (error) {
    showChartError('wave-chart', 'Wave Chart', error);
  }
}

/**
 * Create direction arrow data for spectral wave components (swell + wind waves)
 * @param {Array} directionData - Direction timeseries data
 * @param {Array} heightData - Height data for positioning arrows
 * @param {string} color - Arrow color
 * @returns {Array} Arrow data for scatter series
 */
function createSpectralDirectionArrows(directionData, heightData, color) {
  if (!directionData || directionData.length === 0 || !heightData || heightData.length === 0) {
    return [];
  }

  const arrowData = [];

  // Responsive sampling: fewer arrows on mobile
  const sampleInterval = window.innerWidth < 600 ? 6 : 3;

  for (let i = 0; i < directionData.length; i += sampleInterval) {
    const dirPoint = directionData[i];
    if (!dirPoint || dirPoint.value == null) continue;

    // Find corresponding height for this time to position arrow on the line
    const heightPoint = heightData.find(h => h.time === dirPoint.time);
    if (!heightPoint || heightPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value;
    const yPosition = heightPoint.value; // Position arrow at actual data point

    arrowData.push({
      value: [timestamp, yPosition],
      symbolRotate: calculateArrowRotation(direction),
      itemStyle: {
        color: color,
        opacity: 0.8
      }
    });
  }

  return arrowData;
}

/**
 * Render NOAA spectral dual-chart display (wave heights + periods)
 * Used for Neah Bay (46087) and New Dungeness (46088)
 */
function renderSpectralCharts(waveChart, buoy, ts) {
  // Chart 1: Wave Heights (All three components with fallbacks)
  const sigWaveHeight = ts.wave_height_sig?.data || [];
  const windWaveHeight = ts.wind_wave_height?.data || [];
  const swellHeight = ts.swell_height?.data || [];

  // Get direction data for arrows
  const windWaveDirection = ts.wind_wave_direction?.data || [];
  const swellDirection = ts.swell_direction?.data || [];

  // Create arrow data once (for efficiency)
  const windWaveArrows = createSpectralDirectionArrows(windWaveDirection, windWaveHeight, "#1e88e5");
  const swellArrows = createSpectralDirectionArrows(swellDirection, swellHeight, "#fb8c00");

  // Debug: Check what data we actually have
  logger.debug("WaveChart", `${buoy.name} wave data available`, {
    sig: sigWaveHeight.length,
    wind: windWaveHeight.length,
    swell: swellHeight.length,
    windDir: windWaveDirection.length,
    swellDir: swellDirection.length,
    windArrows: windWaveArrows.length,
    swellArrows: swellArrows.length
  });

  waveChart.setOption({
    title: {
      text: `${buoy.name} - Wave Height Components`,
      left: "center",
      textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14 }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = formatTimeAxis(new Date(params[0].value[0]).toISOString());
        const timestamp = new Date(params[0].value[0]).getTime();
        let res = `<b>${time}</b><br/>`;

        // Show heights (skip direction arrow series in tooltip)
        params.forEach((p) => {
          if (p.seriesName.includes("Dir")) return; // Skip direction series
          if (p.value[1] != null) {
            res += `${p.marker} ${p.seriesName}: ${p.value[1]} m<br/>`;
          }
        });

        // Add direction info if available
        const windDirPoint = windWaveDirection.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000);
        if (windDirPoint && windDirPoint.value != null) {
          const dir = Math.round(windDirPoint.value);
          const compass = degreesToCompass(dir);
          res += `🌊 Wind Wave Dir: ${dir}° (${compass})<br/>`;
        }

        const swellDirPoint = swellDirection.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000);
        if (swellDirPoint && swellDirPoint.value != null) {
          const dir = Math.round(swellDirPoint.value);
          const compass = degreesToCompass(dir);
          res += `🌊 Swell Dir: ${dir}° (${compass})<br/>`;
        }

        return res;
      },
    },
    legend: {
      data: ["Wind Waves", "Ocean Swell", "Total (Significant)"],
      bottom: getResponsiveLegendBottom()
    },
    grid: {
      left: window.innerWidth < 600 ? '12%' : '10%',
      right: window.innerWidth < 600 ? '12%' : '10%',
      top: '15%',
      bottom: '22%',
      containLabel: true
    },
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
      name: "Height (m)",
      min: 0,
      max: (value) => Math.max(0.5, Math.ceil(value.max * 1.1)),
      scale: true
    },
    series: [
      {
        name: "Wind Waves",
        type: "line",
        data: sanitizeSeriesData(windWaveHeight),
        smooth: true,
        connectNulls: false,
        itemStyle: { color: "#1e88e5" },
        showSymbol: false
      },
      {
        name: "Ocean Swell",
        type: "line",
        data: sanitizeSeriesData(swellHeight),
        smooth: true,
        connectNulls: false,
        itemStyle: { color: "#fb8c00" },
        showSymbol: false
      },
      {
        name: "Total (Significant)",
        type: "line",
        data: sanitizeSeriesData(sigWaveHeight),
        smooth: true,
        connectNulls: false,
        lineStyle: {
          type: "dashed",
          width: 2,
          color: "#999999"
        },
        itemStyle: { color: "#999999" },
        showSymbol: false,
        z: 1
      },
      // Direction arrows for wind waves (blue arrows on wind wave line)
      {
        name: "Wind Wave Dir",
        type: "scatter",
        data: windWaveArrows,
        symbol: DIRECTION_ARROW_PATH,
        symbolSize: 14,
        symbolRotate: function(params) {
          return windWaveArrows[params.dataIndex]?.symbolRotate || 0;
        },
        itemStyle: {
          color: function(params) {
            return windWaveArrows[params.dataIndex]?.itemStyle?.color || '#1e88e5';
          },
          opacity: function(params) {
            return windWaveArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
          }
        },
        silent: true,
        z: 3
      },
      // Direction arrows for swell (orange arrows on swell line)
      {
        name: "Swell Dir",
        type: "scatter",
        data: swellArrows,
        symbol: DIRECTION_ARROW_PATH,
        symbolSize: 14,
        symbolRotate: function(params) {
          return swellArrows[params.dataIndex]?.symbolRotate || 0;
        },
        itemStyle: {
          color: function(params) {
            return swellArrows[params.dataIndex]?.itemStyle?.color || '#fb8c00';
          },
          opacity: function(params) {
            return swellArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
          }
        },
        silent: true,
        z: 3
      }
    ]
  }, true);

  // Chart 2: Wave Periods (All three components with fallbacks)
  const avgPeriod = ts.wave_period_avg?.data || [];
  const windWavePeriod = ts.wind_wave_period?.data || [];
  const swellPeriod = ts.swell_period?.data || [];

  // Create arrow data for period chart (positioned on period lines)
  const windWavePeriodArrows = createSpectralDirectionArrows(windWaveDirection, windWavePeriod, "#1e88e5");
  const swellPeriodArrows = createSpectralDirectionArrows(swellDirection, swellPeriod, "#fb8c00");

  // Debug: Check what period data we have
  logger.debug("WaveChart", `${buoy.name} period data available`, {
    avg: avgPeriod.length,
    wind: windWavePeriod.length,
    swell: swellPeriod.length,
    windPeriodArrows: windWavePeriodArrows.length,
    swellPeriodArrows: swellPeriodArrows.length
  });

  const periodChartContainer = document.getElementById("wave-period-chart");
  if (periodChartContainer) {
    periodChartContainer.style.display = "block";

    if (!window.wavePeriodChart) {
      window.wavePeriodChart = echarts.init(periodChartContainer);
      window.addEventListener("resize", () => window.wavePeriodChart.resize());
    }

    window.wavePeriodChart.setOption({
      title: {
        text: `${buoy.name} - Wave Period Components`,
        left: "center",
        textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14 }
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: (params) => {
          if (!params || params.length === 0) return "";
          const time = formatTimeAxis(new Date(params[0].value[0]).toISOString());
          const timestamp = new Date(params[0].value[0]).getTime();
          let res = `<b>${time}</b><br/>`;

          // Show periods (skip direction arrow series in tooltip)
          params.forEach((p) => {
            if (p.seriesName.includes("Dir")) return; // Skip direction series
            if (p.value[1] != null) {
              res += `${p.marker} ${p.seriesName}: ${p.value[1]} s<br/>`;
            }
          });

          // Add direction info if available
          const windDirPoint = windWaveDirection.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000);
          if (windDirPoint && windDirPoint.value != null) {
            const dir = Math.round(windDirPoint.value);
            const compass = degreesToCompass(dir);
            res += `🌊 Wind Wave Dir: ${dir}° (${compass})<br/>`;
          }

          const swellDirPoint = swellDirection.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000);
          if (swellDirPoint && swellDirPoint.value != null) {
            const dir = Math.round(swellDirPoint.value);
            const compass = degreesToCompass(dir);
            res += `🌊 Swell Dir: ${dir}° (${compass})<br/>`;
          }

          return res;
        },
      },
      legend: {
        data: ["Wind Wave Period", "Swell Period", "Average Period"],
        bottom: getResponsiveLegendBottom()
      },
      grid: {
        left: window.innerWidth < 600 ? '12%' : '10%',
        right: window.innerWidth < 600 ? '12%' : '10%',
        top: '15%',
        bottom: '22%',
        containLabel: true
      },
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
        name: "Period (s)",
        min: 0
      },
      series: [
        {
          name: "Wind Wave Period",
          type: "line",
          data: sanitizeSeriesData(windWavePeriod),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: "#1e88e5" },
          showSymbol: false
        },
        {
          name: "Swell Period",
          type: "line",
          data: sanitizeSeriesData(swellPeriod),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: "#fb8c00" },
          showSymbol: false
        },
        {
          name: "Average Period",
          type: "line",
          data: sanitizeSeriesData(avgPeriod),
          smooth: true,
          connectNulls: false,
          lineStyle: {
            type: "dashed",
            width: 2,
            color: "#999999"
          },
          itemStyle: { color: "#999999" },
          showSymbol: false,
          z: 1
        },
        // Direction arrows for wind waves (blue arrows on wind wave period line)
        {
          name: "Wind Wave Dir",
          type: "scatter",
          data: windWavePeriodArrows,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: function(params) {
            return windWavePeriodArrows[params.dataIndex]?.symbolRotate || 0;
          },
          itemStyle: {
            color: function(params) {
              return windWavePeriodArrows[params.dataIndex]?.itemStyle?.color || '#1e88e5';
            },
            opacity: function(params) {
              return windWavePeriodArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
            }
          },
          silent: true,
          z: 3
        },
        // Direction arrows for swell (orange arrows on swell period line)
        {
          name: "Swell Dir",
          type: "scatter",
          data: swellPeriodArrows,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: function(params) {
            return swellPeriodArrows[params.dataIndex]?.symbolRotate || 0;
          },
          itemStyle: {
            color: function(params) {
              return swellPeriodArrows[params.dataIndex]?.itemStyle?.color || '#fb8c00';
            },
            opacity: function(params) {
              return swellPeriodArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
            }
          },
          silent: true,
          z: 3
        }
      ]
    }, true);

    setTimeout(() => {
      if (window.wavePeriodChart) {
        window.wavePeriodChart.resize();
      }
    }, 100);
  }
}

/**
 * Render standard wave chart (all buoys except New Dungeness)
 */
function renderStandardWaveChart(waveChart, buoy, buoyId, ts) {
  // Hide the period chart if it exists
  const periodChartContainer = document.getElementById("wave-period-chart");
  if (periodChartContainer) {
    periodChartContainer.style.display = "none";
  }

  let waveHeightData, wavePeriodData, wavePeriodPeakData, chartTitle, heightLabel, periodLabel;

  if (buoyId === "46087") {
    // Neah Bay - use swell data (open ocean)
    waveHeightData = ts.swell_height?.data || [];
    wavePeriodData = ts.swell_period?.data || [];
    wavePeriodPeakData = null; // NOAA buoys don't need peak period overlay
    chartTitle = `${buoy.name} - Swell Conditions`;
    heightLabel = "Swell Height";
    periodLabel = "Swell Period";
  } else {
    // Canadian buoys - use significant wave height and average period, with peak period as dots
    waveHeightData = ts.wave_height_sig?.data || [];
    wavePeriodData = ts.wave_period_avg?.data || [];
    wavePeriodPeakData = ts.wave_period_peak?.data || [];
    chartTitle = `${buoy.name} - Wave Conditions`;
    heightLabel = "Significant Wave Height";
    periodLabel = "Average Period";
  }

  // Check if this buoy has wave direction data
  // Halibut Bank (4600146), Sentry Shoal (4600131), Angeles Point (46267)
  const hasWaveDirection = (buoyId === "4600146" || buoyId === "4600131" || buoyId === "46267");

  // Select appropriate direction data based on buoy type:
  // - All buoys: Prefer wave_direction_avg (MWD - Mean Wave Direction for NOAA)
  // - Fall back to peak direction if avg not available
  let waveDirectionData = [];
  if (hasWaveDirection) {
    waveDirectionData = ts.wave_direction_avg?.data || ts.wave_direction_peak?.data || [];
  }

  // Create direction arrow data if available
  const { arrowData, maxValue } = hasWaveDirection
    ? createWaveDirectionArrowData(waveDirectionData, waveHeightData)
    : { arrowData: [], maxValue: null };

  // Calculate y-axis max to ensure arrows are visible at top (only if arrows exist)
  const yAxisMax = maxValue ? (value) => Math.max(1, Math.ceil(maxValue * 1.1)) : (value) => Math.max(1, Math.ceil(value.max * 1.1));

  // Build series array dynamically
  const series = [
    {
      name: heightLabel,
      type: "line",
      data: sanitizeSeriesData(waveHeightData),
      smooth: true,
      connectNulls: false,
      yAxisIndex: 0,
      itemStyle: { color: "#1e88e5" },
      areaStyle: { opacity: 0.1 },
    },
    {
      name: periodLabel,
      type: "line",
      data: sanitizeSeriesData(wavePeriodData),
      smooth: true,
      connectNulls: false,
      yAxisIndex: 1,
      itemStyle: { color: "#43a047" },
    }
  ];

  // Build legend data array
  const legendData = [heightLabel, periodLabel];

  // Add peak period as scatter dots for Canadian buoys
  if (wavePeriodPeakData && wavePeriodPeakData.length > 0) {
    legendData.push("Peak Period");
    series.push({
      name: "Peak Period",
      type: "scatter",
      data: sanitizeSeriesData(wavePeriodPeakData),
      symbol: "circle",
      symbolSize: 5,
      yAxisIndex: 1,
      itemStyle: { color: "#66bb6a", opacity: 0.6 },
    });
  }

  // Add wave direction arrows if available (show in legend)
  if (hasWaveDirection && arrowData.length > 0) {
    legendData.push("Wave Direction");
    series.push({
      name: "Wave Direction",
      type: "scatter",
      data: arrowData,
      symbol: 'path://M0,15 L-3,-5 L0,0 L3,-5 Z', // Skinny notched arrow pointing DOWN
      symbolSize: 16,
      symbolRotate: function(dataIndex) {
        return arrowData[dataIndex]?.symbolRotate || 0;
      },
      yAxisIndex: 0,
      itemStyle: {
        color: function(params) {
          return arrowData[params.dataIndex]?.itemStyle?.color || '#1e88e5';
        },
        opacity: function(params) {
          return arrowData[params.dataIndex]?.itemStyle?.opacity || 1.0;
        }
      },
      silent: true,
      z: 2
    });
  }

  waveChart.setOption({
    title: {
      text: chartTitle,
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
          if (p.seriesName === "Wave Direction") return; // Skip arrow series in tooltip
          if (p.value[1] != null) {
            res += `${p.marker} ${p.seriesName}: ${p.value[1]} ${
              p.seriesName.includes("Height") ? "m" : "s"
            }<br/>`;
          }
        });

        // Add wave direction to tooltip if available
        if (hasWaveDirection) {
          const timestamp = new Date(params[0].value[0]).getTime();
          const dirPoint = waveDirectionData.find(d => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000); // Within 30 min
          if (dirPoint && dirPoint.value != null) {
            const dir = Math.round(dirPoint.value);
            const compass = degreesToCompass(dir);
            res += `🌊 Direction: ${dir}° (${compass})<br/>`;
          }
        }

        return res;
      },
    },
    legend: {
      data: legendData,
      bottom: getResponsiveLegendBottom()
    },
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
    yAxis: [
      {
        type: "value",
        name: "Height (m)",
        position: "left",
        min: 0,
        max: yAxisMax,
        scale: true,
        nameTextStyle: { color: "#1e88e5" },
        axisLine: { lineStyle: { color: "#1e88e5" } }
      },
      {
        type: "value",
        name: "Period (s)",
        position: "right",
        nameTextStyle: { color: "#43a047" },
        axisLine: { lineStyle: { color: "#43a047" } }
      },
    ],
    series: series
  }, true);
}

/**
 * Convert degrees to compass direction
 * @param {number} degrees - Wave/wind direction in degrees
 * @returns {string} Compass direction (N, NE, E, etc.)
 */
function degreesToCompass(degrees) {
  const directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}
