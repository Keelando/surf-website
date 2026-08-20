/* -----------------------------
   Wave Chart Module (ES module)
   Handles wave height and period visualization

   Chart helpers (calculateArrowRotation, degreesToCardinal, theme/grid/
   tooltip config, sanitizeSeriesData, showChartError, echarts) still come
   from classic scripts loaded before the entry point.
   ----------------------------- */

import { formatMonthDayTime } from "./shared/format-time.js";
import { DIRECTION_ARROW_PATH } from "./shared/markers.js";

// ECharts instance for the NOAA spectral period chart (created on demand)
let wavePeriodChart = null;

/**
 * Create wave direction arrow data for scatter series
 * @param {Array} waveDirectionData - Array of {time, value} wave direction points
 * @param {Array} waveHeightData - Array of {time, value} wave height points for max calculation
 * @returns {Object} Object with arrowData and maxValue for y-axis scaling
 */
function createWaveDirectionArrowData(waveDirectionData, waveHeightData, colorOverride) {
  if (!waveDirectionData || waveDirectionData.length === 0)
    return { arrowData: [], maxValue: null };

  // Find maximum wave height to position arrows at top
  const allHeights = waveHeightData.map((d) => d.value).filter((v) => v != null && !isNaN(v));

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
    const heightPoint = waveHeightData.find((h) => h.time === dirPoint.time);
    if (!heightPoint || heightPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value; // Wave direction (coming FROM)

    arrowData.push({
      value: [timestamp, arrowYPosition],
      symbolRotate: calculateArrowRotation(direction),
      itemStyle: {
        color: colorOverride || "#1e88e5",
        opacity: 0.7,
      },
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
export function renderWaveChart(waveChart, buoy, buoyId) {
  try {
    const ts = buoy.timeseries;
    const theme = getChartThemeColors();

    if (buoyId === "46087" || buoyId === "46088") {
      // NOAA BUOYS (Neah Bay & New Dungeness) - Dual charts with spectral wave separation
      renderSpectralCharts(waveChart, buoy, ts, theme);
    } else {
      // ALL OTHER BUOYS - Standard single wave chart
      renderStandardWaveChart(waveChart, buoy, buoyId, ts, theme);
    }
  } catch (error) {
    showChartError("wave-chart", "Wave Chart", error);
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
    const heightPoint = heightData.find((h) => h.time === dirPoint.time);
    if (!heightPoint || heightPoint.value == null) continue;

    const timestamp = new Date(dirPoint.time).getTime();
    const direction = dirPoint.value;
    const yPosition = heightPoint.value; // Position arrow at actual data point

    arrowData.push({
      value: [timestamp, yPosition],
      symbolRotate: calculateArrowRotation(direction),
      itemStyle: {
        color: color,
        opacity: 0.8,
      },
    });
  }

  return arrowData;
}

/**
 * Render NOAA spectral dual-chart display (wave heights + periods)
 * Used for Neah Bay (46087) and New Dungeness (46088)
 */
function renderSpectralCharts(waveChart, buoy, ts, theme) {
  const colors = theme.series;
  const textColor = theme.text;
  const mutedText = theme.mutedText;
  const axisColor = theme.axisLine;
  const gridColor = theme.gridLine;
  // Chart 1: Wave Heights (All three components with fallbacks)
  const sigWaveHeight = ts.wave_height_sig?.data || [];
  const windWaveHeight = ts.wind_wave_height?.data || [];
  const swellHeight = ts.swell_height?.data || [];

  // Get direction data for arrows
  const windWaveDirection = ts.wind_wave_direction?.data || [];
  const swellDirection = ts.swell_direction?.data || [];

  // Create arrow data once (for efficiency)
  const windWaveArrows = createSpectralDirectionArrows(
    windWaveDirection,
    windWaveHeight,
    colors.primary,
  );
  const swellArrows = createSpectralDirectionArrows(swellDirection, swellHeight, colors.secondary);

  // Debug: Check what data we actually have
  logger.debug("WaveChart", `${buoy.name} wave data available`, {
    sig: sigWaveHeight.length,
    wind: windWaveHeight.length,
    swell: swellHeight.length,
    windDir: windWaveDirection.length,
    swellDir: swellDirection.length,
    windArrows: windWaveArrows.length,
    swellArrows: swellArrows.length,
  });

  waveChart.setOption(
    {
      backgroundColor: theme.background,
      textStyle: { color: textColor },
      title: {
        text: `${buoy.name} - Wave Height Components`,
        left: "center",
        textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14, color: textColor },
      },
      tooltip: {
        ...getMobileOptimizedTooltipConfig(),
        formatter: (params) => {
          if (!params || params.length === 0) return "";
          const time = formatMonthDayTime(params[0].value[0]);
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
          const windDirPoint = windWaveDirection.find(
            (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
          );
          if (windDirPoint && windDirPoint.value != null) {
            const dir = Math.round(windDirPoint.value);
            const compass = degreesToCardinal(dir);
            res += `🌊 Wind Wave Dir: ${dir}° (${compass})<br/>`;
          }

          const swellDirPoint = swellDirection.find(
            (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
          );
          if (swellDirPoint && swellDirPoint.value != null) {
            const dir = Math.round(swellDirPoint.value);
            const compass = degreesToCardinal(dir);
            res += `🌊 Swell Dir: ${dir}° (${compass})<br/>`;
          }

          return res;
        },
      },
      legend: {
        data: ["Wind Waves", "Ocean Swell", "Total (Significant)"],
        bottom: getResponsiveLegendBottom(),
        textStyle: { color: textColor },
      },
      grid: {
        ...getChartSideGutters(),
        top: "15%",
        bottom: "22%",
        containLabel: true,
      },
      xAxis: {
        type: "time",
        axisLabel: {
          fontSize: window.innerWidth < 600 ? 9 : 10,
          rotate: window.innerWidth < 600 ? 30 : 0,
          formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
          hideOverlap: true,
          margin: 10,
          color: mutedText,
        },
        axisTick: { show: true },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { show: true, lineStyle: { color: gridColor } },
      },
      yAxis: {
        type: "value",
        name: "Height (m)",
        min: 0,
        max: (value) => Math.max(0.5, Math.ceil(value.max * 1.1)),
        scale: true,
        axisLabel: { color: mutedText },
        nameTextStyle: { color: textColor },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { lineStyle: { color: gridColor } },
      },
      series: [
        {
          name: "Wind Waves",
          type: "line",
          data: sanitizeSeriesData(windWaveHeight),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: colors.primary },
          showSymbol: false,
        },
        {
          name: "Ocean Swell",
          type: "line",
          data: sanitizeSeriesData(swellHeight),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: colors.secondary },
          showSymbol: false,
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
            color: colors.tertiary,
          },
          itemStyle: { color: colors.tertiary },
          showSymbol: false,
          z: 1,
        },
        // Direction arrows for wind waves (blue arrows on wind wave line)
        {
          name: "Wind Wave Dir",
          type: "scatter",
          data: windWaveArrows,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: function (params) {
            return windWaveArrows[params.dataIndex]?.symbolRotate || 0;
          },
          itemStyle: {
            color: function (params) {
              return windWaveArrows[params.dataIndex]?.itemStyle?.color || colors.primary;
            },
            opacity: function (params) {
              return windWaveArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
            },
          },
          silent: true,
          z: 3,
        },
        // Direction arrows for swell (orange arrows on swell line)
        {
          name: "Swell Dir",
          type: "scatter",
          data: swellArrows,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: function (params) {
            return swellArrows[params.dataIndex]?.symbolRotate || 0;
          },
          itemStyle: {
            color: function (params) {
              return swellArrows[params.dataIndex]?.itemStyle?.color || colors.secondary;
            },
            opacity: function (params) {
              return swellArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
            },
          },
          silent: true,
          z: 3,
        },
      ],
    },
    true,
  );

  // Chart 2: Wave Periods (All three components with fallbacks)
  const avgPeriod = ts.wave_period_avg?.data || [];
  const windWavePeriod = ts.wind_wave_period?.data || [];
  const swellPeriod = ts.swell_period?.data || [];

  // Create arrow data for period chart (positioned on period lines)
  const windWavePeriodArrows = createSpectralDirectionArrows(
    windWaveDirection,
    windWavePeriod,
    colors.primary,
  );
  const swellPeriodArrows = createSpectralDirectionArrows(
    swellDirection,
    swellPeriod,
    colors.secondary,
  );

  // Debug: Check what period data we have
  logger.debug("WaveChart", `${buoy.name} period data available`, {
    avg: avgPeriod.length,
    wind: windWavePeriod.length,
    swell: swellPeriod.length,
    windPeriodArrows: windWavePeriodArrows.length,
    swellPeriodArrows: swellPeriodArrows.length,
  });

  const periodChartContainer = document.getElementById("wave-period-chart");
  if (periodChartContainer) {
    periodChartContainer.style.display = "block";

    if (!wavePeriodChart) {
      wavePeriodChart = echarts.init(periodChartContainer);
      window.addEventListener("resize", () => wavePeriodChart.resize());
    }

    wavePeriodChart.setOption(
      {
        backgroundColor: theme.background,
        textStyle: { color: textColor },
        title: {
          text: `${buoy.name} - Wave Period Components`,
          left: "center",
          textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14, color: textColor },
        },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: (params) => {
            if (!params || params.length === 0) return "";
            const time = formatMonthDayTime(params[0].value[0]);
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
            const windDirPoint = windWaveDirection.find(
              (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
            );
            if (windDirPoint && windDirPoint.value != null) {
              const dir = Math.round(windDirPoint.value);
              const compass = degreesToCardinal(dir);
              res += `🌊 Wind Wave Dir: ${dir}° (${compass})<br/>`;
            }

            const swellDirPoint = swellDirection.find(
              (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
            );
            if (swellDirPoint && swellDirPoint.value != null) {
              const dir = Math.round(swellDirPoint.value);
              const compass = degreesToCardinal(dir);
              res += `🌊 Swell Dir: ${dir}° (${compass})<br/>`;
            }

            return res;
          },
        },
        legend: {
          data: ["Wind Wave Period", "Swell Period", "Average Period"],
          bottom: getResponsiveLegendBottom(),
          textStyle: { color: textColor },
        },
        grid: {
          ...getChartSideGutters(),
          top: "15%",
          bottom: "22%",
          containLabel: true,
        },
        xAxis: {
          type: "time",
          axisLabel: {
            fontSize: window.innerWidth < 600 ? 9 : 10,
            rotate: window.innerWidth < 600 ? 30 : 0,
            formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
            hideOverlap: true,
            margin: 10,
            color: mutedText,
          },
          axisTick: { show: true },
          axisLine: { lineStyle: { color: axisColor } },
          splitLine: { show: true, lineStyle: { color: gridColor } },
        },
        yAxis: {
          type: "value",
          name: "Period (s)",
          min: 0,
          max: (value) => Math.max(5, Math.ceil(value.max * 1.2)),
          axisLabel: { color: mutedText },
          nameTextStyle: { color: textColor },
          axisLine: { lineStyle: { color: axisColor } },
          splitLine: { lineStyle: { color: gridColor } },
        },
        series: [
          {
            name: "Wind Wave Period",
            type: "line",
            data: sanitizeSeriesData(windWavePeriod),
            smooth: true,
            connectNulls: false,
            itemStyle: { color: colors.primary },
            showSymbol: false,
          },
          {
            name: "Swell Period",
            type: "line",
            data: sanitizeSeriesData(swellPeriod),
            smooth: true,
            connectNulls: false,
            itemStyle: { color: colors.secondary },
            showSymbol: false,
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
              color: colors.tertiary,
            },
            itemStyle: { color: colors.tertiary },
            showSymbol: false,
            z: 1,
          },
          // Direction arrows for wind waves (blue arrows on wind wave period line)
          {
            name: "Wind Wave Dir",
            type: "scatter",
            data: windWavePeriodArrows,
            symbol: DIRECTION_ARROW_PATH,
            symbolSize: 14,
            symbolRotate: function (params) {
              return windWavePeriodArrows[params.dataIndex]?.symbolRotate || 0;
            },
            itemStyle: {
              color: function (params) {
                return windWavePeriodArrows[params.dataIndex]?.itemStyle?.color || colors.primary;
              },
              opacity: function (params) {
                return windWavePeriodArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
              },
            },
            silent: true,
            z: 3,
          },
          // Direction arrows for swell (orange arrows on swell period line)
          {
            name: "Swell Dir",
            type: "scatter",
            data: swellPeriodArrows,
            symbol: DIRECTION_ARROW_PATH,
            symbolSize: 14,
            symbolRotate: function (params) {
              return swellPeriodArrows[params.dataIndex]?.symbolRotate || 0;
            },
            itemStyle: {
              color: function (params) {
                return swellPeriodArrows[params.dataIndex]?.itemStyle?.color || colors.secondary;
              },
              opacity: function (params) {
                return swellPeriodArrows[params.dataIndex]?.itemStyle?.opacity || 0.8;
              },
            },
            silent: true,
            z: 3,
          },
        ],
      },
      true,
    );

    setTimeout(() => {
      if (wavePeriodChart) {
        wavePeriodChart.resize();
      }
    }, 100);
  }
}

/**
 * Render standard wave chart (all buoys except New Dungeness)
 */
function renderStandardWaveChart(waveChart, buoy, buoyId, ts, theme) {
  const colors = theme.series;
  const textColor = theme.text;
  const mutedText = theme.mutedText;
  const axisColor = theme.axisLine;
  const gridColor = theme.gridLine;
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
  } else if (buoyId === "46267") {
    // Angeles Point (NOAA) - significant height + dominant period (DPD).
    // Dominant == peak, so no separate peak overlay.
    waveHeightData = ts.wave_height_sig?.data || [];
    wavePeriodData = ts.wave_period_sig?.data || [];
    wavePeriodPeakData = null;
    chartTitle = `${buoy.name} - Wave Conditions`;
    heightLabel = "Significant Wave Height";
    periodLabel = "Dominant Period";
  } else {
    // EC buoys - significant wave height + significant period, with peak period as dots.
    // Sig period uses two different SWOB names across buoy families; coalesce them.
    waveHeightData = ts.wave_height_sig?.data || [];
    const sigPeriodData = ts.wave_period_sig?.data || [];
    wavePeriodData = sigPeriodData.length ? sigPeriodData : ts.wave_period_sig_basic?.data || [];
    wavePeriodPeakData = ts.wave_period_peak?.data || [];
    chartTitle = `${buoy.name} - Wave Conditions`;
    heightLabel = "Significant Wave Height";
    periodLabel = "Significant Period";
  }

  // Check if this buoy has wave direction data
  // Halibut Bank (4600146), Sentry Shoal (4600131), Angeles Point (46267)
  const hasWaveDirection = buoyId === "4600146" || buoyId === "4600131" || buoyId === "46267";

  // Select appropriate direction data based on buoy type:
  // - All buoys: Prefer wave_direction_avg (MWD - Mean Wave Direction for NOAA)
  // - Fall back to peak direction if avg not available
  let waveDirectionData = [];
  if (hasWaveDirection) {
    waveDirectionData = ts.wave_direction_avg?.data || ts.wave_direction_peak?.data || [];
  }

  // Create direction arrow data if available
  const { arrowData, maxValue } = hasWaveDirection
    ? createWaveDirectionArrowData(waveDirectionData, waveHeightData, colors.primary)
    : { arrowData: [], maxValue: null };

  // Calculate y-axis max to ensure arrows are visible at top (only if arrows exist)
  const yAxisMax = maxValue
    ? (value) => Math.max(1, Math.ceil(maxValue * 1.1))
    : (value) => Math.max(1, Math.ceil(value.max * 1.1));

  // Build series array dynamically
  const series = [
    {
      name: heightLabel,
      type: "line",
      data: sanitizeSeriesData(waveHeightData),
      smooth: true,
      connectNulls: false,
      yAxisIndex: 0,
      itemStyle: { color: colors.primary },
      areaStyle: theme.isDark ? { opacity: 0 } : { opacity: 0.1 },
    },
    {
      name: periodLabel,
      type: "line",
      data: sanitizeSeriesData(wavePeriodData),
      smooth: true,
      connectNulls: false,
      yAxisIndex: 1,
      itemStyle: { color: colors.quaternary },
    },
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
      itemStyle: { color: colors.senary, opacity: 0.6 },
    });
  }

  // Add wave direction arrows if available (show in legend)
  if (hasWaveDirection && arrowData.length > 0) {
    legendData.push("Wave Direction");
    series.push({
      name: "Wave Direction",
      type: "scatter",
      data: arrowData,
      symbol: DIRECTION_ARROW_PATH,
      symbolSize: 16,
      symbolRotate: function (dataIndex) {
        return arrowData[dataIndex]?.symbolRotate || 0;
      },
      yAxisIndex: 0,
      itemStyle: {
        color: function (params) {
          return arrowData[params.dataIndex]?.itemStyle?.color || colors.primary;
        },
        opacity: function (params) {
          return arrowData[params.dataIndex]?.itemStyle?.opacity || 1.0;
        },
      },
      silent: true,
      z: 2,
    });
  }

  waveChart.setOption(
    {
      backgroundColor: theme.background,
      textStyle: { color: textColor },
      title: {
        text: chartTitle,
        left: "center",
        textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14, color: textColor },
      },
      tooltip: {
        ...getMobileOptimizedTooltipConfig(),
        formatter: (params) => {
          if (!params || params.length === 0) return "";
          const time = formatMonthDayTime(params[0].value[0]);
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
            const dirPoint = waveDirectionData.find(
              (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
            ); // Within 30 min
            if (dirPoint && dirPoint.value != null) {
              const dir = Math.round(dirPoint.value);
              const compass = degreesToCardinal(dir);
              res += `🌊 Direction: ${dir}° (${compass})<br/>`;
            }
          }

          return res;
        },
      },
      legend: {
        data: legendData,
        bottom: getResponsiveLegendBottom(),
        textStyle: { color: textColor },
      },
      grid: getResponsiveGridConfig(false),
      xAxis: {
        type: "time",
        axisLabel: {
          fontSize: window.innerWidth < 600 ? 9 : 10,
          rotate: window.innerWidth < 600 ? 30 : 0,
          formatter: (value) => formatCompactTimeLabel(new Date(value).toISOString()),
          hideOverlap: true,
          margin: 10,
          color: mutedText,
        },
        axisTick: { show: true },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { show: true, lineStyle: { color: gridColor } },
      },
      yAxis: [
        {
          type: "value",
          name: "Height (m)",
          position: "left",
          min: 0,
          max: yAxisMax,
          scale: true,
          nameTextStyle: { color: colors.primary },
          axisLine: { lineStyle: { color: colors.primary } },
          axisLabel: { color: mutedText },
          splitLine: { lineStyle: { color: gridColor } },
        },
        {
          type: "value",
          name: "Period (s)",
          position: "right",
          nameTextStyle: { color: colors.quaternary },
          axisLine: { lineStyle: { color: colors.quaternary } },
          axisLabel: { color: mutedText },
          splitLine: { lineStyle: { color: gridColor } },
        },
      ],
      series: series,
    },
    true,
  );
}

// degreesToCompass removed — use degreesToCardinal from chart-utils-v4.js
