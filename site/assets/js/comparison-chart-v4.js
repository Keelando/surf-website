/* -----------------------------
   Comparison Chart Module (ES module)
   Handles multi-buoy wave height comparison

   Chart helpers (calculateArrowRotation, degreesToCardinal, theme/grid/
   tooltip config, sanitizeSeriesData, showChartError) still come from
   classic scripts loaded before the entry point.
   ----------------------------- */

import { formatMonthDayTime } from "./shared/format-time.js";
import { DIRECTION_ARROW_PATH } from "./shared/markers.js";

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
    const hourKey = new Date(
      timestamp.getFullYear(),
      timestamp.getMonth(),
      timestamp.getDate(),
      timestamp.getHours(),
      0,
      0,
      0,
    ).getTime();

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
export function renderComparisonChart(waveComparisonChart, chartData) {
  try {
    if (!chartData) {
      logger.warn("ComparisonChart", "No chart data available");
      return;
    }

    const buoyOrder = ["4600146", "4600304", "4600303", "4600131", "CRPILE"];
    const theme = getChartThemeColors();
    const colors = theme.series;
    const textColor = theme.text;
    const mutedText = theme.mutedText;
    const axisColor = theme.axisLine;
    const gridColor = theme.gridLine;
    const palette = [
      colors.primary,
      colors.quaternary,
      colors.secondary,
      colors.tertiary,
      colors.quinary,
    ];
    const buoyColors = buoyOrder.reduce((acc, id, idx) => {
      acc[id] = palette[idx % palette.length];
      return acc;
    }, {});

    // Track global max wave height across all buoys for arrow positioning
    let globalMaxHeight = 0;

    const series = buoyOrder
      .map((buoyId) => {
        const buoy = chartData[buoyId];
        if (!buoy?.timeseries?.wave_height_sig) return null;

        let data = buoy.timeseries.wave_height_sig.data || [];

        // Downsample high-frequency buoys to hourly for better chart performance
        if (buoyId === "CRPILE") {
          data = downsampleToHourly(data);
          logger.debug(
            "ComparisonChart",
            `Downsampled ${buoy.name} from high-frequency to hourly (${data.length} points)`,
          );
        }

        // Update global max
        for (const d of data) {
          const v = parseFloat(d.value);
          if (!isNaN(v) && v > globalMaxHeight) globalMaxHeight = v;
        }

        return {
          name: buoy.name,
          type: "line",
          data: sanitizeSeriesData(data),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: buoyColors[buoyId], borderColor: theme.symbolBorderColor },
          emphasis: { focus: "series" },
        };
      })
      .filter(Boolean);

    // Apply user threshold from localStorage to first series only
    const threshold = parseFloat(localStorage.getItem("waveThreshold"));
    if (series.length > 0) {
      series[0].markLine = !isNaN(threshold)
        ? {
            symbol: "none",
            data: [
              {
                yAxis: threshold,
                lineStyle: { type: "dashed", color: theme.negative, width: 1.5 },
                label: { formatter: `${threshold}m`, color: theme.negative },
              },
            ],
          }
        : { data: [] }; // explicitly clear so ECharts doesn't keep the old line
    }

    // Add Halibut Bank wave direction arrows
    const halibutBuoy = chartData["4600146"];
    const halibutDirData =
      halibutBuoy?.timeseries?.wave_direction_avg?.data ||
      halibutBuoy?.timeseries?.wave_direction_peak?.data ||
      [];
    const halibutHeightData = halibutBuoy?.timeseries?.wave_height_sig?.data || [];

    if (halibutDirData.length > 0) {
      const arrowYPosition = globalMaxHeight * 1.05;
      const sampleInterval = window.innerWidth < 600 ? 6 : 3;
      const arrowData = [];

      for (let i = 0; i < halibutDirData.length; i += sampleInterval) {
        const dirPoint = halibutDirData[i];
        if (!dirPoint || dirPoint.value == null) continue;

        const heightPoint = halibutHeightData.find((h) => h.time === dirPoint.time);
        if (!heightPoint || heightPoint.value == null) continue;

        arrowData.push({
          value: [new Date(dirPoint.time).getTime(), arrowYPosition],
          symbolRotate: calculateArrowRotation(dirPoint.value),
          itemStyle: { color: colors.primary, opacity: 0.7 },
        });
      }

      if (arrowData.length > 0) {
        series.push({
          name: "Wave Dir (Halibut)",
          type: "scatter",
          data: arrowData,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 14,
          symbolRotate: function (dataIndex) {
            return arrowData[dataIndex]?.symbolRotate || 0;
          },
          itemStyle: {
            color: function (params) {
              return arrowData[params.dataIndex]?.itemStyle?.color || colors.primary;
            },
            opacity: function (params) {
              return arrowData[params.dataIndex]?.itemStyle?.opacity || 0.7;
            },
          },
          silent: true,
          z: 3,
        });
      }
    }

    waveComparisonChart.setOption({
      backgroundColor: theme.background,
      textStyle: { color: textColor },
      title: {
        text: "Sig Wave Height (All)",
        left: "center",
        textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14, color: textColor },
      },

      tooltip: {
        ...getMobileOptimizedTooltipConfig(),
        formatter: (params) => {
          if (!params?.length) return "";
          const time = formatMonthDayTime(params[0].value[0]);
          let res = `<b>${time}</b><br/>`;
          for (const p of params) {
            if (p.seriesName === "Wave Dir (Halibut)") continue;
            if (p.value[1] != null) {
              res += `${p.marker} ${p.seriesName}: ${p.value[1]} m<br/>`;
            }
          }
          // Add Halibut Bank wave direction to tooltip
          if (halibutDirData.length > 0) {
            const timestamp = new Date(params[0].value[0]).getTime();
            const dirPoint = halibutDirData.find(
              (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
            );
            if (dirPoint && dirPoint.value != null) {
              const dir = Math.round(dirPoint.value);
              const compass = degreesToCardinal(dir);
              res += `🌊 Halibut Dir: ${dir}° (${compass})<br/>`;
            }
          }
          return res;
        },
      },

      legend: {
        data: [
          ...buoyOrder.map((id) => chartData[id]?.name).filter(Boolean),
          ...(halibutDirData.length > 0 ? ["Wave Dir (Halibut)"] : []),
        ],
        bottom: "3%", // Fixed lower position for comparison chart (multi-row legend needs more space)
        textStyle: { color: textColor },
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
          color: mutedText,
        },
        axisTick: { show: true },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { show: true, lineStyle: { color: gridColor } },
      },

      yAxis: {
        type: "value",
        name: "Wave Height (m)",
        min: 0,
        max: (value) => {
          const rawMax = value.max || 1;
          // Pad extra when direction arrows are present (they sit at 1.05x max)
          const padding = halibutDirData.length > 0 ? 1.15 : 1.01;
          const padded = Math.ceil(rawMax * padding * 10) / 10;
          return Math.max(1, padded);
        },
        scale: false,
        boundaryGap: [0, 0],
        axisLabel: { color: mutedText },
        nameTextStyle: { color: textColor },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { show: true, lineStyle: { color: gridColor } },
      },

      series: series,
    });
  } catch (error) {
    showChartError("wave-comparison-chart", "Wave Comparison Chart", error);
  }
}
