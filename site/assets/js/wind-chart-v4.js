/* -----------------------------
   Wind Chart Module
   Handles wind speed and gust visualization with direction arrows
   ----------------------------- */

// createWindDirectionArrowData provided by chart-utils-v4.js (loaded earlier)

/**
 * Render wind chart for the selected buoy (index/buoy page).
 * For the dedicated winds page chart, see renderWindChart() in wind-stations.js.
 *
 * @param {Object} windChart - ECharts instance for wind chart
 * @param {Object} buoy - Buoy data including name and timeseries
 */
function renderBuoyWindChart(windChart, buoy) {
  try {
    const ts = buoy.timeseries;
    const theme = getChartThemeColors();
    const colors = theme.series;
    const textColor = theme.text;
    const mutedText = theme.mutedText;
    const axisColor = theme.axisLine;
    const gridColor = theme.gridLine;
    const windSpeedData = ts.wind_speed?.data || [];
    const windGustData = ts.wind_gust?.data || [];
    const windDirectionData = ts.wind_direction?.data || [];

    // Create direction arrow data (returns object with arrowData and maxValue)
    const { arrowData, maxValue } = createWindDirectionArrowData(
      windDirectionData,
      windSpeedData,
      windGustData,
      colors.primary,
    );

    // Calculate y-axis max to ensure arrows are visible at top
    const yAxisMax = maxValue ? Math.ceil(maxValue * 1.1) : null;

    // Build legend data array
    const legendData = ["Wind Speed", "Wind Gust"];
    if (arrowData.length > 0) {
      legendData.push("Wind Direction");
    }

    windChart.setOption({
      backgroundColor: theme.background,
      textStyle: { color: textColor },
      title: {
        text: `${buoy.name} - Wind Conditions`,
        left: "center",
        textStyle: { fontSize: window.innerWidth < 600 ? 12 : 14, color: textColor },
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
          const dirPoint = windDirectionData.find(
            (d) => Math.abs(new Date(d.time).getTime() - timestamp) < 1800000,
          ); // Within 30 min
          if (dirPoint && dirPoint.value != null) {
            const dir = Math.round(dirPoint.value);
            const compass = degreesToCardinal(dir);
            res += `🧭 Direction: ${dir}° (${compass})<br/>`;
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
      yAxis: {
        type: "value",
        name: "Speed (kt)",
        max: yAxisMax, // Set max to accommodate arrows at top
        axisLine: { lineStyle: { color: axisColor } },
        axisLabel: { color: mutedText },
        nameTextStyle: { color: textColor },
        splitLine: { lineStyle: { color: gridColor } },
      },
      series: [
        {
          name: "Wind Speed",
          type: "line",
          data: sanitizeSeriesData(windSpeedData),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: colors.secondary },
          areaStyle: theme.isDark ? { opacity: 0 } : { opacity: 0.1 },
        },
        {
          name: "Wind Gust",
          type: "scatter",
          data: sanitizeSeriesData(windGustData),
          symbol: "circle",
          symbolSize: 6,
          itemStyle: { color: theme.negative },
        },
        {
          name: "Wind Direction",
          type: "scatter",
          data: arrowData,
          symbol: DIRECTION_ARROW_PATH,
          symbolSize: 16,
          symbolRotate: function (dataIndex) {
            // Read rotation from data point
            return arrowData[dataIndex]?.symbolRotate || 0;
          },
          itemStyle: {
            color: function (params) {
              // Read color from data point
              return arrowData[params.dataIndex]?.itemStyle?.color || colors.primary;
            },
            opacity: function (params) {
              // Read opacity from data point
              return arrowData[params.dataIndex]?.itemStyle?.opacity || 1.0;
            },
          },
          silent: true, // Don't trigger mouse events
          z: 2, // Render on top of lines
        },
      ],
    });
  } catch (error) {
    showChartError("wind-chart", "Wind Chart", error);
  }
}

// degreesToCompass removed — use degreesToCardinal from chart-utils-v4.js
