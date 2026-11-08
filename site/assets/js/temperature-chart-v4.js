/* -----------------------------
   Temperature Chart Module (ES module)
   Handles air and sea temperature visualization

   Chart helpers (theme/grid/tooltip config, sanitizeSeriesData,
   showChartError) still come from classic scripts loaded before the
   entry point.
   ----------------------------- */

import { formatMonthDayTime } from "./shared/format-time.js";
import { airTempIsProxy, airTempLabel, caveatCovers } from "./shared/station-meta.js";

/**
 * Render temperature chart for the selected buoy
 * @param {Object} tempChart - ECharts instance for temperature chart
 * @param {Object} buoy - Buoy data including name and timeseries
 * @param {Object} [meta] - The station's stations.json entry (asterisk rule)
 */
export function renderTemperatureChart(tempChart, buoy, meta) {
  // The asterisk rule: a caveated series is starred in the legend (and so the
  // tooltip); charts-v4.js puts the caveat under the chart.
  const airName =
    (airTempIsProxy(meta) ? airTempLabel(meta) : "Air Temperature") +
    (caveatCovers(meta, "air_temp") ? " *" : "");
  const seaName = "Sea Temperature" + (caveatCovers(meta, "sea_temp") ? " *" : "");
  try {
    const ts = buoy.timeseries;
    const theme = getChartThemeColors();
    const colors = theme.series;
    const textColor = theme.text;
    const mutedText = theme.mutedText;
    const axisColor = theme.axisLine;
    const gridColor = theme.gridLine;
    const airTempData = ts.air_temp?.data || [];
    const seaTempData = ts.sea_temp?.data || [];

    // Calculate y-axis range with 1°C padding
    const allTemps = [...airTempData, ...seaTempData]
      .map((d) => d?.value)
      .filter((v) => v != null && !isNaN(v));

    // Functions of the extent ECharts measures over the series still shown, so
    // hiding Air or Sea refits the axis to the other (pinned numbers held it
    // where both had put it).
    let yMin = "dataMin";
    let yMax = "dataMax";
    if (allTemps.length > 0) {
      yMin = (value) => Math.floor(value.min - 1);
      yMax = (value) => Math.ceil(value.max + 1);
    }

    tempChart.setOption({
      backgroundColor: theme.background,
      textStyle: { color: textColor },
      title: {
        text: `${buoy.name} - Temperature`,
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
            if (p.value[1] != null) {
              res += `${p.marker} ${p.seriesName}: ${p.value[1]} °C<br/>`;
            }
          });
          return res;
        },
      },
      legend: {
        data: [airName, seaName],
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
        name: "Temperature (°C)",
        min: yMin,
        max: yMax,
        axisLabel: { formatter: "{value} °C", color: mutedText },
        nameTextStyle: { color: textColor, align: "left" },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { lineStyle: { color: gridColor } },
      },
      series: [
        {
          name: airName,
          type: "line",
          data: sanitizeSeriesData(airTempData),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: colors.secondary },
        },
        {
          name: seaName,
          type: "line",
          data: sanitizeSeriesData(seaTempData),
          smooth: true,
          connectNulls: false,
          itemStyle: { color: colors.quinary },
        },
      ],
    });
  } catch (error) {
    showChartError("temp-chart", "Temperature Chart", error);
  }
}
