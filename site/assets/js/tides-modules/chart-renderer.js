/**
 * Chart Renderer Module
 *
 * Handles all ECharts visualization for tide data:
 * - Astronomical tide predictions
 * - Real-time observations
 * - Geodetic calibration (two methodologies)
 * - Combined water level forecasts
 * - Storm surge forecasts
 * - High/low event markers
 * - Current time indicator
 * - Sunlight time markers
 * - Day filtering and windowing
 */

import { PACIFIC_TZ } from './constants.js';

let tideChart = null;
let currentGeodeticResiduals = []; // Global storage for residuals

/**
 * Get Pacific timezone midnight for a given date
 */
function getPacificMidnight(year, month, day) {
  // Try PST (UTC-8, which is UTC+8 hours for midnight)
  let testDate = new Date(Date.UTC(year, month - 1, day, 8, 0, 0, 0));
  let testHour = parseInt(testDate.toLocaleString('en-US', {
    timeZone: PACIFIC_TZ,
    hour: 'numeric',
    hour12: false
  }));

  // If it's midnight, we found it
  if (testHour === 0) return testDate;

  // Otherwise try PDT (UTC-7, which is UTC+7 hours for midnight)
  testDate = new Date(Date.UTC(year, month - 1, day, 7, 0, 0, 0));
  return testDate;
}

/**
 * Filter data array by day range
 */
function filterByDay(dataArray, timeKey, dayStart, dayEnd) {
  return dataArray.filter(item => {
    const itemDate = new Date(item[timeKey]);
    return itemDate >= dayStart && itemDate < dayEnd;
  });
}

/**
 * Get responsive legend bottom position based on screen size
 */
function getResponsiveLegendBottom() {
  return window.innerWidth < 600 ? 0 : 5;
}

/**
 * Calculate interpolated tide value at a specific time
 */
function interpolateTide(predictions, targetTime) {
  for (let i = 0; i < predictions.length - 1; i++) {
    const t1 = new Date(predictions[i].time);
    const t2 = new Date(predictions[i + 1].time);

    if (targetTime >= t1 && targetTime <= t2) {
      const ratio = (targetTime - t1) / (t2 - t1);
      return predictions[i].value + ratio * (predictions[i + 1].value - predictions[i].value);
    }
  }
  return null;
}

/**
 * Build chart series array based on available data
 */
function buildChartSeries(data) {
  const {
    isCrescentBeach,
    isCrescentChannel,
    times,
    values,
    obsTimes,
    obsValues,
    observations,
    residuals,
    surgeData,
    combinedData,
    dayOffset,
    sunlightTimes,
    targetDateStr,
    dayStart,
    dayEnd,
    predictions
  } = data;

  const series = [];

  // 1. Tide Predictions (Astronomical Tide for DFO, Tide Forecast for geodetic)
  const isGeodetic = isCrescentBeach || isCrescentChannel;
  const tidePredictionName = isGeodetic ? 'Tide Forecast (Geodetic)' : 'Astronomical Tide';
  const tidePredictionColor = isGeodetic ? '#1976d2' : '#0077be';

  series.push({
    name: tidePredictionName,
    type: 'line',
    data: times.map((t, i) => [t, values[i]]),
    smooth: true,
    lineStyle: { color: tidePredictionColor, width: 2 },
    itemStyle: { color: tidePredictionColor },
    showSymbol: false
  });

  // 2. Sunlight times as vertical lines
  if (sunlightTimes) {
    const sunlightEvents = [
      {
        name: 'First Light / Last Light',
        times: [new Date(sunlightTimes.first_light), new Date(sunlightTimes.last_light)],
        color: '#e91e63',
        width: 1
      },
      {
        name: 'Sunrise / Sunset',
        times: [new Date(sunlightTimes.sunrise), new Date(sunlightTimes.sunset)],
        color: '#ff9800',
        width: 1.5
      }
    ];

    sunlightEvents.forEach(event => {
      const markLineData = [];

      event.times.forEach(time => {
        if (time >= dayStart && time <= dayEnd) {
          markLineData.push({
            xAxis: time,
            lineStyle: { color: event.color, type: 'dashed', width: event.width },
            label: { show: false }
          });
        }
      });

      if (markLineData.length > 0) {
        series.push({
          name: event.name,
          type: 'line',
          data: [],
          markLine: {
            silent: true,
            symbol: 'none',
            data: markLineData
          },
          showSymbol: false,
          lineStyle: { opacity: 0 },
          itemStyle: { opacity: 0 },
          tooltip: { show: false }
        });
      }
    });
  }

  // 3. Raw observations (show for all stations except Crescent Channel)
  if (observations.length > 0 && dayOffset === 0 && !isCrescentChannel) {
    series.push({
      name: 'Observation',
      type: 'scatter',
      data: obsTimes.map((t, i) => [t, obsValues[i]]),
      itemStyle: { color: '#43a047' },
      symbolSize: 6,
      z: 10
    });
  }

  // 4. Tidal residuals for Surrey stations (pre-calculated by Surrey)
  if (residuals.length > 0 && dayOffset === 0) {
    series.push({
      name: 'Tidal Residual (Surrey)',
      type: 'line',
      data: residuals,
      smooth: false,
      lineStyle: { color: '#e53935', width: 2, type: 'dashed' },
      itemStyle: { color: '#e53935' },
      showSymbol: true,
      symbolSize: 4,
      z: 8
    });

    // Zero reference line for residuals
    series.push({
      name: 'Zero Reference',
      type: 'line',
      data: [[times[0], 0], [times[times.length - 1], 0]],
      lineStyle: { color: '#999', width: 1, type: 'dotted' },
      showSymbol: false,
      silent: true,
      z: 1
    });
  }

  // 7. Storm surge forecast
  if (surgeData.length > 0) {
    series.push({
      name: 'Storm Surge (Forecast)',
      type: 'line',
      data: surgeData,
      smooth: true,
      lineStyle: { color: '#9c27b0', width: 2 },
      itemStyle: { color: '#9c27b0' },
      showSymbol: false
    });
  }

  // 8. Total water level (only for non-geodetic stations)
  if (combinedData.length > 0) {
    series.push({
      name: 'Total Water Level (Forecast)',
      type: 'line',
      data: combinedData,
      smooth: true,
      lineStyle: { color: '#00897b', width: 3 },
      itemStyle: { color: '#00897b' },
      showSymbol: false,
      z: 5
    });
  }

  // 9. Current time indicator (only for today, skip for geodetic stations)
  if (dayOffset === 0 && !isGeodetic) {
    const now = new Date();
    const currentPredictedTide = interpolateTide(predictions, now);

    if (currentPredictedTide !== null) {
      let currentEstimatedTide = currentPredictedTide;
      let nearestResidual = null;

      // Calculate residual from most recent observation
      if (observations.length > 0) {
        const latestObs = observations[observations.length - 1];
        const obsTime = new Date(latestObs.time);
        const obsValue = latestObs.value;

        const predAtObsTime = interpolateTide(predictions, obsTime);

        if (predAtObsTime !== null) {
          nearestResidual = obsValue - predAtObsTime;
          currentEstimatedTide = currentPredictedTide + nearestResidual;
        }
      }

      series.push({
        name: nearestResidual !== null ? 'Now (Predicted + Residual)' : 'Now (Predicted)',
        type: 'scatter',
        data: [[now, currentEstimatedTide]],
        itemStyle: {
          color: nearestResidual !== null ? '#e53935' : '#ff9800',
          borderColor: '#fff',
          borderWidth: 2
        },
        symbolSize: 12,
        z: 15,
        tooltip: {
          formatter: function(params) {
            const timeStr = now.toLocaleString('en-US', {
              month: 'short',
              day: 'numeric',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false,
              timeZone: PACIFIC_TZ
            });
            let tooltip = `<strong>Now</strong><br/>${timeStr}<br/>`;
            tooltip += `Tide: ${currentEstimatedTide.toFixed(3)} m<br/>`;
            if (nearestResidual !== null) {
              tooltip += `<span style="color: #666; font-size: 0.9em;">Predicted: ${currentPredictedTide.toFixed(3)} m<br/>`;
              tooltip += `Residual: ${nearestResidual >= 0 ? '+' : ''}${nearestResidual.toFixed(3)} m</span>`;
            }
            return tooltip;
          }
        }
      });
    }
  }

  return series;
}

/**
 * Build chart legend items based on available data
 */
function buildLegendData(data) {
  const {
    isCrescentBeach,
    isCrescentChannel,
    observations,
    residuals,
    surgeData,
    combinedData,
    dayOffset
  } = data;

  const legendItems = [];

  // Tide Predictions (Astronomical Tide for DFO, Tide Forecast for geodetic)
  const isGeodetic = isCrescentBeach || isCrescentChannel;
  legendItems.push(isGeodetic ? 'Tide Forecast (Geodetic)' : 'Astronomical Tide');

  // Observation (hide for Crescent Channel)
  if (!isCrescentChannel && observations.length > 0) {
    legendItems.push('Observation');
  }

  if (residuals.length > 0) {
    legendItems.push('Tidal Residual (Surrey)');
  }
  if (surgeData.length > 0) {
    legendItems.push('Storm Surge (Forecast)');
  }
  if (combinedData.length > 0) {
    legendItems.push('Total Water Level (Forecast)');
  }

  // Current time indicator (only for today, skip for geodetic stations)
  if (dayOffset === 0 && !isGeodetic) {
    const hasResidual = observations.length > 0;
    legendItems.push(hasResidual ? 'Now (Predicted + Residual)' : 'Now (Predicted)');
  }

  return legendItems;
}

/**
 * Display tide chart with all series and features
 *
 * @param {string} stationKey - Station identifier
 * @param {number} dayOffset - Day offset from today (0 = today, 1 = tomorrow, etc.)
 * @param {object} tideTimeseriesData - Tide timeseries data
 * @param {object} combinedWaterLevelData - Combined water level data
 * @param {function} getSunlightTimesForDate - Callback to get sunlight times
 */
export function displayTideChart(stationKey, dayOffset, tideTimeseriesData, combinedWaterLevelData, getSunlightTimesForDate) {
  const chartContainer = document.getElementById('tide-chart');

  if (!chartContainer) return;

  // Initialize chart if needed
  if (!tideChart) {
    tideChart = echarts.init(chartContainer);
  }

  // Calculate target day range (Pacific timezone)
  const now = new Date();

  // Get current year/month/day in Pacific timezone using Intl API
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: PACIFIC_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  const parts = formatter.formatToParts(now);
  const year = parseInt(parts.find(p => p.type === 'year').value);
  const month = parseInt(parts.find(p => p.type === 'month').value);
  const day = parseInt(parts.find(p => p.type === 'day').value);

  // Create a date for the current Pacific day and add offset
  const targetDate = new Date(year, month - 1, day);
  targetDate.setDate(targetDate.getDate() + dayOffset);

  // Extract components for date string
  const pacificYear = targetDate.getFullYear();
  const pacificMonth = targetDate.getMonth() + 1;
  const pacificDay = targetDate.getDate();

  const targetDateStr = `${pacificYear}-${String(pacificMonth).padStart(2, '0')}-${String(pacificDay).padStart(2, '0')}`;

  // Get midnight Pacific for the start and end of the target day
  const dayStart = getPacificMidnight(pacificYear, pacificMonth, pacificDay);
  const dayEnd = new Date(dayStart.getTime() + 24 * 60 * 60 * 1000);

  // Get station data
  const stationData = tideTimeseriesData?.stations?.[stationKey];

  if (!stationData) {
    if (tideChart) {
      tideChart.dispose();
      tideChart = null;
    }
    chartContainer.innerHTML = '<p style="text-align: center; color: #999; padding: 2rem;">No tide data available for this station</p>';
    return;
  }

  // Filter predictions for the target day
  const predictions = filterByDay(stationData.predictions || [], 'time', dayStart, dayEnd);

  // Only include observations for today
  let observations = [];
  if (dayOffset === 0) {
    observations = filterByDay(stationData.observations || [], 'time', dayStart, dayEnd);
  }

  if (predictions.length === 0) {
    if (tideChart) {
      tideChart.dispose();
      tideChart = null;
    }
    chartContainer.innerHTML = '<p style="text-align: center; color: #999; padding: 2rem;">No prediction data available for this day</p>';
    return;
  }

  const times = predictions.map(p => new Date(p.time));
  const values = predictions.map(p => p.value);
  const obsTimes = observations.map(o => new Date(o.time));
  const obsValues = observations.map(o => o.value);

  // Determine station type
  const isCrescentChannel = stationKey === 'crescent_channel_ocean';
  const isCrescentBeach = stationKey === 'crescent_beach_ocean';

  // Get Surrey's pre-calculated tidal residuals (if available)
  let residuals = [];
  if (stationData.residuals && stationData.residuals.length > 0 && dayOffset === 0) {
    // Filter residuals for the current day and convert to chart format
    residuals = filterByDay(stationData.residuals, 'time', dayStart, dayEnd).map(r => [
      new Date(r.time),
      r.value
    ]);
  }

  // Store residuals globally for storm surge card
  currentGeodeticResiduals = residuals;

  // Get combined water level data
  let combinedData = [];
  let surgeData = [];
  const isGeodetic = isCrescentBeach || isCrescentChannel; // Surrey stations are geodetic

  if (combinedWaterLevelData?.stations?.[stationKey]) {
    const forecast = combinedWaterLevelData.stations[stationKey].forecast || [];
    const filteredCombined = forecast.filter(item => {
      const itemDate = new Date(item.time);
      return itemDate >= dayStart && itemDate <= dayEnd;
    });

    // Only show total water level for non-geodetic stations
    if (!isGeodetic) {
      combinedData = filteredCombined.map(item => [
        new Date(item.time),
        item.total_water_level_m
      ]);
    }

    // Show storm surge for all stations
    surgeData = filteredCombined.map(item => [
      new Date(item.time),
      item.storm_surge_m
    ]);
  }

  // Get sunlight times
  const sunlightTimes = getSunlightTimesForDate(stationKey, targetDateStr);

  // Prepare data object for series building
  const chartData = {
    isCrescentBeach,
    isCrescentChannel,
    times,
    values,
    obsTimes,
    obsValues,
    observations,
    residuals,
    surgeData,
    combinedData,
    dayOffset,
    sunlightTimes,
    targetDateStr,
    dayStart,
    dayEnd,
    predictions
  };

  // Build chart option
  const option = {
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: function(params) {
        const date = new Date(params[0].value[0]);
        const timeStr = date.toLocaleString('en-US', {
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false,
          timeZone: PACIFIC_TZ
        });
        let result = `${timeStr}<br/>`;
        params.forEach(param => {
          const value = param.value[1];
          if (value !== null && value !== undefined) {
            result += `${param.marker} ${param.seriesName}: ${value.toFixed(3)} m<br/>`;
          }
        });
        return result;
      }
    },
    legend: {
      data: buildLegendData(chartData),
      bottom: getResponsiveLegendBottom(),
      textStyle: { fontSize: 10 }
    },
    grid: {
      left: window.innerWidth < 600 ? '8%' : '8%',
      right: window.innerWidth < 600 ? '4%' : '6%',
      top: '10%',
      bottom: '22%',
      containLabel: true
    },
    xAxis: {
      type: 'time',
      axisLabel: {
        formatter: function(value, index) {
          const date = new Date(value);
          const hour = date.toLocaleString('en-US', {
            hour: '2-digit',
            hour12: false,
            timeZone: PACIFIC_TZ
          });

          if (hour === '00' || index === 0) {
            const monthDay = date.toLocaleString('en-US', {
              month: 'short',
              day: 'numeric',
              timeZone: PACIFIC_TZ
            });
            return `${monthDay} ${hour}h`;
          }

          return `${hour}h`;
        },
        hideOverlap: true,
        interval: 'auto',
        fontSize: window.innerWidth < 600 ? 9 : 10,
        rotate: window.innerWidth < 600 ? 25 : 0
      },
      splitLine: { show: true, lineStyle: { color: "#eee" } }
    },
    yAxis: {
      type: 'value',
      name: window.innerWidth < 600 ? 'height (meters)' : 'Height (m)',
      nameLocation: 'middle',
      nameGap: window.innerWidth < 600 ? 25 : 45,
      nameTextStyle: {
        fontSize: window.innerWidth < 600 ? 9 : 12
      }
    },
    series: buildChartSeries(chartData)
  };

  // Clear and render chart
  tideChart.clear();
  tideChart.setOption(option);

  // Force resize
  setTimeout(() => {
    if (tideChart) {
      tideChart.resize();
    }
  }, 100);
}

/**
 * Get current geodetic residuals
 */
export function getCurrentGeodeticResiduals() {
  return currentGeodeticResiduals;
}

/**
 * Dispose chart on cleanup
 */
export function disposeChart() {
  if (tideChart) {
    tideChart.dispose();
    tideChart = null;
  }
}
