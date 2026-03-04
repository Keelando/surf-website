/* -----------------------------
   Lightstation Charts Module
   Displays 24hr wind speed and wave height trends
   ----------------------------- */

// Helper: Fetch with timeout
async function fetchWithTimeout(url, timeout = 5000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(id);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } catch (error) {
    clearTimeout(id);
    throw error;
  }
}

// Helper: Format timestamp to local time
function formatTimestamp(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString("en-US", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  }).replace(',', '');
}

// Global chart instances
let windSpeedChart = null;
let waveHeightChart = null;
let lightstationTimeseriesData = null;
let allLightstations = [];

/**
 * Load lightstation timeseries data and populate station selector
 */
async function loadLightstationTimeseries() {
  try {
    // Load timeseries data (past 24hr only)
    const data = await fetchWithTimeout(`/data/lightstation_timeseries_24hr.json?t=${Date.now()}`);
    lightstationTimeseriesData = data;
    // Make globally accessible for edge case handling
    window.lightstationTimeseriesData = data;

    // Load ALL lightstations from stations.json (including those without recent data)
    const stationsData = await fetchWithTimeout('/data/stations.json');
    const lightstationsMeta = stationsData.lightstations || {};

    const select = document.getElementById("lightstation-station-select");
    const searchInput = document.getElementById("lightstation-station-search");
    if (!select) return;

    // Build combined list: all stations from metadata, with timeseries data if available
    allLightstations = Object.entries(lightstationsMeta).map(([id, meta]) => {
      const stationKey = meta.name.toUpperCase();
      const hasData = lightstationTimeseriesData[stationKey] !== undefined;

      return [
        stationKey,
        {
          name: stationKey,
          region: meta.region,
          hasRecentData: hasData
        }
      ];
    }).sort((a, b) => a[1].name.localeCompare(b[1].name));

    // Populate dropdown
    populateLightstationDropdown();

    // Set default selection (Merry Island, or first station if not found)
    if (allLightstations.length > 0) {
      // Try to find Merry Island
      const merryIsland = allLightstations.find(([id, station]) =>
        station.name === 'MERRY ISLAND' || id === 'MERRY ISLAND'
      );

      const defaultStation = merryIsland ? merryIsland[0] : allLightstations[0][0];
      select.value = defaultStation;
      renderLightstationCharts(defaultStation);
    }

    // Add change listener to dropdown
    select.addEventListener('change', (e) => {
      renderLightstationCharts(e.target.value);
    });

    // Add search listener
    if (searchInput) {
      searchInput.addEventListener('input', (e) => {
        const searchText = e.target.value.toLowerCase();
        if (!searchText) return;

        // Find first matching station
        const match = allLightstations.find(([id, station]) =>
          station.name.toLowerCase().includes(searchText) ||
          id.toLowerCase().includes(searchText)
        );

        if (match) {
          select.value = match[0];
          renderLightstationCharts(match[0]);
        }
      });
    }

  } catch (error) {
    console.error('Error loading lightstation timeseries:', error);
    const select = document.getElementById("lightstation-station-select");
    if (select) {
      select.innerHTML = '<option value="">Error loading stations</option>';
    }
  }
}

/**
 * Populate station dropdown grouped by region
 */
function populateLightstationDropdown() {
  const select = document.getElementById("lightstation-station-select");
  if (!select || !allLightstations) return;

  select.innerHTML = '';

  // Group stations by region
  const regionGroups = {};
  allLightstations.forEach(([id, station]) => {
    const region = station.region || 'Other';
    if (!regionGroups[region]) {
      regionGroups[region] = [];
    }
    regionGroups[region].push([id, station]);
  });

  // Define region order
  const regionOrder = [
    'STRAIT OF GEORGIA',
    'JUAN DE FUCA STRAIT',
    'WEST COAST VANCOUVER ISLAND',
    'CENTRAL COAST',
    'HECATE STRAIT'
  ];

  // Create optgroups for each region
  regionOrder.forEach(regionName => {
    if (!regionGroups[regionName]) return;

    const optgroup = document.createElement('optgroup');
    optgroup.label = regionName;

    regionGroups[regionName].forEach(([id, station]) => {
      const option = document.createElement('option');
      option.value = id;
      // Add indicator if station doesn't have recent data
      const dataIndicator = station.hasRecentData ? '' : ' (no recent data)';
      option.textContent = station.name + dataIndicator;
      optgroup.appendChild(option);
    });

    select.appendChild(optgroup);
  });
}

/**
 * Render both wind and wave charts for selected station
 */
function renderLightstationCharts(stationName) {
  if (!stationName) return;

  const station = lightstationTimeseriesData ? lightstationTimeseriesData[stationName] : null;

  // Update 24-hour reports title with station name
  const title = document.getElementById('lightstation-24hr-title');
  if (title) {
    title.textContent = `24-Hour Reports: ${stationName}`;
  }

  if (!station) {
    // No recent data - show message
    showNoDataMessage(stationName);
    return;
  }

  renderWindSpeedChart(stationName, station);
  renderWaveHeightChart(stationName, station);
  render24HourTable(stationName, station);
}

/**
 * Show "no data available" message in charts and table
 */
function showNoDataMessage(stationName) {
  const tbody = document.getElementById('lightstation-24hr-body');
  if (tbody) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 2rem; color: #e53e3e;">⚠️ No data available from the past 24 hours for this station.<br/><span style="font-size: 0.9rem; color: #718096; margin-top: 0.5rem; display: inline-block;">Most recent observation may be older than 24 hours.</span></td></tr>';
  }

  // Clear charts
  if (windSpeedChart) {
    windSpeedChart.clear();
    windSpeedChart.setOption({
      title: {
        text: `${stationName} - Wind Speed`,
        subtext: 'No data from the past 24 hours',
        left: 'center',
        textStyle: { fontSize: 18, fontWeight: 600, color: '#004b7c' },
        subtextStyle: { fontSize: 14, color: '#e53e3e' }
      }
    });
  }

  if (waveHeightChart) {
    waveHeightChart.clear();
    waveHeightChart.setOption({
      title: {
        text: `${stationName} - Sea State`,
        subtext: 'No data from the past 24 hours',
        left: 'center',
        textStyle: { fontSize: 18, fontWeight: 600, color: '#004b7c' },
        subtextStyle: { fontSize: 14, color: '#e53e3e' }
      }
    });
  }
}

// Make function globally accessible for card links
window.renderLightstationCharts = renderLightstationCharts;

/**
 * Render 24-hour data table for selected station
 */
function render24HourTable(stationName, station) {
  const tbody = document.getElementById('lightstation-24hr-body');
  if (!tbody) return;

  const timeseries = station.timeseries;

  // Get all unique timestamps from all data series
  const timestamps = new Set();

  ['wind_speed_kt', 'sea_height_ft', 'swell_intensity', 'sea_condition'].forEach(field => {
    if (timeseries[field]) {
      timeseries[field].forEach(point => timestamps.add(point.time));
    }
  });

  // Convert to array and sort by time (newest first)
  const sortedTimes = Array.from(timestamps).sort((a, b) => new Date(b) - new Date(a));

  if (sortedTimes.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 2rem; color: #718096;">No data available for this station</td></tr>';
    return;
  }

  // Build table rows
  let tableHTML = '';
  sortedTimes.forEach(time => {
    // Find data for this timestamp
    const windData = timeseries.wind_speed_kt?.find(p => p.time === time);
    const seaData = timeseries.sea_height_ft?.find(p => p.time === time);
    const swellData = timeseries.swell_intensity?.find(p => p.time === time);
    const conditionData = timeseries.sea_condition?.find(p => p.time === time);
    const directionData = timeseries.wind_direction?.find(p => p.time === time);

    // Format timestamp
    const date = new Date(time);
    const formattedTime = date.toLocaleString("en-US", {
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "America/Vancouver"
    }).replace(',', '');

    // Build wind text
    let windText = '—';
    if (windData && windData.value !== null) {
      const direction = directionData ? directionData.value : '';
      const gusting = windData.gusting ? ' (gusting)' : '';
      windText = `${direction} ${Math.round(windData.value)} kt${gusting}`;
    }

    // Build sea state text
    let seaText = '—';
    if (seaData && seaData.value !== null) {
      seaText = `${seaData.value} ft`;
    }

    // Build swell text
    let swellText = '—';
    if (swellData && swellData.value) {
      swellText = swellData.value;
    }

    // Build conditions text
    let conditionsText = '—';
    if (conditionData && conditionData.value) {
      conditionsText = conditionData.value;
    }

    // Combine sea state and condition if both exist
    if (seaData && seaData.value !== null && conditionData && conditionData.value) {
      seaText = `${seaData.value} ft - ${conditionData.value}`;
      conditionsText = '—'; // Don't duplicate
    }

    tableHTML += `
      <tr style="background: ${sortedTimes.indexOf(time) % 2 === 0 ? 'white' : 'rgba(0, 75, 124, 0.03)'};">
        <td style="padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e7ee; font-size: 0.95rem;">${formattedTime}</td>
        <td style="padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e7ee; font-size: 0.95rem;">${windText}</td>
        <td style="padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e7ee; font-size: 0.95rem;">${seaText}</td>
        <td style="padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e7ee; font-size: 0.95rem;">${swellText}</td>
        <td style="padding: 0.5rem 0.75rem; border-bottom: 1px solid #e0e7ee; font-size: 0.95rem;">${conditionsText}</td>
      </tr>
    `;
  });

  tbody.innerHTML = tableHTML;
}

/**
 * Render wind speed chart
 */
function renderWindSpeedChart(stationName, station) {
  const chartContainer = document.getElementById('lightstation-wind-chart');
  if (!chartContainer) return;

  // Initialize chart if needed
  if (!windSpeedChart) {
    windSpeedChart = echarts.init(chartContainer);
  }

  const timeseries = station.timeseries;
  const windSpeedData = timeseries.wind_speed_kt || [];

  // Prepare data for ECharts
  const speedData = windSpeedData.map(p => [new Date(p.time).getTime(), p.value]);

  // Separate gusting vs non-gusting for visual distinction
  const normalSpeedData = windSpeedData
    .filter(p => !p.gusting)
    .map(p => [new Date(p.time).getTime(), p.value]);

  const gustingSpeedData = windSpeedData
    .filter(p => p.gusting)
    .map(p => [new Date(p.time).getTime(), p.value]);

  const option = {
    backgroundColor: '#ffffff',
    title: {
      text: `${station.name} - Wind Speed`,
      left: 'center',
      textStyle: {
        fontSize: 18,
        fontWeight: 600,
        color: '#004b7c'
      }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = new Date(params[0].value[0]).toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver"
        }).replace(',', '');

        let tooltipText = `<strong>${time}</strong><br/>`;
        params.forEach(param => {
          if (param.value && param.value[1] != null) {
            tooltipText += `${param.marker} ${param.seriesName}: ${Math.round(param.value[1])} kt<br/>`;
          }
        });
        return tooltipText;
      }
    },
    legend: {
      data: ['Wind Speed', 'Gusting'],
      top: 35,
      textStyle: {
        fontSize: 14
      }
    },
    grid: {
      left: '15%',
      right: '4%',
      bottom: '15%',
      top: '20%',
      containLabel: false
    },
    xAxis: {
      type: 'time',
      boundaryGap: false,
      splitLine: {
        show: true,
        lineStyle: {
          color: '#e0e7ee',
          type: 'dashed'
        }
      },
      axisLabel: {
        formatter: (value) => {
          const date = new Date(value);
          return date.toLocaleString("en-US", {
            month: "numeric",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
            timeZone: "America/Vancouver"
          }).replace(',', '\n');
        },
        fontSize: 12
      }
    },
    yAxis: {
      type: 'value',
      name: 'Wind Speed (kt)',
      nameLocation: 'middle',
      nameGap: 35,
      nameTextStyle: {
        fontSize: 13,
        fontWeight: 600
      },
      min: 0
    },
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100
      },
      {
        type: 'slider',
        start: 0,
        end: 100,
        height: 30,
        bottom: '5%'
      }
    ],
    series: [
      {
        name: 'Wind Speed',
        type: 'line',
        data: normalSpeedData,
        smooth: true,
        lineStyle: {
          width: 2,
          color: '#4299e1'
        },
        itemStyle: {
          color: '#4299e1'
        },
        symbol: 'circle',
        symbolSize: 6,
        emphasis: {
          focus: 'series'
        }
      },
      {
        name: 'Gusting',
        type: 'scatter',
        data: gustingSpeedData,
        itemStyle: {
          color: '#e53e3e'
        },
        symbol: 'diamond',
        symbolSize: 8,
        emphasis: {
          focus: 'series'
        }
      }
    ]
  };

  windSpeedChart.setOption(option);
}

/**
 * Render wave height chart
 */
function renderWaveHeightChart(stationName, station) {
  const chartContainer = document.getElementById('lightstation-wave-chart');
  if (!chartContainer) return;

  // Initialize chart if needed
  if (!waveHeightChart) {
    waveHeightChart = echarts.init(chartContainer);
  }

  const timeseries = station.timeseries;
  const waveData = timeseries.sea_height_ft || [];

  if (waveData.length === 0) {
    // Show "no data" message
    waveHeightChart.clear();
    waveHeightChart.setOption({
      title: {
        text: `${station.name} - Sea State`,
        subtext: 'No wave height data available',
        left: 'center',
        textStyle: {
          fontSize: 18,
          fontWeight: 600,
          color: '#004b7c'
        },
        subtextStyle: {
          fontSize: 14,
          color: '#718096'
        }
      }
    });
    return;
  }

  // Prepare data for ECharts
  const heightData = waveData.map(p => [new Date(p.time).getTime(), p.value]);

  const option = {
    backgroundColor: '#ffffff',
    title: {
      text: `${station.name} - Sea State (Wave Height)`,
      left: 'center',
      textStyle: {
        fontSize: 18,
        fontWeight: 600,
        color: '#004b7c'
      }
    },
    tooltip: {
      ...getMobileOptimizedTooltipConfig(),
      formatter: (params) => {
        if (!params || params.length === 0) return "";
        const time = new Date(params[0].value[0]).toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver"
        }).replace(',', '');

        let tooltipText = `<strong>${time}</strong><br/>`;
        params.forEach(param => {
          if (param.value && param.value[1] != null) {
            tooltipText += `${param.marker} ${param.seriesName}: ${param.value[1]} ft<br/>`;
          }
        });
        return tooltipText;
      }
    },
    legend: {
      data: ['Wave Height'],
      top: 35,
      textStyle: {
        fontSize: 14
      }
    },
    grid: {
      left: '15%',
      right: '4%',
      bottom: '15%',
      top: '20%',
      containLabel: false
    },
    xAxis: {
      type: 'time',
      boundaryGap: false,
      splitLine: {
        show: true,
        lineStyle: {
          color: '#e0e7ee',
          type: 'dashed'
        }
      },
      axisLabel: {
        formatter: (value) => {
          const date = new Date(value);
          return date.toLocaleString("en-US", {
            month: "numeric",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
            timeZone: "America/Vancouver"
          }).replace(',', '\n');
        },
        fontSize: 12
      }
    },
    yAxis: {
      type: 'value',
      name: 'Wave Height (ft)',
      nameLocation: 'middle',
      nameGap: 35,
      nameTextStyle: {
        fontSize: 13,
        fontWeight: 600
      },
      min: 0
    },
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100
      },
      {
        type: 'slider',
        start: 0,
        end: 100,
        height: 30,
        bottom: '5%'
      }
    ],
    series: [
      {
        name: 'Wave Height',
        type: 'line',
        data: heightData,
        smooth: true,
        lineStyle: {
          width: 3,
          color: '#38a169'
        },
        itemStyle: {
          color: '#38a169'
        },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(56, 161, 105, 0.3)' },
              { offset: 1, color: 'rgba(56, 161, 105, 0.05)' }
            ]
          }
        },
        symbol: 'circle',
        symbolSize: 6,
        emphasis: {
          focus: 'series'
        }
      }
    ]
  };

  waveHeightChart.setOption(option);
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  loadLightstationTimeseries();

  // Handle window resize
  window.addEventListener('resize', () => {
    if (windSpeedChart) windSpeedChart.resize();
    if (waveHeightChart) waveHeightChart.resize();
  });
});
