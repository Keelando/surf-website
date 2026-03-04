/**
 * Marine Forecasts Page JavaScript
 * Loads and displays Environment Canada marine weather forecasts
 */

let forecastData = null;

/**
 * Load and display forecast data
 */
async function loadForecasts() {
  const container = document.getElementById('forecast-container');

  try {
    forecastData = await fetchWithTimeout('/data/marine_forecast.json');
    displayForecasts();
    updateTimestamp();

  } catch (error) {
    logger.error('Forecasts', 'Error loading forecasts', error);
    container.innerHTML = `
      <div class="error-state">
        <h2>Unable to Load Forecasts</h2>
        <p>Marine forecast data is temporarily unavailable. Please try again later.</p>
      </div>
    `;
  }
}

/**
 * Check if forecast data is stale
 * @returns {Object} Staleness info {isStale, ageHours, message}
 */
function checkFreshness() {
  if (!forecastData || !forecastData.generated_utc) {
    return { isStale: true, ageHours: null, message: 'No timestamp available' };
  }

  const generatedDate = new Date(forecastData.generated_utc);
  const now = new Date();
  const ageMs = now - generatedDate;
  const ageHours = ageMs / (1000 * 60 * 60);

  // Marine forecasts updated every ~6 hours (05h, 11h, 18h UTC)
  // Consider stale if > 8 hours old (missed update + grace period)
  const isStale = ageHours > 8;

  let message = '';
  if (ageHours > 12) {
    message = `⚠️ Data is ${Math.floor(ageHours)} hours old - forecast may be outdated`;
  } else if (ageHours > 8) {
    message = `⚠️ Data is ${Math.floor(ageHours)} hours old - awaiting update`;
  } else if (ageHours < 1) {
    message = `✅ Fresh data (updated ${Math.floor(ageHours * 60)} minutes ago)`;
  } else {
    message = `✅ Recent data (updated ${Math.floor(ageHours)} hours ago)`;
  }

  return { isStale, ageHours, message };
}

/**
 * Display forecast data in the UI
 */
function displayForecasts() {
  const container = document.getElementById('forecast-container');

  if (!forecastData || !forecastData.locations) {
    container.innerHTML = '<div class="error-state"><p>No forecast data available.</p></div>';
    return;
  }

  let html = '';

  // Check data freshness and show warning only if stale
  const freshness = checkFreshness();
  if (freshness.isStale) {
    html += `
      <div class="warning-card warning-gale" style="margin-bottom: 1.5rem;">
        <p style="margin: 0; font-weight: 500;">${freshness.message}</p>
      </div>
    `;
  }

  // Display each zone
  const zones = [
    { key: 'strait_georgia_north', priority: 1 },
    { key: 'strait_georgia_south', priority: 2 }
  ];

  zones.forEach(zone => {
    const zoneData = forecastData.locations[zone.key];
    if (zoneData) {
      html += renderZoneForecast(zone.key, zoneData);
    }
  });

  // Display extended forecast (shared across zones)
  if (forecastData.extended_forecast && forecastData.extended_forecast.length > 0) {
    html += renderExtendedForecast(forecastData.extended_forecast);
  }

  container.innerHTML = html;
}

/**
 * Render forecast for a single zone
 * @param {string} zoneKey - Zone identifier
 * @param {Object} zoneData - Zone forecast data
 * @returns {string} HTML string
 */
function renderZoneForecast(zoneKey, zoneData) {
  const zoneName = zoneData.zone_name || zoneKey.replace(/_/g, ' ');

  // Get source link for this zone
  const sourceLinks = {
    'strait_georgia_north': 'https://weather.gc.ca/marine/forecast_e.html?mapID=03&siteID=14301',
    'strait_georgia_south': 'https://weather.gc.ca/marine/forecast_e.html?mapID=03&siteID=14305'
  };
  const sourceLink = sourceLinks[zoneKey];

  let html = `
    <div class="forecast-zone" id="${zoneKey}">
      <h2>
        ${zoneName}
        ${sourceLink ? `<a href="${sourceLink}" target="_blank" rel="noopener" style="font-size: 0.75em; margin-left: 0.5rem; color: #4299e1; text-decoration: none;">📄 View Source</a>` : ''}
      </h2>
  `;

  // Warnings section
  html += '<div class="zone-warnings">';
  if (zoneData.warnings && zoneData.warnings.length > 0) {
    zoneData.warnings.forEach(warning => {
      html += renderWarningCard(warning);
    });
  } else {
    html += `
      <div class="no-warnings">
        ✅ No active warnings for this zone
      </div>
    `;
  }
  html += '</div>';

  // Current forecast
  if (zoneData.forecast) {
    html += `
      <div class="forecast-section">
        <h3>🌊 Current Forecast</h3>
        <div class="forecast-content">
    `;

    if (zoneData.forecast.period) {
      html += `<div class="forecast-period"><strong>Period:</strong> ${zoneData.forecast.period}</div>`;
    }

    if (zoneData.forecast.wind) {
      html += `<div class="forecast-period"><strong>Wind:</strong> ${zoneData.forecast.wind}</div>`;
    }

    if (zoneData.forecast.weather) {
      html += `<div class="forecast-period"><strong>Weather:</strong> ${zoneData.forecast.weather}</div>`;
    }

    html += `
        </div>
      </div>
    `;
  }

  // Wave forecast (if present)
  if (forecastData.wave_forecast) {
    html += `
      <div class="forecast-section">
        <h3>🌊 Wave Forecast</h3>
        <div class="forecast-content">
    `;

    if (forecastData.wave_forecast.period) {
      html += `<div class="forecast-period"><strong>Period:</strong> ${forecastData.wave_forecast.period}</div>`;
    }

    if (forecastData.wave_forecast.forecast) {
      html += `<div class="forecast-period">${forecastData.wave_forecast.forecast}</div>`;
    }

    html += `
        </div>
      </div>
    `;
  }

  // Metadata
  if (zoneData.issued_utc) {
    const issuedDate = new Date(zoneData.issued_utc);
    html += `
      <div class="forecast-metadata">
        <strong>Issued:</strong> ${formatTimestamp(issuedDate)}
      </div>
    `;
  }

  html += '</div>'; // .forecast-zone

  return html;
}

/**
 * Render a warning card
 * @param {Object} warning - Warning data
 * @returns {string} HTML string
 */
function renderWarningCard(warning) {
  const severityClass = getWarningSeverityClass(warning.type);
  const icon = getWarningIcon(warning.type);

  let issuedText = '';
  if (warning.issued_utc) {
    const issuedDate = new Date(warning.issued_utc);
    issuedText = ` <small>(Issued ${formatTimestamp(issuedDate)})</small>`;
  }

  return `
    <div class="warning-card ${severityClass}">
      <h3>${icon} ${warning.type}</h3>
      <p><strong>Status:</strong> ${warning.status}${issuedText}</p>
    </div>
  `;
}

/**
 * Render extended forecast section
 * @param {Array} extendedForecast - Extended forecast periods
 * @returns {string} HTML string
 */
function renderExtendedForecast(extendedForecast) {
  let html = `
    <div class="forecast-zone">
      <h2>📆 Extended Forecast</h2>
      <div class="extended-forecast">
  `;

  extendedForecast.forEach(period => {
    html += `
      <div class="extended-day">
        <h4>${period.period}</h4>
        <p>${period.forecast}</p>
      </div>
    `;
  });

  html += `
      </div>
  `;

  // Add issued timestamp if available
  if (forecastData && forecastData.generated_utc) {
    const issuedDate = new Date(forecastData.generated_utc);
    html += `
      <div class="forecast-metadata">
        <strong>Issued:</strong> ${formatTimestamp(issuedDate)}
      </div>
    `;
  }

  html += `
    </div>
  `;

  return html;
}

/**
 * Get CSS class for warning severity
 * @param {string} type - Warning type
 * @returns {string} CSS class name
 */
function getWarningSeverityClass(type) {
  const typeLower = type.toLowerCase();

  if (typeLower.includes('storm')) return 'warning-storm';
  if (typeLower.includes('gale')) return 'warning-gale';
  if (typeLower.includes('strong wind')) return 'warning-strong-wind';

  return '';
}

/**
 * Get icon for warning type
 * @param {string} type - Warning type
 * @returns {string} Icon emoji
 */
function getWarningIcon(type) {
  const typeLower = type.toLowerCase();

  if (typeLower.includes('storm')) return '⚠️';
  if (typeLower.includes('gale')) return '💨';
  if (typeLower.includes('strong wind')) return '🌬️';

  return '⚠️';
}

/**
 * Format timestamp for display
 * @param {Date} date - Date object
 * @returns {string} Formatted timestamp
 */
function formatTimestamp(date) {
  const options = {
    weekday: 'short',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short'
  };

  return date.toLocaleString('en-US', options);
}

/**
 * Update the page timestamp
 */
function updateTimestamp() {
  const timestampEl = document.getElementById('timestamp');
  if (timestampEl && forecastData && forecastData.generated_utc) {
    const generatedDate = new Date(forecastData.generated_utc);
    timestampEl.textContent = `Last updated: ${formatTimestamp(generatedDate)}`;
  } else if (timestampEl) {
    const now = new Date();
    timestampEl.textContent = `Page loaded: ${formatTimestamp(now)}`;
  }
}

/**
 * Auto-refresh forecast data every 5 minutes
 */
function startAutoRefresh() {
  setInterval(() => {
    logger.info('Forecasts', 'Auto-refreshing forecast data...');
    loadForecasts();
  }, 5 * 60 * 1000); // 5 minutes
}

/**
 * Scroll to zone section if hash in URL
 */
function scrollToZoneIfNeeded() {
  const hash = window.location.hash;
  if (hash) {
    // Wait a bit for content to load
    setTimeout(() => {
      const element = document.querySelector(hash);
      if (element) {
        element.scrollIntoView({ behavior: 'smooth', block: 'start' });
        // Add a subtle highlight effect
        element.style.transition = 'box-shadow 0.3s ease';
        element.style.boxShadow = '0 0 0 3px rgba(66, 153, 225, 0.5)';
        setTimeout(() => {
          element.style.boxShadow = '';
        }, 2000);
      }
    }, 300);
  }
}

// Initialize on page load - wait for HTMX to load footer with timestamp
document.addEventListener('htmx:load', () => {
  loadForecasts().then(() => {
    scrollToZoneIfNeeded();
  });
  startAutoRefresh();
}, { once: true });
