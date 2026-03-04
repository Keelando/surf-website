/**
 * Utility Functions Module
 * Common helper functions for time formatting, age calculation, etc.
 */

/**
 * Format a date for display in Pacific timezone
 *
 * @param {Date} date - Date to format
 * @returns {string} Formatted date string (e.g., "Jan 6, 14:30")
 */
export function formatTime(date) {
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'America/Vancouver'
  });
}

/**
 * Get a human-readable string describing how long ago a date was
 *
 * @param {Date} date - Date to calculate age from
 * @returns {string} Age string (e.g., "5 minutes ago", "2 hours ago")
 */
export function getAgeString(date) {
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins < 1) return 'just now';
  if (diffMins === 1) return '1 minute ago';
  if (diffMins < 60) return `${diffMins} minutes ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours === 1) return '1 hour ago';
  if (diffHours < 24) return `${diffHours} hours ago`;

  const diffDays = Math.floor(diffHours / 24);
  if (diffDays === 1) return '1 day ago';
  return `${diffDays} days ago`;
}

/**
 * Update the page timestamp display
 *
 * @returns {void}
 */
export function updateTimestamp() {
  const timestampEl = document.getElementById('timestamp');
  if (!timestampEl) return;

  const now = new Date();
  timestampEl.textContent = `Last updated: ${now.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    timeZone: 'America/Vancouver'
  })}`;
}

/**
 * Show error state in the UI
 *
 * @returns {void}
 */
export function showError() {
  document.getElementById('tide-loading').style.display = 'none';
  document.getElementById('tide-current-section').style.display = 'none';
  document.getElementById('tide-error').style.display = 'block';
}

/**
 * Navigate to map view for selected tide station
 * Maps geodetic stations to their corresponding wave station markers
 *
 * @returns {void}
 */
export function showSelectedTideOnMap() {
  const select = document.getElementById('tide-station-select');
  if (!select || !select.value) return;

  const stationKey = select.value;

  // Map geodetic tide stations to their wave station IDs
  const geodeticToWaveMap = {
    'crescent_beach_ocean': 'CRPILE',
    'crescent_channel_ocean': 'CRCHAN'
  };

  // If it's a geodetic station, show the wave station marker instead
  if (geodeticToWaveMap[stationKey]) {
    window.location.href = `/#${geodeticToWaveMap[stationKey]}`;
  } else {
    // Regular tide station
    window.location.href = `/#tide-${stationKey}`;
  }
}
