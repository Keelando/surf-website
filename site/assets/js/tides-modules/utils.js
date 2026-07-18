/**
 * Utility Functions Module
 * Page-specific helpers (error state, map navigation). Time formatting
 * lives in ../shared/format-time.js.
 */

/**
 * Show error state in the UI
 *
 * @returns {void}
 */
export function showError() {
  document.getElementById("tide-loading").style.display = "none";
  document.getElementById("tide-current-section").style.display = "none";
  document.getElementById("tide-error").style.display = "block";
}

/**
 * Navigate to map view for selected tide station
 * Maps geodetic stations to their corresponding wave station markers
 *
 * @returns {void}
 */
export function showSelectedTideOnMap() {
  const select = document.getElementById("tide-station-select");
  if (!select || !select.value) return;

  const stationKey = select.value;

  // Map geodetic tide stations to their wave station IDs
  const geodeticToWaveMap = {
    crescent_beach_ocean: "CRPILE",
    crescent_channel_ocean: "CRCHAN",
  };

  // If it's a geodetic station, show the wave station marker instead
  if (geodeticToWaveMap[stationKey]) {
    window.location.href = `/#${geodeticToWaveMap[stationKey]}`;
  } else {
    // Regular tide station
    window.location.href = `/#tide-${stationKey}`;
  }
}
