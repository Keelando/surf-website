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

/** Days the page offers when the export doesn't say (pre-2026-10-23 files). */
const FALLBACK_DAY_COUNT = 3;

/**
 * Highest day offset the tides page can show, from the timeseries export's
 * `_meta.days_included` — the backend's TIDE_FORECAST_DAYS, so the page never
 * offers a day the data does not cover.
 *
 * @param {Object|null} timeseriesData - tide-timeseries.json
 * @returns {number} e.g. 6 for a week (today + 6)
 */
export function maxDayOffset(timeseriesData) {
  const days = timeseriesData?._meta?.days_included;
  return (Number.isInteger(days) && days > 0 ? days : FALLBACK_DAY_COUNT) - 1;
}

/**
 * The Pacific calendar date `dayOffset` days from today, as a local-midnight
 * Date (only its year/month/day are meaningful).
 *
 * @param {number} dayOffset
 * @param {Date} [now] - Injectable for tests
 * @returns {Date}
 */
export function pacificDate(dayOffset, now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Vancouver",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const part = (type) => parseInt(parts.find((p) => p.type === type).value, 10);
  return new Date(part("year"), part("month") - 1, part("day") + dayOffset);
}

/**
 * "Today (Fri Oct 23)" / "Tomorrow (Sat Oct 24)" / "Sun Oct 25".
 *
 * @param {number} dayOffset
 * @param {Date} [now] - Injectable for tests
 * @returns {string}
 */
export function dayLabel(dayOffset, now = new Date()) {
  const dateStr = pacificDate(dayOffset, now)
    .toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" })
    .replace(",", "");
  if (dayOffset === 0) return `Today (${dateStr})`;
  if (dayOffset === 1) return `Tomorrow (${dateStr})`;
  return dateStr;
}
