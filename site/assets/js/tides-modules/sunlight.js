/**
 * Sunlight Times Module
 * Handles loading and displaying sunrise/sunset data
 */

function setSafeHTML(element, html) {
  if (!element) return;
  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

/**
 * Build the YYYY-MM-DD key used by sunlight_times.json from a Date.
 *
 * @param {Date} date
 * @returns {string} Date in YYYY-MM-DD format
 */
function toDateKey(date) {
  const year = date.getFullYear();
  const month = date.getMonth() + 1;
  const day = date.getDate();
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Daylight length, sunrise to sunset, in seconds.
 *
 * @param {Object|null} sunlight - A day entry from sunlight_times.json
 * @returns {number|null} Seconds of daylight, or null if unusable
 */
function daylightSeconds(sunlight) {
  if (!sunlight || sunlight.error || !sunlight.sunrise || !sunlight.sunset) return null;

  const sunrise = new Date(sunlight.sunrise);
  const sunset = new Date(sunlight.sunset);
  if (isNaN(sunrise) || isNaN(sunset)) return null;

  return (sunset - sunrise) / 1000;
}

/**
 * Describe the change in day length from one day to the next.
 *
 * Seconds are kept alongside minutes deliberately: within a week or so of a
 * solstice the day-over-day change drops below a minute, and a minutes-only
 * display would read a flat "0m" for a fortnight — which is exactly the
 * stretch of the year this is most interesting for.
 *
 * @param {number|null} todaySeconds
 * @param {number|null} tomorrowSeconds
 * @returns {string} Phrase such as "gaining 2m 11s each day", or "" if unknown
 */
function describeDaylightChange(todaySeconds, tomorrowSeconds) {
  if (todaySeconds === null || tomorrowSeconds === null) return "";

  const delta = Math.round(tomorrowSeconds - todaySeconds);
  const magnitude = Math.abs(delta);
  const minutes = Math.floor(magnitude / 60);
  const seconds = magnitude % 60;

  // Around the solstice the change passes through zero; say so rather than
  // rendering a signed "0s" that reads like a rounding artifact.
  if (magnitude === 0) return "day length holding steady";

  const amount = minutes === 0 ? `${seconds}s` : `${minutes}m ${seconds}s`;
  const direction = delta > 0 ? "gaining" : "losing";

  return `${direction} ${amount} each day`;
}

/**
 * Sunlight data store
 */
class SunlightDataStore {
  constructor() {
    this.sunlightTimesData = null;
  }

  /**
   * Load sunlight times data from JSON
   *
   * @returns {Promise<void>}
   */
  async load() {
    try {
      const response = await fetch(`/data/sunlight_times.json?t=${Date.now()}`);
      this.sunlightTimesData = await response.json();
    } catch (error) {
      console.warn("Could not load sunlight times:", error);
      this.sunlightTimesData = null;
    }
  }

  /**
   * Get sunlight times for a specific station and date
   *
   * @param {string} stationKey - Station identifier
   * @param {string} dateStr - Date in YYYY-MM-DD format
   * @returns {Object|null} Sunlight times or null if not found
   */
  getForDate(stationKey, dateStr) {
    if (
      !this.sunlightTimesData ||
      !this.sunlightTimesData.stations ||
      !this.sunlightTimesData.stations[stationKey]
    ) {
      return null;
    }

    const station = this.sunlightTimesData.stations[stationKey];
    return station.days[dateStr] || null;
  }
}

/**
 * Display sunlight times for a station
 *
 * @param {string} stationKey - Station identifier
 * @param {number} dayOffset - Day offset (0=today, 1=tomorrow, 2=day after)
 * @param {SunlightDataStore} sunlightStore - Sunlight data store
 * @param {Object} tideDataStore - Tide data store (for updating day offset)
 * @param {Function} updateChartCallback - Callback to update chart when day changes
 * @returns {void}
 */
export function displaySunlightTimes(
  stationKey,
  dayOffset,
  sunlightStore,
  tideDataStore,
  updateChartCallback,
) {
  const container = document.getElementById("sunlight-widget");
  if (!container) return;

  // Get target date in Pacific timezone based on offset
  const pacific = "America/Vancouver";
  const now = new Date();
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: pacific,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = formatter.formatToParts(now);
  const year = parseInt(parts.find((p) => p.type === "year").value);
  const month = parseInt(parts.find((p) => p.type === "month").value);
  const day = parseInt(parts.find((p) => p.type === "day").value);

  // Create target date and apply offset
  const targetDate = new Date(year, month - 1, day);
  targetDate.setDate(targetDate.getDate() + dayOffset);

  const targetDateStr = toDateKey(targetDate);

  // Get sunlight times for the target date
  const sunlight = sunlightStore.getForDate(stationKey, targetDateStr);

  // And for the day after, to show which way day length is trending. The
  // export runs 4 days ahead while navigation stops at offset 2, so the
  // following day is normally present — but a stale export can leave it
  // missing, in which case the change line is simply omitted.
  const nextDate = new Date(targetDate);
  nextDate.setDate(nextDate.getDate() + 1);
  const nextSunlight = sunlightStore.getForDate(stationKey, toDateKey(nextDate));

  // Check if we have valid sunlight data for this station (not missing or error)
  if (!sunlight || sunlight.error || !sunlight.first_light) {
    container.style.display = "none";
    return;
  }

  // Parse times
  const firstLight = new Date(sunlight.first_light);
  const sunrise = new Date(sunlight.sunrise);
  const sunset = new Date(sunlight.sunset);
  const lastLight = new Date(sunlight.last_light);

  // Validate parsed dates
  if (isNaN(firstLight) || isNaN(sunrise) || isNaN(sunset) || isNaN(lastLight)) {
    container.style.display = "none";
    return;
  }

  // Format to local time (24-hour)
  const timeOptions = {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Vancouver",
  };
  const firstLightStr = firstLight.toLocaleTimeString("en-US", timeOptions);
  const sunriseStr = sunrise.toLocaleTimeString("en-US", timeOptions);
  const sunsetStr = sunset.toLocaleTimeString("en-US", timeOptions);
  const lastLightStr = lastLight.toLocaleTimeString("en-US", timeOptions);

  // Calculate daylight duration
  const daylightDurationMs = sunset - sunrise;
  const hours = Math.floor(daylightDurationMs / (1000 * 60 * 60));
  const minutes = Math.floor((daylightDurationMs % (1000 * 60 * 60)) / (1000 * 60));
  const daylightDuration = `${hours}h ${minutes}m`;

  // Day-over-day trend in day length
  const changeText = describeDaylightChange(
    daylightDurationMs / 1000,
    daylightSeconds(nextSunlight),
  );

  // Format date label for heading
  const dateStr = targetDate.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
  let dayLabel = "";
  if (dayOffset === 0) {
    dayLabel = `Today (${dateStr})`;
  } else if (dayOffset === 1) {
    dayLabel = `Tomorrow (${dateStr})`;
  } else {
    dayLabel = dateStr;
  }

  // Build modern card-based layout with CSS classes for responsive sizing
  setSafeHTML(
    container,
    `
    <div class="tide-data-group">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
        <h3 style="margin: 0;">Sunlight Times for ${dayLabel}</h3>
        <div class="chart-nav-buttons" style="display: flex; gap: 0.5rem;">
          <button id="sunlight-prev-day-btn" title="Previous day" style="padding: 0.5rem 1rem; background: var(--color-primary); color: var(--color-on-primary); border: none; border-radius: 4px; cursor: pointer; font-size: 1rem;">◀</button>
          <button id="sunlight-next-day-btn" title="Next day" style="padding: 0.5rem 1rem; background: var(--color-primary); color: var(--color-on-primary); border: none; border-radius: 4px; cursor: pointer; font-size: 1rem;">▶</button>
        </div>
      </div>
      <div class="sunlight-cards-grid">

        <!-- First Light Card -->
        <div class="sunlight-card sunlight-card-first-light">
          <div class="sunlight-card-icon">🌅</div>
          <div class="sunlight-card-label">First Light</div>
          <div class="sunlight-card-time">${firstLightStr}</div>
          <div class="sunlight-card-sublabel">Civil Dawn</div>
        </div>

        <!-- Sunrise Card -->
        <div class="sunlight-card sunlight-card-sunrise">
          <div class="sunlight-card-icon">🌄</div>
          <div class="sunlight-card-label">Sunrise</div>
          <div class="sunlight-card-time">${sunriseStr}</div>
          <div class="sunlight-card-sublabel">Sun Above Horizon</div>
        </div>

        <!-- Sunset Card -->
        <div class="sunlight-card sunlight-card-sunset">
          <div class="sunlight-card-icon">🌇</div>
          <div class="sunlight-card-label">Sunset</div>
          <div class="sunlight-card-time">${sunsetStr}</div>
          <div class="sunlight-card-sublabel">Sun Below Horizon</div>
        </div>

        <!-- Last Light Card -->
        <div class="sunlight-card sunlight-card-last-light">
          <div class="sunlight-card-icon">🌆</div>
          <div class="sunlight-card-label">Last Light</div>
          <div class="sunlight-card-time">${lastLightStr}</div>
          <div class="sunlight-card-sublabel">Civil Dusk</div>
        </div>

      </div>

      <!-- Daylight Duration Summary -->
      <div class="sunlight-duration">
        <div class="sunlight-duration-icon">☀️</div>
        <div>
          <div class="sunlight-duration-label">Daylight Duration</div>
          <div class="sunlight-duration-value">${daylightDuration}</div>
          ${changeText ? `<div class="sunlight-duration-change">${changeText}</div>` : ""}
        </div>
      </div>
    </div>
  `,
  );

  container.style.display = "block";

  // Add event listeners for sunlight day navigation buttons
  const sunlightPrevBtn = document.getElementById("sunlight-prev-day-btn");
  const sunlightNextBtn = document.getElementById("sunlight-next-day-btn");

  if (sunlightPrevBtn && sunlightNextBtn) {
    // Update button states
    sunlightPrevBtn.disabled = dayOffset === 0;
    sunlightNextBtn.disabled = dayOffset === 2; // We have 3 days of data (0-2)

    sunlightPrevBtn.style.opacity = dayOffset === 0 ? "0.3" : "1";
    sunlightNextBtn.style.opacity = dayOffset === 2 ? "0.3" : "1";
    sunlightPrevBtn.style.cursor = dayOffset === 0 ? "not-allowed" : "pointer";
    sunlightNextBtn.style.cursor = dayOffset === 2 ? "not-allowed" : "pointer";

    // Add click handlers
    sunlightPrevBtn.addEventListener("click", () => {
      const currentOffset = tideDataStore.getDayOffset();
      if (currentOffset > 0) {
        tideDataStore.setDayOffset(currentOffset - 1);
        updateChartCallback();
      }
    });

    sunlightNextBtn.addEventListener("click", () => {
      const currentOffset = tideDataStore.getDayOffset();
      if (currentOffset < 2) {
        tideDataStore.setDayOffset(currentOffset + 1);
        updateChartCallback();
      }
    });
  }
}

export { SunlightDataStore, daylightSeconds, describeDaylightChange, toDateKey };
