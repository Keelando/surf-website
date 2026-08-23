/**
 * Warning Banner Module (ES module)
 * Displays marine weather warnings from Environment Canada
 *
 * DOM half only — zone selection, active-warning collection and severity
 * classification live in shared/warning-zones.js so they can be unit-tested
 * without a browser. See tests/js/warning-zones.test.mjs.
 *
 * Key Improvements:
 * - Variable dismiss durations based on warning severity
 * - Storm warnings have 24h auto-restore (safety critical)
 * - Better visual hierarchy by severity
 * - Dismissal feedback messages
 * - Accessibility improvements
 *
 * Usage:
 *   1. Include this script in your HTML
 *   2. Add a <div id="warning-banner-container"></div> element where you want warnings to appear
 *   3. Call displayWarningBanners() after page load
 */

import {
  collectActiveWarnings,
  getWarningIcon,
  getWarningId,
  getWarningSeverityClass,
} from "./shared/warning-zones.js";

function setSafeHTML(element, html) {
  if (!element) return;
  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

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

// Configuration
const STORAGE_KEY = "dismissed_marine_warnings";

// Dismiss duration - all warnings dismissed for 24 hours
const DISMISS_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

/**
 * Check if warning has been dismissed
 * @param {string} warningId - Warning ID
 * @returns {boolean} True if dismissed and not expired
 */
function isWarningDismissed(warningId) {
  try {
    const dismissed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
    const dismissedTime = dismissed[warningId];

    if (!dismissedTime) return false;

    const now = Date.now();
    const elapsed = now - dismissedTime;

    // Check if dismissal has expired (24 hours)
    if (elapsed > DISMISS_DURATION_MS) {
      delete dismissed[warningId];
      localStorage.setItem(STORAGE_KEY, JSON.stringify(dismissed));
      return false;
    }

    return true;
  } catch (error) {
    logger.error("WarningBanner", "Error checking dismissed warnings", error);
    return false;
  }
}

/**
 * Show dismissal feedback message
 * @param {HTMLElement} banner - Warning banner element
 * @param {string} message - Feedback message
 */
function showDismissalFeedback(banner, message) {
  // Create feedback element
  const feedback = document.createElement("div");
  feedback.className = "warning-dismissal-feedback";
  feedback.textContent = message;
  feedback.style.cssText = `
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    background: rgba(0, 0, 0, 0.85);
    color: white;
    padding: 1rem 2rem;
    border-radius: 8px;
    z-index: 10000;
    font-size: 0.95rem;
    text-align: center;
    max-width: 90%;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
  `;

  document.body.appendChild(feedback);

  // Fade out and remove after 3 seconds
  setTimeout(() => {
    feedback.style.transition = "opacity 0.5s ease";
    feedback.style.opacity = "0";
    setTimeout(() => feedback.remove(), 500);
  }, 3000);
}

/**
 * Fetch and display warning banners
 * @param {string} containerId - ID of container element (default: 'warning-banner-container')
 */
async function displayWarningBanners(containerId = "warning-banner-container") {
  const container = document.getElementById(containerId);

  if (!container) {
    logger.warn("WarningBanner", `Warning banner container '${containerId}' not found`);
    return;
  }

  try {
    const data = await fetchWithTimeout(`/data/marine_forecast.json?t=${Date.now()}`);

    const warnings = collectActiveWarnings(data);

    // Filter out dismissed warnings
    const activeWarnings = warnings.filter((warning) => {
      const warningId = getWarningId(warning);
      const isDismissed = isWarningDismissed(warningId);
      return !isDismissed;
    });

    if (activeWarnings.length === 0) {
      // No active warnings - hide container
      container.style.display = "none";
      return;
    }

    // Combine all warnings into a single banner
    const combinedBanner = createCombinedWarningBanner(activeWarnings);
    setSafeHTML(container, combinedBanner);
    container.style.display = "block";

    // Attach dismiss handler - dismisses all warnings at once
    const dismissBtn = container.querySelector(".warning-dismiss-btn");
    if (dismissBtn) {
      dismissBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();

        // Dismiss all active warnings
        activeWarnings.forEach((warning) => {
          const warningId = getWarningId(warning);
          const dismissed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
          dismissed[warningId] = Date.now();
          localStorage.setItem(STORAGE_KEY, JSON.stringify(dismissed));
        });

        // Show feedback and remove banner
        showDismissalFeedback(
          container.querySelector(".warning-banner"),
          "All warnings hidden for 24 hours",
        );
        const banner = container.querySelector(".warning-banner");
        if (banner) {
          banner.style.opacity = "0";
          banner.style.transition = "opacity 0.3s ease";
          setTimeout(() => {
            container.style.display = "none";
            banner.remove();
          }, 300);
        }
      });
    }
  } catch (error) {
    console.error("[WarningBanner] Error loading warnings:", error);
    logger.error("WarningBanner", "Error loading marine forecast warnings", error);
    container.style.display = "none";
  }
}

/**
 * Create HTML for a combined warning banner showing all active warnings
 * @param {Array} warnings - Array of warning objects
 * @returns {string} HTML string
 */
function createCombinedWarningBanner(warnings) {
  // Determine the highest severity for styling
  const highestSeverity = warnings[0]; // Already sorted by severity
  const severityClass = getWarningSeverityClass(highestSeverity.type);
  const icon = getWarningIcon(highestSeverity.type);

  // Every route into the forecasts page carries the zone in the hash. The page
  // resolves hash before its stored last-choice, so clicking a warning always
  // lands on the zone the warning is about — previously it honoured whatever
  // zone you happened to have picked last and the banner appeared to lie.
  const zoneHref = (warning) => `/forecasts.html#${encodeURIComponent(warning.zone_key)}`;

  // Build warning text
  let warningText = "";
  if (warnings.length === 1) {
    warningText = `<strong>${warnings[0].type.toUpperCase()}</strong> in effect for ${warnings[0].zone_name}`;
  } else {
    // Multiple warnings — each is its own link, so a two-zone banner can send
    // you to either one rather than only to the most severe.
    warningText = warnings
      .map(
        (w) =>
          `<a class="warning-zone-link" href="${zoneHref(w)}"><strong>${w.type.toUpperCase()}</strong> for ${w.zone_name}</a>`,
      )
      .join(" • ");
  }

  return `
    <div class="warning-banner ${severityClass}" role="alert" aria-live="assertive">
      <div class="warning-banner-content">
        <span class="warning-icon" aria-hidden="true">${icon}</span>
        <div class="warning-text">
          ${warningText}
        </div>
        <a href="${zoneHref(highestSeverity)}" class="warning-details-link">View Forecasts →</a>
        <button class="warning-dismiss-btn" aria-label="Dismiss for 24h" title="Dismiss for 24h">×</button>
      </div>
    </div>
  `;
}

// Auto-initialize if container exists on page load
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    if (document.getElementById("warning-banner-container")) {
      displayWarningBanners();
    }
  });
} else {
  // DOM already loaded
  if (document.getElementById("warning-banner-container")) {
    displayWarningBanners();
  }
}

// Track if warnings have been loaded to prevent duplicate calls
let warningsLoaded = false;
let loadingInProgress = false;

// Also listen for htmx afterSwap events (for pages using htmx to load the container)
document.addEventListener("htmx:afterSwap", (event) => {
  // Skip if already loaded or currently loading (no logging to reduce console noise)
  if (warningsLoaded || loadingInProgress) {
    return;
  }

  // With outerHTML swap, the target IS the newly swapped element
  // Check if it's the warning banner container or contains it
  const isWarningBanner =
    event.detail.target.id === "warning-banner-container" ||
    event.detail.target.querySelector?.("#warning-banner-container") !== null;

  if (isWarningBanner) {
    loadingInProgress = true;
    displayWarningBanners().finally(() => {
      loadingInProgress = false;
      warningsLoaded = true;
    });
  } else {
    // After any swap, check if container now exists (might have been swapped in)
    const container = document.getElementById("warning-banner-container");
    if (container && !warningsLoaded && !loadingInProgress) {
      loadingInProgress = true;
      displayWarningBanners().finally(() => {
        loadingInProgress = false;
        warningsLoaded = true;
      });
    }
  }
});
