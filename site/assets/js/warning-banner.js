/**
 * Warning Banner Module (ES module)
 * Displays marine weather warnings from Environment Canada
 *
 * DOM half only — zone selection, active-warning collection and severity
 * classification live in shared/warning-zones.js so they can be unit-tested
 * without a browser. See tests/js/warning-zones.test.mjs.
 *
 * Dismissal: the × hides every warning currently in effect for 24 hours, at
 * every severity — there is no per-severity duration, despite what this
 * comment claimed for a long time. What keeps that safe is the dismissal id
 * (see getWarningId): it includes the issue time, so a re-issued or newly
 * issued warning is a different id and breaks through a live dismissal.
 * EC re-issues these roughly every six hours, which is the real backstop.
 *
 * Usage:
 *   1. Include this script in your HTML
 *   2. Add a <div id="warning-banner-container"></div> element where you want warnings to appear
 *   3. Call displayWarningBanners() after page load
 */

import { setSafeHTML } from "./shared/safe-html.js";
import { listZones } from "./shared/marine-zones.js";
import { readBannerZones } from "./shared/warning-preferences.js";
import {
  collectActiveWarnings,
  getBannerZones,
  summarizeBannerWarnings,
  getWarningIcon,
  getWarningId,
  getWarningSeverityClass,
} from "./shared/warning-zones.js";

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

/**
 * The viewport where warning-banner-v4.css collapses the banner into a single
 * tap target: `.warning-details-link` is stretched over the whole banner and
 * the per-zone links have `pointer-events: none`. Keep this in step with the
 * 768px breakpoint there — if the two disagree, the one link a phone can tap
 * goes somewhere the layout did not intend.
 */
const COMPACT_QUERY = "(max-width: 768px)";

/** Every warning in effect, on the forecasts page. See #warning-jump there. */
const ALL_WARNINGS_HREF = "/forecasts.html#warning-jump";

/**
 * Whether the banner is currently drawn as one tap target.
 * @returns {boolean}
 */
function isCompactLayout() {
  return typeof window.matchMedia === "function" && window.matchMedia(COMPACT_QUERY).matches;
}

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

    // The reader's zone choice, resolved against the zones this document
    // actually carries. Storage is read here and nowhere else — the zone
    // filtering itself stays pure in shared/warning-zones.js. A reader who has
    // never opened the picker on the forecasts page has no stored key and gets
    // the default pair; the storm floor applies either way.
    const availableZoneKeys = listZones(data).map((zone) => zone.zoneKey);
    const bannerZones = getBannerZones(readBannerZones(localStorage), availableZoneKeys);

    const warnings = collectActiveWarnings(data, bannerZones);

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
 * How many warnings the banner names individually before it starts counting.
 *
 * A single Pacific storm can warn every zone we carry at once, and the storm
 * floor means all of them reach the banner whether or not the reader follows
 * those waters. Nine entries is not a banner, it is a list — and on a phone
 * `.warning-text` is a single truncated line, so entries past the first few are
 * invisible anyway while still pushing the ones that matter off the end.
 * Three named zones plus an honest count beats nine silently clipped.
 *
 * The warnings are already sorted severity-first then the reader's own zones
 * first, so the three shown are the three most worth showing.
 */
const MAX_NAMED_ZONES = 3;

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

  // On a phone the banner is one tap target covering everything, so its single
  // destination cannot be one zone's forecast: whoever tapped it was told about
  // several waters and picked none of them yet. Send them to the full list of
  // warnings on the forecasts page and let them choose there. On a wider screen
  // the per-zone links are live and this button is one control among them, so
  // it still opens the most severe zone directly.
  const compact = isCompactLayout();
  const detailsHref = compact ? ALL_WARNINGS_HREF : zoneHref(highestSeverity);
  const detailsText = compact ? "See all warnings \u2192" : "View Forecasts \u2192";
  const zoneLink = (warning, text) =>
    `<a class="warning-zone-link" href="${zoneHref(warning)}">${text}</a>`;

  const { shown, sameType, moreLabel } = summarizeBannerWarnings(warnings, MAX_NAMED_ZONES);
  const moreText = moreLabel
    ? ` <a class="warning-more-link" href="/forecasts.html">+${moreLabel}</a>`
    : "";

  // Build warning text
  let warningText = "";
  if (warnings.length === 1) {
    warningText = `<strong>${warnings[0].type.toUpperCase()}</strong> in effect for ${warnings[0].zone_name}`;
  } else if (sameType) {
    // The many-storms case: one system warning several zones produces the same
    // type over and over, and repeating "STORM WARNING" three times buries the
    // only thing that varies. State the type once, then list the waters.
    const zoneList = shown.map((w) => zoneLink(w, w.zone_name)).join(", ");
    warningText = `<strong>${shown[0].type.toUpperCase()}</strong> in effect for ${zoneList}${moreText}`;
  } else {
    // Mixed severities — each entry carries its own type, and each is its own
    // link, so a two-zone banner can send you to either one rather than only to
    // the most severe.
    warningText =
      shown
        .map((w) => zoneLink(w, `<strong>${w.type.toUpperCase()}</strong> for ${w.zone_name}`))
        .join(" • ") + moreText;
  }

  return `
    <div class="warning-banner ${severityClass}" role="alert" aria-live="assertive">
      <div class="warning-banner-content">
        <span class="warning-icon" aria-hidden="true">${icon}</span>
        <div class="warning-text">
          ${warningText}
        </div>
        <a href="${detailsHref}" class="warning-details-link">${detailsText}</a>
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

/**
 * Re-render when the reader changes their zone selection.
 *
 * The picker lives on the forecasts page, which carries this banner too, so a
 * zone toggled there must take effect immediately rather than at the next page
 * load — otherwise the control appears not to work.
 */
/**
 * Re-render when the layout crosses the compact breakpoint.
 *
 * The banner picks its destination once, at render time, so a rotation from
 * portrait to landscape would otherwise leave a phone-sized banner pointing at
 * one zone (or a desktop one pointing at the list) until the next page load.
 */
if (typeof window.matchMedia === "function") {
  const compactQuery = window.matchMedia(COMPACT_QUERY);
  const onChange = () => {
    if (document.getElementById("warning-banner-container")) {
      displayWarningBanners();
    }
  };
  // addListener is the Safari < 14 spelling; still worth the two lines.
  if (typeof compactQuery.addEventListener === "function") {
    compactQuery.addEventListener("change", onChange);
  } else if (typeof compactQuery.addListener === "function") {
    compactQuery.addListener(onChange);
  }
}

window.addEventListener("warning-zones:changed", () => {
  if (document.getElementById("warning-banner-container")) {
    displayWarningBanners();
  }
});

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
