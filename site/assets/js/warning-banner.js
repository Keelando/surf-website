/**
 * Warning Banner Module - IMPROVED VERSION
 * Displays marine weather warnings from Environment Canada
 *
 * Key Improvements:
 * - Variable dismiss durations based on warning severity
 * - Storm warnings have 12h auto-restore (safety critical)
 * - Better visual hierarchy by severity
 * - Dismissal feedback messages
 * - Accessibility improvements
 *
 * Usage:
 *   1. Include this script in your HTML
 *   2. Add a <div id="warning-banner-container"></div> element where you want warnings to appear
 *   3. Call displayWarningBanners() after page load
 */

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
const STORAGE_KEY = 'dismissed_marine_warnings';

// Dismiss duration - all warnings dismissed for 12 hours
const DISMISS_DURATION_MS = 12 * 60 * 60 * 1000; // 12 hours

// Maximum age before dismissal is always cleared (safety backstop)
const MAX_DISMISS_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

/**
 * Generate unique ID for a warning
 * @param {Object} warning - Warning object
 * @returns {string} Unique warning ID
 */
function getWarningId(warning) {
  // Use zone, type, and issued time to create unique ID
  const issued = warning.issued_utc || 'unknown';
  return `${warning.zone_key}_${warning.type}_${issued}`;
}

/**
 * Get dismiss duration - always 12 hours for all warnings
 * @returns {number} Duration in ms
 */
function getDismissDuration() {
  return DISMISS_DURATION_MS;
}

/**
 * Check if warning has been dismissed
 * @param {string} warningId - Warning ID
 * @returns {boolean} True if dismissed and not expired
 */
function isWarningDismissed(warningId) {
  try {
    const dismissed = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
    const dismissedTime = dismissed[warningId];

    if (!dismissedTime) return false;

    const now = Date.now();
    const elapsed = now - dismissedTime;

    // Check if dismissal has expired (12 hours)
    if (elapsed > DISMISS_DURATION_MS) {
      delete dismissed[warningId];
      localStorage.setItem(STORAGE_KEY, JSON.stringify(dismissed));
      return false;
    }

    return true;
  } catch (error) {
    logger.error('WarningBanner', 'Error checking dismissed warnings', error);
    return false;
  }
}

/**
 * Dismiss a warning
 * @param {string} warningId - Warning ID to dismiss
 */
function dismissWarning(warningId) {
  try {
    const dismissed = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
    dismissed[warningId] = Date.now();
    localStorage.setItem(STORAGE_KEY, JSON.stringify(dismissed));

    // Feedback message - always 12 hours
    const feedbackMsg = 'Warning hidden for 12 hours';

    // Remove warning banner from DOM
    const banner = document.querySelector(`[data-warning-id="${warningId}"]`);
    if (banner) {
      // Show feedback message
      showDismissalFeedback(banner, feedbackMsg);

      // Fade out and remove
      banner.style.opacity = '0';
      banner.style.transition = 'opacity 0.3s ease';

      setTimeout(() => {
        banner.remove();

        // Hide container if no warnings left
        const container = document.getElementById('warning-banner-container');
        if (container && container.querySelectorAll('.warning-banner').length === 0) {
          container.style.display = 'none';
        }
      }, 300);
    }
  } catch (error) {
    logger.error('WarningBanner', 'Error dismissing warning', error);
  }
}

/**
 * Show dismissal feedback message
 * @param {HTMLElement} banner - Warning banner element
 * @param {string} message - Feedback message
 */
function showDismissalFeedback(banner, message) {
  // Create feedback element
  const feedback = document.createElement('div');
  feedback.className = 'warning-dismissal-feedback';
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
    feedback.style.transition = 'opacity 0.5s ease';
    feedback.style.opacity = '0';
    setTimeout(() => feedback.remove(), 500);
  }, 3000);
}

/**
 * Fetch and display warning banners
 * @param {string} containerId - ID of container element (default: 'warning-banner-container')
 */
async function displayWarningBanners(containerId = 'warning-banner-container') {
  const container = document.getElementById(containerId);

  if (!container) {
    logger.warn('WarningBanner', `Warning banner container '${containerId}' not found`);
    return;
  }

  try {
    const data = await fetchWithTimeout(`/data/marine_forecast.json?t=${Date.now()}`);

    const warnings = collectActiveWarnings(data);

    // Filter out dismissed warnings
    const activeWarnings = warnings.filter(warning => {
      const warningId = getWarningId(warning);
      const isDismissed = isWarningDismissed(warningId);
      return !isDismissed;
    });

    if (activeWarnings.length === 0) {
      // No active warnings - hide container
      container.style.display = 'none';
      return;
    }

    // Combine all warnings into a single banner
    const combinedBanner = createCombinedWarningBanner(activeWarnings);
    container.innerHTML = combinedBanner;
    container.style.display = 'block';

    // Attach dismiss handler - dismisses all warnings at once
    const dismissBtn = container.querySelector('.warning-dismiss-btn');
    if (dismissBtn) {
      dismissBtn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();

        // Dismiss all active warnings
        activeWarnings.forEach(warning => {
          const warningId = getWarningId(warning);
          const dismissed = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
          dismissed[warningId] = Date.now();
          localStorage.setItem(STORAGE_KEY, JSON.stringify(dismissed));
        });

        // Show feedback and remove banner
        showDismissalFeedback(container.querySelector('.warning-banner'), 'All warnings hidden for 12 hours');
        const banner = container.querySelector('.warning-banner');
        if (banner) {
          banner.style.opacity = '0';
          banner.style.transition = 'opacity 0.3s ease';
          setTimeout(() => {
            container.style.display = 'none';
            banner.remove();
          }, 300);
        }
      });
    };

  } catch (error) {
    console.error('[WarningBanner] Error loading warnings:', error);
    logger.error('WarningBanner', 'Error loading marine forecast warnings', error);
    container.style.display = 'none';
  }
}

/**
 * Collect all active warnings from forecast data
 * @param {Object} data - Marine forecast data
 * @returns {Array} Array of warning objects
 */
function collectActiveWarnings(data) {
  const warnings = [];

  if (!data.locations) return warnings;

  for (const [zoneKey, zoneData] of Object.entries(data.locations)) {
    if (zoneData.warnings && Array.isArray(zoneData.warnings)) {
      zoneData.warnings.forEach(warning => {
        if (warning.status === 'IN EFFECT') {
          warnings.push({
            ...warning,
            zone_key: zoneKey,
            zone_name: zoneData.zone_name || warning.location
          });
        }
      });
    }
  }

  // Sort by severity (Storm > Gale > Strong Wind)
  warnings.sort((a, b) => {
    const severityOrder = {
      'Storm warning': 1,
      'Storm': 1,
      'Gale warning': 2,
      'Gale': 2,
      'Strong wind warning': 3,
      'Strong wind': 3,
      'Wind warning': 4,
      'Wind': 4
    };

    const aSeverity = severityOrder[a.type] || 99;
    const bSeverity = severityOrder[b.type] || 99;

    return aSeverity - bSeverity;
  });

  return warnings;
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

  // Build warning text
  let warningText = '';
  if (warnings.length === 1) {
    warningText = `<strong>${warnings[0].type.toUpperCase()}</strong> in effect for ${warnings[0].zone_name}`;
  } else {
    // Multiple warnings - list them
    const warningsList = warnings.map(w => `<strong>${w.type.toUpperCase()}</strong> for ${w.zone_name}`).join(' • ');
    warningText = warningsList;
  }

  return `
    <div class="warning-banner ${severityClass}" role="alert" aria-live="assertive">
      <div class="warning-banner-content">
        <span class="warning-icon" aria-hidden="true">${icon}</span>
        <div class="warning-text">
          ${warningText}
        </div>
        <a href="/forecasts.html" class="warning-details-link">View Forecasts →</a>
        <button class="warning-dismiss-btn" aria-label="Dismiss for 12h" title="Dismiss for 12h">×</button>
      </div>
    </div>
  `;
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
  if (typeLower.includes('strong wind') || typeLower.includes('wind')) {
    return 'warning-strong-wind';
  }
  if (typeLower.includes('waterspout') || typeLower.includes('water spout')) {
    return 'warning-waterspout';
  }

  return 'warning-default';
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
  if (typeLower.includes('strong wind') || typeLower.includes('wind')) return '🌬️';

  return '⚠️';
}

// Auto-initialize if container exists on page load
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('warning-banner-container')) {
      displayWarningBanners();
    }
  });
} else {
  // DOM already loaded
  if (document.getElementById('warning-banner-container')) {
    displayWarningBanners();
  }
}

// Track if warnings have been loaded to prevent duplicate calls
let warningsLoaded = false;
let loadingInProgress = false;

// Also listen for htmx afterSwap events (for pages using htmx to load the container)
document.addEventListener('htmx:afterSwap', (event) => {
  // Skip if already loaded or currently loading (no logging to reduce console noise)
  if (warningsLoaded || loadingInProgress) {
    return;
  }

  // With outerHTML swap, the target IS the newly swapped element
  // Check if it's the warning banner container or contains it
  const isWarningBanner = event.detail.target.id === 'warning-banner-container' ||
                          event.detail.target.querySelector?.('#warning-banner-container') !== null;

  if (isWarningBanner) {
    loadingInProgress = true;
    displayWarningBanners().finally(() => {
      loadingInProgress = false;
      warningsLoaded = true;
    });
  } else {
    // After any swap, check if container now exists (might have been swapped in)
    const container = document.getElementById('warning-banner-container');
    if (container && !warningsLoaded && !loadingInProgress) {
      loadingInProgress = true;
      displayWarningBanners().finally(() => {
        loadingInProgress = false;
        warningsLoaded = true;
      });
    }
  }
});
