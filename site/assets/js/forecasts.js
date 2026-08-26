/**
 * Marine Forecasts Page JavaScript (ES module)
 * Loads and displays Environment Canada marine weather forecasts
 *
 * Still uses fetchWithTimeout and logger globals from the classic scripts
 * loaded before this one (chart-utils-v4.js, logger.js).
 */

import { setSafeHTML } from "./shared/safe-html.js";
import { formatForecastTimestamp } from "./shared/format-time.js";
import {
  DEFAULT_ZONE_KEY,
  listZones as listZonesIn,
  orderZonesForDisplay,
  pickerZoneLabel,
} from "./shared/marine-zones.js";
import { renderWarningZoneControls } from "./warning-zone-picker.js";
import {
  collectActiveWarnings,
  getWarningIcon,
  getWarningSeverityClass as bannerSeverityClass,
} from "./shared/warning-zones.js";

let forecastData = null;
let selectedZoneKey = null;

// Display-only metadata. The parser no longer keeps a zone map — keys are
// slugified from the XML — so anything missing here simply renders without a
// source link rather than dropping the zone.
// weather.gc.ca siteID per zone, for the "View source" link. Strings, not
// numbers — several carry a leading zero. Verified against the live pages
// listed at https://weather.gc.ca/marine/region_e.html?mapID=03 (2026-08-21).
const ZONE_SITE_IDS = {
  haro_strait: "06100",
  howe_sound: "06400",
  johnstone_strait: "06800",
  juan_de_fuca_strait_east_entrance: "07003",
  juan_de_fuca_strait_west_entrance: "07007",
  juan_de_fuca_strait_central_strait: "07010",
  strait_of_georgia_north_of_nanaimo: "14301",
  strait_of_georgia_south_of_nanaimo: "14305",
  west_coast_vancouver_island_south: "16200",
};

// Session-scoped on purpose. Clicking through zones in one sitting should
// stick, but a new visit should open on home waters rather than on wherever
// curiosity left off weeks ago — the warning picker now covers the "keep me
// posted about that zone" need, so the page no longer has to remember for it.
const ZONE_STORAGE_KEY = "selected_marine_zone";

/**
 * Flatten the loaded forecast document into a selectable zone list.
 * @returns {Array<Object>} Zone list from shared/marine-zones.js
 */
function listZones() {
  return listZonesIn(forecastData);
}

/**
 * Pick the zone to show: URL hash, then last choice, then the default.
 * @param {Array<Object>} zones - Zone list from listZones()
 * @returns {string|null} Zone key
 */
function resolveInitialZone(zones) {
  const keys = zones.map((z) => z.zoneKey);
  if (keys.length === 0) return null;

  const hashKey = window.location.hash.slice(1);
  if (keys.includes(hashKey)) return hashKey;

  let stored = null;
  try {
    stored = sessionStorage.getItem(ZONE_STORAGE_KEY);
    // The same key used to live in localStorage, where it outlived the visit.
    // Clear it on the way past so an old choice cannot keep overriding the
    // default, and so the key does not linger unread in readers' browsers.
    localStorage.removeItem(ZONE_STORAGE_KEY);
  } catch (error) {
    logger.warn("Forecasts", "Could not read stored zone", error);
  }
  if (stored && keys.includes(stored)) return stored;

  return keys.includes(DEFAULT_ZONE_KEY) ? DEFAULT_ZONE_KEY : keys[0];
}

/**
 * Populate the zone <select>.
 *
 * Areas with several zones become an <optgroup> whose options drop the
 * repeated area name; an area with a single zone would make an optgroup
 * labelled identically to its one child, so those are emitted as plain
 * top-level options instead.
 *
 * Home waters are pinned to the top by orderZonesForDisplay(); everything
 * else keeps document order.
 *
 * @param {Array<Object>} zones - Zone list from listZones()
 */
function buildZoneSelector(zones) {
  const wrapper = document.getElementById("forecast-zone-selector");
  const select = document.getElementById("forecast-zone-select");
  if (!wrapper || !select) return;

  // A single zone is not worth a picker.
  if (zones.length < 2) {
    wrapper.hidden = true;
    return;
  }

  const makeOption = (zone, label) => {
    const option = document.createElement("option");
    option.value = zone.zoneKey;
    option.textContent = label;
    return option;
  };

  const appendArea = (areaName, areaZones) => {
    if (areaZones.length === 1) {
      select.appendChild(makeOption(areaZones[0], pickerZoneLabel(areaZones[0], false)));
      return;
    }
    const optgroup = document.createElement("optgroup");
    optgroup.label = areaName;
    areaZones.forEach((zone) => {
      optgroup.appendChild(makeOption(zone, pickerZoneLabel(zone, true)));
    });
    select.appendChild(optgroup);
  };

  select.textContent = "";

  orderZonesForDisplay(zones).forEach((group) => appendArea(group.areaName, group.zones));

  select.value = selectedZoneKey;
  wrapper.hidden = false;

  if (!select.dataset.listenerAttached) {
    select.addEventListener("change", (event) => {
      selectedZoneKey = event.target.value;
      try {
        sessionStorage.setItem(ZONE_STORAGE_KEY, selectedZoneKey);
      } catch (error) {
        logger.warn("Forecasts", "Could not store zone selection", error);
      }
      displayForecasts();
    });
    select.dataset.listenerAttached = "true";
  }
}

/**
 * Load and display forecast data
 */
async function loadForecasts() {
  const container = document.getElementById("forecast-container");

  try {
    forecastData = await fetchWithTimeout("/data/marine_forecast.json");
    displayForecasts();
  } catch (error) {
    logger.error("Forecasts", "Error loading forecasts", error);
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
    return { isStale: true, ageHours: null, message: "No timestamp available" };
  }

  const generatedDate = new Date(forecastData.generated_utc);
  const now = new Date();
  const ageMs = now - generatedDate;
  const ageHours = ageMs / (1000 * 60 * 60);

  // Marine forecasts updated every ~6 hours (05h, 11h, 18h UTC)
  // Consider stale if > 8 hours old (missed update + grace period)
  const isStale = ageHours > 8;

  let message = "";
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
  const container = document.getElementById("forecast-container");

  const zones = listZones();
  if (zones.length === 0) {
    container.innerHTML = '<div class="error-state"><p>No forecast data available.</p></div>';
    return;
  }

  if (!zones.some((z) => z.zoneKey === selectedZoneKey)) {
    selectedZoneKey = resolveInitialZone(zones);
  }
  buildZoneSelector(zones);

  const zone = zones.find((z) => z.zoneKey === selectedZoneKey);

  let html = "";

  // Check data freshness and show warning only if stale
  const freshness = checkFreshness();
  if (freshness.isStale) {
    html += `
      <div class="warning-card warning-gale" style="margin-bottom: 1.5rem;">
        <p style="margin: 0; font-weight: 500;">${freshness.message}</p>
      </div>
    `;
  }

  // Display the selected zone only
  html += renderZoneForecast(zone.zoneKey, zone.zoneData, zone.areaData);

  // Display extended forecast (per zone; area-level copy when zones agree)
  const extended = zone.zoneData.extended_forecast || zone.areaData.extended_forecast;
  if (extended && extended.length > 0) {
    html += renderExtendedForecast(extended, zone.areaData);
  }

  setSafeHTML(container, html);

  // After the card exists: the inline toggle mounts into it, and the card is
  // rebuilt wholesale on every render.
  renderWarningZoneControls(zones, selectedZoneKey);
  renderWarningJumpStrip(zones);
}

/**
 * Drop the redundant " warning" from a type, for a chip that already reads as
 * one. "Strong wind warning" → "Strong wind".
 *
 * @param {string} type - Warning type from the forecast document
 * @returns {string} Short label
 */
function shortWarningType(type) {
  return String(type || "Warning").replace(/\s*warning$/i, "");
}

/**
 * Collapse a warning list into one entry per type, keeping the order they
 * arrived in.
 *
 * A single Pacific system warns several zones with the same words, so the
 * ungrouped strip read "Strong wind — Howe Sound", "Strong wind — Haro
 * Strait", "Strong wind — …" five times over: the type shouted, the zone
 * whispered, and the zone is the only part that varies. Stating the type once
 * and listing its waters after it puts the varying part in front.
 *
 * @param {Array<Object>} warnings - Active warnings, already sorted
 * @returns {Array<{type: string, warnings: Array<Object>}>} One entry per type
 */
function groupWarningsByType(warnings) {
  const groups = new Map();

  warnings.forEach((warning) => {
    // Keyed case-insensitively because the type is prose from the bulletin,
    // and the same warning is not always capitalised the same way.
    const key = String(warning.type || "Warning").toLowerCase();
    if (!groups.has(key)) {
      groups.set(key, { type: warning.type, warnings: [] });
    }
    groups.get(key).warnings.push(warning);
  });

  return Array.from(groups.values());
}

/**
 * Render the jump-to-warning strip above both forecast kinds.
 *
 * Deliberately unfiltered: it is passed every zone the document carries, not
 * the reader's banner subscription. The sitewide banner is narrow on purpose,
 * and zone filtering creates a blind spot — this is the page where that blind
 * spot gets closed, so a reader can see a gale two zones over even though they
 * chose not to be interrupted by it.
 *
 * One row per warning type, the waters listed inside it. Each water is its own
 * link, so the strip is still the way into any single zone's forecast.
 *
 * @param {Array<Object>} zones - Zone list from shared/marine-zones.js
 * @returns {void}
 */
function renderWarningJumpStrip(zones) {
  const strip = document.getElementById("warning-jump");
  const list = document.getElementById("warning-jump-list");
  if (!strip || !list) return;

  const warnings = collectActiveWarnings(
    forecastData,
    zones.map((zone) => zone.zoneKey),
  );

  list.textContent = "";
  strip.hidden = warnings.length === 0;
  if (warnings.length === 0) return;

  groupWarningsByType(warnings).forEach((group) => {
    const item = document.createElement("li");
    item.className = `warning-jump-group ${bannerSeverityClass(group.type)}`;

    const icon = document.createElement("span");
    icon.setAttribute("aria-hidden", "true");
    icon.textContent = getWarningIcon(group.type);

    const type = document.createElement("span");
    type.className = "warning-jump-type";
    // Chips carry their meaning in colour, so they must not also depend on it:
    // the type is spelled out on every row.
    type.textContent = shortWarningType(group.type);

    item.append(icon, type);

    group.warnings.forEach((warning) => {
      item.appendChild(warningJumpLink(warning));
    });

    list.appendChild(item);
  });
}

/**
 * One zone link inside a grouped warning row.
 *
 * @param {Object} warning - Active warning
 * @returns {HTMLAnchorElement}
 */
function warningJumpLink(warning) {
  const link = document.createElement("a");
  link.className = "warning-jump-item";
  link.href = `#${encodeURIComponent(warning.zone_key)}`;
  link.textContent = warning.zone_name || warning.zone_key;
  // The type is named once by the row, so name it again for a reader who
  // arrives at the link alone — a screen reader running the links list has no
  // row heading beside it.
  link.setAttribute("aria-label", `${warning.type} — ${warning.zone_name || warning.zone_key}`);

  // The href alone is not enough. Clicking the chip for the zone already on
  // screen changes nothing about the hash, so no hashchange fires and the
  // page appears to ignore the click; and following the hash normally skips
  // the scroll-and-highlight the deep link gives you.
  link.addEventListener("click", (event) => {
    event.preventDefault();
    if (window.location.hash.slice(1) !== warning.zone_key) {
      window.location.hash = warning.zone_key;
    }
    selectedZoneKey = warning.zone_key;
    displayForecasts();
    scrollToZoneIfNeeded();
  });

  return link;
}

/**
 * Render forecast for a single zone
 * @param {string} zoneKey - Zone identifier
 * @param {Object} zoneData - Zone forecast data
 * @returns {string} HTML string
 */
function renderZoneForecast(zoneKey, zoneData, areaData) {
  const zoneName = zoneData.zone_name || zoneKey.replace(/_/g, " ");

  // Get source link for this zone (omitted for zones we have no siteID for)
  const siteId = ZONE_SITE_IDS[zoneKey];
  const sourceLink = siteId
    ? `https://weather.gc.ca/marine/forecast_e.html?mapID=03&siteID=${siteId}`
    : null;

  let html = `
    <div class="forecast-zone" id="${zoneKey}">
      <h2>
        ${zoneName}
        ${sourceLink ? `<a href="${sourceLink}" target="_blank" rel="noopener" style="font-size: 0.75em; margin-left: 0.5rem; color: var(--color-accent-blue); text-decoration: none;">View source</a>` : ""}
      </h2>
      <div class="zone-alert-toggle" id="zone-alert-toggle"></div>
  `;

  // Warnings section
  html += '<div class="zone-warnings">';
  if (zoneData.warnings && zoneData.warnings.length > 0) {
    zoneData.warnings.forEach((warning) => {
      html += renderWarningCard(warning);
    });
  } else {
    html += `
      <div class="no-warnings">
        ✅ No active warnings for this zone
      </div>
    `;
  }
  html += "</div>";

  // Current forecast
  if (zoneData.forecast) {
    html += `
      <div class="forecast-section">
        <h3>Current Forecast</h3>
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
  if (areaData.wave_forecast) {
    html += `
      <div class="forecast-section">
        <h3>Wave Forecast</h3>
        <div class="forecast-content">
    `;

    if (areaData.wave_forecast.period) {
      html += `<div class="forecast-period"><strong>Period:</strong> ${areaData.wave_forecast.period}</div>`;
    }

    if (areaData.wave_forecast.forecast) {
      html += `<div class="forecast-period">${areaData.wave_forecast.forecast}</div>`;
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
        <strong>Issued:</strong> ${formatForecastTimestamp(issuedDate)}
      </div>
    `;
  }

  html += "</div>"; // .forecast-zone

  return html;
}

/**
 * Render a warning card
 * @param {Object} warning - Warning data
 * @returns {string} HTML string
 */
function renderWarningCard(warning) {
  const severityClass = getWarningSeverityClass(warning.type);

  let issuedText = "";
  if (warning.issued_utc) {
    const issuedDate = new Date(warning.issued_utc);
    issuedText = ` <small>(Issued ${formatForecastTimestamp(issuedDate)})</small>`;
  }

  return `
    <div class="warning-card ${severityClass}">
      <h3>${warning.type}</h3>
      <p><strong>Status:</strong> ${warning.status}${issuedText}</p>
    </div>
  `;
}

/**
 * Render extended forecast section
 * @param {Array} extendedForecast - Extended forecast periods
 * @returns {string} HTML string
 */
function renderExtendedForecast(extendedForecast, areaData) {
  let html = `
    <div class="forecast-zone">
      <h2>Extended Forecast</h2>
      <div class="extended-forecast">
  `;

  extendedForecast.forEach((period) => {
    html += `
      <div class="extended-day">
        <h3>${period.period}</h3>
        <p>${period.forecast}</p>
      </div>
    `;
  });

  html += `
      </div>
  `;

  // Add issued timestamp if available
  if (areaData && areaData.generated_utc) {
    const issuedDate = new Date(areaData.generated_utc);
    html += `
      <div class="forecast-metadata">
        <strong>Issued:</strong> ${formatForecastTimestamp(issuedDate)}
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

  if (typeLower.includes("storm")) return "warning-storm";
  if (typeLower.includes("gale")) return "warning-gale";
  if (typeLower.includes("strong wind")) return "warning-strong-wind";

  return "";
}

/**
 * Auto-refresh forecast data every 5 minutes
 */
function startAutoRefresh() {
  setInterval(
    () => {
      logger.info("Forecasts", "Auto-refreshing forecast data...");
      loadForecasts();
    },
    5 * 60 * 1000,
  ); // 5 minutes
}

/**
 * Scroll to zone section if hash in URL
 */
function scrollToZoneIfNeeded() {
  const hash = window.location.hash;
  if (hash) {
    // Wait a bit for content to load
    setTimeout(() => {
      // getElementById, not querySelector: the hash is arbitrary user input and
      // querySelector *parses* it as a selector, so anything that isn't a valid
      // one (`#wave-../etc`) throws a SyntaxError and kills the rest of this
      // handler. Now that the map popups link here with `#wave-<station>`,
      // the page receives hashes it did not author.
      const element = document.getElementById(hash.slice(1));
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
        // Add a subtle highlight effect
        element.style.transition = "box-shadow 0.3s ease";
        element.style.boxShadow = "0 0 0 3px rgba(66, 153, 225, 0.5)";
        setTimeout(() => {
          element.style.boxShadow = "";
        }, 2000);
      }
    }, 300);
  }
}

/**
 * Follow a zone hash arriving while the page is already open.
 *
 * The sitewide warning banner links to `/forecasts.html#<zone_key>`. When the
 * reader is already on this page that is a same-document navigation: no load,
 * no `htmx:load`, so without this the page kept showing whatever zone was
 * stored from last time and silently ignored the warning that was clicked.
 *
 * @returns {void}
 */
function handleZoneHashChange() {
  const hashKey = window.location.hash.slice(1);
  if (!hashKey || hashKey === selectedZoneKey) return;

  const zones = listZones();
  if (!zones.some((z) => z.zoneKey === hashKey)) return;

  selectedZoneKey = hashKey;
  displayForecasts();
  scrollToZoneIfNeeded();
}

window.addEventListener("hashchange", handleZoneHashChange);

// Initialize on page load - wait for HTMX to load footer with timestamp
document.addEventListener(
  "htmx:load",
  () => {
    loadForecasts().then(() => {
      scrollToZoneIfNeeded();
    });
    startAutoRefresh();
  },
  { once: true },
);
