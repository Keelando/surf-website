/**
 * Marine Forecasts Page JavaScript (ES module)
 * Loads and displays Environment Canada marine weather forecasts
 *
 * Still uses fetchWithTimeout and logger globals from the classic scripts
 * loaded before this one (chart-utils-v4.js, logger.js).
 */

import { icon as lineIcon } from "./shared/icons.js";
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
  } finally {
    document.querySelector(".forecast-main-content")?.removeAttribute("aria-busy");
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

  // One card: the forecaster's words for the selected zone, condensed to
  // about a phone screen (user 2026-11-08), source links at its foot.
  html += renderZoneCard(zone.zoneKey, zone.zoneData, zone.areaData);

  setSafeHTML(container, html);

  renderWarningZoneControls(zones);
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

  // Always shown: the banner settings live on this strip (user 2026-11-08),
  // so it must be there on a quiet day too, saying so.
  list.textContent = "";
  strip.hidden = false;
  list.hidden = warnings.length === 0;
  const heading = document.getElementById("warning-jump-heading");
  if (heading)
    heading.textContent = warnings.length ? "Warnings in effect" : "No warnings in effect";
  if (warnings.length === 0) return;

  groupWarningsByType(warnings).forEach((group) => {
    const item = document.createElement("li");
    item.className = `warning-jump-group ${bannerSeverityClass(group.type)}`;

    const icon = document.createElement("span");
    icon.className = "warning-jump-icon";
    icon.setAttribute("aria-hidden", "true");
    setSafeHTML(icon, lineIcon("warning"));

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
  link.setAttribute("aria-label", `${warning.type}: ${warning.zone_name || warning.zone_key}`);

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
 * The selected zone's forecast, as one card (user 2026-11-08: "zone first,
 * condensed"). The forecaster's text is shown verbatim, never shortened: it
 * is someone else's forecast. What shrinks is everything around it: one
 * heading with the issue time, warnings as one line each, the coming days as
 * one line each instead of three cards, and the source as two small links
 * instead of a box with two full-width buttons.
 *
 * @param {string} zoneKey - Zone identifier
 * @param {Object} zoneData - Zone forecast data
 * @param {Object} areaData - The zone's area (wave forecast, area-level extended)
 * @returns {string} HTML string
 */
function renderZoneCard(zoneKey, zoneData, areaData) {
  const zoneName = zoneData.zone_name || zoneKey.replace(/_/g, " ");
  const issued = zoneData.issued_utc
    ? `<p class="zone-issued">Issued ${formatForecastTimestamp(new Date(zoneData.issued_utc))}</p>`
    : "";

  let html = `
    <article class="zone-card" id="${zoneKey}" aria-labelledby="zone-card-title">
      <h3 class="zone-card-title" id="zone-card-title">${zoneName}</h3>
      ${issued}
  `;

  const warnings = zoneData.warnings || [];
  html += warnings.length
    ? `<ul class="zone-warning-lines">${warnings.map((w) => renderWarningLine(w, zoneData.issued_utc)).join("")}</ul>`
    : `<p class="zone-no-warnings">No warnings for this zone.</p>`;

  const forecast = zoneData.forecast;
  if (forecast) {
    if (forecast.period) html += `<p class="zone-period">${forecast.period}</p>`;
    if (forecast.wind) html += zoneLine("Wind", forecast.wind);
    if (forecast.weather) html += zoneLine("Weather", forecast.weather);
  }

  const waves = areaData.wave_forecast;
  if (waves && waves.forecast) {
    html += zoneLine(waves.period ? `Waves, ${waves.period}` : "Waves", waves.forecast);
  }

  const extended = zoneData.extended_forecast || areaData.extended_forecast;
  if (extended && extended.length > 0) {
    html += `<dl class="zone-days">${extended
      .map((day) => `<div class="zone-day"><dt>${day.period}</dt><dd>${day.forecast}</dd></div>`)
      .join("")}</dl>`;
  }

  html += renderSourceLinks(zoneKey, zoneName);
  html += "</article>";
  return html;
}

/**
 * One labelled paragraph of the forecaster's text.
 *
 * @param {string} label - "Wind", "Weather", "Waves, …"
 * @param {string} text - The forecaster's words, verbatim
 * @returns {string} HTML string
 */
function zoneLine(label, text) {
  return `<p class="zone-text"><span class="zone-label">${label}</span> ${text}</p>`;
}

/**
 * A zone's own warning, one line: type, status, issue time. Same --wb-*
 * palette as the banner and the warnings strip, so one warning looks the
 * same in all three places.
 *
 * @param {Object} warning - Warning data
 * @param {string} [zoneIssuedUtc] - The bulletin's issue time, shown in the
 *   card heading; a warning issued with it doesn't repeat it
 * @returns {string} HTML string
 */
function renderWarningLine(warning, zoneIssuedUtc) {
  const sameAsZone =
    warning.issued_utc &&
    zoneIssuedUtc &&
    Math.abs(new Date(warning.issued_utc) - new Date(zoneIssuedUtc)) < 60 * 1000;
  const issued =
    warning.issued_utc && !sameAsZone
      ? ` <span class="zone-warning-issued">issued ${formatForecastTimestamp(new Date(warning.issued_utc))}</span>`
      : "";
  return `
    <li class="zone-warning-line ${getWarningSeverityClass(warning.type)}">
      ${lineIcon("warning")}<span class="zone-warning-type">${warning.type}</span>
      <span class="zone-warning-status">${String(warning.status || "").toLowerCase()}</span>${issued}
    </li>`;
}

/**
 * The bulletin every BC marine zone appears in. Verified 2026-09-06: FQCN13
 * CWVR carries all 23 Pacific marine areas, so it is an honest destination for
 * a zone we have no per-zone siteID for, and a useful second link for one we
 * do — a reader comparing zones gets them all on one page.
 */
const ALL_ZONES_BULLETIN =
  "https://weather.gc.ca/marine/marine_bulletins_e.html?Bulletin=fqcn13.cwvr";

/**
 * Where this forecast came from: the card's last line.
 *
 * The site re-presents someone else's bulletins, so a reader must always be
 * able to reach the original in one click, and a reader who can put our
 * rendering beside the source can catch our next transcription error. Two
 * small links do that; the earlier box with two full-width buttons cost
 * ~200px on a phone for the same two clicks.
 *
 * @param {string} zoneKey - Zone identifier
 * @param {string} zoneName - Zone display name
 * @returns {string} HTML string
 */
function renderSourceLinks(zoneKey, zoneName) {
  const siteId = ZONE_SITE_IDS[zoneKey];
  const arrow = '<span class="forecast-external-arrow" aria-hidden="true">\u2197</span>';

  // Zones we hold a siteID for get their own EC page first. The rest fall
  // through to the all-zones bulletin rather than to a landing page, which
  // would imply a precision we cannot deliver.
  const zoneLink = siteId
    ? `<a href="https://weather.gc.ca/marine/forecast_e.html?mapID=03&siteID=${siteId}"
          target="_blank" rel="noopener"
          aria-label="${zoneName} forecast on Environment Canada (opens weather.gc.ca in a new tab)"
       >This zone ${arrow}</a> · `
    : "";

  return `
    <p class="zone-source">
      Environment Canada's bulletin, the authoritative source:
      ${zoneLink}<a href="${ALL_ZONES_BULLETIN}" target="_blank" rel="noopener"
         aria-label="All BC marine forecasts, full bulletin (opens weather.gc.ca in a new tab)"
      >All BC zones ${arrow}</a>
    </p>
  `;
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
