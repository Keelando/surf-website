/**
 * Lightstation Page (ES module)
 * Region-grouped condition cards for all BC lightstations
 */

import { viewLightstationChart } from "./lightstation-charts.js";
import { centerMapOnLightstation } from "./lightstation-map.js";
import { formatWeekdayDayTime, getShortAgeString } from "./shared/format-time.js";
import {
  describeMissedReports,
  describeNextReport,
  describeSchedule,
  describeSlots,
} from "./shared/lightstation-schedule.js";
import { setSafeHTML } from "./shared/safe-html.js";
import { staleThresholdLabel } from "./shared/staleness.js";
import { orderRegions } from "./shared/station-meta.js";

// Lightstation metadata keyed by several name/ID formats (module-local;
// was window.stationMetadata before the ES-module conversion)
let stationMetadata = {};

// Load and display lightstation data
async function loadLightstationData() {
  try {
    // Load station metadata for details expansion
    try {
      const metaResponse = await fetch("/data/stations.json");
      const metaData = await metaResponse.json();
      if (metaData.lightstations) {
        Object.values(metaData.lightstations).forEach((station) => {
          // Store with multiple key formats for easier lookup
          // Format 1: Title case name (e.g., "Addenbroke Island")
          stationMetadata[station.name] = station;
          // Format 2: Uppercase with spaces (e.g., "ADDENBROKE ISLAND") - matches latest_lightstation.json
          stationMetadata[station.name.toUpperCase()] = station;
          // Format 3: Uppercase with underscores (e.g., "ADDENBROKE_ISLAND") - matches ID
          stationMetadata[station.id] = station;
        });
      }
    } catch (err) {
      console.warn("Could not load station metadata:", err);
    }

    const response = await fetch("/data/latest_lightstation.json");
    const data = await response.json();

    // Group stations by region
    const regions = {};
    for (const [stationName, stationData] of Object.entries(data)) {
      const region = stationData.region || "OTHER";
      if (!regions[region]) {
        regions[region] = [];
      }
      regions[region].push({ name: stationName, ...stationData });
    }

    // Render grouped by region
    const container = document.getElementById("lightstations-container");
    container.textContent = "";

    const orderedRegions = orderRegions(regions);

    for (const region of orderedRegions) {
      const section = document.createElement("div");
      section.className = "region-section";
      section.setAttribute("data-region", region);

      // Collapse all regions except Strait of Georgia by default
      if (region !== "STRAIT OF GEORGIA") {
        section.classList.add("collapsed");
      }

      const header = document.createElement("div");
      header.className = "region-header";

      // Add toggle arrow, region name, and station count
      const toggleArrow = document.createElement("span");
      toggleArrow.className = "region-toggle-btn";
      toggleArrow.textContent = "▼";

      const stationCount = document.createElement("span");
      stationCount.style.fontSize = "0.8em";
      stationCount.style.fontWeight = "normal";
      stationCount.style.opacity = "0.8";
      const count = regions[region].length;
      stationCount.textContent = ` (${count} station${count === 1 ? "" : "s"})`;

      header.textContent = "";
      header.appendChild(toggleArrow);
      header.appendChild(document.createTextNode(" " + region + " "));
      header.appendChild(stationCount);

      // Add click handler to toggle collapse
      header.addEventListener("click", () => {
        section.classList.toggle("collapsed");
      });

      section.appendChild(header);

      const grid = document.createElement("div");
      grid.className = "lightstation-grid";

      // Sort stations alphabetically
      regions[region].sort((a, b) => a.name.localeCompare(b.name));

      for (const station of regions[region]) {
        const card = createStationCard(station);
        grid.appendChild(card);
      }

      section.appendChild(grid);
      container.appendChild(section);
    }

    // Handle hash navigation (e.g., #lightstation-CHROME_ISLAND)
    handleLightstationHash();
  } catch (error) {
    console.error("Failed to load lightstation data:", error);
    const fallbackContainer = document.getElementById("lightstations-container");
    if (fallbackContainer) {
      setSafeHTML(
        fallbackContainer,
        '<p style="text-align: center; color: var(--color-error-text); padding: 2rem;">Failed to load lightstation data.</p>',
      );
    }
  }
}

// Handle hash-based navigation to specific lightstation
function handleLightstationHash() {
  const hash = window.location.hash;
  if (!hash || !hash.startsWith("#lightstation-")) return;

  // Extract station ID from hash (e.g., "#lightstation-CHROME_ISLAND" -> "CHROME_ISLAND")
  const stationId = hash.replace("#lightstation-", "");

  // Find the card with this station ID
  const targetCard = document.querySelector(`[data-station-id="${stationId}"]`);
  if (!targetCard) {
    console.warn("Station not found:", stationId);
    return;
  }

  // Find the region section containing this card
  const regionSection = targetCard.closest(".region-section");
  if (!regionSection) return;

  // Collapse all region sections first
  document.querySelectorAll(".region-section").forEach((section) => {
    section.classList.add("collapsed");
  });

  // Expand the target region
  regionSection.classList.remove("collapsed");

  // Scroll to the region header with the card
  setTimeout(() => {
    regionSection.scrollIntoView({ behavior: "smooth", block: "start" });
  }, 300);
}

/**
 * Name the bulletin(s) a station's observations arrive in, for the details
 * panel. `bulletins` is written by export_lightstation_json.py.
 *
 * @param {Array<string>|undefined} bulletins - Product codes, e.g. ["SXCN"]
 * @returns {string} Display text, or "" when we have nothing to say
 */
function describeBulletins(bulletins) {
  if (!Array.isArray(bulletins) || bulletins.length === 0) return "";
  const labels = { SXCN: "SXCN (coded)", FPCN61: "FPCN61 (written)" };
  return bulletins.map((code) => labels[code] || code).join(" + ");
}

function createStationCard(station) {
  const card = document.createElement("div");
  card.className = "lightstation-card";
  // Add data attribute for hash navigation
  if (stationMetadata[station.name]) {
    card.setAttribute("data-station-id", stationMetadata[station.name].id);
  }

  const title = document.createElement("h3");
  title.textContent = station.name;
  card.appendChild(title);

  // Wind
  if (!station.wind_calm) {
    const windRow = createConditionRow(
      "Wind",
      `${station.wind_direction || "N/A"} ${station.wind_speed_kt || "N/A"} kt${station.wind_gusting ? " (gusting)" : ""}${station.wind_estimated ? " (est)" : ""}`,
    );
    card.appendChild(windRow);
  } else {
    card.appendChild(createConditionRow("Wind", "CALM"));
  }

  // Sea state
  if (station.sea_height_ft !== null || station.sea_condition) {
    const seaText =
      station.sea_height_ft !== null
        ? `${station.sea_height_ft} ft ${station.sea_condition || ""}`
        : station.sea_condition || "N/A";
    card.appendChild(createConditionRow("Sea State", seaText));
  }

  // Swell
  if (station.swell_intensity || station.swell_direction) {
    const swellText =
      `${station.swell_intensity || ""} ${station.swell_direction || ""} swell`.trim();
    card.appendChild(createConditionRow("Swell", swellText || "N/A"));
  }

  // Report time with age
  if (station.observation_time) {
    const reportTime = document.createElement("div");
    reportTime.className = "report-time";

    // How old the reading is, said first and said plainly. The whole line used
    // to be 0.85rem muted italic with the age in parentheses at the end, which
    // is the least prominent place on the card for the one number a reader
    // needs — the colour badge below is a nicety, this is the substance. The
    // age carries the alert colour itself when the station is overdue, so it
    // does not depend on the badge to be noticed.
    const formattedDate = formatWeekdayDayTime(station.observation_time);
    const age = document.createElement("span");
    age.className = station.stale ? "report-age report-age-stale" : "report-age";
    age.textContent = getShortAgeString(station.observation_time);
    reportTime.appendChild(age);
    reportTime.appendChild(document.createTextNode(` \u00b7 ${formattedDate}`));
    card.appendChild(reportTime);

    // When to check back. Lightkeeper reports land on a fixed daily cycle, so
    // "next ~14:40" is knowable and is the thing a reader waiting on this
    // station actually wants.
    // Either when to check back, or — once the station has stopped — how much
    // it has missed. Never both, and never a next-report promise from a
    // station that is not reporting.
    const nextReport = describeNextReport(station.schedule, station.stale);
    const missed = station.stale
      ? describeMissedReports(station.schedule, station.observation_time)
      : null;
    if (nextReport || missed) {
      const nextLine = document.createElement("div");
      // Deliberately the same muted styling as the next-report line it
      // replaces: the age above is already red and the badge below says
      // STALE, so a third red on one card would be noise, not emphasis.
      nextLine.className = "report-time report-next";
      nextLine.textContent = missed || `Next report: ${nextReport}`;
      card.appendChild(nextLine);
    }
  } else if (station.report_time_str) {
    const reportTime = document.createElement("div");
    reportTime.className = "report-time";
    reportTime.textContent = `Reported: ${station.report_time_str}`;
    card.appendChild(reportTime);
  }

  // Staleness warning
  if (station.stale) {
    const warning = document.createElement("div");
    warning.className = "stale-warning";
    warning.style.color = "var(--color-accent-red)";
    warning.style.fontWeight = "600";
    warning.style.marginTop = "0.5rem";
    // No number here. It used to print the threshold — "(>9h old)" — directly
    // under a line reading "5 days ago", which reads as a contradiction and
    // badly understates the gap. The age line above is the substance; this is
    // just the flag. The threshold that triggered it goes in the tooltip and
    // in the details panel, where it explains rather than competes.
    warning.title = `Flagged after ${staleThresholdLabel(station).replace(">", "")} without a report`;
    warning.textContent = "⚠️ STALE DATA";
    card.appendChild(warning);
  }

  // Navigation links container
  const navLinks = document.createElement("div");
  navLinks.style.display = "flex";
  navLinks.style.gap = "0.5rem";
  navLinks.style.marginTop = "0.75rem";

  // View historical data link
  const chartLink = document.createElement("a");
  chartLink.className = "view-chart-link";
  chartLink.href = "#lightstation-data-table-section";
  chartLink.textContent = "View historical";
  chartLink.style.flex = "1";
  chartLink.style.textAlign = "center";
  chartLink.style.padding = "0.4rem";
  chartLink.style.background = "var(--color-surface-alt)";
  chartLink.style.border = "1px solid var(--color-border-light)";
  chartLink.style.borderRadius = "4px";
  chartLink.style.textDecoration = "none";
  chartLink.style.fontSize = "0.85rem";
  chartLink.addEventListener("click", (e) => {
    e.preventDefault();
    viewLightstationChart(station.name);
  });
  navLinks.appendChild(chartLink);

  // Show on Map button
  const mapLink = document.createElement("a");
  mapLink.className = "view-chart-link";
  mapLink.href = "#lightstation-map-section";
  mapLink.textContent = "📍 Show on Map";
  mapLink.style.flex = "1";
  mapLink.style.textAlign = "center";
  mapLink.style.padding = "0.4rem";
  mapLink.style.background = "var(--color-surface-alt)";
  mapLink.style.border = "1px solid var(--color-border-light)";
  mapLink.style.borderRadius = "4px";
  mapLink.style.textDecoration = "none";
  mapLink.style.fontSize = "0.85rem";
  mapLink.addEventListener("click", (e) => {
    e.preventDefault();
    const stationId = stationMetadata[station.name] ? stationMetadata[station.name].id : null;

    if (stationId) {
      // Scroll to map section
      const mapSection = document.getElementById("lightstation-map-section");
      if (mapSection) {
        mapSection.scrollIntoView({ behavior: "smooth", block: "start" });
      }

      // Center map on lightstation after scroll
      setTimeout(() => {
        centerMapOnLightstation(stationId);
      }, 800);
    }
  });
  navLinks.appendChild(mapLink);

  card.appendChild(navLinks);

  // Station details toggle button
  const detailsToggle = document.createElement("div");
  detailsToggle.className = "station-details-toggle";
  setSafeHTML(detailsToggle, 'Station Details <span class="toggle-icon">▼</span>');

  // Station details content (initially hidden)
  const detailsContent = document.createElement("div");
  detailsContent.className = "station-details-content";

  // Add metadata rows. `window.stationMetadata` until the ES-module
  // conversion moved the registry to the module-scoped `stationMetadata` on
  // line 13 — this one call site kept the `window.` prefix, so the guard was
  // always false and every card's "Station Details" panel opened empty.
  if (stationMetadata[station.name]) {
    const meta = stationMetadata[station.name];

    if (meta.id) {
      const idRow = document.createElement("div");
      idRow.className = "detail-row";
      setSafeHTML(
        idRow,
        `<span class="detail-label">Station ID:</span><span class="detail-value">${meta.id}</span>`,
      );
      detailsContent.appendChild(idRow);
    }

    if (meta.lat && meta.lon) {
      const coordRow = document.createElement("div");
      coordRow.className = "detail-row";
      setSafeHTML(
        coordRow,
        `<span class="detail-label">Coordinates:</span><span class="detail-value">${meta.lat.toFixed(4)}°N, ${Math.abs(meta.lon).toFixed(4)}°W</span>`,
      );
      detailsContent.appendChild(coordRow);
    }

    if (meta.region) {
      const regionRow = document.createElement("div");
      regionRow.className = "detail-row";
      setSafeHTML(
        regionRow,
        `<span class="detail-label">Region:</span><span class="detail-value">${meta.region}</span>`,
      );
      detailsContent.appendChild(regionRow);
    }

    if (meta.established) {
      const estRow = document.createElement("div");
      estRow.className = "detail-row";
      setSafeHTML(
        estRow,
        `<span class="detail-label">Established:</span><span class="detail-value">${meta.established}</span>`,
      );
      detailsContent.appendChild(estRow);
    }

    // Publishing schedule, observed rather than declared. The registry's
    // `update_frequency_hours` says "every 3 hours" for all of them, which is
    // wrong for the stations that report four times a day in daylight only,
    // and for the ones that appear in both bulletin cycles — it is used only
    // as the fallback when a station has too little history to infer from.
    const scheduleText = describeSchedule(station.schedule, meta.update_frequency_hours);
    if (scheduleText) {
      const freqRow = document.createElement("div");
      freqRow.className = "detail-row";
      setSafeHTML(
        freqRow,
        `<span class="detail-label">Reports:</span><span class="detail-value">${scheduleText}</span>`,
      );
      detailsContent.appendChild(freqRow);
    }

    const slotsText = describeSlots(station.schedule);
    if (slotsText) {
      const slotsRow = document.createElement("div");
      slotsRow.className = "detail-row";
      slotsRow.style.flexDirection = "column";
      slotsRow.style.alignItems = "flex-start";
      setSafeHTML(
        slotsRow,
        `<span class="detail-label">Report times:</span><span class="detail-value ls-slot-times">${slotsText}</span>`,
      );
      detailsContent.appendChild(slotsRow);
    }

    if (meta.notes) {
      const notesRow = document.createElement("div");
      notesRow.className = "detail-row";
      notesRow.style.flexDirection = "column";
      notesRow.style.alignItems = "flex-start";
      setSafeHTML(
        notesRow,
        `<span class="detail-label">Notes:</span><span class="detail-value" style="text-align: left; margin-top: 0.3rem; font-size: 0.8rem; color: var(--color-text-muted);">${meta.notes}</span>`,
      );
      detailsContent.appendChild(notesRow);
    }
  }

  // Which EC product this station's readings actually arrive in, measured from
  // 30 days of its own history by the export rather than declared anywhere.
  // It is the reason a reading here can be timed differently from the same
  // reading on EC's page: the two bulletins are issued on different cycles,
  // and nine stations appear in both. Rendered outside the registry guard
  // above because it comes from the observations, not from stations.json.
  // The threshold that drives the badge, explained where it can inform rather
  // than compete with the age. It is per station — 9 h for the three-hourly
  // ones, 18 h for the daylight-only ones whose normal overnight gap is 15 h.
  if (Number.isFinite(station.stale_after_hours)) {
    const staleRow = document.createElement("div");
    staleRow.className = "detail-row";
    setSafeHTML(
      staleRow,
      `<span class="detail-label">Flagged stale after:</span><span class="detail-value">${Math.round(station.stale_after_hours)} h without a report</span>`,
    );
    detailsContent.appendChild(staleRow);
  }

  const bulletinText = describeBulletins(station.bulletins);
  if (bulletinText) {
    const bulletinRow = document.createElement("div");
    bulletinRow.className = "detail-row";
    setSafeHTML(
      bulletinRow,
      `<span class="detail-label">Bulletin:</span><span class="detail-value">${bulletinText}</span>`,
    );
    detailsContent.appendChild(bulletinRow);
  }

  // Toggle functionality
  detailsToggle.addEventListener("click", () => {
    detailsToggle.classList.toggle("expanded");
    detailsContent.classList.toggle("expanded");
  });

  card.appendChild(detailsToggle);
  card.appendChild(detailsContent);

  // Where this reading came from. The buoy cards have carried a per-card
  // source link for a while; this is the lightstation equivalent, and it is
  // here for the same two reasons: the site re-presents someone else's
  // bulletins and a reader must be able to reach the original, and a reader
  // who can put our rendering beside EC's is the one who catches our next
  // transcription error. (The McInnes Island coordinate error sat on this page
  // until a mariner happened to notice.)
  //
  // One URL serves every station: EC's Lightstation Reports page is a single
  // list with no per-station anchor, and — verified 2026-09-06 — it carries
  // all 21 stations we render, including the seven that reach us only in
  // FPCN61. Keep the ?mapID/&siteID query string; without it EC renders the
  // same list under a "The web address you have entered is incorrect" banner,
  // which reads as our broken link rather than theirs.
  // Same footer the buoy cards use — "🔗 View Source Data" under a rule, in
  // the shared .buoy-source-link-wrap / .ls-card-source-wrap styling (one rule,
  // two selectors, in style-v4.css) so the two card types stay identical.
  const sourceWrap = document.createElement("p");
  sourceWrap.className = "ls-card-source-wrap";
  const sourceLink = document.createElement("a");
  sourceLink.className = "ls-card-source-link";
  sourceLink.href =
    "https://weather.gc.ca/marine/weatherConditions-lightstation_e.html?mapID=02&siteID=16200";
  sourceLink.target = "_blank";
  sourceLink.rel = "noopener noreferrer";
  sourceLink.setAttribute(
    "aria-label",
    `${station.name} on Environment Canada's Lightstation Reports (opens weather.gc.ca in a new tab)`,
  );
  sourceLink.textContent = "🔗 View Source Data";
  sourceWrap.appendChild(sourceLink);
  card.appendChild(sourceWrap);

  return card;
}

function createConditionRow(label, value) {
  const row = document.createElement("div");
  row.className = "condition-row";

  const labelEl = document.createElement("span");
  labelEl.className = "condition-label";
  labelEl.textContent = label + ":";

  const valueEl = document.createElement("span");
  valueEl.className = "condition-value";
  valueEl.textContent = value;

  row.appendChild(labelEl);
  row.appendChild(valueEl);

  return row;
}

// Function to show selected lightstation on map (from dropdown)
function showSelectedLightstationOnMap() {
  const select = document.getElementById("lightstation-station-select");
  if (!select || !select.value) {
    console.warn("No lightstation selected");
    return;
  }

  const stationName = select.value;

  // Get station ID from metadata
  if (stationMetadata[stationName]) {
    const stationId = stationMetadata[stationName].id;

    // Scroll to map section
    const mapSection = document.getElementById("lightstation-map-section");
    if (mapSection) {
      mapSection.scrollIntoView({ behavior: "smooth", block: "start" });
    }

    // Center map on lightstation after scroll
    setTimeout(() => {
      centerMapOnLightstation(stationId);
    }, 800);
  }
}

// "Show on Map" button next to the chart dropdown (listener moved here
// from lightstation-charts.js — the handler is this module's function)
const lightstationMapBtn = document.getElementById("show-lightstation-on-map-btn");
if (lightstationMapBtn) {
  lightstationMapBtn.addEventListener("click", showSelectedLightstationOnMap);
}

// Load data on page load
loadLightstationData();
