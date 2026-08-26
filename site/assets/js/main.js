/* -----------------------------
   Index Page - Buoy Cards (ES module entry point)

   Importing charts-v4.js and stations-map.js is what pulls the whole
   module graph onto the page. degreesToCardinal, getDirectionalArrow,
   fetchWithTimeout, and logger still come from classic scripts loaded
   before this one (chart-utils-v4.js, logger.js).
   ----------------------------- */

import { applyCardBorder, buildBuoyCardHTML, wireBuoyCardEvents } from "./buoy-card.js";
import { buildHistoryTableHTML } from "./buoy-history.js";
import { applyWaveThreshold, clearWaveThreshold, setTimeRange } from "./charts-v4.js";
import { formatNumericDayTime } from "./shared/format-time.js";
import { formatDataAge } from "./shared/staleness.js";
import { centerMapOnBuoy, showSelectedBuoyOnMap, showSelectedSurgeOnMap } from "./stations-map.js";
import { setSafeHTML } from "./shared/safe-html.js";

// Station metadata from stations.json (single source of truth for
// per-station behavior — see docs/project/BUOY_CARD_REFACTOR.md).
// Populated by loadBuoyData(); missing entries fall back to EC-style
// defaults inside the shared predicates.
let stationMeta = {};

// Latest buoy snapshot, cached so the hero panel can re-render if the hero
// component swaps in (via HTMX) after the data has already been fetched.
let heroBuoySnapshot = null;

// Render the live conditions panel over the hero image. Reuses the snapshot
// already loaded by loadBuoyData() — no second network request.
function renderHeroConditions(buoy) {
  const el = document.getElementById("hero-conditions");
  if (!el || !buoy) return;

  const dash = "—";
  // Wind speed/gust are already in knots in the JSON — display as-is, rounded.
  const fmtKt = (v) => (v == null ? dash : `${Math.round(v)} kt`);
  // Period: significant period (EC publishes it under two SWOB names), falling
  // back to avg then peak — matches the EC buoy card convention.
  const period =
    buoy.wave_period_sig ??
    buoy.wave_period_sig_basic ??
    buoy.wave_period_avg ??
    buoy.wave_period_peak;

  const stat = (label, value) =>
    `<div class="hero-stat"><span class="hero-stat-label">${label}</span>` +
    `<span class="hero-stat-value">${value}</span></div>`;

  const age = formatDataAge(buoy.age_minutes);
  const updatedText = age ? `Updated ${age}` : "Update time unavailable";

  // Wind direction: cardinal label plus an arrow rotated to the wind direction
  // (reuses the same helper/convention as the buoy cards).
  const windCardinal = buoy.wind_direction_cardinal ?? dash;
  const windArrow =
    buoy.wind_direction != null ? getDirectionalArrow(buoy.wind_direction, "wind") : "";
  const windDirValue = `${windCardinal}${windArrow}`;

  // Wave direction: cardinal + arrow, peak falling back to avg — matches the EC
  // buoy card convention.
  const waveCardinal =
    buoy.wave_direction_peak_cardinal ?? buoy.wave_direction_avg_cardinal ?? dash;
  const waveDirDeg = buoy.wave_direction_peak ?? buoy.wave_direction_avg;
  const waveArrow = waveDirDeg != null ? getDirectionalArrow(waveDirDeg, "wave") : "";
  const waveDirValue = `${waveCardinal}${waveArrow}`;

  const stationName = buoy.name || "Halibut Bank";

  el.innerHTML = `
    <div class="hero-station">${stationName} · live conditions</div>
    <div class="hero-cond-group">
      <span class="hero-group-label">Waves</span>
      ${stat("Direction", waveDirValue)}
      ${stat("Sig. height", buoy.wave_height_sig != null ? `${buoy.wave_height_sig} m` : dash)}
      ${stat("Sig. period", period != null ? `${period} s` : dash)}
    </div>
    <div class="hero-cond-group">
      <span class="hero-group-label">Wind</span>
      ${stat("Direction", windDirValue)}
      ${stat("Speed", fmtKt(buoy.wind_speed))}
      ${stat("Gust", fmtKt(buoy.wind_gust))}
    </div>
    <div class="hero-updated${buoy.stale ? " stale" : ""}">${updatedText}</div>
  `;
}

// Show the scroll affordances only while the content actually overflows: the
// "← Scroll table horizontally →" banner above the table, and the grab cursor
// on the scroller itself.
//
// Overflow is what matters, not viewport width — a wide phone or a narrow
// desktop card can go either way, and the history table's width depends on its
// column contents. Re-measured on resize (card width changes with the grid's
// breakpoints, not just the window).
function trackScrollAffordance(el) {
  const hint = el.parentElement?.querySelector(".history-scroll-hint");

  const update = () => {
    // 1px tolerance: sub-pixel layout rounding can report a phantom overflow.
    const overflows = el.scrollWidth - el.clientWidth > 1;
    el.classList.toggle("draggable", overflows);
    if (hint) hint.hidden = !overflows;
  };

  update();
  if (typeof ResizeObserver === "function") {
    new ResizeObserver(update).observe(el);
  }
}

// Enable click-and-drag horizontal panning on an overflow-x:auto container,
// matching the touch-drag behaviour mobile already gets natively. Mouse only —
// touch is left to the browser's native scrolling so we don't fight it.
function enableDragScroll(el) {
  if (!el || el.dataset.dragScroll === "on") return;
  el.dataset.dragScroll = "on";
  trackScrollAffordance(el);

  let isDown = false;
  let startX = 0;
  let startScroll = 0;

  // Is the pointer over the horizontal scrollbar gutter rather than content?
  // The scrollbar belongs to this element, so without this check pointerdown on
  // the thumb starts a drag and setPointerCapture steals the event from the
  // browser's native thumb handling — the content then pans 1:1 with the mouse
  // instead of tracking the thumb.
  //
  // clientHeight excludes the scrollbar, so anything past it inside the padding
  // box is gutter. Overlay scrollbars (macOS, mobile) reserve no space, making
  // this always false — correct, since there is no gutter to protect.
  function isOnScrollbar(e) {
    const borderTop = parseFloat(getComputedStyle(el).borderTopWidth) || 0;
    const y = e.clientY - el.getBoundingClientRect().top - borderTop;
    return y > el.clientHeight;
  }

  el.addEventListener("pointerdown", (e) => {
    if (e.pointerType !== "mouse" || e.button !== 0) return;
    if (isOnScrollbar(e)) return; // let the native scrollbar do its job
    isDown = true;
    startX = e.clientX;
    startScroll = el.scrollLeft;
    el.setPointerCapture(e.pointerId);
    el.classList.add("dragging");
  });

  el.addEventListener("pointermove", (e) => {
    if (!isDown) return;
    e.preventDefault(); // suppress text selection while dragging
    el.scrollLeft = startScroll - (e.clientX - startX);
  });

  const end = (e) => {
    if (!isDown) return;
    isDown = false;
    if (el.hasPointerCapture(e.pointerId)) el.releasePointerCapture(e.pointerId);
    el.classList.remove("dragging");
  };
  el.addEventListener("pointerup", end);
  el.addEventListener("pointercancel", end);
}

async function loadBuoyData() {
  const container = document.getElementById("buoy-container");

  // Grouped by geographic region
  const buoyGroups = [
    {
      region: "Strait of Georgia",
      stations: [
        "4600146", // Halibut Bank
        "4600304", // English Bay
        "4600303", // Southern Georgia Strait
        "4600131", // Sentry Shoal
      ],
    },
    {
      region: "Boundary Bay",
      stations: [
        "CRPILE", // Crescent Beach Ocean
        "CRCHAN", // Crescent Channel
      ],
    },
    {
      region: "Juan de Fuca Strait",
      stations: [
        "46087", // Neah Bay
        "46088", // New Dungeness
        "46267", // Angeles Point
      ],
    },
    {
      region: "West Coast Vancouver Island",
      stations: [
        "4600206", // La Perouse Bank
      ],
    },
  ];
  // COLEB excluded - wind-only station, available in charts only

  try {
    // Metadata is optional: if stations.json fails, cards render with
    // EC-style defaults rather than not at all.
    const [data, stationsData] = await Promise.all([
      fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`),
      fetchWithTimeout("/data/stations.json").catch((err) => {
        logger.warn("BuoyData", "stations.json unavailable, using defaults", err);
        return null;
      }),
    ]);
    if (stationsData?.buoys) stationMeta = stationsData.buoys;

    // Cache + render the hero conditions panel (Halibut Bank) from this snapshot.
    heroBuoySnapshot = data;
    renderHeroConditions(data["4600146"]);

    container.textContent = "";

    // Find most recent observation time
    let mostRecentTime = null;
    Object.values(data).forEach((buoy) => {
      if (buoy.observation_time) {
        const time = new Date(buoy.observation_time);
        if (!mostRecentTime || time > mostRecentTime) {
          mostRecentTime = time;
        }
      }
    });

    // Add "Last Updated" header (24-hour, shorter format: "11/11 19:58")
    if (mostRecentTime) {
      const updateHeader = document.createElement("div");
      updateHeader.className = "last-updated-header";
      updateHeader.textContent = `Last Updated: ${formatNumericDayTime(mostRecentTime)}`;
      container.appendChild(updateHeader);
    }

    // Render buoys grouped by region
    buoyGroups.forEach((group) => {
      // Create region group container
      const regionGroup = document.createElement("div");
      regionGroup.className = "region-group";

      // Add region header (clickable to collapse/expand)
      const regionHeader = document.createElement("div");
      regionHeader.className = "region-header";
      regionHeader.style.cursor = "pointer";
      regionHeader.style.userSelect = "none";
      setSafeHTML(
        regionHeader,
        `<span class="region-toggle-btn">▼</span> ${group.region} <span class="region-station-count">(${group.stations.length} stations)</span>`,
      );
      regionHeader.addEventListener("click", () => toggleRegion(group.region));
      regionGroup.appendChild(regionHeader);
      regionGroup.id = `region-${group.region.replace(/\s+/g, "-")}`;

      // Create grid container for this region's cards
      const cardsGrid = document.createElement("div");
      cardsGrid.className = "buoy-cards-grid";

      // Render stations in this region
      group.stations.forEach((id) => {
        const b = data[id];
        if (!b) return;

        const meta = stationMeta[id];

        const card = document.createElement("div");
        card.className = "buoy-card";
        card.id = `buoy-${id}`; // Add ID for anchor linking from map
        applyCardBorder(card, meta);

        setSafeHTML(card, buildBuoyCardHTML(b, id, meta, formatNumericDayTime));

        wireBuoyCardEvents(card, id, {
          onDetails: toggleCardDetails,
          onHistory: toggleCardHistory,
          onSpreadInfo: toggleSpreadInfo,
          onMap: scrollToMap,
          onCharts: scrollToCharts,
        });

        cardsGrid.appendChild(card);
      }); // end stations forEach

      // Add grid to region group, then add region group to container
      regionGroup.appendChild(cardsGrid);
      container.appendChild(regionGroup);

      // Collapse Boundary Bay and Juan de Fuca by default (keep Strait of Georgia expanded)
      if (group.region !== "Strait of Georgia") {
        const toggleBtn = regionHeader.querySelector(".region-toggle-btn");
        if (toggleBtn && cardsGrid) {
          cardsGrid.style.display = "none";
          toggleBtn.textContent = "▶";
        }
      }
    }); // end buoyGroups forEach

    // Handle hash navigation after cards are loaded
    handleHashNavigation();
  } catch (err) {
    logger.error("BuoyData", "Error loading buoy data", err);
    setSafeHTML(
      container,
      `<p class="error">⚠️ Error loading buoy data. Please try again later.</p>`,
    );
  }
}

// Scroll to charts section and select buoy
function scrollToCharts(buoyId) {
  const buoySelector = document.getElementById("buoy-selector");
  const chartsSection = document.getElementById("charts-section");

  // Prefer scrolling to the selector dropdown if it exists
  const scrollTarget = buoySelector || chartsSection;
  if (!scrollTarget) return;

  // Smooth scroll to buoy selector
  scrollTarget.scrollIntoView({ behavior: "smooth", block: "start" });

  // Wait for scroll to complete, then trigger chart selection
  setTimeout(() => {
    const chartSelect = document.getElementById("chart-buoy-select");
    if (chartSelect) {
      chartSelect.value = buoyId;
      chartSelect.dispatchEvent(new Event("change"));

      // Add highlight pulse effect to the selector
      if (buoySelector) {
        buoySelector.classList.add("highlight-pulse");
        setTimeout(() => buoySelector.classList.remove("highlight-pulse"), 2000);
      }
    }
  }, 800);
}

// Scroll to map section and center on buoy
function scrollToMap(buoyId) {
  const mapSection = document.getElementById("map-section");
  if (!mapSection) return;

  // Smooth scroll to map section
  mapSection.scrollIntoView({ behavior: "smooth", block: "start" });

  // Wait for scroll to complete, then center map on buoy
  setTimeout(() => {
    centerMapOnBuoy(buoyId);

    // Add highlight pulse effect
    mapSection.classList.add("highlight-pulse");
    setTimeout(() => mapSection.classList.remove("highlight-pulse"), 2000);
  }, 800);
}

// Toggle region collapse/expand
function toggleRegion(regionName) {
  const regionGroup = document.getElementById(`region-${regionName.replace(/\s+/g, "-")}`);
  if (!regionGroup) return;

  const cardsGrid = regionGroup.querySelector(".buoy-cards-grid");
  const toggleBtn = regionGroup.querySelector(".region-toggle-btn");

  if (cardsGrid && toggleBtn) {
    const isHidden = cardsGrid.style.display === "none";
    cardsGrid.style.display = isHidden ? "grid" : "none";
    toggleBtn.textContent = isHidden ? "▼" : "▶";

    // Save state to localStorage
    const regionKey = `region-${regionName}-collapsed`;
    localStorage.setItem(regionKey, isHidden ? "false" : "true");
  }
}

// Is the element currently hidden? Reads the *computed* display, not the
// inline one: these sections get their initial `display: none` from the
// .card-collapsible / .spread-explainer CSS classes, so the inline property is
// empty until the first toggle. (Checking el.style.display made the first
// click a no-op — every toggle needed two presses.)
function isElementHidden(el) {
  return getComputedStyle(el).display === "none";
}

// Toggle card details (full metrics)
function toggleCardDetails(buoyId) {
  const detailsDiv = document.getElementById(`card-details-${buoyId}`);
  const button = document.querySelector(`#buoy-${buoyId} .toggle-details-btn`);

  if (detailsDiv && button) {
    const isHidden = isElementHidden(detailsDiv);
    detailsDiv.style.display = isHidden ? "block" : "none";
    button.textContent = isHidden ? "▲ Hide Details" : "▼ Show Details";
  }
}

// Toggle angular spread explanation
function toggleSpreadInfo(buoyId) {
  const infoDiv = document.getElementById(`spread-info-${buoyId}`);
  if (infoDiv) {
    const isHidden = isElementHidden(infoDiv);
    infoDiv.style.display = isHidden ? "block" : "none";
  }
}

// Toggle card history table
async function toggleCardHistory(buoyId) {
  const historyDiv = document.getElementById(`card-history-${buoyId}`);
  const button = document.querySelector(`#buoy-${buoyId} .toggle-history-btn`);

  if (!historyDiv || !button) return;

  const isHidden = isElementHidden(historyDiv);

  if (isHidden) {
    // Auto-collapse Details section when opening History
    const detailsDiv = document.getElementById(`card-details-${buoyId}`);
    const detailsButton = document.querySelector(`#buoy-${buoyId} .toggle-details-btn`);
    if (detailsDiv && !isElementHidden(detailsDiv)) {
      detailsDiv.style.display = "none";
      if (detailsButton) detailsButton.textContent = "▼ Show Details";
    }

    // Load and display history
    button.textContent = "Loading...";
    button.disabled = true;

    try {
      const timeseriesData = await fetchWithTimeout(
        `/data/buoy_timeseries_48h.json?t=${Date.now()}`,
      );
      const buoyData = timeseriesData[buoyId];

      if (buoyData && buoyData.timeseries) {
        setSafeHTML(historyDiv, renderHistoryTable(buoyId, buoyData.timeseries));
        enableDragScroll(historyDiv.querySelector(".history-scroll"));
        const hideBtn = historyDiv.querySelector(".hide-history-btn");
        if (hideBtn) hideBtn.addEventListener("click", () => toggleCardHistory(buoyId));
        historyDiv.style.display = "block";
        button.textContent = "▲ Hide History";
        button.disabled = false;
      } else {
        setSafeHTML(historyDiv, '<p class="history-empty">No historical data available</p>');
        historyDiv.style.display = "block";
        button.textContent = "▲ Hide History";
        button.disabled = false;
      }
    } catch (error) {
      logger.error("BuoyData", "Error loading history", error);
      setSafeHTML(historyDiv, '<p class="history-error">Error loading historical data</p>');
      historyDiv.style.display = "block";
      button.textContent = "▲ Hide History";
      button.disabled = false;
    }
  } else {
    historyDiv.style.display = "none";
    button.textContent = "▼ Show History (12h)";
  }
}

// Render history table. Structure and per-station field selection live in
// buoy-history.js; main.js only supplies the viewport-dependent bits.
function renderHistoryTable(buoyId, timeseries) {
  return buildHistoryTableHTML(timeseries, stationMeta[buoyId]);
}

// Handle hash navigation from map (e.g., /#buoy-46087)
function handleHashNavigation() {
  const hash = window.location.hash;
  if (hash && hash.startsWith("#buoy-")) {
    const buoyId = hash.replace("#buoy-", "");
    const buoyCard = document.getElementById(`buoy-${buoyId}`);

    if (buoyCard) {
      // Find the parent region
      const regionGroup = buoyCard.closest(".region-group");
      if (regionGroup) {
        const cardsGrid = regionGroup.querySelector(".buoy-cards-grid");
        const toggleBtn = regionGroup.querySelector(".region-toggle-btn");

        // Expand region if collapsed
        if (cardsGrid && cardsGrid.style.display === "none") {
          cardsGrid.style.display = "grid";
          if (toggleBtn) toggleBtn.textContent = "▼";
        }
      }

      // Scroll to the card after a short delay to ensure rendering
      setTimeout(() => {
        buoyCard.scrollIntoView({ behavior: "smooth", block: "center" });
        buoyCard.classList.add("highlight-pulse");
        setTimeout(() => buoyCard.classList.remove("highlight-pulse"), 2000);
      }, 300);
    }
  }
}

// Wait for HTMX to load the shared components before initializing
document.addEventListener(
  "htmx:load",
  function () {
    loadBuoyData();
  },
  { once: true },
);

// The hero header loads via HTMX independently of the data fetch; if it swaps
// in after the snapshot has already arrived, render the cached conditions.
document.addEventListener("htmx:load", function () {
  if (heroBuoySnapshot && document.getElementById("hero-conditions")) {
    renderHeroConditions(heroBuoySnapshot["4600146"]);
  }
});

setInterval(loadBuoyData, 5 * 60 * 1000);

// Handle hash navigation when hash changes (clicking map links)
window.addEventListener("hashchange", handleHashNavigation);

// Auto-open site intro on desktop (replaces inline script removed for CSP compliance)
(function () {
  const d = document.querySelector(".site-intro-details");
  if (d && window.innerWidth > 600) d.open = true;
})();

// Event listeners replacing onclick= attributes (CSP compliance)
document.addEventListener("click", function (e) {
  const btn = e.target.closest(".time-range-btn");
  if (btn) {
    const hours = parseInt(btn.dataset.hours, 10);
    if (hours) setTimeRange(hours);
  }
});

const applyBtn = document.getElementById("apply-wave-threshold-btn");
if (applyBtn)
  applyBtn.addEventListener("click", function () {
    applyWaveThreshold();
  });

const clearBtn = document.getElementById("clear-wave-threshold-btn");
if (clearBtn)
  clearBtn.addEventListener("click", function () {
    clearWaveThreshold();
  });

const buoyMapBtn = document.getElementById("show-buoy-on-map-btn");
if (buoyMapBtn)
  buoyMapBtn.addEventListener("click", function (e) {
    showSelectedBuoyOnMap(e);
  });

const surgeMapBtn = document.getElementById("show-surge-on-map-btn");
if (surgeMapBtn)
  surgeMapBtn.addEventListener("click", function (e) {
    showSelectedSurgeOnMap(e);
  });
