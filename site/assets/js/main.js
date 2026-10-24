/* -----------------------------
   Index Page - Station List (ES module entry point)

   Importing charts-v4.js and stations-map.js is what pulls the whole
   module graph onto the page. fetchWithTimeout and logger still come from
   classic scripts loaded before this one (chart-utils-v4.js, logger.js).
   ----------------------------- */

import { buildHistoryTableHTML } from "./buoy-history.js";
import {
  applyWaveThreshold,
  clearWaveThreshold,
  getBuoyTimeseries,
  setTimeRange,
} from "./charts-v4.js";
import { setSafeHTML } from "./shared/safe-html.js";
import { buildStationDetail, buildStationList, refreshAges } from "./station-list.js";
import { centerMapOnBuoy, showSelectedBuoyOnMap, showSelectedSurgeOnMap } from "./stations-map.js";

// Station metadata from stations.json (single source of truth for
// per-station behavior — see docs/project/BUOY_CARD_REFACTOR.md).
// Populated by loadBuoyData(); missing entries fall back to EC-style
// defaults inside the shared predicates.
let stationMeta = {};

// Latest buoy snapshot, kept so a row's panel can be built when it opens.
let latestSnapshot = {};

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

// Station ids whose rows are open, and whose history is open, so the
// five-minute refresh can rebuild the list without collapsing anything.
function openState(container) {
  const ids = (selector) =>
    [...container.querySelectorAll(selector)].map((el) => el.closest(".sl-item").id.slice(5));
  return {
    rows: ids('.sl-row[aria-expanded="true"]'),
    histories: ids('[data-action="history"][aria-expanded="true"]'),
  };
}

// Rebuild the list, keeping open rows, open histories and focus.
function renderStationList(container, data) {
  const { rows, histories } = openState(container);
  const focusedId = document.activeElement?.closest?.("#station-list .sl-item")?.id;

  setSafeHTML(container, buildStationList(data, stationMeta));

  for (const id of rows) toggleRow(id, true);
  for (const id of histories) toggleHistory(id, true);
  if (focusedId) document.querySelector(`#${focusedId} .sl-row`)?.focus();
}

// Ages count up between the five-minute fetches: "1h 25m" has to be right to
// the minute. Only the text changes, unless a station crosses into late or
// down, which changes what its row shows and so rebuilds the list.
function tickAges() {
  const container = document.getElementById("station-list");
  if (!container || !latestSnapshot) return;
  if (refreshAges(container, latestSnapshot)) renderStationList(container, latestSnapshot);
}

async function loadBuoyData() {
  const container = document.getElementById("station-list");
  if (!container) return;

  try {
    // Metadata is optional: if stations.json fails, rows render with
    // EC-style defaults rather than not at all.
    const [data, stationsData] = await Promise.all([
      fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`),
      fetchWithTimeout("/data/stations.json").catch((err) => {
        logger.warn("BuoyData", "stations.json unavailable, using defaults", err);
        return null;
      }),
    ]);
    if (stationsData?.buoys) stationMeta = stationsData.buoys;
    latestSnapshot = data;

    renderStationList(container, data);
    container.removeAttribute("aria-busy");

    handleHashNavigation();
  } catch (err) {
    logger.error("BuoyData", "Error loading buoy data", err);
    setSafeHTML(
      container,
      `<p class="sl-error">Couldn't load the latest readings. Try again in a few minutes.</p>`,
    );
  }
}

// Open or close one station's row; its panel is built the first time.
function toggleRow(id, open) {
  const item = document.getElementById(`buoy-${id}`);
  const row = item?.querySelector(".sl-row");
  const detail = item?.querySelector(".sl-detail");
  if (!row || !detail) return;

  const opening = open ?? row.getAttribute("aria-expanded") !== "true";
  if (opening && !detail.hasChildNodes() && latestSnapshot[id]) {
    setSafeHTML(detail, buildStationDetail(latestSnapshot[id], id, stationMeta[id]));
  }
  row.setAttribute("aria-expanded", String(opening));
  detail.hidden = !opening;
}

// Show or hide the 12-hour table inside an open panel, fetched on first use.
async function toggleHistory(id, open) {
  const item = document.getElementById(`buoy-${id}`);
  const button = item?.querySelector('[data-action="history"]');
  const slot = item?.querySelector(".sl-history");
  if (!button || !slot) return;

  const opening = open ?? button.getAttribute("aria-expanded") !== "true";
  button.setAttribute("aria-expanded", String(opening));
  slot.hidden = !opening;
  if (!opening || slot.hasChildNodes()) return;

  setSafeHTML(slot, '<p class="history-empty">Loading…</p>');
  try {
    const timeseries = (await getBuoyTimeseries())[id]?.timeseries;
    if (timeseries) {
      setSafeHTML(slot, buildHistoryTableHTML(timeseries, stationMeta[id]));
      enableDragScroll(slot.querySelector(".history-scroll"));
    } else {
      setSafeHTML(slot, '<p class="history-empty">No history available</p>');
    }
  } catch (error) {
    logger.error("BuoyData", "Error loading history", error);
    setSafeHTML(slot, '<p class="history-error">Couldn\'t load the history</p>');
  }
}

// One listener for every row and panel control; the list is rebuilt on each
// refresh, so per-element listeners would have to be re-attached each time.
function wireStationList() {
  const container = document.getElementById("station-list");
  if (!container) return;
  container.addEventListener("click", (e) => {
    const item = e.target.closest(".sl-item");
    if (!item) return;
    const id = item.id.slice(5);

    if (e.target.closest(".sl-row")) {
      toggleRow(id);
      return;
    }
    if (e.target.closest(".hide-history-btn")) {
      toggleHistory(id, false);
      item.querySelector('[data-action="history"]')?.focus();
      return;
    }
    const action = e.target.closest("button[data-action]")?.dataset.action;
    if (action === "history") toggleHistory(id);
    else if (action === "charts") scrollToCharts(id);
    else if (action === "map") scrollToMap(id);
  });
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

  mapSection.scrollIntoView({ behavior: "smooth", block: "start" });
  centerMapOnBuoy(buoyId);
}

// A map popup's "View data" link lands on /#buoy-<id>: open that row.
function handleHashNavigation() {
  const hash = window.location.hash;
  if (!hash.startsWith("#buoy-")) return;
  const id = hash.slice(6);
  const item = document.getElementById(`buoy-${id}`);
  if (!item) return;

  toggleRow(id, true);
  setTimeout(() => {
    item.scrollIntoView({ behavior: "smooth", block: "center" });
    item.classList.add("highlight-pulse");
    setTimeout(() => item.classList.remove("highlight-pulse"), 2000);
  }, 300);
}

// The list is static markup, so the fetch starts at once rather than
// waiting for the HTMX components: it is the first thing on the page.
wireStationList();
loadBuoyData();

// The map key folds away when the reader clicks or taps anywhere at all
// (the map, the page, or the open key itself) or presses Escape. Its own
// summary is left to the browser, which toggles it.
// Off the key: pointerdown, in the capture phase, because Leaflet stops some
// pointer events on its markers and controls from bubbling.
// On the key: click, NOT pointerdown. Closing the <details> while a press
// inside its own content is in progress crashes the Chromium renderer
// (reproduced 2026-10-24, Playwright Chromium 145).
function wireMapKey() {
  const key = document.querySelector(".map-key");
  const summary = key?.querySelector("summary");
  if (!key || !summary) return;
  document.addEventListener(
    "pointerdown",
    (e) => {
      if (key.open && !key.contains(e.target)) key.open = false;
    },
    true,
  );
  key.addEventListener("click", (e) => {
    if (key.open && !summary.contains(e.target)) key.open = false;
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && key.open) {
      key.open = false;
      key.querySelector("summary")?.focus();
    }
  });
}
wireMapKey();

setInterval(loadBuoyData, 5 * 60 * 1000);
setInterval(tickAges, 60 * 1000);

// Handle hash navigation when hash changes (clicking map links)
window.addEventListener("hashchange", handleHashNavigation);

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
