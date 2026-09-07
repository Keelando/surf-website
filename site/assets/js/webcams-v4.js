/* ==========================================================================
   Webcams Page - Main JavaScript (ES module)
   ========================================================================== */

import { formatFullTimestamp, formatMonthDayTime } from "./shared/format-time.js";
import { createAngularSpreadVectorElement } from "./shared/markers.js";
import { setSafeHTML } from "./shared/safe-html.js";

// ==========================================================================
// Configuration
// ==========================================================================

const webcamRegions = {
  english_bay: { name: "English Bay - Vancouver" },
  salish_sea_south: { name: "Salish Sea - Boundary Bay - White Rock" },
  west_coast_vi: { name: "West Coast Vancouver Island (Tofino)" },
};

// Staleness, in wall-clock minutes rather than multiples of each cam's update
// interval. The interval multiple (3x) that used to drive this meant the six
// cams crossed into STALE at four different ages, none of which meant anything
// to a reader looking at a picture; and the DOWN threshold sat at 24 h, which
// is a day of showing a dead camera as merely "stale".
//
// The site's closest precedent is the winds page (`wind-stations.js`): 2 h
// dimmed, 4 h moved to the offline list. Webcams are tighter because they
// update every 10-20 min, so an hour is already three to six missed frames.
const STALE_THRESHOLD_MINUTES = 60;
const DOWN_THRESHOLD_MINUTES = 180;

// Daylight-only cams stop overnight BY DESIGN — fetch_webcam.py skips them
// outside [sunrise - margin, sunset + margin]. Judged on wall-clock age they
// would all show DOWN every night, so their age is measured from whichever is
// later, the last frame or the moment the capture window opened.
//
// `daylightOnly` and `daylightMarginMinutes` come from /data/stations.json,
// the same registry fields fetch_webcam.py reads, so this page cannot state a
// window the capture does not use. The sunrise/sunset comes from
// /data/sunlight_times.json, which carries an entry per camera at the camera's
// own position — keyed `webcam_<id>` because `whiterock` is both a camera id
// and a tide station key and the tide station used to win.
const WEBCAM_SUNLIGHT_PREFIX = "webcam_";

// What this PAGE adds to a camera: which section it sits in, whose readings to
// show beside it, whose credit to print, and where its files live. Everything
// else — name, location, cadence, stream delay, daylight policy — is read from
// /data/stations.json by hydrateWebcams(), because config/stations.json owns
// it. See lib/webcam/registry.py for the backend half of the same split.
const webcams = [
  {
    id: "ambleside",
    region: "english_bay",
    dataPath: "/data/ambleside/",
    attribution: {
      text: "Webcam screenshots provided by Hollyburn Sailing Club",
      url: "https://www.hollyburnsailingclub.ca/",
    },
    conditions: [
      { label: "English Bay Buoy", buoyStation: "4600304", fields: ["wind", "waves"] },
      { label: "Jericho Sailing Centre", windStation: "JERICHO", fields: ["wind"] },
    ],
  },
  {
    id: "whiterock",
    region: "salish_sea_south",
    dataPath: "/data/wrcam/",
    conditions: [
      {
        label: "White Rock East Beach",
        customStation: "whiterock_east",
        fields: ["wind_speed_only"],
      },
      { label: "Crescent Pile", buoyStation: "CRPILE", fields: ["wind", "waves"] },
    ],
  },
  {
    id: "boundarybay",
    region: "salish_sea_south",
    dataPath: "/data/bbcam/",
    conditions: [
      {
        label: "White Rock East Beach",
        customStation: "whiterock_east",
        fields: ["wind_speed_only"],
      },
      { label: "Crescent Pile", buoyStation: "CRPILE", fields: ["wind", "waves"] },
    ],
  },
  {
    id: "mudbay",
    region: "salish_sea_south",
    dataPath: "/data/mudbay/",
    conditions: [
      {
        label: "White Rock East Beach",
        customStation: "whiterock_east",
        fields: ["wind_speed_only"],
      },
      { label: "Crescent Pile", buoyStation: "CRPILE", fields: ["wind", "waves"] },
    ],
  },
  {
    id: "mudbay_sw",
    region: "salish_sea_south",
    dataPath: "/data/mudbay_sw/",
    conditions: [
      {
        label: "White Rock East Beach",
        customStation: "whiterock_east",
        fields: ["wind_speed_only"],
      },
      { label: "Crescent Pile", buoyStation: "CRPILE", fields: ["wind", "waves"] },
    ],
  },
  {
    id: "coxbay",
    region: "west_coast_vi",
    dataPath: "/data/coxbay/",
    conditions: [
      { label: "La Perouse Bank", buoyStation: "4600206", fields: ["wind", "waves_detailed"] },
    ],
  },
];

/**
 * Fill each entry above with what config/stations.json says the camera is.
 *
 * Mutates in place: the array is captured by the auto-refresh timer and the
 * slideshow lookups, so replacing it would leave those holding stale objects.
 * A camera the registry does not list is dropped rather than rendered with
 * blank headings — the registry is the roster, not a decoration on it.
 *
 * @returns {Promise<void>} resolves once `webcams` is safe to render
 */
async function hydrateWebcams() {
  let registry = {};
  try {
    const response = await fetch("/data/stations.json");
    registry = (await response.json()).webcams || {};
  } catch (error) {
    console.error("Failed to load station registry for webcams:", error);
  }

  for (let i = webcams.length - 1; i >= 0; i--) {
    const webcam = webcams[i];
    const meta = registry[webcam.id];
    if (!meta) {
      console.warn(`Webcam ${webcam.id} is not in stations.json; not rendering it`);
      webcams.splice(i, 1);
      continue;
    }
    Object.assign(webcam, {
      name: meta.name,
      location: meta.location,
      updateInterval: meta.update_frequency_minutes,
      streamDelay: meta.stream_delay_minutes ?? null,
      daylightOnly: Boolean(meta.daylight_only),
      daylightMarginMinutes: meta.daylight_margin_minutes,
      dataUrl: webcam.dataPath + "latest.json",
      imageUrl: webcam.dataPath + "latest.jpg",
      slideshowUrl: webcam.dataPath + "slideshow_manifest.json",
      slideshowPath: webcam.dataPath,
    });
  }
}

// ==========================================================================
// State
// ==========================================================================

let cachedMarineData = null;
const slideshowState = {};

// ==========================================================================
// Utility Functions
// ==========================================================================

function createElement(tag, className, content) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (content) el.textContent = content;
  return el;
}

// ==========================================================================
// Data Fetching
// ==========================================================================

async function fetchMarineData() {
  if (cachedMarineData) return cachedMarineData;

  try {
    const [buoyResponse, windResponse, whiterockResponse] = await Promise.all([
      fetch("/data/latest_buoy_v2.json"),
      fetch("/data/latest_wind.json"),
      fetch("/data/whiterock_weather.json"),
    ]);

    cachedMarineData = {
      buoy: buoyResponse.ok ? await buoyResponse.json() : {},
      wind: windResponse.ok ? await windResponse.json() : {},
      custom: {
        whiterock_east: whiterockResponse.ok ? await whiterockResponse.json() : null,
      },
    };
    return cachedMarineData;
  } catch (error) {
    console.error("Failed to fetch marine data:", error);
    return { buoy: {}, wind: {}, custom: {} };
  }
}

// Sunrise/sunset for the daylight-only cams. One fetch, cached for the page's
// life: the file carries several days and only changes nightly.
let cachedSunlightTimes = null;
let sunlightTimesPromise = null;

async function fetchSunlightTimes() {
  if (cachedSunlightTimes) return cachedSunlightTimes;
  if (!sunlightTimesPromise) {
    sunlightTimesPromise = fetch("/data/sunlight_times.json")
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        cachedSunlightTimes = data;
        return data;
      })
      .catch((error) => {
        console.error("Failed to load sunlight times:", error);
        return null;
      });
  }
  return sunlightTimesPromise;
}

/**
 * When the capture window opened for a daylight-only cam, and whether we are
 * inside it now.
 *
 * Returns null for a 24/7 cam, and also whenever the answer is unknown — a
 * missing file, an unlisted station, a date the export does not cover. A null
 * makes the caller fall back to plain wall-clock age, which is the safe
 * direction: it can over-report staleness, never hide a dead camera.
 *
 * @param {Object} webcam - Entry from `webcams`
 * @param {number} now - Epoch ms
 * @returns {{openedAt: number, closesAt: number, isOpen: boolean}|null}
 */
function captureWindow(webcam, now) {
  const marginMinutes = webcam.daylightMarginMinutes;
  if (!webcam.daylightOnly || !marginMinutes || !cachedSunlightTimes) return null;

  const station = cachedSunlightTimes.stations?.[WEBCAM_SUNLIGHT_PREFIX + webcam.id];
  if (!station?.days) return null;

  const margin = marginMinutes * 60 * 1000;

  // The export keys days by local date, and a window can span UTC midnight, so
  // scan every day it holds and take the one containing `now` — falling back to
  // the most recent window that has already opened.
  let current = null;
  let latestOpened = null;
  for (const day of Object.values(station.days)) {
    if (!day?.sunrise || !day?.sunset) continue;
    const openedAt = new Date(day.sunrise).getTime() - margin;
    const closesAt = new Date(day.sunset).getTime() + margin;
    if (Number.isNaN(openedAt) || Number.isNaN(closesAt)) continue;
    if (now >= openedAt && now <= closesAt) current = { openedAt, closesAt, isOpen: true };
    if (openedAt <= now && (!latestOpened || openedAt > latestOpened.openedAt)) {
      latestOpened = { openedAt, closesAt, isOpen: false };
    }
  }
  return current || latestOpened;
}

/**
 * Age of the displayed frame in minutes, and whether the cam is off duty.
 *
 * For a daylight-only cam the clock starts at the later of the last frame and
 * the capture window opening, so the overnight pause does not accumulate into
 * an age the cam had no chance to avoid.
 *
 * @param {Object} metadata - The cam's latest.json
 * @param {Object} webcam - Entry from `webcams`
 * @returns {{ageMinutes: number, offDuty: boolean}|null} null with no timestamp
 */
function webcamAge(metadata, webcam) {
  if (!metadata?.timestamp) return null;

  const now = Date.now();
  const lastUpdate = new Date(metadata.timestamp).getTime();
  if (Number.isNaN(lastUpdate)) return null;

  const window = captureWindow(webcam, now);
  if (window && !window.isOpen) {
    return { ageMinutes: (now - lastUpdate) / (1000 * 60), offDuty: true };
  }

  const clockStart = window ? Math.max(lastUpdate, window.openedAt) : lastUpdate;
  return { ageMinutes: (now - clockStart) / (1000 * 60), offDuty: false };
}

function formatAge(ageMinutes) {
  if (ageMinutes >= 2880) {
    // >= 48 hours: show days
    const days = Math.round(ageMinutes / 1440);
    return `${days} day${days !== 1 ? "s" : ""} ago`;
  } else if (ageMinutes >= 120) {
    // >= 2 hours but < 48 hours: show hours
    const hours = Math.round(ageMinutes / 60);
    return `${hours} hour${hours !== 1 ? "s" : ""} ago`;
  }
  // < 2 hours: show minutes
  return `${Math.round(ageMinutes)} min ago`;
}

/**
 * Write the "Last updated" line and set the card's staleness class.
 *
 * Both the initial render and the periodic metadata refresh need this and had
 * drifted into two copies of the same twenty lines; the copies are how a
 * threshold change lands in one place and not the other.
 *
 * @param {Element} card - The .webcam-card element (carries the state class)
 * @param {Element} timestampEl - The .webcam-timestamp element
 * @param {Object} metadata - The cam's latest.json
 * @param {Object} webcam - Entry from `webcams`
 */
function renderTimestamp(card, timestampEl, metadata, webcam) {
  const age = webcamAge(metadata, webcam);
  let text = "Last updated: " + formatFullTimestamp(metadata.timestamp);

  card.classList.remove("webcam-stale", "webcam-stale-error");

  // offDuty: a daylight-only cam outside its capture window is not late, it is
  // done for the day. The card already says "Screen grabs stop at night".
  if (age && !age.offDuty && age.ageMinutes > STALE_THRESHOLD_MINUTES) {
    const shown = Math.round(age.ageMinutes);
    const isDown = age.ageMinutes > DOWN_THRESHOLD_MINUTES;
    const severity = isDown ? "stale-error" : "";
    const label = isDown ? "DOWN" : "STALE";
    text += ` <span class="stale-indicator ${severity}" title="No new frame for ${shown} minutes">${label} (${formatAge(shown)})</span>`;
    card.classList.add(isDown ? "webcam-stale-error" : "webcam-stale");
  }

  setSafeHTML(timestampEl, text);
}

async function loadWebcamMetadata(webcam, card) {
  try {
    const response = await fetch(webcam.dataUrl);
    const metadata = await response.json();

    if (card) {
      const timestampEl = card.querySelector(".webcam-timestamp");
      if (timestampEl) {
        await fetchSunlightTimes();
        renderTimestamp(card, timestampEl, metadata, webcam);
      }
    }
    return metadata;
  } catch (error) {
    console.error(`Failed to load metadata for ${webcam.name}:`, error);
    return null;
  }
}

// ==========================================================================
// SVG Arrow Components (Unified)
// ==========================================================================

const ARROW_COLORS = {
  wind: "#dc2626",
  wave: "#2563eb",
};

function createDirectionalArrow(degrees, type = "wind") {
  const color = ARROW_COLORS[type] || ARROW_COLORS.wind;
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("width", "20");
  svg.setAttribute("height", "20");
  svg.setAttribute("viewBox", "-6 -10 12 24");
  svg.style.transform = `rotate(${degrees}deg)`;

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", "M0,12 L-5,-8 L0,-5 L5,-8 Z");
  path.setAttribute("fill", color);
  path.setAttribute("stroke", color);
  path.setAttribute("stroke-width", "1.5");

  svg.appendChild(path);
  return svg;
}

// ==========================================================================
// Condition Display Components
// ==========================================================================

function createWindDisplay(data, showLabel = false) {
  const windSpeed = data.wind_speed ?? data.wind_speed_kt;
  const windGust = data.wind_gust ?? data.wind_gust_kt;

  if (windSpeed == null || data.wind_direction == null) return null;

  const container = createElement("div", "condition-section");

  if (showLabel) {
    container.appendChild(createElement("div", "condition-section-label", "Wind:"));
  }

  const windDiv = createElement("div", "condition-wind");

  const arrow = createElement("span", "condition-wind-arrow");
  arrow.appendChild(createDirectionalArrow(data.wind_direction, "wind"));
  windDiv.appendChild(arrow);

  const details = createElement("div", "condition-wind-details");
  setSafeHTML(
    details,
    `
    <span class="wind-cardinal">${data.wind_direction_cardinal || ""}</span>
    <span class="wind-degrees">(${Math.round(data.wind_direction)}°)</span>
    <span class="wind-speed">${windSpeed.toFixed(0)}</span>
    ${windGust ? `<span class="wind-gust">G ${windGust.toFixed(0)}</span>` : ""}
    <span class="wind-gust">kt</span>
  `,
  );
  windDiv.appendChild(details);

  container.appendChild(windDiv);
  return container;
}

function createSimpleWaveDisplay(data) {
  if (data.wave_height_sig == null) return null;

  const container = createElement("div", "condition-section");
  container.appendChild(createElement("div", "condition-section-label", "Waves:"));

  const waveDiv = createElement("div", "condition-waves");
  setSafeHTML(
    waveDiv,
    `
    <span class="wave-icon">🌊</span>
    <div class="wave-details">
      <span class="wave-height">${data.wave_height_sig.toFixed(2)}m</span>
      ${data.wave_period_sig ? `<span class="wave-period">@ ${data.wave_period_sig.toFixed(1)}s</span>` : ""}
    </div>
  `,
  );

  container.appendChild(waveDiv);
  return container;
}

function getSpreadDescription(spread, type = "peak") {
  const thresholds =
    type === "peak"
      ? { veryGood: 25, good: 35, moderate: 45 }
      : { veryGood: 30, good: 45, moderate: 60 };

  const labels =
    type === "peak"
      ? { veryGood: "very organized", good: "organized", moderate: "moderate", bad: "confused" }
      : { veryGood: "very clean", good: "clean", moderate: "mixed", bad: "messy" };

  // var() references keep the labels theme-reactive (resolved values would
  // go stale when the user toggles dark mode after render).
  const colors = {
    veryGood: "var(--color-accent-green, #38a169)",
    good: "var(--color-accent-green, #48bb78)",
    moderate: "var(--color-accent-orange, #d69e2e)",
    bad: "var(--color-accent-red, #e53e3e)",
  };

  let level = "bad";
  if (spread < thresholds.veryGood) level = "veryGood";
  else if (spread < thresholds.good) level = "good";
  else if (spread < thresholds.moderate) level = "moderate";

  return { label: labels[level], color: colors[level] };
}

function createDetailedWaveDisplay(data) {
  if (data.wave_height_sig == null) return null;

  const container = createElement("div", "condition-section");
  container.appendChild(createElement("div", "condition-section-label", "Waves:"));

  const waveDiv = createElement("div", "condition-waves");
  const waveDetails = createElement("div", "wave-details-extended");

  // Icon section with direction arrow
  const iconSection = createElement("div", "wave-icon-section");
  iconSection.innerHTML = '<span class="wave-icon">🌊</span>';

  if (data.wave_direction_peak != null) {
    const arrowContainer = createElement("span", "wave-arrow-container");
    arrowContainer.appendChild(createDirectionalArrow(data.wave_direction_peak, "wave"));
    iconSection.appendChild(arrowContainer);
  }
  waveDetails.appendChild(iconSection);

  // Data grid
  const dataGrid = createElement("div", "wave-data-grid");

  // Significant height
  const sigMetric = createElement("div", "wave-metric");
  setSafeHTML(
    sigMetric,
    `<span class="wave-label">Sig:</span> <span class="wave-value">${data.wave_height_sig.toFixed(1)}m @ ${data.wave_period_sig ? data.wave_period_sig.toFixed(1) + "s" : "N/A"}</span>`,
  );
  dataGrid.appendChild(sigMetric);

  // Peak height
  if (data.wave_height_max != null) {
    const peakMetric = createElement("div", "wave-metric");
    setSafeHTML(
      peakMetric,
      `<span class="wave-label">Peak:</span> <span class="wave-value">${data.wave_height_max.toFixed(1)}m @ ${data.wave_period_peak ? data.wave_period_peak.toFixed(1) + "s" : "N/A"}</span>`,
    );
    dataGrid.appendChild(peakMetric);
  }

  // Direction
  if (data.wave_direction_peak != null) {
    const dirMetric = createElement("div", "wave-metric");
    setSafeHTML(
      dirMetric,
      `<span class="wave-label">Dir:</span> <span class="wave-value">${data.wave_direction_peak_cardinal || ""} (${Math.round(data.wave_direction_peak)}°)</span>`,
    );
    dataGrid.appendChild(dirMetric);

    // Peak spread
    if (data.wave_direction_spread_peak != null) {
      const peakDesc = getSpreadDescription(data.wave_direction_spread_peak, "peak");
      const peakSpreadMetric = createElement("div", "wave-metric");
      setSafeHTML(
        peakSpreadMetric,
        `<span class="wave-label">Peak Spread:</span> <span class="wave-value">${Math.round(data.wave_direction_spread_peak)}° <span style="color: ${peakDesc.color}; font-weight: 600;">(${peakDesc.label})</span> <span style="font-size: 0.85em; color: var(--color-text-muted);">(dominant swell)</span></span>`,
      );
      dataGrid.appendChild(peakSpreadMetric);

      // Average spread
      if (data.wave_direction_spread_avg != null) {
        const avgDesc = getSpreadDescription(data.wave_direction_spread_avg, "avg");
        const avgSpreadMetric = createElement("div", "wave-metric");
        setSafeHTML(
          avgSpreadMetric,
          `<span class="wave-label">Avg Spread:</span> <span class="wave-value">${Math.round(data.wave_direction_spread_avg)}° <span style="color: ${avgDesc.color}; font-weight: 600;">(${avgDesc.label})</span> <span style="font-size: 0.85em; color: var(--color-text-muted);">(all frequencies)</span></span>`,
        );
        dataGrid.appendChild(avgSpreadMetric);
      }

      // Visual spread vectors
      const vectorsContainer = createElement("div", "wave-spread-vectors");

      const peakVectorDiv = createElement("div", "wave-spread-vector");
      peakVectorDiv.appendChild(createElement("div", "wave-spread-vector-label", "Peak Spread"));
      const peakSvg = createAngularSpreadVectorElement(
        data.wave_direction_peak,
        data.wave_direction_spread_peak,
        70,
      );
      if (peakSvg) peakVectorDiv.appendChild(peakSvg);
      peakVectorDiv.appendChild(
        createElement("div", "wave-spread-vector-caption", "Dominant swell"),
      );
      vectorsContainer.appendChild(peakVectorDiv);

      if (data.wave_direction_spread_avg != null) {
        const avgVectorDiv = createElement("div", "wave-spread-vector");
        avgVectorDiv.appendChild(
          createElement("div", "wave-spread-vector-label", "Average Spread"),
        );
        const avgSvg = createAngularSpreadVectorElement(
          data.wave_direction_peak,
          data.wave_direction_spread_avg,
          70,
        );
        if (avgSvg) avgVectorDiv.appendChild(avgSvg);
        avgVectorDiv.appendChild(
          createElement("div", "wave-spread-vector-caption", "All frequencies"),
        );
        vectorsContainer.appendChild(avgVectorDiv);
      }

      dataGrid.appendChild(vectorsContainer);
    }
  }

  waveDetails.appendChild(dataGrid);
  waveDiv.appendChild(waveDetails);
  container.appendChild(waveDiv);
  return container;
}

function createConditionRow(label, data, fields) {
  if (!data || data.stale) return null;

  const row = createElement("div", "condition-row");
  row.appendChild(createElement("div", "condition-station-name", label));

  const dataDiv = createElement("div", "condition-data");

  // Wind display
  if (fields.includes("wind") || fields.includes("wind_speed_only")) {
    const windDisplay = createWindDisplay(data, true);
    if (windDisplay) dataDiv.appendChild(windDisplay);
  }

  // Wave display
  if (fields.includes("waves")) {
    const waveDisplay = createSimpleWaveDisplay(data);
    if (waveDisplay) dataDiv.appendChild(waveDisplay);
  } else if (fields.includes("waves_detailed")) {
    const waveDisplay = createDetailedWaveDisplay(data);
    if (waveDisplay) dataDiv.appendChild(waveDisplay);
  }

  // Timestamp
  if (data.observation_time) {
    dataDiv.appendChild(
      createElement("div", "condition-timestamp", formatMonthDayTime(data.observation_time)),
    );
  }

  row.appendChild(dataDiv);
  return row;
}

function getStationData(condition, marineData) {
  if (condition.buoyStation) return marineData.buoy?.[condition.buoyStation];
  if (condition.customStation) return marineData.custom?.[condition.customStation];
  if (condition.windStation) return marineData.wind?.[condition.windStation];
  return null;
}

function createConditionsSection(conditions, marineData, id = null) {
  if (!marineData || !conditions?.length) return null;

  // Deduplicate conditions by station key
  const seen = new Set();
  const uniqueConditions = conditions.filter((c) => {
    const key = c.buoyStation || c.customStation || c.windStation;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const section = createElement("div", "marine-conditions-banner region-conditions");
  if (id) section.id = id;

  section.appendChild(createElement("h3", null, "Current Marine Conditions"));

  const stack = createElement("div", "conditions-stack");

  uniqueConditions.forEach((condition) => {
    const data = getStationData(condition, marineData);
    const row = createConditionRow(condition.label, data, condition.fields || []);
    if (row) stack.appendChild(row);
  });

  if (stack.children.length === 0) return null;

  section.appendChild(stack);
  return section;
}

// ==========================================================================
// Webcam Card
// ==========================================================================

async function createWebcamCard(webcam, metadata) {
  const card = createElement("div", "webcam-card");
  card.id = webcam.id;
  card.dataset.webcamId = webcam.id;

  // Header
  const header = createElement("div", "webcam-header");

  // Title with daylight indicator
  const title = createElement("h3");
  const titleText = document.createTextNode(webcam.name + " ");
  title.appendChild(titleText);

  // Add sun/moon indicator
  const indicator = createElement("span", "daylight-indicator");
  if (webcam.daylightOnly) {
    indicator.textContent = "🌞";
    indicator.title = "Daylight only - screen grabs during daytime hours";
    indicator.style.fontSize = "0.8em";
  } else {
    indicator.textContent = "🌞🌙";
    indicator.title = "24/7 - screen grabs day and night";
    indicator.style.fontSize = "0.8em";
  }
  title.appendChild(indicator);

  header.appendChild(title);
  header.appendChild(createElement("p", "webcam-location", webcam.location));
  card.appendChild(header);

  // Image container
  const imageContainer = createElement("div", "webcam-image-container");
  const image = createElement("img", "webcam-image");
  image.src = webcam.imageUrl + "?t=" + Date.now();
  image.alt = webcam.name + " webcam view";
  image.loading = "lazy";
  imageContainer.appendChild(image);
  card.appendChild(imageContainer);

  // Slideshow controls
  const controls = createElement("div", "slideshow-controls");

  const prevBtn = createElement("button", "slideshow-nav prev", "‹");
  prevBtn.addEventListener("click", () => navigateSlideshow(webcam.id, 1));

  const dotsContainer = createElement("div", "slideshow-dots");

  const nextBtn = createElement("button", "slideshow-nav next", "›");
  nextBtn.addEventListener("click", () => navigateSlideshow(webcam.id, -1));

  controls.appendChild(prevBtn);
  controls.appendChild(dotsContainer);
  controls.appendChild(nextBtn);
  card.appendChild(controls);

  // Info section
  const info = createElement("div", "webcam-info");

  // Attribution
  if (webcam.attribution) {
    const attr = createElement("div", "webcam-attribution");
    if (webcam.attribution.url) {
      try {
        const validatedUrl = new URL(webcam.attribution.url);
        setSafeHTML(
          attr,
          `${webcam.attribution.text} &mdash; <a href="${validatedUrl.href}" target="_blank" rel="noopener">⛵ Visit their website</a>`,
        );
      } catch {
        attr.textContent = webcam.attribution.text;
      }
    } else {
      attr.textContent = webcam.attribution.text;
    }
    info.appendChild(attr);
  }

  // Down notice
  if (webcam.downNotice) {
    const notice = createElement("div", "webcam-update-notice webcam-down-notice");
    notice.textContent = "⚠️ " + webcam.downNotice;
    info.appendChild(notice);
  }

  // Update interval notice
  const updateNotice = createElement("div", "webcam-update-notice");
  // A direct-image cam has no stream to buffer, and the registry says so with
  // 0 rather than by omitting the field. "~0 min stream delay" is not a
  // sentence, so say what 0 means.
  let delayText = "unknown delay";
  if (webcam.streamDelay === 0) {
    delayText = "no stream delay";
  } else if (webcam.streamDelay != null) {
    delayText = `~${webcam.streamDelay} min stream delay`;
  }
  let noticeText = `Updated every ${webcam.updateInterval || 10} minutes • ${delayText}`;

  // Add daylight-only note
  if (webcam.daylightOnly) {
    noticeText += " • Screen grabs stop at night";
  }

  updateNotice.textContent = noticeText;
  info.appendChild(updateNotice);

  if (metadata) {
    // Timestamp with staleness check
    const timestampEl = createElement("div", "webcam-timestamp");
    renderTimestamp(card, timestampEl, metadata, webcam);
    info.appendChild(timestampEl);

    // Source link
    if (metadata.source || metadata.url) {
      const source = createElement("div", "webcam-source");
      if (metadata.url) {
        const link = createElement("a");
        link.href = metadata.url;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = metadata.source || "View Source";
        source.appendChild(document.createTextNode("Source: "));
        source.appendChild(link);
      } else {
        source.textContent = "Source: " + metadata.source;
      }
      info.appendChild(source);
    }

    // Map link
    const mapLink = createElement("div", "webcam-map-link");
    const mapAnchor = createElement("a");
    mapAnchor.href = `/?station=${webcam.id}#map-section`;
    mapAnchor.textContent = "📍 Show on map";
    mapLink.appendChild(mapAnchor);
    info.appendChild(mapLink);

    // Refresh button
    const refreshBtn = createElement("button", "refresh-button", "Refresh Image");
    refreshBtn.addEventListener("click", () => refreshWebcam(webcam, card, image));
    info.appendChild(refreshBtn);
  }

  card.appendChild(info);
  return card;
}

async function refreshWebcam(webcam, card, image) {
  image.src = webcam.imageUrl + "?t=" + Date.now();
  loadWebcamMetadata(webcam, card);

  const state = slideshowState[webcam.id];
  if (state) {
    state.currentIndex = 0;
    updateSlideshowDisplay(webcam.id);
  }

  cachedMarineData = null;
  const freshData = await fetchMarineData();
  updateConditionsBanner(freshData);
}

function updateConditionsBanner(marineData) {
  if (!marineData) return;

  // Collect all conditions from all webcams
  const allConditions = webcams.flatMap((w) => w.conditions || []);
  const newBanner = createConditionsSection(allConditions, marineData, "marine-conditions-banner");
  const existingBanner = document.getElementById("marine-conditions-banner");

  if (existingBanner && newBanner) {
    existingBanner.replaceWith(newBanner);
  } else if (!existingBanner && newBanner) {
    const container = document.getElementById("webcams-container");
    container?.parentNode?.insertBefore(newBanner, container);
  }
}

// ==========================================================================
// Slideshow Management
// ==========================================================================

async function loadSlideshow(webcam) {
  try {
    const response = await fetch(webcam.slideshowUrl + "?t=" + Date.now());
    if (!response.ok) return;

    const manifest = await response.json();
    if (!manifest?.length) return;

    slideshowState[webcam.id] = {
      images: manifest,
      currentIndex: 0,
      basePath: webcam.slideshowPath,
    };

    const card = document.querySelector(`[data-webcam-id="${webcam.id}"]`);
    if (card && manifest.length > 1) {
      const controls = card.querySelector(".slideshow-controls");
      const dotsContainer = card.querySelector(".slideshow-dots");

      controls.classList.add("visible");
      dotsContainer.innerHTML = "";

      // Create dots (reversed: rightmost = newest)
      manifest
        .slice()
        .reverse()
        .forEach((_, reverseIndex) => {
          const actualIndex = manifest.length - 1 - reverseIndex;
          const dot = createElement("div", "slideshow-dot" + (actualIndex === 0 ? " active" : ""));
          dot.dataset.index = actualIndex;
          dot.addEventListener("click", () => goToSlide(webcam.id, actualIndex));
          dotsContainer.appendChild(dot);
        });
    }
  } catch (error) {
    console.error(`Failed to load slideshow for ${webcam.name}:`, error);
  }
}

function navigateSlideshow(webcamId, direction) {
  const state = slideshowState[webcamId];
  if (!state?.images) return;

  const newIndex = state.currentIndex + direction;
  if (newIndex < 0 || newIndex >= state.images.length) return;

  state.currentIndex = newIndex;
  updateSlideshowDisplay(webcamId);
}

function goToSlide(webcamId, index) {
  const state = slideshowState[webcamId];
  if (!state?.images) return;

  state.currentIndex = index;
  updateSlideshowDisplay(webcamId);
}

function updateSlideshowDisplay(webcamId) {
  const state = slideshowState[webcamId];
  if (!state) return;

  const card = document.querySelector(`[data-webcam-id="${webcamId}"]`);
  if (!card) return;

  const image = card.querySelector(".webcam-image");
  const currentImage = state.images[state.currentIndex];

  image.src = state.basePath + currentImage.path + "?t=" + Date.now();

  // Update timestamp
  const timestamp = card.querySelector(".webcam-timestamp");
  if (timestamp) {
    timestamp.textContent = "Captured: " + formatFullTimestamp(currentImage.timestamp);

    // Age indicator
    let ageIndicator = card.querySelector(".slideshow-age-indicator");
    if (state.currentIndex > 0) {
      const webcam = webcams.find((w) => w.id === webcamId);
      const minutesAgo = state.currentIndex * (webcam?.updateInterval || 10);

      if (!ageIndicator) {
        ageIndicator = createElement("div", "slideshow-age-indicator");
        timestamp.parentNode.insertBefore(ageIndicator, timestamp.nextSibling);
      }
      ageIndicator.textContent = `(${minutesAgo} minutes ago)`;
    } else if (ageIndicator) {
      ageIndicator.remove();
    }
  }

  // Update nav buttons
  const prevBtn = card.querySelector(".slideshow-nav.prev");
  const nextBtn = card.querySelector(".slideshow-nav.next");
  if (prevBtn) prevBtn.disabled = state.currentIndex === state.images.length - 1;
  if (nextBtn) nextBtn.disabled = state.currentIndex === 0;

  // Update dots
  card.querySelectorAll(".slideshow-dot").forEach((dot) => {
    dot.classList.toggle("active", parseInt(dot.dataset.index) === state.currentIndex);
  });
}

// ==========================================================================
// Page Initialization
// ==========================================================================

async function loadWebcams() {
  const container = document.getElementById("webcams-container");
  if (!container) return;

  // Before the length check: hydrateWebcams() drops any camera the registry
  // does not list, so "no webcams" is a conclusion it can reach.
  await hydrateWebcams();

  if (webcams.length === 0) {
    container.innerHTML =
      '<p style="text-align: center; color: var(--color-text-muted); padding: 2rem;">No webcams currently available.</p>';
    return;
  }

  container.innerHTML = "";

  // Both before the first card is built: renderTimestamp() needs the sunlight
  // times to know whether a daylight-only cam is off duty, and without them it
  // falls back to wall-clock age and paints those cams DOWN all night.
  const [marineData] = await Promise.all([fetchMarineData(), fetchSunlightTimes()]);

  // Group webcams by region
  const grouped = {};
  webcams.forEach((webcam) => {
    const region = webcam.region || "other";
    if (!grouped[region]) grouped[region] = [];
    grouped[region].push(webcam);
  });

  // Render each region
  for (const [regionKey, regionWebcams] of Object.entries(grouped)) {
    const regionInfo = webcamRegions[regionKey];
    const regionContainer = createElement("div", "webcam-region");

    // Region header
    if (regionInfo) {
      const header = createElement("div", "webcam-region-header");
      const count = regionWebcams.length;
      setSafeHTML(
        header,
        `<h2><span class="webcam-region-toggle-btn">▼</span>${regionInfo.name} <span class="webcam-region-count">(${count} webcam${count !== 1 ? "s" : ""})</span></h2>`,
      );
      header.addEventListener("click", () => regionContainer.classList.toggle("collapsed"));
      regionContainer.appendChild(header);
    }

    // Content wrapper
    const content = createElement("div", "webcam-region-content");

    // Regional conditions
    const regionConditions = regionWebcams.flatMap((w) => w.conditions || []);
    const conditionsSection = createConditionsSection(regionConditions, marineData);
    if (conditionsSection) content.appendChild(conditionsSection);

    // Webcam grid
    const grid = createElement("div", "webcam-grid");
    const webcamsToLoadSlideshow = [];

    for (const webcam of regionWebcams) {
      try {
        const metadata = await loadWebcamMetadata(webcam);
        const card = await createWebcamCard(webcam, metadata);
        grid.appendChild(card);
        webcamsToLoadSlideshow.push(webcam);
      } catch (error) {
        console.error(`Failed to load webcam ${webcam.name}:`, error);
        const errorCard = createElement("div", "webcam-card");
        setSafeHTML(
          errorCard,
          `
          <div class="webcam-header">
            <h3>${webcam.name}</h3>
            <p class="webcam-location">${webcam.location}</p>
          </div>
          <div class="webcam-error">Failed to load webcam data. Please try again later.</div>
        `,
        );
        grid.appendChild(errorCard);
      }
    }

    content.appendChild(grid);
    regionContainer.appendChild(content);
    container.appendChild(regionContainer);

    // Load slideshows after DOM insertion
    webcamsToLoadSlideshow.forEach(loadSlideshow);
  }

  // Handle hash anchor
  if (window.location.hash) {
    setTimeout(() => {
      const target = document.getElementById(window.location.hash.substring(1));
      target?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 300);
  }
}

// ==========================================================================
// Auto-Refresh
// ==========================================================================

function startAutoRefresh() {
  const REFRESH_INTERVAL = 5 * 60 * 1000;
  const MAX_REFRESH_TIME = 30 * 60 * 1000;
  const startTime = Date.now();
  let refreshCount = 0;

  const intervalId = setInterval(() => {
    if (Date.now() - startTime >= MAX_REFRESH_TIME) {
      clearInterval(intervalId);
      console.log("Auto-refresh stopped after 30 minutes.");
      showRefreshNotice();
      return;
    }

    refreshCount++;
    console.log(`Auto-refreshing webcam images... (${refreshCount}/6)`);

    cachedMarineData = null;

    fetchMarineData().then((marineData) => {
      webcams.forEach((webcam) => {
        const card = document.querySelector(`[data-webcam-id="${webcam.id}"]`);
        if (!card) return;

        const state = slideshowState[webcam.id];
        if (!state || state.currentIndex === 0) {
          const image = card.querySelector(".webcam-image");
          image.src = webcam.imageUrl + "?t=" + Date.now();
        }

        if (marineData && webcam === webcams[0]) {
          updateConditionsBanner(marineData);
        }

        loadSlideshow(webcam);
        loadWebcamMetadata(webcam, card);
      });
    });
  }, REFRESH_INTERVAL);
}

function showRefreshNotice() {
  const notice = createElement("div", "refresh-notice");
  notice.innerHTML = `
    Auto-refresh stopped after 30 minutes
    <button type="button" class="reload-page-btn">Refresh Page</button>
  `;
  document.body.appendChild(notice);

  setTimeout(() => {
    notice.style.opacity = "0";
    setTimeout(() => notice.remove(), 500);
  }, 10000);
}

// ==========================================================================
// Initialize
// ==========================================================================

function initWebcamsPage() {
  loadWebcams();
  startAutoRefresh();
}

// Event delegation for intro section collapse + refresh button (CSP compliance)
document.addEventListener("click", function (e) {
  var header = e.target.closest(".intro-section-header");
  if (header) {
    header.parentElement.classList.toggle("collapsed");
  }

  var reloadBtn = e.target.closest(".reload-page-btn");
  if (reloadBtn) {
    e.preventDefault();
    window.location.reload();
  }
});

// Start when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initWebcamsPage);
} else {
  initWebcamsPage();
}
