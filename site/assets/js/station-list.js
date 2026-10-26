/* -----------------------------
   Home-page station list (ES module)

   One compact row per wave station, grouped by region, that expands in place
   to the station's details. Replaces the buoy cards on the home page
   (docs/project/UI_OVERHAUL.md, "Brief for the mockup"). Pure HTML-string
   builders over a snapshot entry `b` from latest_buoy_v2.json and its
   stations.json metadata `meta`; main.js keeps the fetching and the wiring.

   Which fields hold the displayed height, period and direction is decided in
   shared/station-meta.js, the same priorities the history table reads.
   ----------------------------- */

import { avgSpreadDescriptor, peakSpreadDescriptor } from "./buoy-card.js";
import { formatWeekdayDayTime } from "./shared/format-time.js";
import { icon } from "./shared/icons.js";
import {
  isNoaaStation,
  isPileStation,
  pickWaveDirection,
  pickWavePeriod,
  sourceUrl,
  usesDominantPeriod,
  usesSwellDisplay,
  waveHeightField,
  waveHeightPrecision,
} from "./shared/station-meta.js";
import { liveAgeMinutes, reportStatus } from "./shared/staleness.js";
import { waveAmplitude } from "./shared/wave-scale.js";

/**
 * The home page's regions in display order: each one's heading, and its
 * column in the two-column desktop layout (on a phone they simply stack).
 * Which buoy is in which region is the registry's `region`
 * (config/stations.json), ordered by its `list_order`; nothing here names a
 * station.
 *
 * An ORDERING, not a whitelist, like orderRegions() in shared/station-meta.js:
 * a region not listed here still appears, after these, under its own name,
 * and so does a station the registry has no region for (or every station,
 * when stations.json could not be loaded).
 */
export const REGIONS = [
  { region: "STRAIT OF GEORGIA", label: "Strait of Georgia", column: 0 },
  { region: "BOUNDARY BAY", label: "Boundary Bay", column: 0 },
  { region: "JUAN DE FUCA STRAIT", label: "Juan de Fuca Strait", column: 1 },
  { region: "WEST COAST VANCOUVER ISLAND", label: "West Coast Vancouver Island", column: 1 },
];

const UNGROUPED_LABEL = "Other stations";

/**
 * The stations with readings, grouped by their registry region and put in
 * display order: REGIONS first, then any other region, then stations without
 * one. Within a region, by the registry's `list_order` (not the file's order:
 * JS enumerates number-like keys such as "4600146" numerically); stations
 * without one follow, then stations missing from the registry.
 *
 * @param {Object} data - latest_buoy_v2.json payload (keys starting "_" are not stations)
 * @param {Object} stationMeta - stations.json `buoys`
 * @returns {{label: string, column: number, stations: string[]}[]}
 */
export function stationGroups(data, stationMeta, regions = REGIONS) {
  const order = (id) => stationMeta[id]?.list_order ?? Number.POSITIVE_INFINITY;
  const ids = [
    ...Object.keys(stationMeta)
      .filter((id) => data[id])
      .sort((a, b) => order(a) - order(b)),
    ...Object.keys(data).filter((id) => !id.startsWith("_") && !stationMeta[id]),
  ];
  const byRegion = new Map();
  for (const id of ids) {
    const region = stationMeta[id]?.region ?? "";
    if (!byRegion.has(region)) byRegion.set(region, []);
    byRegion.get(region).push(id);
  }
  const lastColumn = regions.at(-1)?.column ?? 0;
  const known = regions
    .filter((r) => byRegion.has(r.region))
    .map((r) => ({ label: r.label, column: r.column, stations: byRegion.get(r.region) }));
  const rest = [...byRegion.keys()]
    .filter((region) => !regions.some((r) => r.region === region))
    // Named regions alphabetically, the stations without one last.
    .sort((a, b) => (a === "" ? 1 : b === "" ? -1 : a.localeCompare(b)))
    .map((region) => ({
      label: region || UNGROUPED_LABEL,
      column: lastColumn,
      stations: byRegion.get(region),
    }));
  return [...known, ...rest];
}

const DASH = "—";

/**
 * Compass arrow pointing the way the wind or waves are GOING, which for a
 * meteorological "from" bearing is a down arrow turned by that bearing.
 * Decorative: the cardinal next to it carries the meaning.
 */
export function directionArrow(degrees) {
  if (degrees == null || !Number.isFinite(Number(degrees))) return "";
  return (
    `<svg class="sl-arrow" viewBox="0 0 16 16" aria-hidden="true" ` +
    `style="transform: rotate(${Math.round(degrees)}deg)">` +
    `<path d="M8 2v12M4.5 10.5 8 14l3.5-3.5" fill="none" stroke="currentColor" ` +
    `stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`
  );
}

const CHEVRON =
  `<svg class="sl-chevron" viewBox="0 0 16 16" aria-hidden="true">` +
  `<path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" stroke-width="2" ` +
  `stroke-linecap="round" stroke-linejoin="round"/></svg>`;

// Column labels, on each column's first region heading at every width: over
// the numbers' columns when the rows sit side by side, "Waves · Wind" at the
// heading's right when they stack (home.css). Hidden from screen readers:
// each cell already names itself.
const HEADING_LABELS =
  `<span class="sl-head-labels" aria-hidden="true">` +
  `<span class="sl-head-label sl-head-label--waves">Waves</span>` +
  `<span class="sl-head-sep"> · </span>` +
  `<span class="sl-head-label sl-head-label--wind">Wind</span></span>`;

/**
 * "52m", "1h 25m", "4h", "15d": whole units, rounded down. Minutes are kept
 * through the second hour because that is where an hourly station is either
 * on time or late, and "1h" alone hid a 59-minute difference.
 */
export function compactAge(minutes) {
  if (minutes == null || !Number.isFinite(minutes)) return null;
  const m = Math.max(0, Math.floor(minutes));
  if (m < 60) return `${m}m`;
  if (m < 120) return m === 60 ? "1h" : `1h ${m - 60}m`;
  if (m < 48 * 60) return `${Math.floor(m / 60)}h`;
  return `${Math.floor(m / 1440)}d`;
}

/**
 * The row's freshness line and dot state. Age leads; the state word is only
 * added when something is wrong. Both are measured against `now`
 * (shared/staleness.js), so neither goes stale while the page stays open.
 *
 * @param {Object} b - One station's latest-observation entry
 * @param {number} [now] - Epoch ms; injectable for tests
 * @returns {{state: "ok"|"late"|"down"|"out"|"none", text: string}}
 */
export function freshness(b, now = Date.now()) {
  if (b.no_data) return { state: "none", text: "No data" };
  if (b.out_of_service) return { state: "out", text: "Out of service" };
  const status = reportStatus(b, now);
  const age = compactAge(liveAgeMinutes(b, now));
  const ago = age ? `${age} ago` : "time unknown";
  if (status === "ok") return { state: "ok", text: ago };
  return { state: status, text: `${ago} · ${status}` };
}

/**
 * A freshness line in its parts: "17d ago · down" → "17d", " ago", "down".
 * Each is its own span in the row, so a phone can drop " ago" and a down
 * row's state word (home.css). Lines without an age ("Out of service") come
 * back whole as `age`.
 */
function ageParts(text) {
  const [main, word = ""] = text.split(" · ");
  const age = main.endsWith(" ago") ? main.slice(0, -4) : main;
  return { age, ago: age === main ? "" : " ago", word };
}

/**
 * Bring every row's age line up to `now` without rebuilding the list, so
 * open rows and their charts are left alone. Returns true when a row's state
 * changed (ok to late, say): its numbers and notes depend on the state, so
 * the caller rebuilds the list then.
 *
 * @param {ParentNode} container - The #station-list element
 * @param {Object} data - The latest_buoy_v2.json payload the rows came from
 * @param {number} [now]
 * @returns {boolean}
 */
export function refreshAges(container, data, now = Date.now()) {
  let stateChanged = false;
  for (const item of container.querySelectorAll(".sl-item")) {
    const b = data?.[item.id.replace(/^buoy-/, "")];
    const row = item.querySelector(".sl-row");
    const text = item.querySelector(".sl-age-text");
    if (!b || !row || !text) continue;
    const fresh = freshness(b, now);
    if (!row.classList.contains(`sl-row--${fresh.state}`)) stateChanged = true;
    // Only the age itself: a state change rebuilds the list, which `true` asks for.
    text.textContent = ageParts(fresh.text).age;
  }
  return stateChanged;
}

export function rowReading(b, meta) {
  const heightValue = b[waveHeightField(meta)];
  const { value: periodValue, tag } = pickWavePeriod(b, meta);
  const { cardinal, degrees } = pickWaveDirection(b, meta);
  const windDegrees = b.wind_direction_deg ?? b.wind_direction ?? null;
  return {
    height: heightValue != null ? heightValue.toFixed(waveHeightPrecision(meta)) : null,
    period: periodValue != null ? periodValue.toFixed(1) : null,
    periodTag: tag,
    isSwell: usesSwellDisplay(meta),
    waveCardinal: cardinal,
    waveDegrees: degrees ?? null,
    wind: b.wind_speed != null ? String(Math.round(b.wind_speed)) : null,
    gust: b.wind_gust != null ? String(Math.round(b.wind_gust)) : null,
    windCardinal: b.wind_direction_cardinal ?? null,
    windDegrees,
  };
}

/**
 * The arrow alone, for a row's small line: the cardinal letters cost the width
 * a phone needs for station names. Screen readers get the cardinal instead,
 * and the open panel shows both.
 */
function directionPart(cardinal, degrees) {
  const spoken = cardinal ? `<span class="visually-hidden">, from ${cardinal}</span>` : "";
  return `${directionArrow(degrees)}${spoken}`;
}

/** The waves cell: height large, period and direction small beneath. */
function waveCell(r) {
  if (r.height == null) {
    return `<span class="sl-cell sl-cell--waves"><span class="visually-hidden">Waves: </span><span class="sl-big sl-none">${DASH}</span></span>`;
  }
  const swell = r.isSwell ? `<span class="sl-swell-word"> swell</span>` : "";
  const period = r.period != null ? `${r.period} s${swell}` : "";
  const sub = `${period}${directionPart(r.waveCardinal, r.waveDegrees)}`;
  return (
    `<span class="sl-cell sl-cell--waves"><span class="visually-hidden">Waves: </span>` +
    `<span class="sl-big">${r.height}<span class="sl-unit"> m</span></span>` +
    `<span class="sl-sub">${sub}</span></span>`
  );
}

/** The wind cell: speed large, gust and direction small beneath. */
function windCell(r) {
  if (r.wind == null) {
    return `<span class="sl-cell sl-cell--wind"><span class="visually-hidden">Wind: </span><span class="sl-big sl-none">${DASH}</span></span>`;
  }
  const gust = r.gust != null ? `G ${r.gust}` : "";
  const sub = `${gust}${directionPart(r.windCardinal, r.windDegrees)}`;
  return (
    `<span class="sl-cell sl-cell--wind"><span class="visually-hidden">Wind: </span>` +
    `<span class="sl-big">${r.wind}<span class="sl-unit"> kt</span></span>` +
    `<span class="sl-sub">${sub}</span></span>`
  );
}

// The swell line's height, in px. Its width is the row's spare space between
// name and numbers (home.css: 80px to 220px), so the path is drawn 1:1
// (no viewBox) out to SWELL_DRAWN and the SVG clips it to whatever it gets.
const SWELL_HEIGHT = 20;
const SWELL_DRAWN = 400;

/**
 * Geometry of a row's swell line: amplitude from wave height measured against
 * what is rough for this station (`roughM`, the registry's
 * rough_wave_height_m, scaled by shared/wave-scale.js as the header wave is),
 * wavelength from period (a long period draws wide lazy rollers, a short one
 * tight chop), and the way it drifts from the direction arrow's horizontal
 * component. Null when there is no height to draw.
 *
 * Amplitude scales by height alone, not by steepness, on purpose: the line's
 * height answers "how big is it here", the one thing a glance down the list
 * should compare. Period already sets the wavelength, so the drawn line's
 * steepness (amplitude over wavelength) follows height/period anyway: the
 * same height looks steeper at a short period. That is a likeness, not a
 * physical steepness (H/L, with L growing as the period squared).
 *
 * @returns {{amplitude: number, wavelength: number, drift: -1|0|1}|null}
 */
export function swellGeometry(height, period, degrees, roughM) {
  if (height == null || !Number.isFinite(height)) return null;
  const factor = waveAmplitude(height, roughM);
  const p = period != null && Number.isFinite(period) && period > 0 ? period : 4;
  // The arrow is a down arrow turned clockwise by the "from" bearing, so its
  // on-screen x component is -sin(bearing): waves from the west drift right.
  const x = degrees != null && Number.isFinite(degrees) ? -Math.sin((degrees * Math.PI) / 180) : 0;
  return {
    amplitude: Math.round((((SWELL_HEIGHT / 2 - 2) * factor) / 1.2) * 10) / 10,
    wavelength: Math.round(Math.min(72, Math.max(14, p * 6))),
    drift: Math.abs(x) < 0.2 ? 0 : x > 0 ? 1 : -1,
  };
}

const swellOpen = (cls) =>
  `<svg class="sl-swell${cls}" height="${SWELL_HEIGHT}" aria-hidden="true" focusable="false">`;

/**
 * A faint sine line behind the row's spare width. Decorative, so hidden from
 * screen readers; "" when there is no height reading. It is static; only the
 * open row's line moves (home.css).
 */
export function swellLine(height, period, degrees, roughM) {
  const mid = SWELL_HEIGHT / 2;
  const g = swellGeometry(height, period, degrees, roughM);
  if (!g) return "";
  // One wavelength spare at each end, so the line can travel a full
  // wavelength either way without its ends showing.
  const half = g.wavelength / 2;
  let d = `M${-g.wavelength} ${mid}Q${-g.wavelength + half / 2} ${mid - 2 * g.amplitude} ${-half} ${mid}`;
  for (let x = 0; x <= SWELL_DRAWN + g.wavelength; x += half) d += `T${x} ${mid}`;
  return (
    `${swellOpen("")}<path class="sl-swell-wave" data-drift="${g.drift}" ` +
    `style="--swell-shift: ${g.drift * g.wavelength}px; --swell-period: ${((period ?? 4) * 1.5).toFixed(1)}s" d="${d}"/></svg>`
  );
}

/**
 * The swell line's stand-in on a row without numbers: flat, dashed and muted,
 * so the column of lines stays unbroken and the gap reads as "no signal".
 */
export const FLAT_LINE = `${swellOpen(" sl-swell--flat")}<path class="sl-swell-flat" d="M0 ${SWELL_HEIGHT / 2}H${SWELL_DRAWN}"/></svg>`;

/**
 * One station row: a button that toggles its detail panel. A station that is
 * down shows no numbers in the row: a reading days old next to live ones
 * reads as current. Its last report is still in the panel, labelled. Side
 * by side, the age line ("17d ago · down") says why and a flat dashed line
 * holds the swell line's place; stacked, the second line says "No recent
 * data" instead, so every row is the same two lines (home.css).
 *
 * The name and swell line share one lead cell, a flex row, so the line takes
 * whatever width the name leaves and the name never yields to the line.
 */
export function buildStationRow(b, id, meta) {
  const name = b.name || meta?.name || id;
  const fresh = freshness(b);
  const showValues = fresh.state === "ok" || fresh.state === "late";

  let values = `<span class="sl-cell sl-cell--empty sl-none">No recent data</span>`;
  let swell = FLAT_LINE;
  if (showValues) {
    const r = rowReading(b, meta);
    values = waveCell(r) + windCell(r);
    swell = swellLine(
      r.height != null ? Number(r.height) : null,
      r.period != null ? Number(r.period) : null,
      r.waveDegrees,
      meta?.rough_wave_height_m,
    );
  }

  // One inline run inside the flex age line, so its spaces survive.
  const { age, ago, word } = ageParts(fresh.text);
  const ageLine =
    `<span class="sl-age-words"><span class="sl-age-text">${age}</span>` +
    (ago ? `<span class="sl-age-ago">${ago}</span>` : "") +
    (word ? `<span class="sl-age-state"> · ${word}</span>` : "") +
    `</span>`;

  return `
    <button type="button" class="sl-row sl-row--${fresh.state}" aria-expanded="false" aria-controls="sl-detail-${id}">
      <span class="sl-lead">
        <span class="sl-name">
          <span class="sl-title">${name}</span>
          <span class="sl-age"><span class="sl-dot" aria-hidden="true"></span>${ageLine}</span>
        </span>
        ${swell}
      </span>
      ${values}
      ${CHEVRON}
    </button>`;
}

/** `<div>` of label/value pairs; pairs with a null value are skipped. */
function statGroup(title, pairs) {
  const items = pairs
    .filter(([, value]) => value != null && value !== "")
    .map(([label, value]) => `<div class="sl-stat"><dt>${label}</dt><dd>${value}</dd></div>`)
    .join("");
  if (!items) return "";
  return `<div class="sl-group"><h3 class="sl-group-title">${title}</h3><dl class="sl-stats">${items}</dl></div>`;
}

function withUnit(value, unit, decimals) {
  if (value == null) return null;
  const v = decimals != null ? Number(value).toFixed(decimals) : value;
  return `${v}<span class="sl-unit"> ${unit}</span>`;
}

function bearing(cardinal, degrees) {
  if (cardinal == null && degrees == null) return null;
  const deg = degrees != null ? ` <span class="sl-muted">${Math.round(degrees)}°</span>` : "";
  return `${cardinal ?? ""}${deg}${directionArrow(degrees)}`;
}

function spread(value, descriptor) {
  if (value == null) return null;
  return `${value}° <span class="sl-muted">${descriptor(value).desc}</span>`;
}

const PERIOD_LABELS = {
  dominant: "Dominant period",
  avg: "Average period",
  peak: "Peak period",
};

/** The waves group: headline values first, then whatever extras the buoy sends. */
function waveStats(b, meta) {
  const precision = waveHeightPrecision(meta);
  const swell = usesSwellDisplay(meta);
  const { value: period, tag } = pickWavePeriod(b, meta);
  const { cardinal, degrees } = pickWaveDirection(b, meta);

  const pairs = [
    [swell ? "Swell height" : "Sig. height", withUnit(b[waveHeightField(meta)], "m", precision)],
    [swell ? "Swell period" : (PERIOD_LABELS[tag] ?? "Sig. period"), withUnit(period, "s", 1)],
    ["Direction", bearing(cardinal, degrees)],
  ];

  if (isNoaaStation(meta)) {
    if (!swell) {
      pairs.push(["Swell", withUnit(b.swell_height, "m", 1)]);
      pairs.push(["Swell period", withUnit(b.swell_period, "s", 1)]);
    } else {
      pairs.push(["Combined height", withUnit(b.wave_height_sig, "m", 1)]);
    }
    pairs.push(["Wind waves", withUnit(b.wind_wave_height, "m", 1)]);
    pairs.push(["Wind-wave period", withUnit(b.wind_wave_period, "s", 1)]);
    pairs.push(["Wind-wave dir.", bearing(b.wind_wave_direction_cardinal, b.wind_wave_direction)]);
  } else {
    // EC buoy families report the high end differently; take whichever exists.
    const high =
      b.wave_height_max_avg != null
        ? ["Avg max height", b.wave_height_max_avg]
        : b.wave_height_peak != null
          ? ["Peak height", b.wave_height_peak]
          : ["Max height", b.wave_height_max];
    pairs.push([high[0], withUnit(high[1], "m", precision)]);
    pairs.push(["Avg height", withUnit(b.wave_height_avg, "m", precision)]);
    if (tag !== "avg") pairs.push(["Avg period", withUnit(b.wave_period_avg, "s", 1)]);
    if (tag !== "peak") {
      pairs.push([
        b.wave_period_peak != null ? "Peak period" : "Largest-wave period",
        withUnit(b.wave_period_peak ?? b.wave_period_max_wave, "s", 1),
      ]);
    }
    pairs.push(["Peak spread", spread(b.wave_direction_spread_peak, peakSpreadDescriptor)]);
    pairs.push(["Avg spread", spread(b.wave_direction_spread_avg, avgSpreadDescriptor)]);
  }
  return statGroup("Waves", pairs);
}

/** Wind, then the water/air group. Pile sensors over-report precision. */
function windAndWeatherStats(b, meta) {
  const tempDecimals = isPileStation(meta) ? 1 : null;
  return (
    statGroup("Wind", [
      ["Speed", withUnit(b.wind_speed != null ? Math.round(b.wind_speed) : null, "kt")],
      ["Gust", withUnit(b.wind_gust != null ? Math.round(b.wind_gust) : null, "kt")],
      ["Direction", bearing(b.wind_direction_cardinal, b.wind_direction_deg ?? b.wind_direction)],
    ]) +
    statGroup("Water & air", [
      ["Sea", withUnit(b.sea_temp, "°C", tempDecimals)],
      ["Air", withUnit(b.air_temp, "°C", tempDecimals)],
      ["Pressure", withUnit(b.pressure, "hPa")],
    ])
  );
}

/**
 * The station's state, in plain words, for the top of the panel: a reader
 * has to know the numbers are old BEFORE reading them. "" when it is fine.
 */
function stateNote(b, fresh) {
  if (fresh.state === "down" && b.observation_time) {
    return `<p class="sl-note sl-note--state sl-note--down">No report since ${formatWeekdayDayTime(b.observation_time)}. These are its last readings, not current conditions.</p>`;
  }
  if (fresh.state === "late") {
    return `<p class="sl-note sl-note--state sl-note--late">This report is later than the station usually sends one; conditions may have changed.</p>`;
  }
  if (fresh.state === "out") {
    const reason = b.out_of_service?.reason ? ` ${b.out_of_service.reason}` : "";
    return `<p class="sl-note sl-note--state sl-note--down">Out of service.${reason}</p>`;
  }
  return "";
}

/** Plain-words notes under the numbers: backup sensor, terms, caveats. */
function detailNotes(b, meta) {
  const notes = [];
  if (b.wave_sensor) {
    notes.push(
      `<p class="sl-note">Waves from the ${b.wave_sensor} sensor: the station's main wave sensor has no reading.</p>`,
    );
  }
  if (usesDominantPeriod(meta)) {
    notes.push(
      `<p class="sl-note">Dominant period: the wave period carrying the most energy (NOAA's term for peak period).</p>`,
    );
  }
  if (meta?.caveat) notes.push(`<p class="sl-note">${meta.caveat}</p>`);
  return notes.join("");
}

/** Text actions at the foot of the panel. */
function detailActions(b, meta) {
  const hasChartData = b.wave_height_sig != null || b.wind_speed != null;
  const link = sourceUrl(meta);
  return `
      <div class="sl-actions">
        <button type="button" class="sl-action" data-action="history" aria-expanded="false">${icon("history")}12-hour history</button>
        ${hasChartData ? `<button type="button" class="sl-action" data-action="charts">${icon("chart")}Charts</button>` : ""}
        <button type="button" class="sl-action" data-action="map">${icon("map")}Show on map</button>
        ${link ? `<a class="sl-action" href="${link}" target="_blank" rel="noopener noreferrer">${meta?.source ?? "Source"}${icon("external", "icon--trailing")}<span class="visually-hidden"> (opens in a new tab)</span></a>` : ""}
      </div>`;
}

// The panel's last control, so a long panel (or its history table) can be
// closed from the bottom without scrolling back up to the row.
const CLOSE_BUTTON = `<button type="button" class="sl-action sl-close" data-action="close">${icon("collapse")}Close</button>`;

/** The panel a row opens: readings, notes, actions, the lazy history slot, Close. */
export function buildStationDetail(b, id, meta) {
  if (b.no_data) {
    return `<p class="sl-note">No readings from this station are on hand.</p>${detailActions(b, meta)}${CLOSE_BUTTON}`;
  }
  const fresh = freshness(b);
  const state = stateNote(b, fresh);
  // A down station's note already gives the time of its last report.
  const observed =
    b.observation_time && fresh.state !== "down"
      ? `<p class="sl-observed">Observed ${formatWeekdayDayTime(b.observation_time)}</p>`
      : "";
  return `
      ${state}
      ${observed}
      <div class="sl-groups">${waveStats(b, meta)}${windAndWeatherStats(b, meta)}</div>
      ${detailNotes(b, meta)}
      ${detailActions(b, meta)}
      <div class="sl-history" id="sl-history-${id}" hidden></div>
      ${CLOSE_BUTTON}`;
}

/** The whole list: regions as small headings over their rows, in columns. */
export function buildStationList(data, stationMeta, regions = REGIONS) {
  const columns = [];
  for (const group of stationGroups(data, stationMeta, regions)) {
    const firstInColumn = columns[group.column] == null;
    const rows = group.stations
      .map(
        (id) => `
        <li class="sl-item" id="buoy-${id}">
          ${buildStationRow(data[id], id, stationMeta[id])}
          <div class="sl-detail" id="sl-detail-${id}" hidden></div>
        </li>`,
      )
      .join("");
    const headingId = `sl-region-${group.label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
    columns[group.column] =
      (columns[group.column] ?? "") +
      `<section class="sl-region" aria-labelledby="${headingId}">
        <div class="sl-region-head">
          <h2 class="sl-region-title" id="${headingId}">${group.label}</h2>${firstInColumn ? HEADING_LABELS : ""}
        </div>
        <ul class="sl-rows">${rows}</ul>
      </section>`;
  }
  return columns
    .filter(Boolean)
    .map((html) => `<div class="sl-column">${html}</div>`)
    .join("");
}
