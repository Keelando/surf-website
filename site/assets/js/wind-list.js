/* -----------------------------
   Winds page station list (ES module)

   One compact row per station with wind readings (the wind stations and the
   buoys that measure wind), in the home page's look, sortable by its column
   heads, shown either as one list or grouped by region (user 2026-11-06:
   "it's nice to see the entire list sortable", with a toggle for regions).
   Replaces the conditions table (docs/project/UI_OVERHAUL.md, step 3).

   Pure HTML-string builders over the wind data store's normalized entries
   (wind-data.js `latestAll`) and their stations.json metadata; wind-stations.js
   keeps the fetching and the wiring. Age and status come from the home list's
   helpers, so both pages judge a report the same way.
   ----------------------------- */

import { countryName, flagIcon } from "./shared/flags.js";
import { formatWeekdayDayTime } from "./shared/format-time.js";
import { icon } from "./shared/icons.js";
import { liveAgeMinutes, outOfServiceHTML } from "./shared/staleness.js";
import { ageParts, compactAge, directionArrow, freshness } from "./station-list.js";

/** Escape a registry string for an HTML attribute value (main's wind table had this). */
function escapeAttr(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

/**
 * Region headings in display order, north and inside first, then out to the
 * open coast. The registry's `region` (config/stations.json) decides which
 * station is where; like the home page's REGIONS this is an ORDERING, not a
 * whitelist: an unlisted region still shows, after these, under its own name.
 */
export const WIND_REGIONS = [
  { region: "STRAIT OF GEORGIA", label: "Strait of Georgia" },
  { region: "HOWE SOUND", label: "Howe Sound" },
  { region: "BOUNDARY BAY", label: "Boundary Bay" },
  { region: "HARO STRAIT & SAN JUANS", label: "Haro Strait & San Juans" },
  { region: "JUAN DE FUCA STRAIT", label: "Juan de Fuca Strait" },
  { region: "WEST COAST VANCOUVER ISLAND", label: "West Coast Vancouver Island" },
];

const UNGROUPED_LABEL = "Other stations";

/**
 * The sortable columns. `first` is the direction a column sorts in when it is
 * picked: strongest wind first, names A to Z, freshest report first.
 */
export const SORTS = {
  name: { label: "Station", first: "asc" },
  age: { label: "Age", first: "asc" },
  // Spelled out where the list is wide; "Dir" on a phone (winds-v4.css).
  direction: { label: "Dir", wide: "Direction", first: "asc" },
  speed: { label: "Wind", first: "desc" },
  gust: { label: "Gust", first: "desc" },
  temp: { label: "Temp", first: "desc" },
  pressure: { label: "Pressure", first: "asc" },
};

export const DEFAULT_SORT = { key: "speed", dir: "desc" };

const DASH = "—";

/**
 * One row's worth of facts from a store entry `s` and its registry entry.
 * Numbers are only "shown" for a station reporting on time or late: a
 * reading days old beside live ones reads as current (the home page's rule).
 */
export function windEntry(id, s, meta = {}, now = Date.now()) {
  const fresh = freshness(s, now);
  const showValues = fresh.state === "ok" || fresh.state === "late";
  const num = (v) => (v != null && Number.isFinite(Number(v)) ? Number(v) : null);
  return {
    id,
    name: s.name || meta.name || id,
    shortName: meta.short_name || s.name || meta.name || id,
    region: meta.region ?? "",
    isBuoy: s._isWindType === false || meta.is_buoy === true,
    fresh,
    showValues,
    // The shared rule (NaN-safe, falls back to the export's age_minutes), so a
    // row's sort key and its age text never disagree.
    ageMinutes: liveAgeMinutes(s, now),
    speed: num(s.wind_speed_kt),
    gust: num(s.wind_gust_kt),
    degrees: num(s.wind_direction_deg),
    cardinal: s.wind_direction_cardinal ?? null,
    temp: num(s.air_temp_c),
    pressure: num(s.pressure_hpa),
    s,
    meta,
  };
}

/** The value a column sorts on, or null when the row has nothing to show there. */
function sortValue(e, key) {
  if (key === "name") return e.name.toLowerCase();
  if (key === "age") return e.ageMinutes;
  if (!e.showValues) return null;
  return { direction: e.degrees, speed: e.speed, gust: e.gust, temp: e.temp, pressure: e.pressure }[
    key
  ];
}

/**
 * Sorted copy of `entries`. Rows with nothing in the sorted column go last
 * whichever way it runs, then by name, so the order is always the same for
 * the same data.
 */
export function sortEntries(entries, { key, dir } = DEFAULT_SORT) {
  const sign = dir === "desc" ? -1 : 1;
  return [...entries].sort((a, b) => {
    const va = sortValue(a, key);
    const vb = sortValue(b, key);
    if (va == null && vb != null) return 1;
    if (vb == null && va != null) return -1;
    if (va != null && vb != null && va !== vb) {
      return (typeof va === "string" ? va.localeCompare(vb) : va - vb) * sign;
    }
    return a.name.localeCompare(b.name);
  });
}

/** The sort a column-head click asks for: the same column flips, a new one starts at its `first`. */
export function nextSort(current, key) {
  if (current.key === key) return { key, dir: current.dir === "asc" ? "desc" : "asc" };
  return { key, dir: SORTS[key]?.first ?? "asc" };
}

/**
 * Entries grouped by registry region in WIND_REGIONS order, then any other
 * region alphabetically, then stations without one; each group sorted.
 * @returns {{label: string, entries: Object[]}[]}
 */
export function regionGroups(entries, sort = DEFAULT_SORT, regions = WIND_REGIONS) {
  const byRegion = new Map();
  for (const e of entries) {
    if (!byRegion.has(e.region)) byRegion.set(e.region, []);
    byRegion.get(e.region).push(e);
  }
  const known = regions
    .filter((r) => byRegion.has(r.region))
    .map((r) => ({ label: r.label, entries: sortEntries(byRegion.get(r.region), sort) }));
  const rest = [...byRegion.keys()]
    .filter((region) => !regions.some((r) => r.region === region))
    .sort((a, b) => (a === "" ? 1 : b === "" ? -1 : a.localeCompare(b)))
    .map((region) => ({
      label: region || UNGROUPED_LABEL,
      entries: sortEntries(byRegion.get(region), sort),
    }));
  return [...known, ...rest];
}

const CHEVRON =
  `<svg class="sl-chevron" viewBox="0 0 16 16" aria-hidden="true">` +
  `<path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" stroke-width="2" ` +
  `stroke-linecap="round" stroke-linejoin="round"/></svg>`;

const fixed = (v, d) => (v == null ? null : v.toFixed(d));

/** A number cell; `spoken` names it for screen readers, which hear no column heads. */
function numCell(cls, value, spoken, unit = "") {
  const shown = value == null ? `<span class="sl-none">${DASH}</span>` : value;
  const unitHTML = unit && value != null ? `<span class="sl-unit"> ${unit}</span>` : "";
  return `<span class="wl-cell ${cls}"><span class="visually-hidden">${spoken} </span>${shown}${unitHTML}</span>`;
}

/**
 * One station row: a button that toggles its panel. The name block holds the
 * name over its status light and age, as the home page's rows do; the
 * numbers follow in the column heads' tracks (winds-v4.css). A phone shows
 * the registry's short name when the full one won't fit; screen readers get
 * the full name either way.
 */
export function buildWindRow(e) {
  const { age, ago, word } = ageParts(e.fresh.text);
  const ageLine =
    `<span class="sl-age-words"><span class="sl-age-text">${age}</span>` +
    (ago ? `<span class="sl-age-ago">${ago}</span>` : "") +
    (word ? `<span class="sl-age-state"> · ${word}</span>` : "") +
    `</span>`;

  let values;
  if (e.showValues) {
    const cardinal = e.cardinal ? `<span class="wl-cardinal">${e.cardinal}</span>` : "";
    const dir =
      e.degrees != null || e.cardinal
        ? `<span class="wl-cell wl-dir"><span class="visually-hidden">from </span>${directionArrow(e.degrees)}${cardinal}</span>`
        : `<span class="wl-cell wl-dir"><span class="sl-none">${DASH}</span></span>`;
    values =
      dir +
      numCell("wl-speed", e.speed != null ? Math.round(e.speed) : null, "wind", "kt") +
      numCell(
        "wl-gust",
        e.gust != null
          ? `<span class="wl-gust-label" aria-hidden="true">G </span>${Math.round(e.gust)}`
          : null,
        "gust",
      ) +
      numCell("wl-temp", fixed(e.temp, 1), "air temperature", "°C") +
      numCell("wl-pressure", fixed(e.pressure, 1), "pressure", "hPa");
  } else {
    const text = e.fresh.state === "out" ? "Out of service" : "No recent data";
    values = `<span class="wl-cell wl-empty sl-none">${text}</span>`;
  }

  const buoy = e.isBuoy ? `<span class="wl-kind">buoy</span>` : "";
  // The flag shows on a wide list only (winds-v4.css); the panel has it too.
  const flag = flagIcon(e.meta.country, { className: "wl-flag" });
  return `
    <button type="button" class="wl-row wl-row--${e.fresh.state}" aria-expanded="false" aria-controls="wl-detail-${e.id}">
      <span class="wl-name">
        <span class="visually-hidden">${e.name}${e.isBuoy ? " (buoy)" : ""}</span>
        <span class="sl-title wl-title" aria-hidden="true"><span class="wl-title-full">${e.name}</span><span class="wl-title-short">${e.shortName}</span>${flag}${buoy}</span>
        <span class="sl-age"><span class="sl-dot" aria-hidden="true"></span>${ageLine}</span>
      </span>
      ${values}
      ${CHEVRON}
    </button>`;
}

/** `<div>` of label/value pairs; pairs with a null value are skipped. */
function statGroup(title, pairs) {
  const items = pairs
    .filter(([, value]) => value != null)
    .map(([label, value]) => `<div class="sl-stat"><dt>${label}</dt><dd>${value}</dd></div>`)
    .join("");
  return items
    ? `<div class="sl-group"><h3 class="sl-group-title">${title}</h3><dl class="sl-stats">${items}</dl></div>`
    : "";
}

const withUnit = (value, unit) =>
  value == null ? null : `${value}<span class="sl-unit"> ${unit}</span>`;

/** The note that opens a panel whose station is late, down or out of service. */
function stateNote(e) {
  const { state } = e.fresh;
  const age = compactAge(e.ageMinutes);
  if (state === "late") {
    const when = age == null ? "." : `: the last report came ${age} ago.`;
    return `<p class="sl-note sl-note--state sl-note--late">Running late${when}</p>`;
  }
  if (state === "down") {
    const when = e.s.observation_time
      ? ` Its last report, ${formatWeekdayDayTime(e.s.observation_time)}, is below.`
      : "";
    return `<p class="sl-note sl-note--state sl-note--down">No report for ${age ?? "a long time"}.${when}</p>`;
  }
  if (state === "out") {
    return `<p class="sl-note sl-note--state sl-note--down">${outOfServiceHTML(e.s.out_of_service, e.meta.source_url)}</p>`;
  }
  return "";
}

/** The panel a row opens: readings, notes, actions, Close. */
export function buildWindDetail(e) {
  const { s, meta } = e;
  const out = e.fresh.state === "out";
  const observed =
    s.observation_time && e.fresh.state !== "down" && !out
      ? `<p class="sl-observed">Observed ${formatWeekdayDayTime(s.observation_time)}</p>`
      : "";
  const bearing =
    e.degrees != null
      ? `${e.cardinal ?? ""} <span class="sl-muted">${Math.round(e.degrees)}°</span>`.trim()
      : e.cardinal;
  // An out-of-service station's last numbers are the frozen ones; leave them out.
  const groups = out
    ? ""
    : statGroup("Wind", [
        ["Speed", withUnit(e.speed != null ? Math.round(e.speed) : null, "kt")],
        ["Gust", withUnit(e.gust != null ? Math.round(e.gust) : null, "kt")],
        ["From", bearing],
      ]) +
      statGroup("Weather", [
        ["Air", withUnit(fixed(e.temp, 1), "°C")],
        ["Pressure", withUnit(fixed(e.pressure, 1), "hPa")],
        [
          "Humidity",
          withUnit(s.humidity_percent != null ? Math.round(s.humidity_percent) : null, "%"),
        ],
      ]);
  const kind = e.isBuoy ? "Ocean buoy" : "Weather station";
  const source = meta.source ? ` · ${meta.source}` : "";
  const where = countryName(meta.country)
    ? ` · ${flagIcon(meta.country, { className: "wl-flag" })} ${countryName(meta.country)}`
    : "";
  const notes = [meta.caveat, meta.note]
    .filter(Boolean)
    .map((text) => `<p class="sl-note">${text}</p>`)
    .join("");
  const link = meta.source_url
    ? `<a class="sl-action" href="${escapeAttr(meta.source_url)}" target="_blank" rel="noopener noreferrer">${meta.source ?? "Source"}${icon("external", "icon--trailing")}<span class="visually-hidden"> (opens in a new tab)</span></a>`
    : "";
  return `
      ${stateNote(e)}
      ${observed}
      <div class="sl-groups">${groups}</div>
      <p class="sl-note wl-kind-note">${kind}${source}${where}</p>
      ${notes}
      <div class="sl-actions">
        <button type="button" class="sl-action" data-action="chart">${icon("chart")}Chart</button>
        <button type="button" class="sl-action" data-action="map">${icon("map")}Show on map</button>
        ${link}
      </div>
      <button type="button" class="sl-action sl-close" data-action="close">${icon("collapse")}Close</button>`;
}

/** A column head's text: a short and a wide form where it has both. */
function label(sort) {
  if (!sort.wide) return sort.label;
  return `<span class="wl-label-short">${sort.label}</span><span class="wl-label-wide">${sort.wide}</span>`;
}

/** One column head: a sort button, pressed when it is the active sort. */
function sortButton(key, sort) {
  const active = sort.key === key;
  const arrow = active ? (sort.dir === "asc" ? "▲" : "▼") : "";
  const spoken = active
    ? `<span class="visually-hidden">, sorted ${sort.dir === "asc" ? "ascending" : "descending"}</span>`
    : "";
  return (
    `<button type="button" class="wl-sort wl-sort--${key}" data-sort="${key}" aria-pressed="${active}">` +
    `${label(SORTS[key])}${spoken}<span class="wl-sort-arrow" aria-hidden="true">${arrow}</span></button>`
  );
}

/**
 * The column heads. On their own row, in the rows' tracks, so each sits over
 * its numbers; Station and Age share the name column, as the name and its
 * age share the row's.
 */
export function buildSortHead(sort = DEFAULT_SORT) {
  return `
    <div class="wl-head" role="group" aria-label="Sort the stations">
      <span class="wl-head-name">${sortButton("name", sort)}${sortButton("age", sort)}</span>
      ${["direction", "speed", "gust", "temp", "pressure"].map((key) => sortButton(key, sort)).join("")}
    </div>`;
}

/**
 * A wide list's row actions: map, chart and source as icons at the row's
 * right end (user 2026-11-06), so a desktop reader needn't open the panel
 * for them. They sit beside the row's button, not inside it (a button
 * can't hold buttons), laid over its spare track (winds-v4.css). Out of the
 * tab order: the panel carries the same three, labelled, one tab stop per
 * station rather than four. A phone shows none of them.
 */
export function buildRowActions(e) {
  const source = e.meta.source ?? "the source";
  const link = e.meta.source_url
    ? `<a class="wl-act" href="${escapeAttr(e.meta.source_url)}" target="_blank" rel="noopener noreferrer" tabindex="-1" title="${escapeAttr(source)} (opens in a new tab)" aria-label="${escapeAttr(e.name)} at ${escapeAttr(source)} (opens in a new tab)">${icon("external")}</a>`
    : "";
  return `
          <span class="wl-actions">
            <button type="button" class="wl-act" data-action="map" tabindex="-1" title="Show on the map" aria-label="Show ${e.name} on the map">${icon("map")}</button>
            <button type="button" class="wl-act" data-action="chart" tabindex="-1" title="Wind chart" aria-label="Wind chart for ${e.name}">${icon("chart")}</button>
            ${link}
          </span>`;
}

function rowsHTML(entries) {
  return entries
    .map(
      (e) => `
        <li class="sl-item wl-item" id="wind-row-${e.id}" data-station-id="${e.id}">
          ${buildWindRow(e)}
          ${buildRowActions(e)}
          <div class="sl-detail wl-detail" id="wl-detail-${e.id}" hidden></div>
        </li>`,
    )
    .join("");
}

/** A region's id for the page: "Haro Strait & San Juans" → "haro-strait-san-juans". */
export function regionSlug(label) {
  return label
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

const REGION_CHEVRON =
  `<svg class="wl-region-chevron" viewBox="0 0 16 16" aria-hidden="true">` +
  `<path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" stroke-width="2" ` +
  `stroke-linecap="round" stroke-linejoin="round"/></svg>`;

/**
 * The whole list: the column heads, then either every station in one block
 * ("all") or a block per region ("region"). In region view each heading is
 * a button that folds its stations away (user 2026-11-06); `collapsed` holds
 * the slugs of the folded ones, and a folded region keeps its count in view.
 */
export function buildWindList(
  entries,
  { view = "all", sort = DEFAULT_SORT, collapsed = new Set() } = {},
) {
  const head = buildSortHead(sort);
  if (view !== "region") {
    return `${head}<ul class="sl-rows wl-rows">${rowsHTML(sortEntries(entries, sort))}</ul>`;
  }
  const groups = regionGroups(entries, sort)
    .map((group) => {
      const slug = regionSlug(group.label);
      const open = !collapsed.has(slug);
      const n = group.entries.length;
      return `<section class="sl-region wl-region" aria-labelledby="wl-region-${slug}">
        <h2 class="sl-region-title wl-region-title" id="wl-region-${slug}">
          <button type="button" class="wl-region-toggle" data-region="${slug}" aria-expanded="${open}" aria-controls="wl-rows-${slug}">
            <span class="wl-region-name">${group.label}</span>
            <span class="wl-region-count">${n} ${n === 1 ? "station" : "stations"}</span>
            ${REGION_CHEVRON}
          </button>
        </h2>
        <ul class="sl-rows wl-rows" id="wl-rows-${slug}"${open ? "" : " hidden"}>${rowsHTML(group.entries)}</ul>
      </section>`;
    })
    .join("");
  return head + groups;
}
