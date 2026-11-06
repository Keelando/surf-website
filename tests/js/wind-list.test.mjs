import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { mock, test } from "node:test";

// Ages are measured against the clock, so the clock is pinned.
const NOW = Date.parse("2026-11-06T20:30:00Z");
mock.timers.enable({ apis: ["Date"], now: NOW });
const MIN = 60_000;

// wind-list.js imports station-list.js, which imports buoy-card.js, which
// reaches for helpers chart-utils-v4.js puts on the page as a classic script.
globalThis.getDirectionalArrow = (deg, kind) => `[${kind}@${deg}]`;
globalThis.degreesToCardinal = () => "N";

const {
  DEFAULT_SORT,
  WIND_REGIONS,
  buildWindDetail,
  buildWindList,
  buildRowActions,
  buildWindRow,
  nextSort,
  regionGroups,
  regionSlug,
  sortEntries,
  windEntry,
} = await import("../../site/assets/js/wind-list.js");

const iso = (minutesAgo) => new Date(NOW - minutesAgo * MIN).toISOString();

/** A store entry (wind-data.js normalizeWindLatest), reporting on time unless told otherwise. */
function station(name, speed, extra = {}) {
  return {
    name,
    wind_speed_kt: speed,
    wind_gust_kt: speed == null ? null : speed + 8,
    wind_direction_deg: 112,
    wind_direction_cardinal: "ESE",
    air_temp_c: 10.1,
    pressure_hpa: 1005.3,
    observation_time: iso(24),
    status: "ok",
    late_after_minutes: 186,
    down_after_minutes: 720,
    _isWindType: true,
    ...extra,
  };
}

const META = {
  CWGT: { name: "Sisters Island", short_name: "Sisters Is.", region: "STRAIT OF GEORGIA" },
  CWAS: { name: "Pam Rocks", region: "HOWE SOUND" },
  KBLI: { name: "Bellingham Airport", region: "HARO STRAIT & SAN JUANS" },
  SISW1: { name: "Smith Island", region: "JUAN DE FUCA STRAIT" },
  "4600146": { name: "Halibut Bank", short_name: "Halibut", region: "STRAIT OF GEORGIA" },
};

const entries = [
  windEntry("CWGT", station("Sisters Island", 41), META.CWGT),
  windEntry("CWAS", station("Pam Rocks", 12), META.CWAS),
  windEntry("KBLI", station("Bellingham Airport", 18), META.KBLI),
  // Down: last report 13 hours ago, so its numbers are not shown or sorted on.
  windEntry("SISW1", station("Smith Island", 35, { observation_time: iso(780) }), META.SISW1),
  windEntry(
    "4600146",
    station("Halibut Bank", 29, { _isWindType: false }),
    META["4600146"],
  ),
];

test("the default sort is strongest wind first", () => {
  assert.deepEqual(DEFAULT_SORT, { key: "speed", dir: "desc" });
  const ids = sortEntries(entries).map((e) => e.id);
  assert.deepEqual(ids, ["CWGT", "4600146", "KBLI", "CWAS", "SISW1"]);
});

test("a row with nothing to show sorts last whichever way the column runs", () => {
  const asc = sortEntries(entries, { key: "speed", dir: "asc" }).map((e) => e.id);
  assert.equal(asc.at(-1), "SISW1");
  assert.deepEqual(asc.slice(0, 2), ["CWAS", "KBLI"]);
});

test("name and age sort every row, down ones included", () => {
  const byName = sortEntries(entries, { key: "name", dir: "asc" }).map((e) => e.name);
  assert.deepEqual(byName, [
    "Bellingham Airport",
    "Halibut Bank",
    "Pam Rocks",
    "Sisters Island",
    "Smith Island",
  ]);
  const freshestFirst = sortEntries(entries, { key: "age", dir: "asc" }).map((e) => e.id);
  assert.equal(freshestFirst.at(-1), "SISW1");
});

test("a column head flips its own sort and starts a new one at its natural direction", () => {
  assert.deepEqual(nextSort({ key: "speed", dir: "desc" }, "speed"), { key: "speed", dir: "asc" });
  assert.deepEqual(nextSort({ key: "speed", dir: "desc" }, "name"), { key: "name", dir: "asc" });
  assert.deepEqual(nextSort({ key: "name", dir: "asc" }, "gust"), { key: "gust", dir: "desc" });
});

test("regions come in WIND_REGIONS order, each sorted, unknown regions after", () => {
  const extra = windEntry("X", station("Somewhere", 5), { region: "MARS" });
  const groups = regionGroups([...entries, extra]);
  assert.deepEqual(
    groups.map((g) => g.label),
    ["Strait of Georgia", "Howe Sound", "Haro Strait & San Juans", "Juan de Fuca Strait", "MARS"],
  );
  assert.deepEqual(
    groups[0].entries.map((e) => e.id),
    ["CWGT", "4600146"],
  );
});

test("every region in the registry has a heading in WIND_REGIONS", () => {
  const registry = JSON.parse(
    readFileSync(new URL("../../config/stations.json", import.meta.url), "utf8"),
  );
  const known = new Set(WIND_REGIONS.map((r) => r.region));
  const used = new Set(
    [...Object.values(registry.wind), ...Object.values(registry.buoys)].map((s) => s.region),
  );
  const missing = [...used].filter((r) => !known.has(r));
  assert.deepEqual(missing, []);
});

test("a row: full name for screen readers, short name for a phone, buoy tag, live age", () => {
  const html = buildWindRow(entries[4]);
  assert.match(html, /<span class="visually-hidden">Halibut Bank \(buoy\)<\/span>/);
  assert.match(html, /wl-title-short">Halibut</);
  assert.match(html, /wl-kind">buoy</);
  assert.match(html, /sl-age-text">24m</);
  assert.match(html, /sl-age-ago"> ago</);
  assert.match(html, /wl-row--ok/);
  // Gust carries its "G" for the stacked layout; hidden from screen readers.
  assert.match(html, /wl-gust-label" aria-hidden="true">G <\/span>37/);
});

test("a down row shows no numbers, and its panel says how long", () => {
  const down = entries[3];
  assert.equal(down.fresh.state, "down");
  const row = buildWindRow(down);
  assert.match(row, /No recent data/);
  assert.doesNotMatch(row, /wl-speed/);
  assert.match(buildWindDetail(down), /No report for 13h/);
});

test("an out-of-service station says so and keeps its frozen numbers out of the panel", () => {
  const out = windEntry(
    "whiterock_east",
    station("White Rock", 11.7, { out_of_service: { since: "2026-10-23", reason: "Defective." } }),
    { name: "White Rock", source_url: "https://example.org/wr" },
  );
  assert.equal(out.fresh.state, "out");
  assert.match(buildWindRow(out), /Out of service/);
  const panel = buildWindDetail(out);
  assert.match(panel, /Currently down for service\. Defective\./);
  assert.doesNotMatch(panel, /sl-stats/);
});

test("the list: column heads with the active sort pressed, regions only in region view", () => {
  const all = buildWindList(entries);
  assert.match(all, /data-sort="speed" aria-pressed="true">Wind<span class="visually-hidden">, sorted descending/);
  assert.doesNotMatch(all, /wl-region-title/);
  const byRegion = buildWindList(entries, { view: "region", sort: { key: "name", dir: "asc" } });
  assert.equal((byRegion.match(/wl-region-title/g) || []).length, 4);
  assert.match(byRegion, /data-sort="name" aria-pressed="true"/);
});

test("region view: each heading folds its rows, and a folded one keeps its count", () => {
  assert.equal(regionSlug("Haro Strait & San Juans"), "haro-strait-san-juans");
  const html = buildWindList(entries, {
    view: "region",
    collapsed: new Set(["strait-of-georgia"]),
  });
  assert.match(
    html,
    /data-region="strait-of-georgia" aria-expanded="false" aria-controls="wl-rows-strait-of-georgia"/,
  );
  assert.match(html, /<ul class="sl-rows wl-rows" id="wl-rows-strait-of-georgia" hidden>/);
  assert.match(html, /wl-region-count">2 stations</);
  assert.match(html, /data-region="howe-sound" aria-expanded="true"/);
  assert.match(html, /wl-region-count">1 station</);
});

test("the direction head has a phone form and a wide form", () => {
  const html = buildWindList(entries);
  assert.match(html, /wl-label-short">Dir<\/span><span class="wl-label-wide">Direction</);
});

test("row icons: map and chart always, the source link only where there is one; out of the tab order", () => {
  const withLink = buildRowActions(
    windEntry("CWGT", station("Sisters Island", 41), {
      ...META.CWGT,
      source: "Environment Canada",
      source_url: "https://example.org/cwgt",
    }),
  );
  assert.match(withLink, /data-action="map" tabindex="-1"[^>]*aria-label="Show Sisters Island on the map"/);
  assert.match(withLink, /data-action="chart" tabindex="-1"/);
  assert.match(withLink, /aria-label="Sisters Island at Environment Canada \(opens in a new tab\)"/);
  assert.doesNotMatch(buildRowActions(entries[1]), /<a /);
});

test("a drawn flag from the registry's country, beside the name and in the panel", async () => {
  const { flagIcon } = await import("../../site/assets/js/shared/flags.js");
  assert.equal(flagIcon("XX"), "");
  assert.match(flagIcon("CA"), /^<svg class="flag" viewBox="0 0 32 16"[^>]*aria-hidden="true"/);
  assert.match(flagIcon("US", { label: true }), /role="img" aria-label="United States"/);
  const e = windEntry("SISW1", station("Smith Island", 20), { ...META.SISW1, country: "US" });
  assert.match(buildWindRow(e), /<svg class="flag wl-flag"/);
  assert.match(buildWindDetail(e), /United States<\/p>/);
});
