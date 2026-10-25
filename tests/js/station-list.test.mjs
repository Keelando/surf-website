import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { mock, test } from "node:test";

// Ages are measured against the clock now, not read from the export's
// snapshot, so the clock is pinned: 49 minutes after the fixture's reading.
const NOW = Date.parse("2026-10-23T22:54:00Z");
mock.timers.enable({ apis: ["Date"], now: NOW });
const MIN = 60_000;

// station-list.js imports buoy-card.js, which reaches for the arrow/cardinal
// helpers that chart-utils-v4.js puts on the page as a classic script.
globalThis.getDirectionalArrow = (deg, kind) => `[${kind}@${deg}]`;
globalThis.degreesToCardinal = () => "N";

const {
  STATION_GROUPS,
  buildStationDetail,
  buildStationList,
  buildStationRow,
  compactAge,
  directionArrow,
  freshness,
  refreshAges,
  rowReading,
  swellGeometry,
  swellLine,
} = await import("../../site/assets/js/station-list.js");

const EC = { source: "Environment Canada", type: "wave_buoy" };
const NOAA_DOMINANT = { source: "NOAA NDBC", type: "wave_buoy" };
const NOAA_SWELL = { source: "NOAA NDBC", type: "wave_buoy", wave_display: "swell" };
const PILE = { source: "Surrey FlowWorks", type: "pile_mounted_wave_station" };

const halibut = {
  name: "Halibut Bank",
  observation_time: "2026-10-23T22:05:00+00:00",
  status: "ok",
  age_minutes: 49,
  wave_height_sig: 0.6,
  wave_height_max: 0.9,
  wave_height_avg: 0.4,
  wave_period_sig: 3.7,
  wave_period_avg: 3.4,
  wave_period_peak: 3.9,
  wave_direction_peak: 262,
  wave_direction_peak_cardinal: "W",
  wave_direction_spread_peak: 18,
  wave_direction_spread_avg: 24,
  wind_speed: 9.3,
  wind_gust: 10.5,
  wind_direction: 272,
  wind_direction_cardinal: "W",
  sea_temp: 11.5,
  air_temp: 11,
  pressure: 1026.8,
};

test("compactAge: minutes, then hours and minutes through the second hour, then hours, then days", () => {
  assert.equal(compactAge(0), "0m");
  assert.equal(compactAge(49.9), "49m");
  assert.equal(compactAge(59.99), "59m");
  assert.equal(compactAge(60), "1h");
  assert.equal(compactAge(61), "1h 1m");
  assert.equal(compactAge(85), "1h 25m");
  assert.equal(compactAge(119.9), "1h 59m");
  assert.equal(compactAge(120), "2h");
  assert.equal(compactAge(47 * 60 + 59), "47h");
  assert.equal(compactAge(48 * 60), "2d");
  assert.equal(compactAge(-5), "0m"); // a clock a little ahead of ours
  assert.equal(compactAge(null), null);
  assert.equal(compactAge(Number.NaN), null);
});

test("freshness leads with the age and names only a problem", () => {
  assert.deepEqual(freshness(halibut), { state: "ok", text: "49m ago" });
  assert.deepEqual(freshness({ status: "late", age_minutes: 250 }), {
    state: "late",
    text: "4h ago · late",
  });
  assert.deepEqual(freshness({ status: "down", age_minutes: 22607 }), {
    state: "down",
    text: "15d ago · down",
  });
  // The owner's word beats our clock.
  assert.equal(freshness({ status: "ok", out_of_service: {} }).state, "out");
  assert.equal(freshness({ no_data: true }).state, "none");
});

test("freshness falls back to the legacy stale flag without a status", () => {
  assert.equal(freshness({ stale: true, age_minutes: 300 }).state, "late");
});

// ---- Sanity: the age and the dot follow the clock, not the export --------
//
// An hourly EC buoy as exported: thresholds from its own rhythm (186 min late,
// 372 down after the 12 h floor is applied in the real data; small numbers
// here keep the boundaries readable).
const timed = (minutesAgo, extra = {}) => ({
  name: "Test",
  observation_time: new Date(NOW - minutesAgo * MIN).toISOString(),
  status: "ok",
  age_minutes: 3, // a stale snapshot: the export ran long ago
  late_after_minutes: 186,
  down_after_minutes: 744,
  ...extra,
});

test("the age is measured from observation_time, not the export's age_minutes", () => {
  assert.equal(freshness(timed(25)).text, "25m ago");
  assert.equal(freshness(timed(85)).text, "1h 25m ago");
  assert.equal(freshness(timed(150)).text, "2h ago");
  // No observation_time: the snapshot is all there is.
  assert.equal(freshness({ status: "ok", age_minutes: 12 }).text, "12m ago");
});

test("the dot turns late and down exactly at the station's own thresholds", () => {
  // Strictly greater than, as lib/report_status.py compares.
  assert.equal(freshness(timed(186)).state, "ok");
  assert.equal(freshness(timed(187)).state, "late");
  assert.equal(freshness(timed(187)).text, "3h ago · late");
  assert.equal(freshness(timed(744)).state, "late");
  assert.equal(freshness(timed(745)).state, "down");
  assert.equal(freshness(timed(745)).text, "12h ago · down");
});

test("an export that stopped updating cannot keep a station green", () => {
  // Exported as "ok, 3 minutes old", but the reading is now 5 h old.
  assert.deepEqual(freshness(timed(300)), { state: "late", text: "5h ago · late" });
});

test("the live clock never makes a station look better than the export said", () => {
  // Exported down for a reason of its own (an override, a clock skew upstream).
  assert.equal(freshness(timed(10, { status: "down" })).state, "down");
  assert.equal(freshness(timed(10, { status: "late" })).state, "late");
});

test("refreshAges updates the text in place and reports a state change", () => {
  const data = { A: timed(59), B: timed(100) };
  const fakeItem = (id, state) => {
    const text = { textContent: "" };
    const row = { classList: { contains: (c) => c === `sl-row--${state}` } };
    return {
      id: `buoy-${id}`,
      text,
      querySelector: (sel) => (sel === ".sl-row" ? row : sel === ".sl-age-text" ? text : null),
    };
  };
  const items = [fakeItem("A", "ok"), fakeItem("B", "ok")];
  const container = { querySelectorAll: () => items };

  assert.equal(refreshAges(container, data), false);
  // The age itself; " ago" is a separate span a phone drops.
  assert.equal(items[0].text.textContent, "59m");
  assert.equal(items[1].text.textContent, "1h 40m");

  // Ninety minutes on, B (now 190 min) has crossed its late threshold.
  assert.equal(refreshAges(container, data, NOW + 90 * MIN), true);
  // The state word changes with a rebuild, which `true` asks for.
  assert.equal(items[1].text.textContent, "3h");
});

test("every dot state has its own colour, in both themes", () => {
  // Each state freshness() can return must set the dot's --led, late and
  // down must differ from ok and from each other, and every token must be
  // defined for light and dark, or the dot says nothing.
  const css = readFileSync(new URL("../../site/assets/css/home.css", import.meta.url), "utf8");
  const led = (selector) => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const block = new RegExp(`${escaped}[^{]*\\{([^}]*)\\}`).exec(css)?.[1] ?? "";
    return /--led:\s*var\((--[\w-]+)\)/.exec(block)?.[1] ?? null;
  };
  const tokens = {
    ok: led(".sl-dot "),
    late: led(".sl-row--late .sl-dot"),
    down: led(".sl-row--down .sl-dot"),
    out: led(".sl-row--out .sl-dot"),
    none: led(".sl-row--none .sl-dot"),
  };
  assert.deepEqual(tokens, {
    ok: "--sl-ok",
    late: "--sl-late-dot",
    down: "--sl-down-dot",
    out: "--sl-down-dot",
    none: "--sl-down-dot",
  });

  const values = (token) => [...css.matchAll(new RegExp(`${token}:\\s*([^;]+);`, "g"))].map((m) => m[1]);
  for (const token of ["--sl-ok", "--sl-late-dot", "--sl-down-dot"]) {
    assert.equal(values(token).length, 2, `${token} has a light and a dark value`);
  }
  for (const theme of [0, 1]) {
    const [ok, late, down] = ["--sl-ok", "--sl-late-dot", "--sl-down-dot"].map((t) => values(t)[theme]);
    assert.equal(new Set([ok, late, down]).size, 3, `three distinct dot colours (theme ${theme})`);
  }
});

test("rowReading follows each station family's field priorities", () => {
  const ec = rowReading(halibut, EC);
  assert.equal(ec.height, "0.6");
  assert.equal(ec.period, "3.7");
  assert.equal(ec.waveCardinal, "W");
  assert.equal(ec.wind, "9");
  assert.equal(ec.gust, "11");

  const neah = rowReading(
    { wave_height_sig: 2.1, swell_height: 1.8, swell_period: 11, swell_direction: 280 },
    NOAA_SWELL,
  );
  assert.equal(neah.height, "1.8");
  assert.equal(neah.period, "11.0");
  assert.equal(neah.isSwell, true);

  const pile = rowReading({ wave_height_sig: 0.09514, wave_period_avg: 2.3 }, PILE);
  assert.equal(pile.height, "0.10");
  assert.equal(pile.periodTag, "avg");
});

test("directionArrow turns a down arrow by the 'from' bearing", () => {
  assert.match(directionArrow(270), /rotate\(270deg\)/);
  assert.match(directionArrow(270), /aria-hidden="true"/);
  assert.equal(directionArrow(null), "");
});

test("a fresh row shows waves and wind, with the units small", () => {
  const html = buildStationRow(halibut, "4600146", EC);
  assert.match(html, /aria-expanded="false"/);
  assert.match(html, /aria-controls="sl-detail-4600146"/);
  assert.match(html, /0\.6<span class="sl-unit"> m<\/span>/);
  assert.match(html, /9<span class="sl-unit"> kt<\/span>/);
  assert.match(html, /G 11/);
  // Direction is an arrow on screen, a cardinal for screen readers.
  assert.match(html, /<span class="visually-hidden">, from W<\/span>/);
  assert.match(html, /sl-row--ok/);
});

test("a down row shows no numbers: days-old readings would pass for live", () => {
  const html = buildStationRow(
    { ...halibut, status: "down", age_minutes: 22607 },
    "4600303",
    EC,
  );
  assert.match(html, /sl-row--down/);
  // " ago" and the state word are their own spans, so a phone can drop them.
  assert.match(
    html,
    /<span class="sl-age-text">\d+[mhd][^<]*<\/span><span class="sl-age-ago"> ago<\/span><span class="sl-age-state"> · down</,
  );
  assert.doesNotMatch(html, /sl-big/);
  // Only the phone's line-2 stand-in, no readings.
  assert.match(html, /sl-cell--empty sl-none">No recent data</);
  assert.doesNotMatch(html, /sl-cell--waves|sl-cell--wind/);
});

test("swell amplitude is relative to what is rough at that station", () => {
  // 1 m is rough in the Strait of Georgia and a quiet day at La Perouse.
  const georgia = swellGeometry(1, 5, 270, "4600146").amplitude;
  const laPerouse = swellGeometry(1, 5, 270, "4600206").amplitude;
  assert.ok(georgia > laPerouse);
  assert.equal(georgia, swellGeometry(2.5, 5, 270, "4600206").amplitude);
});

test("swell geometry: height sets amplitude, period wavelength, bearing drift", () => {
  const calm = swellGeometry(0.1, 3, 270);
  const rough = swellGeometry(3, 10, 270);
  assert.ok(rough.amplitude > calm.amplitude);
  // Clamped at both ends: a flat calm still shows, a storm stays in the box.
  assert.ok(swellGeometry(0, 3, 0).amplitude > 0);
  assert.equal(swellGeometry(3, 3, 0).amplitude, swellGeometry(9, 3, 0).amplitude);
  assert.ok(rough.wavelength > calm.wavelength);
  assert.equal(swellGeometry(1, 0.5, 0).wavelength, 14);
  assert.equal(swellGeometry(1, 30, 0).wavelength, 72);
  // Same convention as the row's arrow: from the west travels right.
  assert.equal(swellGeometry(1, 5, 270).drift, 1);
  assert.equal(swellGeometry(1, 5, 90).drift, -1);
  assert.equal(swellGeometry(1, 5, 0).drift, 0);
  assert.equal(swellGeometry(1, 5, null).drift, 0);
  assert.equal(swellGeometry(null, 5, 90), null);
});

test("a row's swell line is decorative, and absent when down", () => {
  const fresh = buildStationRow(halibut, "4600146", EC);
  assert.match(fresh, /<svg class="sl-swell"[^>]*aria-hidden="true"/);
  assert.match(fresh, /sl-swell-wave/);
  const down = buildStationRow({ ...halibut, status: "down", age_minutes: 22607 }, "x", EC);
  assert.doesNotMatch(down, /sl-swell/);
  assert.equal(swellLine(null, 4, 90, "4600146"), "");
});

test("a swell-display row tags its period in a span a phone can drop", () => {
  const html = buildStationRow(
    { ...halibut, swell_height: 1.5, swell_period: 9.1 },
    "46087",
    { ...EC, wave_display: "swell" },
  );
  assert.match(html, /9\.1 s<span class="sl-swell-word"> swell<\/span>/);
});

test("a row with no wind reading shows a dash, not 'NaN'", () => {
  const html = buildStationRow({ ...halibut, wind_speed: null, wind_gust: null }, "x", EC);
  assert.match(html, /sl-big sl-none">—/);
  assert.doesNotMatch(html, /NaN|undefined/);
});

test("the detail panel lists only the values the station sends", () => {
  const html = buildStationDetail(halibut, "4600146", EC);
  assert.match(html, /Sig\. height/);
  assert.match(html, /Max height/);
  assert.match(html, /Peak spread/);
  assert.match(html, /very organized/);
  assert.match(html, /Pressure/);
  assert.doesNotMatch(html, /Wind waves/); // NOAA-only
  assert.doesNotMatch(html, /undefined|null|NaN/);
  assert.match(html, /id="sl-history-4600146"/);
  // Each action carries a line icon.
  assert.match(html, /data-action="charts"><svg class="icon"/);
});

test("the source link names its owner and says it opens a new tab", () => {
  const meta = { ...EC, source_url: "https://example.org/station" };
  const html = buildStationDetail(halibut, "4600146", meta);
  assert.match(html, /href="https:\/\/example\.org\/station"/);
  assert.match(html, /Environment Canada<svg class="icon icon--trailing"/);
  assert.match(html, /\(opens in a new tab\)/);
});

test("NOAA dominant-period stations explain the term", () => {
  const html = buildStationDetail(
    { ...halibut, wave_direction_peak: 250, wind_wave_height: 0.3 },
    "46088",
    NOAA_DOMINANT,
  );
  assert.match(html, /Dominant period/);
  assert.match(html, /Wind waves/);
  assert.match(html, /NOAA's term for peak period/);
});

test("a late station's panel says so in plain words", () => {
  const html = buildStationDetail({ ...halibut, status: "late" }, "4600146", EC);
  assert.match(html, /sl-note--late/);
});

test("a down station's warning comes BEFORE its old readings", () => {
  const html = buildStationDetail({ ...halibut, status: "down" }, "4600303", EC);
  const warning = html.indexOf("No report since");
  assert.ok(warning >= 0);
  assert.ok(warning < html.indexOf("Sig. height"), "warning must precede the numbers");
  // The warning carries the time, so no second "Observed" line.
  assert.doesNotMatch(html, /sl-observed/);
});

test("the list groups stations by region into columns and skips missing ones", () => {
  const data = { "4600146": halibut, 46087: { ...halibut, name: "Neah Bay" } };
  const html = buildStationList(data, {});
  assert.equal((html.match(/class="sl-column"/g) || []).length, 2);
  assert.match(html, /Strait of Georgia/);
  assert.match(html, /Juan de Fuca Strait/);
  assert.doesNotMatch(html, /Boundary Bay/); // no data for its stations
  assert.match(html, /id="buoy-4600146"/);
  // Column labels sit inside each column's first row, once per column.
  assert.equal((html.match(/>Waves</g) || []).length, 2);
  assert.equal((html.match(/sl-row--labelled/g) || []).length, 2);
  assert.match(html, /sl-row--labelled[^>]*>\s*<span class="sl-label sl-label--waves"/);
  // ... and, for a phone's stacked rows, on each column's first region heading.
  assert.equal((html.match(/sl-head-labels/g) || []).length, 2);
});

test("every station appears in exactly one group", () => {
  const ids = STATION_GROUPS.flatMap((g) => g.stations);
  assert.equal(ids.length, 10);
  assert.equal(new Set(ids).size, 10);
});
