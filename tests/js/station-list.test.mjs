import assert from "node:assert/strict";
import { test } from "node:test";

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
  rowReading,
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

test("compactAge rounds down to whole minutes, hours, then days", () => {
  assert.equal(compactAge(49.9), "49m");
  assert.equal(compactAge(60), "1h");
  assert.equal(compactAge(47 * 60 + 59), "47h");
  assert.equal(compactAge(48 * 60), "2d");
  assert.equal(compactAge(null), null);
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
  assert.match(html, /No recent data/);
  assert.doesNotMatch(html, /sl-big/);
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
});

test("every station appears in exactly one group", () => {
  const ids = STATION_GROUPS.flatMap((g) => g.stations);
  assert.equal(ids.length, 10);
  assert.equal(new Set(ids).size, 10);
});
