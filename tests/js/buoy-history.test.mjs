import assert from "node:assert/strict";
import { test } from "node:test";

// buoy-history.js reads degreesToCardinal off the page (chart-utils-v4.js is a
// classic script). Stub it before importing so the builders run in node.
globalThis.degreesToCardinal = (deg) => (deg == null ? null : `D${deg}`);

const {
  buildHistoryNote,
  buildHistoryRows,
  buildHistoryTableHTML,
  buildHistoryTimes,
  formatHistoryTimeCell,
  formatHistoryWind,
  selectHistorySeries,
} = await import("../../site/assets/js/buoy-history.js");

const EC = { source: "Environment Canada", type: "wave_buoy" };
const NOAA_DOMINANT = { source: "NOAA NDBC", type: "wave_buoy" };
const NOAA_SWELL = { source: "NOAA NDBC", type: "wave_buoy", wave_display: "swell" };
const PILE = {
  source: "Surrey FlowWorks",
  type: "pile_mounted_wave_station",
  update_frequency_minutes: 15,
};

/** Minimal timeseries channel. */
const chan = (pairs) => ({ data: pairs.map(([time, value]) => ({ time, value })) });

const NOW = new Date("2026-07-18T12:00:00Z");
const T = (h, m = 0) =>
  `2026-07-18T${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:00Z`;

// --- series selection ---------------------------------------------------

test("selectHistorySeries reads the height field the station displays", () => {
  const ts = {
    wave_height_sig: chan([[T(10), 1.2]]),
    swell_height: chan([[T(10), 3.4]]),
  };
  assert.equal(selectHistorySeries(ts, EC).waveHeight[0].value, 1.2);
  assert.equal(selectHistorySeries(ts, NOAA_SWELL).waveHeight[0].value, 3.4);
});

test("selectHistorySeries orders period channels by station priority", () => {
  const ts = {
    wave_period_sig: chan([[T(10), 5]]),
    wave_period_sig_basic: chan([[T(10), 6]]),
    wave_period_avg: chan([[T(10), 7]]),
    wave_period_peak: chan([[T(10), 8]]),
    swell_period: chan([[T(10), 9]]),
  };
  // EC: four fallbacks, sig first
  assert.equal(selectHistorySeries(ts, EC).wavePeriods.length, 4);
  assert.equal(selectHistorySeries(ts, EC).wavePeriods[0][0].value, 5);
  // NOAA dominant: only wave_period_sig (which holds DPD)
  assert.deepEqual(
    selectHistorySeries(ts, NOAA_DOMINANT).wavePeriods.map((c) => c[0].value),
    [5],
  );
  // Swell display: swell period only
  assert.deepEqual(
    selectHistorySeries(ts, NOAA_SWELL).wavePeriods.map((c) => c[0].value),
    [9],
  );
});

test("selectHistorySeries tolerates missing channels", () => {
  const s = selectHistorySeries({}, EC);
  assert.deepEqual(s.waveHeight, []);
  assert.deepEqual(s.wind.speed, []);
  assert.deepEqual(s.wavePeriods, [[], [], [], []]);
});

// --- time axis ----------------------------------------------------------

test("buildHistoryTimes returns the 12h window newest-first", () => {
  const selected = selectHistorySeries(
    {
      wave_height_sig: chan([
        ["2026-07-17T23:00:00Z", 1], // 13h before NOW — outside the window
        [T(4), 1],
        [T(8), 1],
        [T(11), 1],
      ]),
    },
    EC,
  );
  assert.deepEqual(buildHistoryTimes(selected, EC, NOW), [T(11), T(8), T(4)]);
});

test("buildHistoryTimes falls back to wind then temperature for the axis", () => {
  const windOnly = selectHistorySeries({ wind_speed: chan([[T(9), 12]]) }, EC);
  assert.deepEqual(buildHistoryTimes(windOnly, EC, NOW), [T(9)]);

  const tempOnly = selectHistorySeries({ sea_temp: chan([[T(9), 14]]) }, EC);
  assert.deepEqual(buildHistoryTimes(tempOnly, EC, NOW), [T(9)]);
});

test("buildHistoryTimes thins sub-hourly reporters to on-the-hour rows", () => {
  const ts = {
    wave_height_sig: chan([
      [T(9, 0), 1],
      [T(9, 15), 1],
      [T(9, 30), 1],
      [T(10, 0), 1],
    ]),
  };
  assert.equal(buildHistoryTimes(selectHistorySeries(ts, PILE), PILE, NOW).length, 2);
  // An hourly station keeps every sample it was sent
  assert.equal(buildHistoryTimes(selectHistorySeries(ts, EC), EC, NOW).length, 4);
});

// --- cell formatting ----------------------------------------------------

test("formatHistoryTimeCell shows the date only when it changes", () => {
  const d = new Date("2026-07-18T17:00:00Z"); // 10:00 Vancouver, a Saturday
  const first = formatHistoryTimeCell(d, null);
  assert.match(first.html, /<br\/>/);
  assert.equal(formatHistoryTimeCell(d, first.date).html.includes("<br/>"), false);
});

test("formatHistoryTimeCell omits the minutes on the hour", () => {
  const onHour = formatHistoryTimeCell(new Date("2026-07-18T17:00:00Z"), null);
  assert.match(onHour.html, /10h$/);
  const offHour = formatHistoryTimeCell(new Date("2026-07-18T17:10:00Z"), null);
  assert.match(offHour.html, /10h10$/);
});

test("formatHistoryTimeCell pins the weekday to Vancouver, not the viewer", () => {
  // 06:00 UTC Sunday is still 23:00 Saturday in Vancouver.
  const { date } = formatHistoryTimeCell(new Date("2026-07-19T06:00:00Z"), null);
  assert.equal(date, "Sa-18");
});

test("formatHistoryWind renders cardinal, speed and gust", () => {
  assert.equal(formatHistoryWind(12.4, 270, 18.6), "D270 12 gust 19");
  assert.equal(formatHistoryWind(12.4, 270, null), "D270 12");
  assert.equal(formatHistoryWind(null, 270, 18), "—");
});

// --- rows ---------------------------------------------------------------

test("buildHistoryRows falls through the period priority per row", () => {
  const ts = {
    wave_height_sig: chan([
      [T(10), 1.0],
      [T(11), 1.0],
    ]),
    // sig missing at 11:00, so that row must fall back to avg
    wave_period_sig: chan([[T(10), 5.0]]),
    wave_period_avg: chan([
      [T(10), 9.9],
      [T(11), 7.0],
    ]),
  };
  const selected = selectHistorySeries(ts, EC);
  const rows = buildHistoryRows([T(10), T(11)], selected, EC);
  assert.match(rows, /5\.0/);
  assert.match(rows, /7\.0/);
  assert.equal(rows.includes("9.9"), false); // sig wins where present
});

test("buildHistoryRows uses centimetre precision for pile stations", () => {
  const ts = { wave_height_sig: chan([[T(10), 0.125]]) };
  assert.match(buildHistoryRows([T(10)], selectHistorySeries(ts, PILE), PILE), />0\.13</);
  assert.match(buildHistoryRows([T(10)], selectHistorySeries(ts, EC), EC), />0\.1</);
});

test("buildHistoryRows dashes missing values", () => {
  const rows = buildHistoryRows([T(10)], selectHistorySeries({}, EC), EC);
  assert.equal((rows.match(/—/g) || []).length, 5); // wind + 4 numeric columns
});

// --- notes and whole table ----------------------------------------------

test("buildHistoryNote matches the station's period convention", () => {
  assert.match(buildHistoryNote(NOAA_SWELL), /swell data/);
  assert.match(buildHistoryNote(NOAA_DOMINANT), /dominant period/);
  assert.equal(buildHistoryNote(EC), "");
  assert.equal(buildHistoryNote(PILE), "");
});

test("buildHistoryTableHTML emits six columns and the hide button", () => {
  const ts = { wave_height_sig: chan([[T(10), 1.1]]) };
  const html = buildHistoryTableHTML(ts, EC, { now: NOW });
  assert.equal((html.match(/<th>/g) || []).length, 6);
  assert.match(html, /class="history-table"/);
  assert.match(html, /hide-history-btn/);
});

test("buildHistoryTableHTML emits the scroll hint hidden", () => {
  // Always present but hidden; only trackScrollAffordance() can know whether
  // the table overflows, which needs a laid-out DOM.
  const ts = { wave_height_sig: chan([[T(10), 1.1]]) };
  const html = buildHistoryTableHTML(ts, EC, { now: NOW });
  assert.match(html, /class="history-scroll-hint" hidden/);
});

test("buildHistoryTableHTML renders with no data at all", () => {
  const html = buildHistoryTableHTML({}, EC, { now: NOW });
  assert.match(html, /<tbody>/);
  assert.equal((html.match(/<td>/g) || []).length, 0); // header row only, no body rows
});
