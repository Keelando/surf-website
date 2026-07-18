import assert from "node:assert/strict";
import { test } from "node:test";

// buoy-card.js reaches for the arrow/cardinal helpers that chart-utils-v4.js
// puts on the page as a classic script. Stub them before importing so the
// builders can be exercised in node.
globalThis.getDirectionalArrow = (deg, kind) => `[${kind}@${deg}]`;
globalThis.degreesToCardinal = () => "N";

const {
  buildCompactView,
  buildEcWaveDetails,
  buildNavLinks,
  buildSpreadSection,
  buildTempPressure,
  buildWaveLine,
  buildWindLine,
  freshnessState,
  sourceBadge,
} = await import("../../site/assets/js/buoy-card.js");

const EC = { source: "Environment Canada", type: "wave_buoy" };
const NOAA_DOMINANT = { source: "NOAA NDBC", type: "wave_buoy" };
const NOAA_SWELL = { source: "NOAA NDBC", type: "wave_buoy", wave_display: "swell" };
const PILE = { source: "Surrey FlowWorks", type: "pile_mounted_wave_station" };

test("freshnessState splits fresh / stale / down at 3h and 12h", () => {
  assert.deepEqual(freshnessState({ age_minutes: 30 }), {
    ageMinutes: 30,
    isDown: false,
    isStale: false,
  });
  assert.deepEqual(freshnessState({ age_minutes: 240 }), {
    ageMinutes: 240,
    isDown: false,
    isStale: true,
  });
  // Down wins over stale so the two callouts never both render
  assert.deepEqual(freshnessState({ age_minutes: 1000 }), {
    ageMinutes: 1000,
    isDown: true,
    isStale: false,
  });
  // Missing age is treated as fresh, not as an outage
  assert.deepEqual(freshnessState({}), { ageMinutes: 0, isDown: false, isStale: false });
});

test("sourceBadge picks the badge from metadata, defaulting to EC", () => {
  assert.match(sourceBadge(NOAA_DOMINANT), /NOAA/);
  assert.match(sourceBadge(PILE), /Surrey/);
  assert.match(sourceBadge(EC), /Env Canada/);
  assert.match(sourceBadge(undefined), /Env Canada/);
});

test("wind line shows cardinal, speed, gust, degrees and arrow", () => {
  const html = buildWindLine({
    wind_speed: 14.6,
    wind_gust: 20.2,
    wind_direction_cardinal: "WNW",
    wind_direction: 295.4,
  });
  assert.match(html, /WNW 15 G 20 kn \(295°\)/);
  assert.match(html, /\[wind@295.4\]/);
});

test("wind line omits the gust when there isn't one", () => {
  const html = buildWindLine({ wind_speed: 8, wind_direction_cardinal: "S", wind_direction: 180 });
  assert.match(html, /S 8 kn \(180°\)/);
  assert.doesNotMatch(html, / G /);
});

test("wind line falls back to No data without a speed", () => {
  assert.match(buildWindLine({ wind_direction: 180 }), /No data/);
});

test("swell-display stations show swell height and period, unlabelled", () => {
  const html = buildWaveLine(
    { swell_height: 2.34, swell_period: 11.27, swell_direction_cardinal: "W", swell_direction: 270 },
    NOAA_SWELL,
  );
  assert.match(html, /🌊 Swell:/);
  assert.match(html, /W 2.3m @ 11.3s \(270°\)/);
  // Swell period needs no type tag, and the dominant footnote is NOAA-only
  assert.doesNotMatch(html, /dominant/);
});

test("NOAA non-swell stations tag the period as dominant and add the footnote", () => {
  const html = buildWaveLine(
    {
      wave_height_sig: 1.2,
      wave_period_sig: 6.5,
      wave_direction_peak_cardinal: "NW",
      wave_direction_peak: 315,
    },
    NOAA_DOMINANT,
  );
  assert.match(html, /🌊 Sig Wave:/);
  assert.match(html, /1.2m @ 6.5s/);
  assert.match(html, />dominant</);
  assert.match(html, /NOAA's term for peak period/);
});

test("EC stations use the significant period untagged", () => {
  const html = buildWaveLine(
    { wave_height_sig: 0.9, wave_period_sig: 4.1, wave_direction_peak_cardinal: "WSW" },
    EC,
  );
  assert.match(html, /0.9m @ 4.1s/);
  assert.doesNotMatch(html, />sig</);
  assert.doesNotMatch(html, /dominant/);
});

test("EC stations coalesce the alternate SWOB sig-period name", () => {
  const html = buildWaveLine({ wave_height_sig: 0.9, wave_period_sig_basic: 3.8 }, EC);
  assert.match(html, /0.9m @ 3.8s/);
  assert.doesNotMatch(html, />avg</);
});

test("EC period falls back to avg then peak, and the fallback is tagged", () => {
  const avg = buildWaveLine({ wave_height_sig: 1.0, wave_period_avg: 5.0 }, EC);
  assert.match(avg, /5.0s <span[^>]*>avg</);

  const peak = buildWaveLine({ wave_height_sig: 1.0, wave_period_peak: 7.0 }, EC);
  assert.match(peak, /7.0s <span[^>]*>peak</);
});

test("pile stations render wave heights to two decimals", () => {
  const html = buildWaveLine({ wave_height_sig: 0.125, wave_period_sig: 2.5 }, PILE);
  assert.match(html, /0.13m/);
});

test("compact view replaces the readings with an outage notice when down", () => {
  const b = { wind_speed: 10, wind_direction_cardinal: "N", wave_height_sig: 1 };
  const html = buildCompactView(b, EC, { ageMinutes: 1000, isDown: true, isStale: false });
  assert.match(html, /Station Down - No recent data available/);
  assert.match(html, /Last data received 17 hours ago/);
  assert.doesNotMatch(html, /💨 Wind:/);
});

test("temps keep source precision except on pile stations", () => {
  assert.match(buildTempPressure({ sea_temp: 9.8, air_temp: 12.1 }, EC), /9.8 °C \| <b>Air:<\/b> 12.1/);
  assert.match(buildTempPressure({ sea_temp: 9.8456, air_temp: 12.1234 }, PILE), /9.8 °C/);
  assert.match(buildTempPressure({}, EC), /— °C/);
});

test("spread section is empty when the station reports no spread", () => {
  assert.equal(buildSpreadSection({ wave_direction_peak: 270 }, "4600146"), "");
});

test("spread descriptors follow the organized-to-confused scale", () => {
  const at = (peak) => buildSpreadSection({ wave_direction_spread_peak: peak }, "x");
  assert.match(at(20), /very organized/);
  assert.match(at(30), /organized/);
  assert.match(at(40), /moderate/);
  assert.match(at(50), /confused/);
});

test("the spread explainer only appears when both spreads are present", () => {
  const both = buildSpreadSection(
    { wave_direction_spread_peak: 20, wave_direction_spread_avg: 30 },
    "4600146",
  );
  assert.match(both, /id="spread-info-4600146"/);
  const peakOnly = buildSpreadSection({ wave_direction_spread_peak: 20 }, "4600146");
  assert.doesNotMatch(peakOnly, /id="spread-info-/);
});

test("EC details are empty without any additional metrics", () => {
  assert.equal(buildEcWaveDetails({ wave_height_sig: 1.0 }, EC, "4600146"), "");
});

test("EC details label the high-end reading by which field exists", () => {
  const avgMax = buildEcWaveDetails({ wave_height_sig: 1.0, wave_height_max_avg: 1.6 }, EC, "x");
  assert.match(avgMax, /Avg Max Height:<\/b> 1.6 m/);
  assert.match(avgMax, /1.6× sig/);

  const peak = buildEcWaveDetails({ wave_height_peak: 2.0 }, EC, "x");
  assert.match(peak, /Peak Height:<\/b> 2.0 m/);
  // No sig height means no ratio to show
  assert.doesNotMatch(peak, /× sig/);

  const max = buildEcWaveDetails({ wave_height_max: 2.5 }, EC, "x");
  assert.match(max, /Max Height:<\/b> 2.5 m/);
});

test("charts button is disabled only when there is nothing to chart", () => {
  assert.doesNotMatch(buildNavLinks({ wave_height_sig: 1 }), /disabled/);
  assert.doesNotMatch(buildNavLinks({ wind_speed: 5 }), /disabled/);
  assert.match(buildNavLinks({ sea_temp: 9 }), /disabled/);
});
