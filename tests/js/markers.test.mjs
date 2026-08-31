import assert from "node:assert/strict";
import { test } from "node:test";
import {
  DIRECTION_ARROW_PATH,
  createAngularSpreadVector,
  createDirectionalMarker,
} from "../../site/assets/js/shared/markers.js";

test("DIRECTION_ARROW_PATH matches the chart-utils original", () => {
  assert.equal(DIRECTION_ARROW_PATH, "path://M0,15 L-3,-5 L0,0 L3,-5 Z");
});

test("marker rotates by the raw meteorological direction", () => {
  const html = createDirectionalMarker(225, 12.4);
  assert.match(html, /rotate\(225deg\)/);
});

test("wind marker labels rounded knots", () => {
  assert.match(createDirectionalMarker(90, 12.6), />13kt</);
  assert.match(createDirectionalMarker(90, 12.4), />12kt</);
});

test("wave marker labels height to one decimal in metres", () => {
  const html = createDirectionalMarker(315, 1.25, { type: "wave" });
  assert.match(html, />1\.3m</);
});

test("wave-inferred marker is wave blue but hollow, not a grey arrow", () => {
  const html = createDirectionalMarker(180, 0.8, { type: "wave-inferred" });
  assert.match(html, />0\.8m</);
  assert.match(html, /--map-arrow-wave/);
  assert.doesNotMatch(html, /--map-arrow-nodir/);
  // Hollow: the arrow is filled with the marker background, not its own colour
  assert.match(html, /fill="var\(--map-marker-bg/);
});

test("measured wave marker stays solid", () => {
  assert.match(createDirectionalMarker(180, 0.8, { type: "wave" }), /fill="currentColor"/);
});

test("arrow colour follows type", () => {
  assert.match(createDirectionalMarker(0, 1, { type: "wave" }), /--map-arrow-wave/);
  assert.match(createDirectionalMarker(0, 1, { type: "wind" }), /--map-arrow-wind/);
});

test("stale marker is dimmed; fresh is opaque", () => {
  assert.match(createDirectionalMarker(0, 5, { stale: true }), /opacity: 0\.35/);
  assert.match(createDirectionalMarker(0, 5), /opacity: 1/);
});

test("missing value omits the label but keeps the arrow", () => {
  const html = createDirectionalMarker(45, null);
  assert.doesNotMatch(html, /kt</);
  assert.match(html, /<svg/);
});

test("marker svg is hidden from assistive tech", () => {
  assert.match(createDirectionalMarker(0, 5), /aria-hidden="true"/);
});

test("spread vector returns empty string for missing inputs", () => {
  assert.equal(createAngularSpreadVector(null, 30), "");
  assert.equal(createAngularSpreadVector(180, null), "");
});

test("spread vector rotates the main arrow by the mean direction", () => {
  const svg = createAngularSpreadVector(200, 40, 70);
  assert.match(svg, /rotate\(200 35 35\)/);
});

test("spread vector sizes the viewBox from the size argument", () => {
  assert.match(createAngularSpreadVector(90, 20, 60), /viewBox="0 0 60 60"/);
  assert.match(createAngularSpreadVector(90, 20), /viewBox="0 0 70 70"/);
});

test("spread sector uses the large-arc flag only past 180 degrees", () => {
  const narrow = createAngularSpreadVector(0, 90, 70);
  const wide = createAngularSpreadVector(0, 270, 70);
  assert.match(narrow, /A 31.4,31.4 0 0,1/);
  assert.match(wide, /A 31.4,31.4 0 1,1/);
});

test("spread vector draws all four cardinal labels", () => {
  const svg = createAngularSpreadVector(45, 30);
  for (const cardinal of [">N<", ">E<", ">S<", ">W<"]) {
    assert.ok(svg.includes(cardinal), `missing ${cardinal}`);
  }
});

test("wave marker adds a wind-speed line when a speed is supplied", () => {
  const html = createDirectionalMarker(270, 1.4, { type: "wave", windSpeed: 12.6 });
  assert.match(html, />1\.4m</);
  assert.match(html, />13kt</);
  assert.match(html, /--map-arrow-wind/);
});

test("wave marker without a wind speed keeps one label line", () => {
  const html = createDirectionalMarker(270, 1.4, { type: "wave" });
  assert.doesNotMatch(html, /kt</);
});

test("wave-inferred marker also takes the wind-speed line", () => {
  assert.match(createDirectionalMarker(180, 0.8, { type: "wave-inferred", windSpeed: 9 }), />9kt</);
});

test("wind marker ignores windSpeed - its own label is already the wind", () => {
  const html = createDirectionalMarker(0, 20, { type: "wind", windSpeed: 9 });
  assert.match(html, />20kt</);
  assert.doesNotMatch(html, />9kt</);
});

test("a calm 0kt still renders - 0 is a reading, not a missing value", () => {
  assert.match(createDirectionalMarker(0, 1.0, { type: "wave", windSpeed: 0 }), />0kt</);
});
