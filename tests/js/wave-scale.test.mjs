import assert from "node:assert/strict";
import { test } from "node:test";

const { ROUGH_HEIGHT_M, waveAmplitude } = await import("../../site/assets/js/shared/wave-scale.js");

test("a station's rough height maps to 1.0, clamped 0.2 to 1.2", () => {
  assert.equal(waveAmplitude(0, "4600146"), 0.2);
  assert.equal(waveAmplitude(ROUGH_HEIGHT_M["4600146"], "4600146"), 1);
  assert.equal(waveAmplitude(ROUGH_HEIGHT_M["4600206"], "4600206"), 1);
  assert.equal(waveAmplitude(50, "4600206"), 1.2);
});

test("the same height is rougher in the Strait of Georgia than at La Perouse", () => {
  assert.ok(waveAmplitude(1, "4600146") > waveAmplitude(1, "4600206"));
});

test("an unlisted station still gets a scale", () => {
  const a = waveAmplitude(1, "nowhere");
  assert.ok(a > 0.2 && a < 1.2);
});

test("every home-page wave station has a rough height", async () => {
  globalThis.getDirectionalArrow = () => "";
  globalThis.degreesToCardinal = () => "N";
  const { STATION_GROUPS } = await import("../../site/assets/js/station-list.js");
  for (const id of STATION_GROUPS.flatMap((g) => g.stations)) {
    assert.ok(ROUGH_HEIGHT_M[id] > 0, `${id} has no rough height`);
  }
});
