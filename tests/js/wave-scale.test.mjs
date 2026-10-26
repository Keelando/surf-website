import assert from "node:assert/strict";
import { test } from "node:test";

const { DEFAULT_ROUGH_M, waveAmplitude } = await import("../../site/assets/js/shared/wave-scale.js");

test("a station's rough height maps to 1.0, clamped 0.2 to 1.2", () => {
  assert.equal(waveAmplitude(0, 1), 0.2);
  assert.equal(waveAmplitude(1, 1), 1);
  assert.equal(waveAmplitude(2.5, 2.5), 1);
  assert.equal(waveAmplitude(50, 2.5), 1.2);
});

test("the same height is rougher in the Strait of Georgia than at La Perouse", () => {
  assert.ok(waveAmplitude(1, 1) > waveAmplitude(1, 2.5));
});

test("a missing or unusable rough height falls back to the default scale", () => {
  const expected = waveAmplitude(1, DEFAULT_ROUGH_M);
  for (const rough of [undefined, null, 0, -1, Number.NaN, "1"]) {
    assert.equal(waveAmplitude(1, rough), expected, String(rough));
  }
});
