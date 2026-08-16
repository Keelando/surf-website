import assert from "node:assert/strict";
import { test } from "node:test";
import {
  daylightSeconds,
  describeDaylightChange,
  toDateKey,
} from "../../site/assets/js/tides-modules/sunlight.js";

// Real White Rock values from sunlight_times.json. Mid-August is a losing
// stretch of roughly three minutes a day.
const AUG_16 = {
  sunrise: "2026-08-16T13:05:31.677811+00:00",
  sunset: "2026-08-17T03:26:10.994615+00:00",
};
const AUG_17 = {
  sunrise: "2026-08-17T13:06:56.961815+00:00",
  sunset: "2026-08-18T03:24:20.930461+00:00",
};

test("toDateKey zero-pads month and day", () => {
  assert.equal(toDateKey(new Date(2026, 0, 5)), "2026-01-05");
  assert.equal(toDateKey(new Date(2026, 11, 31)), "2026-12-31");
});

test("daylightSeconds measures sunrise to sunset", () => {
  // 13:05:31 to 03:26:10 the next UTC day = 14h 20m 39s
  assert.equal(Math.round(daylightSeconds(AUG_16)), 14 * 3600 + 20 * 60 + 39);
});

test("daylightSeconds rejects missing, errored and unparseable days", () => {
  assert.equal(daylightSeconds(null), null);
  assert.equal(daylightSeconds({ error: "polar night" }), null);
  assert.equal(daylightSeconds({ sunrise: AUG_16.sunrise }), null);
  assert.equal(daylightSeconds({ sunrise: "not a date", sunset: AUG_16.sunset }), null);
});

test("describeDaylightChange reports losing days in mid-August", () => {
  const today = daylightSeconds(AUG_16);
  const tomorrow = daylightSeconds(AUG_17);
  assert.equal(describeDaylightChange(today, tomorrow), "losing 3m 15s each day");
});

test("describeDaylightChange reports gaining days", () => {
  const today = daylightSeconds(AUG_17);
  const tomorrow = daylightSeconds(AUG_16);
  assert.equal(describeDaylightChange(today, tomorrow), "gaining 3m 15s each day");
});

test("describeDaylightChange omits the minutes component under a minute", () => {
  // Near a solstice the change falls to seconds — the case the seconds
  // component exists for.
  assert.equal(describeDaylightChange(3600, 3618), "gaining 18s each day");
  assert.equal(describeDaylightChange(3600, 3582), "losing 18s each day");
});

test("describeDaylightChange calls out a steady day rather than a signed zero", () => {
  assert.equal(describeDaylightChange(3600, 3600), "day length holding steady");
  // Sub-second differences round to steady, not to "gaining 0s".
  assert.equal(describeDaylightChange(3600, 3600.4), "day length holding steady");
});

test("describeDaylightChange yields nothing when either day is unusable", () => {
  assert.equal(describeDaylightChange(null, 3600), "");
  assert.equal(describeDaylightChange(3600, null), "");
});
