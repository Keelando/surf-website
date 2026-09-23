import assert from "node:assert/strict";
import { test } from "node:test";
import {
  formatLightstationWind,
  silentStationText,
} from "../../site/assets/js/shared/lightstation-format.js";

test("full wind report", () => {
  assert.equal(
    formatLightstationWind({
      wind_direction: "SOUTHEAST",
      wind_speed_kt: 24,
      wind_gusting: true,
      wind_estimated: true,
    }),
    "SOUTHEAST 24 kt (gusting) (est)",
  );
});

test("0 kt is a reading, not N/A", () => {
  assert.equal(formatLightstationWind({ wind_direction: "NORTH", wind_speed_kt: 0 }), "NORTH 0 kt");
});

test("calm wins over any stray speed", () => {
  assert.equal(formatLightstationWind({ wind_calm: true, wind_speed_kt: null }), "Calm");
});

test("a report with no wind at all returns null, never 'N/A N/A kt'", () => {
  assert.equal(formatLightstationWind({ wind_speed_kt: null, wind_direction: null }), null);
  assert.equal(formatLightstationWind(null), null);
});

test("direction without speed says so", () => {
  assert.equal(
    formatLightstationWind({ wind_direction: "WEST", wind_speed_kt: null }),
    "WEST speed not given",
  );
});

test("silent station wording does not claim it never reported", () => {
  const t = silentStationText();
  assert.equal(t.status, "No new reports");
  assert.match(t.lastReport, /^Last report: more than \d+ days ago$/);
});
